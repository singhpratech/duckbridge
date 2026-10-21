//! Backend `duckdb-neo`: DuckDB's Rust bindings for the v2 C API (DuckDB 2.0). See [`super`].
//!
//! Same names and signatures as the `duckdb-v1` backend (`v1.rs`), built differently:
//!
//! * One process-wide [`Environment`]; [`open`] / [`open_readonly`] / [`open_in_memory`] create an
//!   instance with the database attached as its default, [`Connection`] keeps the instance, so
//!   [`try_clone`] is `instance.connect()`.
//! * Results are read through Arrow: `arrow_query` gives the Arrow C data interface, imported into
//!   our own `arrow` crate (duckdb-neo has no Arrow dependency). Rows ([`Row`]) and values
//!   ([`ValueRef`]) are read off those arrays, the same way duckdb-rs 1.x reads them, so the value
//!   model is the same: [`Value`], [`ValueRef`], [`TimeUnit`], [`Decimal`], [`ListType`] are
//!   DuckBridge-owned types with duckdb-rs' variants and semantics.
//! * duckdb-neo has no table appender: [`Appender`] buffers rows, writes them into data chunks of a
//!   column data collection, and inserts the collection with one `INSERT ... SELECT` per flush.
//! * Errors carry DuckDB's message only (duckdb-neo appends ` (code N)`, dropped here), as v1.
//!
//! Also used by the DuckDB 2.0 extension (same duckdb-neo crate, `build-extension`), which has no
//! connection and works on the chunks DuckDB lends its callbacks: [`Kind`] / [`Kind::of_type`] /
//! [`write_column`] fill a table function's output chunk from `Value` rows ([`ChunkMut`] covers owned
//! and borrowed chunks), [`import_arrow`] turns a COPY input exported with duckdb-neo's
//! `ArrowExporter` into Arrow record batches.

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, bail, Context as _, Result};
use duckdb_neo as neo;
use neo::column_data_collection::ColumnDataCollection;
use neo::data_chunk::DataChunk;
use neo::environment::{Environment, StorageLocation};
use neo::instance::{AttachBuilder, Instance};
use neo::logical_type::LogicalType;
use neo::query_result::ArrowQueryResult;
use neo::statement::PreparedStatement;
use neo::data_chunk::DataChunkRef;
use neo::logical_type::LogicalTypeID;
use neo::types::{
    BlobValue, DateValue, DecimalSignature, HugeDecimal, IntervalValue, LongDecimal, ShortDecimal, TimeNsValue, TimeValue, TimestampMsValue, TimestampNsValue,
    TimestampSecValue, TimestampTzValue, TimestampValue, UuidValueRaw, WordDecimal,
};
use neo::vector::{Vector, VectorElement};
use neo::{DuckDBType, Parameters, QueryParameter};

/// The Arrow crate the backend speaks (results come in through the Arrow C data interface).
pub use arrow;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array, Decimal32Array, Decimal64Array,
    DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, IntervalMonthDayNanoArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    StringArray, StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, IntervalUnit, Schema, SchemaRef, TimeUnit as ArrowUnit, UInt16Type, UInt32Type, UInt8Type};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

/// Rows per Arrow array of a streaming query ([`stream`]); DuckDB's vector size, as duckdb-rs
/// 1.x hands out one array per data chunk.
const STREAM_BATCH: usize = 2048;
/// Rows per Arrow array of the row helpers.
const ROWS_BATCH: usize = 2048 * 8;
/// Rows per data chunk the appender writes.
const CHUNK_ROWS: usize = 2048;
/// The appender inserts on its own once this many rows are pending.
const APPEND_AUTO_FLUSH: usize = 1 << 17;

// ------------------------------------------------------------------ errors

/// A DuckDB error: the engine's message, without duckdb-neo's ` (code N)` suffix.
#[derive(Debug)]
pub struct DuckError(String);
impl fmt::Display for DuckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.0) }
}
impl std::error::Error for DuckError {}

fn de(e: neo::Error) -> anyhow::Error {
    anyhow::Error::new(DuckError(if e.message.is_empty() { format!("DuckDB error (code {})", e.code as u32) } else { e.message }))
}

trait Neo<T> { fn d(self) -> Result<T>; }
impl<T> Neo<T> for neo::Result<T> { fn d(self) -> Result<T> { self.map_err(de) } }

// ------------------------------------------------------------------ connection

fn environment() -> Result<&'static Environment> {
    static ENV: OnceLock<Environment> = OnceLock::new();
    if let Some(e) = ENV.get() { return Ok(e); }
    let e = Environment::new().d()?;
    Ok(ENV.get_or_init(|| e))
}

/// A DuckDB connection (it keeps its instance alive).
pub struct Connection {
    con: neo::connection::Connection,
    instance: Arc<Instance>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("Connection") }
}

impl Connection {
    fn of(instance: Instance) -> Result<Connection> {
        let instance = Arc::new(instance);
        Ok(Connection { con: instance.connect().d()?, instance })
    }

    /// A handle that cancels this connection's running query from another thread.
    pub fn interrupt_handle(&self) -> InterruptHandle { InterruptHandle(self.con.interrupt_handle()) }

    /// Prepare one statement (binding errors surface here, as with duckdb-rs).
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        Ok(Statement { prep: self.con.prepare(sql, false).d()? })
    }
}

/// Cancels the running query of a [`Connection`] (`interrupt`), from any thread.
#[derive(Debug, Clone)]
pub struct InterruptHandle(neo::connection::InterruptHandle);
impl InterruptHandle {
    /// Request cancellation (nothing happens when the connection is idle or gone).
    pub fn interrupt(&self) { let _ = self.0.interrupt(); }
}

/// Open (or create) the database file at `path`.
pub fn open(path: impl AsRef<std::path::Path>) -> Result<Connection> {
    Connection::of(environment()?.open(StorageLocation::OnDisk(path_text(path.as_ref())?)).d()?)
}

/// Open the database file at `path` read-only.
pub fn open_readonly(path: impl AsRef<std::path::Path>) -> Result<Connection> {
    let instance = Instance::new(environment()?).d()?;
    instance.attach(AttachBuilder::new(StorageLocation::OnDisk(path_text(path.as_ref())?)).option("read_only", "true").make_default()).d()?;
    Connection::of(instance)
}

/// A fresh in-memory database.
pub fn open_in_memory() -> Result<Connection> {
    Connection::of(environment()?.open(StorageLocation::InMemory).d()?)
}

fn path_text(p: &std::path::Path) -> Result<String> {
    p.to_str().map(str::to_owned).ok_or_else(|| anyhow!("database path is not valid UTF-8: {}", p.display()))
}

/// Another connection to the same database as `con` (not available inside the extension, see
/// [`super::super::conn`]).
pub fn try_clone(con: &Connection) -> Result<Connection> {
    Ok(Connection { con: con.instance.connect().d()?, instance: con.instance.clone() })
}

// ------------------------------------------------------------------ parameters

/// A value that can be bound to a `?` parameter.
pub trait ToSql: QueryParameter {}
impl<T: QueryParameter + ?Sized> ToSql for T {}

/// Statement parameters: `[]` (none) or `&[&dyn ToSql]` (positional).
pub trait Params {
    #[doc(hidden)]
    fn with_params<R>(self, f: impl FnOnce(Parameters<'_>) -> R) -> R;
}
impl Params for [&dyn ToSql; 0] {
    fn with_params<R>(self, f: impl FnOnce(Parameters<'_>) -> R) -> R { f(Parameters::None) }
}
impl Params for &[&dyn ToSql] {
    fn with_params<R>(self, f: impl FnOnce(Parameters<'_>) -> R) -> R {
        let v: Vec<&dyn QueryParameter> = self.iter().map(|p| *p as &dyn QueryParameter).collect();
        if v.is_empty() { f(Parameters::None) } else { f(Parameters::Positional(&v)) }
    }
}

// ------------------------------------------------------------------ statements

/// Run one or more `;`-separated statements, discarding any result.
pub fn exec(con: &Connection, sql: &str) -> Result<()> {
    for st in con.con.parse(sql).d()? {
        con.con.execute(st.d()?, Parameters::None).d()?;
    }
    Ok(())
}

/// Run a single statement and return the number of rows it changed.
pub fn execute(con: &Connection, sql: &str) -> Result<usize> {
    con.con.execute(sql, Parameters::None).d()
}

/// `SET <name>=<value>`. `value` is SQL text and goes in verbatim (quote string settings yourself).
pub fn set_setting(con: &Connection, name: &str, value: &str) -> Result<()> {
    exec(con, &format!("SET {name}={value}"))
}

/// First column of the first row of `sql` (an error when there is no row).
pub fn query_scalar<T: FromSql>(con: &Connection, sql: &str) -> Result<T> {
    query_one(con, sql, |r| r.get::<_, T>(0))
}

/// `f` applied to the first row of `sql` (an error when there is no row).
pub fn query_one<T>(con: &Connection, sql: &str, f: impl FnOnce(&Row<'_>) -> Result<T>) -> Result<T> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    match rows.next()? {
        Some(r) => f(r),
        None => Err(anyhow::Error::new(DuckError("Query returned no rows".into()))),
    }
}

/// `f` applied to every row of `sql`, collected.
pub fn query_rows<T>(con: &Connection, sql: &str, mut f: impl FnMut(&Row<'_>) -> Result<T>) -> Result<Vec<T>> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? { out.push(f(r)?); }
    Ok(out)
}

/// Like [`query_rows`] with positional `?` parameters bound to `params`.
pub fn query_rows_with<T>(con: &Connection, sql: &str, params: &[&dyn ToSql], mut f: impl FnMut(&Row<'_>) -> Result<T>) -> Result<Vec<T>> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query(params)?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? { out.push(f(r)?); }
    Ok(out)
}

/// Visit every row of `sql` without collecting (for large results).
pub fn for_each_row(con: &Connection, sql: &str, mut f: impl FnMut(&Row<'_>) -> Result<()>) -> Result<()> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? { f(r)?; }
    Ok(())
}

/// The strings of a `VARCHAR[]` column of `r` (empty for NULL, non-list or non-string values).
pub fn list_of_strings(r: &Row<'_>, i: usize) -> Vec<String> {
    match r.get_ref(i) {
        Ok(ValueRef::List(list, idx)) => {
            let arr: ArrayRef = match list { ListType::Regular(a) => a.value(idx), ListType::Large(a) => a.value(idx) };
            if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
                (0..sa.len()).filter(|&j| !sa.is_null(j)).map(|j| sa.value(j).to_string()).collect()
            } else if let Some(sa) = arr.as_any().downcast_ref::<StringViewArray>() {
                (0..sa.len()).filter(|&j| !sa.is_null(j)).map(|j| sa.value(j).to_string()).collect()
            } else if let Some(sa) = arr.as_any().downcast_ref::<LargeStringArray>() {
                (0..sa.len()).filter(|&j| !sa.is_null(j)).map(|j| sa.value(j).to_string()).collect()
            } else {
                vec![]
            }
        }
        _ => vec![],
    }
}

/// A prepared statement on a [`Connection`].
pub struct Statement<'c> {
    prep: PreparedStatement<'c>,
}

impl<'c> Statement<'c> {
    /// Execute with `params` (`[]` for none) and read the result row by row.
    pub fn query<P: Params>(&mut self, params: P) -> Result<Rows<'_>> {
        let res = params.with_params(|p| self.prep.execute_arrow(p, Some(ROWS_BATCH))).d()?;
        Ok(Rows::new(Box::new(ArrowResult::new(res)?)))
    }
}

// ------------------------------------------------------------------ Arrow results

/// A source of Arrow struct arrays (one per result chunk) with a known schema.
trait Source {
    fn next_batch(&mut self) -> Result<Option<StructArray>>;
    fn schema(&self) -> &SchemaRef;
}

/// An Arrow query result imported through the C data interface.
struct ArrowResult<'c> {
    res: ArrowQueryResult<'c>,
    ffi_schema: FFI_ArrowSchema,
    schema: SchemaRef,
}

impl<'c> ArrowResult<'c> {
    fn new(res: ArrowQueryResult<'c>) -> Result<Self> {
        let raw = res.schema().d()?.into_raw();
        // SAFETY: duckdb-neo's ArrowSchema is the Arrow C data interface struct, as arrow's FFI type.
        let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute::<neo::ffi::ArrowSchema, FFI_ArrowSchema>(raw) };
        let schema = Arc::new(Schema::try_from(&ffi_schema).context("reading the result's Arrow schema")?);
        Ok(ArrowResult { res, ffi_schema, schema })
    }
}

impl Source for ArrowResult<'_> {
    fn next_batch(&mut self) -> Result<Option<StructArray>> {
        let Some(a) = self.res.next_array().d()? else { return Ok(None) };
        // SAFETY: as above, for the array struct; DuckDB built it for `ffi_schema`.
        let ffi_array: FFI_ArrowArray = unsafe { std::mem::transmute::<neo::ffi::ArrowArray, FFI_ArrowArray>(a.into_raw()) };
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, &self.ffi_schema) }.context("importing a result chunk")?;
        Ok(Some(StructArray::from(data)))
    }
    fn schema(&self) -> &SchemaRef { &self.schema }
}

impl<S: Source + ?Sized> Source for &mut S {
    fn next_batch(&mut self) -> Result<Option<StructArray>> { (**self).next_batch() }
    fn schema(&self) -> &SchemaRef { (**self).schema() }
}

// ------------------------------------------------------------------ rows

/// A row cursor of an executed [`Statement`].
pub struct Rows<'s> {
    src: Box<dyn Source + 's>,
    row: Option<Row<'s>>,
    len: usize,
    started: bool,
}

impl<'s> Rows<'s> {
    fn new(src: Box<dyn Source + 's>) -> Rows<'s> { Rows { src, row: None, len: 0, started: false } }

    /// The next row, `None` at the end.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<&Row<'s>>> {
        if self.started {
            if let Some(r) = self.row.as_mut() {
                if r.idx + 1 < self.len { r.idx += 1; return Ok(self.row.as_ref()); }
            }
        }
        loop {
            match self.src.next_batch()? {
                None => { self.row = None; self.started = true; return Ok(None); }
                Some(sa) if sa.is_empty() => continue,
                Some(sa) => {
                    self.len = sa.len();
                    self.started = true;
                    self.row = Some(Row { cols: sa.columns().to_vec(), schema: self.src.schema().clone(), idx: 0, _rows: PhantomData });
                    return Ok(self.row.as_ref());
                }
            }
        }
    }
}

/// One result row (`get`, `get_ref`, `get_ref_unwrap`).
pub struct Row<'s> {
    cols: Vec<ArrayRef>,
    schema: SchemaRef,
    idx: usize,
    _rows: PhantomData<&'s ()>,
}

/// A column of a [`Row`]: its position or its name.
pub trait RowIndex {
    #[doc(hidden)]
    fn idx(&self, row: &Row<'_>) -> Result<usize>;
}
impl RowIndex for usize {
    fn idx(&self, row: &Row<'_>) -> Result<usize> {
        if *self < row.cols.len() { Ok(*self) } else { Err(anyhow::Error::new(DuckError(format!("Invalid column index: {self}")))) }
    }
}
impl RowIndex for &str {
    fn idx(&self, row: &Row<'_>) -> Result<usize> {
        row.schema.index_of(self).map_err(|_| anyhow::Error::new(DuckError(format!("Invalid column name: {self}"))))
    }
}

impl<'s> Row<'s> {
    /// Number of result columns.
    pub fn column_count(&self) -> usize { self.cols.len() }

    /// Column `idx` converted to `T`.
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        let i = idx.idx(self)?;
        T::column_result(value_ref(self.idx, &self.cols[i], self.schema.field(i))).with_context(|| format!("column {i} ({})", self.schema.field(i).name()))
    }

    /// Column `idx` as a borrowed value.
    pub fn get_ref<I: RowIndex>(&self, idx: I) -> Result<ValueRef<'_>> {
        let i = idx.idx(self)?;
        Ok(value_ref(self.idx, &self.cols[i], self.schema.field(i)))
    }

    /// Like [`Row::get_ref`], panicking on a bad index.
    pub fn get_ref_unwrap<I: RowIndex>(&self, idx: I) -> ValueRef<'_> { self.get_ref(idx).unwrap() }
}

impl<'s> AsRef<Row<'s>> for Row<'s> {
    fn as_ref(&self) -> &Row<'s> { self }
}

// ------------------------------------------------------------------ value model

/// Time unit of [`ValueRef::Time64`] / [`ValueRef::Timestamp`] values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit { Second, Millisecond, Microsecond, Nanosecond }

impl TimeUnit {
    /// `value` in this unit as microseconds.
    pub fn to_micros(&self, value: i64) -> i64 {
        match self { TimeUnit::Second => value * 1_000_000, TimeUnit::Millisecond => value * 1000, TimeUnit::Microsecond => value, TimeUnit::Nanosecond => value / 1000 }
    }
}

/// A DuckDB `DECIMAL(width, scale)` value: the scaled integer payload with its width and scale
/// (same semantics as duckdb-rs' `Decimal`).
#[derive(Copy, Clone, Debug)]
pub struct Decimal { width: u8, scale: u8, value: i128 }

/// Error building an invalid [`Decimal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    InvalidWidth { width: u8, max: u8 },
    ScaleExceedsWidth { scale: u8, width: u8 },
    ValueExceedsWidth { digits: u8, width: u8 },
}
impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalError::InvalidWidth { width, max } => write!(f, "decimal width {width} is outside 1..={max}"),
            DecimalError::ScaleExceedsWidth { scale, width } => write!(f, "decimal scale {scale} exceeds width {width}"),
            DecimalError::ValueExceedsWidth { digits, width } => write!(f, "decimal value has {digits} digits, more than width {width}"),
        }
    }
}
impl std::error::Error for DecimalError {}

fn decimal_digits(value: i128) -> u8 {
    let mut v = value.unsigned_abs();
    let mut d = 1u8;
    while v >= 10 { v /= 10; d += 1; }
    d
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool { self.scale == other.scale && self.value == other.value }
}
impl Eq for Decimal {}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = usize::from(self.scale);
        let raw = self.value.unsigned_abs().to_string();
        if scale == 0 { return f.pad_integral(!self.value.is_negative(), "", &raw); }
        let mut m = String::new();
        if raw.len() <= scale {
            m.push_str("0.");
            for _ in 0..(scale - raw.len()) { m.push('0'); }
            m.push_str(&raw);
        } else {
            let point = raw.len() - scale;
            m.push_str(&raw[..point]);
            m.push('.');
            m.push_str(&raw[point..]);
        }
        f.pad_integral(!self.value.is_negative(), "", &m)
    }
}

impl Decimal {
    /// DuckDB's maximum `DECIMAL` width.
    pub const MAX_WIDTH: u8 = 38;

    /// A valid `DECIMAL(width, scale)` (width 1..=38, scale <= width, value fits the width).
    pub fn new(width: u8, scale: u8, value: i128) -> std::result::Result<Self, DecimalError> {
        if width == 0 || width > Self::MAX_WIDTH { return Err(DecimalError::InvalidWidth { width, max: Self::MAX_WIDTH }); }
        if scale > width { return Err(DecimalError::ScaleExceedsWidth { scale, width }); }
        let digits = decimal_digits(value);
        if digits > width { return Err(DecimalError::ValueExceedsWidth { digits, width }); }
        Ok(Self { width, scale, value })
    }
    /// Declared precision.
    pub const fn width(self) -> u8 { self.width }
    /// Number of fractional digits.
    pub const fn scale(self) -> u8 { self.scale }
    /// Scaled integer payload.
    pub const fn value(self) -> i128 { self.value }
}

/// The list array behind a [`ValueRef::List`].
#[derive(Debug, Clone, Copy)]
pub enum ListType<'a> { Regular(&'a ListArray), Large(&'a LargeListArray) }

/// The dictionary array behind a [`ValueRef::Enum`].
#[derive(Debug, Clone, Copy)]
pub enum EnumType<'a> {
    UInt8(&'a DictionaryArray<UInt8Type>),
    UInt16(&'a DictionaryArray<UInt16Type>),
    UInt32(&'a DictionaryArray<UInt32Type>),
}

/// A borrowed result value (variants as duckdb-rs' `ValueRef`).
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    HugeInt(i128),
    UHugeInt(u128),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    Timestamp(TimeUnit, i64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
    Geometry(&'a [u8]),
    Date32(i32),
    Time64(TimeUnit, i64),
    Interval { months: i32, days: i32, nanos: i64 },
    List(ListType<'a>, usize),
    Enum(EnumType<'a>, usize),
    Struct(&'a StructArray, usize),
    Array(&'a FixedSizeListArray, usize),
    Map(&'a MapArray, usize),
    Union(&'a ArrayRef, usize),
}

impl<'a> ValueRef<'a> {
    /// The text of a `Text` or `Enum` value.
    pub fn as_str(&self) -> Result<&'a str> {
        match *self {
            ValueRef::Text(t) => Ok(std::str::from_utf8(t)?),
            ValueRef::Enum(e, idx) => {
                let (values, key) = match e {
                    EnumType::UInt8(a) => (a.values(), a.key(idx)),
                    EnumType::UInt16(a) => (a.values(), a.key(idx)),
                    EnumType::UInt32(a) => (a.values(), a.key(idx)),
                };
                let key = key.ok_or_else(|| anyhow!("Invalid column type"))?;
                if let Some(s) = values.as_any().downcast_ref::<StringArray>() { return Ok(s.value(key)); }
                if let Some(s) = values.as_any().downcast_ref::<StringViewArray>() { return Ok(s.value(key)); }
                bail!("enum dictionary values are not strings")
            }
            _ => bail!("Invalid column type"),
        }
    }
}

/// An owned value, for the [`Appender`] (variants as duckdb-rs' `Value`).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    HugeInt(i128),
    UHugeInt(u128),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    Timestamp(TimeUnit, i64),
    Text(String),
    Blob(Vec<u8>),
    Geometry(Vec<u8>),
    Date32(i32),
    Time64(TimeUnit, i64),
    Interval { months: i32, days: i32, nanos: i64 },
    List(Vec<Value>),
    Enum(String),
    Struct(Vec<(String, Value)>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Union(Box<Value>),
}

macro_rules! down {
    ($a:expr, $t:ty) => { $a.as_any().downcast_ref::<$t>().expect(concat!("array is not a ", stringify!($t))) };
}

/// Row `row` of `column` as a [`ValueRef`] (the mapping duckdb-rs 1.x applies to the same arrays).
fn value_ref<'a>(row: usize, column: &'a ArrayRef, field: &arrow::datatypes::Field) -> ValueRef<'a> {
    if column.is_null(row) { return ValueRef::Null; }
    let geometry = field.metadata().get("ARROW:extension:name").is_some_and(|n| n.contains("wkb") || n.contains("geometry"));
    match column.data_type() {
        DataType::Null => ValueRef::Null,
        DataType::Utf8 => ValueRef::Text(down!(column, StringArray).value(row).as_bytes()),
        DataType::LargeUtf8 => ValueRef::Text(down!(column, LargeStringArray).value(row).as_bytes()),
        DataType::Utf8View => ValueRef::Text(down!(column, StringViewArray).value(row).as_bytes()),
        DataType::Binary if geometry => ValueRef::Geometry(down!(column, BinaryArray).value(row)),
        DataType::LargeBinary if geometry => ValueRef::Geometry(down!(column, LargeBinaryArray).value(row)),
        DataType::BinaryView if geometry => ValueRef::Geometry(down!(column, BinaryViewArray).value(row)),
        DataType::Binary => ValueRef::Blob(down!(column, BinaryArray).value(row)),
        DataType::LargeBinary => ValueRef::Blob(down!(column, LargeBinaryArray).value(row)),
        DataType::BinaryView => ValueRef::Blob(down!(column, BinaryViewArray).value(row)),
        DataType::FixedSizeBinary(_) => ValueRef::Blob(down!(column, FixedSizeBinaryArray).value(row)),
        DataType::Boolean => ValueRef::Boolean(down!(column, BooleanArray).value(row)),
        DataType::Int8 => ValueRef::TinyInt(down!(column, Int8Array).value(row)),
        DataType::Int16 => ValueRef::SmallInt(down!(column, Int16Array).value(row)),
        DataType::Int32 => ValueRef::Int(down!(column, Int32Array).value(row)),
        DataType::Int64 => ValueRef::BigInt(down!(column, Int64Array).value(row)),
        DataType::UInt8 => ValueRef::UTinyInt(down!(column, UInt8Array).value(row)),
        DataType::UInt16 => ValueRef::USmallInt(down!(column, UInt16Array).value(row)),
        DataType::UInt32 => ValueRef::UInt(down!(column, UInt32Array).value(row)),
        DataType::UInt64 => ValueRef::UBigInt(down!(column, UInt64Array).value(row)),
        DataType::Float16 => ValueRef::Float(down!(column, Float16Array).value(row).to_f32()),
        DataType::Float32 => ValueRef::Float(down!(column, Float32Array).value(row)),
        DataType::Float64 => ValueRef::Double(down!(column, Float64Array).value(row)),
        DataType::Decimal32(w, s) => decimal_ref(*w, *s, down!(column, Decimal32Array).value(row) as i128),
        DataType::Decimal64(w, s) => decimal_ref(*w, *s, down!(column, Decimal64Array).value(row) as i128),
        DataType::Decimal128(w, s) => {
            let v = down!(column, Decimal128Array).value(row);
            if *s == 0 && *w == Decimal::MAX_WIDTH { ValueRef::HugeInt(v) } else { decimal_ref(*w, *s, v) }
        }
        DataType::Timestamp(ArrowUnit::Second, _) => ValueRef::Timestamp(TimeUnit::Second, down!(column, TimestampSecondArray).value(row)),
        DataType::Timestamp(ArrowUnit::Millisecond, _) => ValueRef::Timestamp(TimeUnit::Millisecond, down!(column, TimestampMillisecondArray).value(row)),
        DataType::Timestamp(ArrowUnit::Microsecond, _) => ValueRef::Timestamp(TimeUnit::Microsecond, down!(column, TimestampMicrosecondArray).value(row)),
        DataType::Timestamp(ArrowUnit::Nanosecond, _) => ValueRef::Timestamp(TimeUnit::Nanosecond, down!(column, TimestampNanosecondArray).value(row)),
        DataType::Date32 => ValueRef::Date32(down!(column, Date32Array).value(row)),
        DataType::Time32(ArrowUnit::Second) => ValueRef::Time64(TimeUnit::Second, down!(column, Time32SecondArray).value(row) as i64),
        DataType::Time32(_) => ValueRef::Time64(TimeUnit::Millisecond, down!(column, Time32MillisecondArray).value(row) as i64),
        DataType::Time64(ArrowUnit::Nanosecond) => ValueRef::Time64(TimeUnit::Nanosecond, down!(column, Time64NanosecondArray).value(row)),
        DataType::Time64(_) => ValueRef::Time64(TimeUnit::Microsecond, down!(column, Time64MicrosecondArray).value(row)),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = down!(column, IntervalMonthDayNanoArray).value(row);
            ValueRef::Interval { months: v.months, days: v.days, nanos: v.nanoseconds }
        }
        DataType::LargeList(..) => ValueRef::List(ListType::Large(down!(column, LargeListArray)), row),
        DataType::List(..) => ValueRef::List(ListType::Regular(down!(column, ListArray)), row),
        DataType::Dictionary(key, _) => ValueRef::Enum(
            match key.as_ref() {
                DataType::UInt8 => EnumType::UInt8(down!(column, DictionaryArray<UInt8Type>)),
                DataType::UInt16 => EnumType::UInt16(down!(column, DictionaryArray<UInt16Type>)),
                DataType::UInt32 => EnumType::UInt32(down!(column, DictionaryArray<UInt32Type>)),
                t => panic!("Unsupported key type: {t:?}"),
            },
            row,
        ),
        DataType::Struct(_) => ValueRef::Struct(down!(column, StructArray), row),
        DataType::Map(..) => ValueRef::Map(down!(column, MapArray), row),
        DataType::FixedSizeList(..) => ValueRef::Array(down!(column, FixedSizeListArray), row),
        DataType::Union(..) => ValueRef::Union(column, row),
        t => unreachable!("invalid value: {t}"),
    }
}

fn decimal_ref(w: u8, s: i8, v: i128) -> ValueRef<'static> {
    match u8::try_from(s).ok().and_then(|s| Decimal::new(w.clamp(1, 38), s, v).ok()) {
        Some(d) => ValueRef::Decimal(d),
        None => ValueRef::Double(v as f64 / 10f64.powi(s as i32)),
    }
}

// ------------------------------------------------------------------ FromSql

/// A Rust type a column value converts to (`Row::get`, `query_scalar`).
pub trait FromSql: Sized {
    #[doc(hidden)]
    fn column_result(v: ValueRef<'_>) -> Result<Self>;
}

fn invalid_type() -> anyhow::Error { anyhow::Error::new(DuckError("Invalid column type".into())) }
fn out_of_range(v: impl fmt::Display) -> anyhow::Error { anyhow::Error::new(DuckError(format!("Value {v} out of range"))) }

/// `decimal` rounded half away from zero to an integer.
fn rounded(d: Decimal) -> Option<i128> {
    if d.scale == 0 { return Some(d.value); }
    let p = 10i128.checked_pow(d.scale as u32)?;
    let (q, r) = (d.value / p, d.value % p);
    if r.unsigned_abs() * 2 >= p.unsigned_abs() { if d.value < 0 { q.checked_sub(1) } else { q.checked_add(1) } } else { Some(q) }
}

/// An integer-valued column as i128 (`Err` for non-numeric values); text is parsed.
fn integral(v: ValueRef<'_>) -> Result<std::result::Result<i128, u128>> {
    Ok(Ok(match v {
        ValueRef::TinyInt(i) => i as i128,
        ValueRef::SmallInt(i) => i as i128,
        ValueRef::Int(i) => i as i128,
        ValueRef::BigInt(i) => i as i128,
        ValueRef::HugeInt(i) => i,
        ValueRef::UHugeInt(i) => return Ok(i128::try_from(i).map_err(|_| i)),
        ValueRef::UTinyInt(i) => i as i128,
        ValueRef::USmallInt(i) => i as i128,
        ValueRef::UInt(i) => i as i128,
        ValueRef::UBigInt(i) => i as i128,
        // truncated toward zero, as duckdb-rs' `cast::From<f64>` does
        ValueRef::Float(f) => { if !f.is_finite() || f.abs() >= 1.7e38 { return Err(out_of_range(f)); } f.trunc() as i128 }
        ValueRef::Double(f) => { if !f.is_finite() || f.abs() >= 1.7e38 { return Err(out_of_range(f)); } f.trunc() as i128 }
        ValueRef::Decimal(d) => rounded(d).ok_or_else(|| out_of_range(d))?,
        ValueRef::Timestamp(_, i) => i as i128,
        ValueRef::Date32(i) => i as i128,
        ValueRef::Time64(TimeUnit::Microsecond, i) => i as i128,
        ValueRef::Text(_) => v.as_str()?.parse::<i128>().map_err(|_| invalid_type())?,
        _ => return Err(invalid_type()),
    }))
}

macro_rules! from_sql_int {
    ($($t:ty),*) => {$(
        impl FromSql for $t {
            fn column_result(v: ValueRef<'_>) -> Result<Self> {
                match integral(v)? {
                    Ok(i) => <$t>::try_from(i).map_err(|_| out_of_range(i)),
                    Err(u) => <$t>::try_from(u).map_err(|_| out_of_range(u)),
                }
            }
        }
    )*};
}
from_sql_int!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, usize);

impl FromSql for u128 {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match integral(v)? { Ok(i) => u128::try_from(i).map_err(|_| out_of_range(i)), Err(u) => Ok(u) }
    }
}

fn float_of(v: ValueRef<'_>) -> Result<f64> {
    Ok(match v {
        ValueRef::Float(f) => f as f64,
        ValueRef::Double(f) => f,
        ValueRef::Decimal(d) => d.value as f64 / 10f64.powi(d.scale as i32),
        ValueRef::Text(_) => v.as_str()?.parse::<f64>().map_err(|_| invalid_type())?,
        _ => match integral(v)? { Ok(i) => i as f64, Err(u) => u as f64 },
    })
}
impl FromSql for f64 { fn column_result(v: ValueRef<'_>) -> Result<Self> { float_of(v) } }
impl FromSql for f32 {
    fn column_result(v: ValueRef<'_>) -> Result<Self> { if let ValueRef::Float(f) = v { Ok(f) } else { float_of(v).map(|f| f as f32) } }
}

impl FromSql for bool {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match v { ValueRef::Boolean(b) => Ok(b), _ => i8::column_result(v).map(|i| i != 0) }
    }
}

impl FromSql for String {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match v {
            ValueRef::Date32(d) => Ok(crate::core::script::date_from_days(d).format("%F").to_string()),
            ValueRef::Time64(u, t) => Ok(chrono::NaiveTime::MIN.overflowing_add_signed(chrono::Duration::microseconds(u.to_micros(t))).0.format("%T%.f").to_string()),
            ValueRef::Timestamp(u, t) => Ok(crate::core::script::naive_ts(u, t).format("%F %T%.f").to_string()),
            _ => v.as_str().map(str::to_owned),
        }
    }
}

impl FromSql for Vec<u8> {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match v { ValueRef::Blob(b) | ValueRef::Text(b) | ValueRef::Geometry(b) => Ok(b.to_vec()), _ => Err(invalid_type()) }
    }
}

impl FromSql for Decimal {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match v { ValueRef::Decimal(d) => Ok(d), _ => match integral(v)? { Ok(i) => Decimal::new(38, 0, i).map_err(|_| out_of_range(i)), Err(u) => Err(out_of_range(u)) } }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn column_result(v: ValueRef<'_>) -> Result<Self> {
        match v { ValueRef::Null => Ok(None), _ => T::column_result(v).map(Some) }
    }
}

// ------------------------------------------------------------------ appender

/// Bulk insert into one table. duckdb-neo has no table appender: rows are buffered, written into
/// data chunks of a column data collection on [`Appender::flush`] (and on drop, and every
/// `APPEND_AUTO_FLUSH` rows), and inserted with one `INSERT INTO <table> SELECT * FROM <collection>`.
/// The collection's column types follow the values (the first non-NULL one per column), and the
/// INSERT casts them to the table's types, as the v1 appender's per-value appends do.
pub struct Appender<'c> {
    con: &'c Connection,
    target: String,
    ncols: usize,
    rows: Vec<Vec<Value>>,
}

/// An appender on `schema.table`.
pub fn appender<'c>(con: &'c Connection, schema: &str, table: &str) -> Result<Appender<'c>> {
    let target = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let ncols = {
        let mut st = con.prepare(&format!("SELECT * FROM {target} LIMIT 0"))?;
        let rows = st.query([])?;
        rows.src.schema().fields().len()
    };
    Ok(Appender { con, target, ncols, rows: Vec::new() })
}

fn quote_ident(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }

impl Appender<'_> {
    /// Append one row (one value per column, in table order).
    pub fn append_row(&mut self, row: Vec<Value>) -> Result<()> {
        if row.len() != self.ncols {
            bail!("Invalid Input Error: Call to EndRow before all columns have been appended to! ({} values for {} columns of {})", row.len(), self.ncols, self.target);
        }
        self.rows.push(row);
        if self.rows.len() >= APPEND_AUTO_FLUSH { self.flush()?; }
        Ok(())
    }

    /// Write the pending rows to the table.
    pub fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() { return Ok(()); }
        let rows = std::mem::take(&mut self.rows);
        let con = &self.con.con;
        let kinds: Vec<Kind> = (0..self.ncols).map(|c| column_kind(&rows, c)).collect();
        let types: Vec<LogicalType> = kinds.iter().map(|k| k.logical_type(con)).collect::<Result<_>>()?;
        let mut app = ColumnDataCollection::new(con, types.clone()).d()?.to_append().d()?;
        for block in rows.chunks(CHUNK_ROWS) {
            let mut chunk = DataChunk::create(con, &types, true).d()?;
            for (c, k) in kinds.iter().enumerate() { write_column(&mut chunk, c, *k, block)?; }
            app.append(&mut chunk).d()?;
        }
        const NAME: &str = "__duckbridge_append";
        let stmt = con.parse(format!("INSERT INTO {} SELECT * FROM {NAME}", self.target)).d()?
            .next().ok_or_else(|| anyhow!("no INSERT statement"))?.d()?
            .add_collection(app.to_normal(), NAME).d()?;
        stmt.query(con, Parameters::None).d()?.drain().d()?;
        Ok(())
    }
}

impl Drop for Appender<'_> {
    fn drop(&mut self) { let _ = self.flush(); }
}

/// The vector type of one column written from `Value`s (an appended column, or a table function's
/// output column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Bool, I8, I16, I32, I64, I128, U8, U16, U32, U64, U128, F32, F64, Text, Blob, Date, TimeUs, TimeNs, TsS, TsMs, TsUs, TsNs, Interval,
    /// DECIMAL(width, scale); takes `Decimal`, its text (`"12.50"`, `"-3."`), integers and doubles
    /// (rounded half away from zero to `scale` digits).
    Decimal { width: u8, scale: u8 },
    /// UUID; takes the text form or the 16 bytes (big-endian).
    Uuid,
    /// TIMESTAMP WITH TIME ZONE; takes the UTC instant as a `Timestamp`.
    TsTz,
}

fn kind_of(v: &Value) -> Option<Kind> {
    Some(match v {
        Value::Null => return None,
        Value::Boolean(_) => Kind::Bool,
        Value::TinyInt(_) => Kind::I8,
        Value::SmallInt(_) => Kind::I16,
        Value::Int(_) => Kind::I32,
        Value::BigInt(_) => Kind::I64,
        Value::HugeInt(_) => Kind::I128,
        Value::UHugeInt(_) => Kind::U128,
        Value::UTinyInt(_) => Kind::U8,
        Value::USmallInt(_) => Kind::U16,
        Value::UInt(_) => Kind::U32,
        Value::UBigInt(_) => Kind::U64,
        Value::Float(_) => Kind::F32,
        Value::Double(_) => Kind::F64,
        Value::Blob(_) | Value::Geometry(_) => Kind::Blob,
        Value::Date32(_) => Kind::Date,
        Value::Time64(TimeUnit::Nanosecond, _) => Kind::TimeNs,
        Value::Time64(..) => Kind::TimeUs,
        Value::Timestamp(TimeUnit::Second, _) => Kind::TsS,
        Value::Timestamp(TimeUnit::Millisecond, _) => Kind::TsMs,
        Value::Timestamp(TimeUnit::Microsecond, _) => Kind::TsUs,
        Value::Timestamp(TimeUnit::Nanosecond, _) => Kind::TsNs,
        Value::Interval { .. } => Kind::Interval,
        // DECIMAL goes in as its text (the INSERT casts it to the column's DECIMAL), as do the rest
        _ => Kind::Text,
    })
}

/// The kind of column `c`: that of its non-NULL values; VARCHAR when they disagree or are all NULL.
fn column_kind(rows: &[Vec<Value>], c: usize) -> Kind {
    let mut k = None;
    for r in rows {
        match (k, kind_of(&r[c])) {
            (_, None) => {}
            (None, Some(x)) => k = Some(x),
            (Some(a), Some(b)) if a == b => {}
            _ => return Kind::Text,
        }
    }
    k.unwrap_or(Kind::Text)
}

impl Kind {
    /// The kind that writes vectors of `t` (a declared output column type); `None` for types it
    /// cannot write (nested types, ENUM, ...).
    pub fn of_type(t: &LogicalType) -> Option<Kind> {
        use LogicalTypeID as T;
        Some(match t.id().ok()? {
            T::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN => Kind::Bool,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT => Kind::I8,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT => Kind::I16,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER => Kind::I32,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT => Kind::I64,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT => Kind::I128,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT => Kind::U8,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT => Kind::U16,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER => Kind::U32,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT => Kind::U64,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT => Kind::U128,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT => Kind::F32,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE => Kind::F64,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR => Kind::Text,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB => Kind::Blob,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_DATE => Kind::Date,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIME => Kind::TimeUs,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS => Kind::TimeNs,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC => Kind::TsS,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS => Kind::TsMs,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP => Kind::TsUs,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS => Kind::TsNs,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ => Kind::TsTz,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL => Kind::Interval,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_UUID => Kind::Uuid,
            T::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL => { let d = DecimalSignature::from_logical_type(t).ok()?; Kind::Decimal { width: d.width, scale: d.scale } }
            _ => return None,
        })
    }

    fn logical_type(self, con: &neo::connection::Connection) -> Result<LogicalType> {
        match self {
            Kind::Bool => bool::logical_type(con),
            Kind::I8 => i8::logical_type(con),
            Kind::I16 => i16::logical_type(con),
            Kind::I32 => i32::logical_type(con),
            Kind::I64 => i64::logical_type(con),
            Kind::I128 => i128::logical_type(con),
            Kind::U8 => u8::logical_type(con),
            Kind::U16 => u16::logical_type(con),
            Kind::U32 => u32::logical_type(con),
            Kind::U64 => u64::logical_type(con),
            Kind::U128 => u128::logical_type(con),
            Kind::F32 => f32::logical_type(con),
            Kind::F64 => f64::logical_type(con),
            Kind::Text => String::logical_type(con),
            Kind::Blob => BlobValue::logical_type(con),
            Kind::Date => DateValue::logical_type(con),
            Kind::TimeUs => TimeValue::logical_type(con),
            Kind::TimeNs => TimeNsValue::logical_type(con),
            Kind::TsS => TimestampSecValue::logical_type(con),
            Kind::TsMs => TimestampMsValue::logical_type(con),
            Kind::TsUs => TimestampValue::logical_type(con),
            Kind::TsNs => TimestampNsValue::logical_type(con),
            Kind::Interval => IntervalValue::logical_type(con),
            Kind::Decimal { width, scale } => DecimalSignature { width, scale }.logical_type(con),
            Kind::Uuid => UuidValueRaw::logical_type(con),
            Kind::TsTz => TimestampTzValue::logical_type(con),
        }
        .d()
    }
}

/// The text a value goes in as when its column is VARCHAR.
fn value_text(v: &Value) -> Option<String> {
    use crate::core::script::{date_from_days, naive_ts, time_from_units};
    Some(match v {
        Value::Null => return None,
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(x) => x.to_string(),
        Value::SmallInt(x) => x.to_string(),
        Value::Int(x) => x.to_string(),
        Value::BigInt(x) => x.to_string(),
        Value::HugeInt(x) => x.to_string(),
        Value::UHugeInt(x) => x.to_string(),
        Value::UTinyInt(x) => x.to_string(),
        Value::USmallInt(x) => x.to_string(),
        Value::UInt(x) => x.to_string(),
        Value::UBigInt(x) => x.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::Double(f) => format!("{f:?}"),
        Value::Decimal(d) => d.to_string(),
        Value::Text(s) | Value::Enum(s) => s.clone(),
        Value::Blob(b) | Value::Geometry(b) => b.iter().map(|x| format!("\\x{x:02X}")).collect(),
        Value::Date32(d) => date_from_days(*d).to_string(),
        Value::Time64(u, t) => time_from_units(*u, *t),
        Value::Timestamp(u, t) => naive_ts(*u, *t).format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        Value::Interval { months, days, nanos } => format!("{months} months {days} days {} microseconds", nanos / 1000),
        other => format!("{other:?}"),
    })
}

/// A data chunk whose vectors can be written: an owned [`DataChunk`] or one DuckDB lends a
/// callback ([`DataChunkRef`], e.g. a table function's output).
pub trait ChunkMut {
    /// Vector `c` as `T`, borrowed mutably.
    fn vector_mut<T: VectorElement>(&mut self, c: usize) -> neo::Result<Vector<'_, T>>;
}
impl ChunkMut for DataChunk<'_> {
    fn vector_mut<T: VectorElement>(&mut self, c: usize) -> neo::Result<Vector<'_, T>> { self.get_vector_at_mut::<T>(c) }
}
impl ChunkMut for DataChunkRef<'_> {
    fn vector_mut<T: VectorElement>(&mut self, c: usize) -> neo::Result<Vector<'_, T>> { self.get_vector_at_mut::<T>(c) }
}

/// `v` as the mantissa of a DECIMAL(_, scale): exact for `Decimal`s, their text and integers, rounded
/// half away from zero for doubles and longer fractions.
fn decimal_mantissa(v: &Value, scale: u8) -> Option<i128> {
    let p = 10i128.checked_pow(scale as u32)?;
    let rescale = |m: i128, s: u8| -> Option<i128> {
        if s <= scale { m.checked_mul(10i128.checked_pow((scale - s) as u32)?) } else {
            let d = 10i128.checked_pow((s - scale) as u32)?;
            let (q, r) = (m / d, m % d);
            Some(if r.abs() * 2 >= d { q + m.signum() } else { q })
        }
    };
    match v {
        Value::Decimal(d) => rescale(d.value(), d.scale()),
        Value::Text(t) => {
            let t = t.trim();
            let (neg, digits) = match t.strip_prefix('-') { Some(r) => (true, r), None => (false, t.strip_prefix('+').unwrap_or(t)) };
            let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
            if int.is_empty() && frac.is_empty() || !int.chars().chain(frac.chars()).all(|c| c.is_ascii_digit()) { return None; }
            let all = format!("{int}{frac}");
            let all = all.trim_start_matches('0');
            let m: i128 = if all.is_empty() { 0 } else { all.parse().ok()? };
            let m = rescale(m, u8::try_from(frac.len()).ok()?)?;
            Some(if neg { -m } else { m })
        }
        Value::Double(f) => { let x = (f * p as f64).round(); (x.is_finite() && x.abs() < 1e38).then_some(x as i128) }
        Value::Float(f) => { let x = (*f as f64 * p as f64).round(); (x.is_finite() && x.abs() < 1e38).then_some(x as i128) }
        Value::TinyInt(x) => (*x as i128).checked_mul(p),
        Value::SmallInt(x) => (*x as i128).checked_mul(p),
        Value::Int(x) => (*x as i128).checked_mul(p),
        Value::BigInt(x) => (*x as i128).checked_mul(p),
        Value::HugeInt(x) => x.checked_mul(p),
        Value::UTinyInt(x) => (*x as i128).checked_mul(p),
        Value::USmallInt(x) => (*x as i128).checked_mul(p),
        Value::UInt(x) => (*x as i128).checked_mul(p),
        Value::UBigInt(x) => (*x as i128).checked_mul(p),
        _ => None,
    }
}

fn uuid_raw(v: &Value) -> Option<UuidValueRaw> {
    match v {
        Value::Text(t) => uuid::Uuid::parse_str(t.trim()).ok().map(|u| UuidValueRaw::from_bytes(*u.as_bytes())),
        Value::Blob(b) => <[u8; 16]>::try_from(b.as_slice()).ok().map(UuidValueRaw::from_bytes),
        _ => None,
    }
}

/// Write column `c` of `rows` into `chunk` as `kind` (the vector gets `rows.len()` rows). A value
/// that does not fit `kind` is an error naming the column index and the value.
pub fn write_column<C: ChunkMut + ?Sized>(chunk: &mut C, c: usize, kind: Kind, rows: &[Vec<Value>]) -> Result<()> {
    let n = rows.len();
    macro_rules! fill {
        ($t:ty, $($p:pat => $e:expr),+) => {{
            let mut v = chunk.vector_mut::<$t>(c).d()?;
            v.set_size(n).d()?;
            for (i, r) in rows.iter().enumerate() {
                match &r[c] {
                    $($p => v.write(i, Some($e)).d()?,)+
                    Value::Null => v.write(i, None).d()?,
                    other => bail!("value {other:?} does not fit column {c} ({kind:?})"),
                }
            }
        }};
    }
    match kind {
        Kind::Bool => fill!(bool, Value::Boolean(x) => *x),
        Kind::I8 => fill!(i8, Value::TinyInt(x) => *x),
        Kind::I16 => fill!(i16, Value::SmallInt(x) => *x),
        Kind::I32 => fill!(i32, Value::Int(x) => *x),
        Kind::I64 => fill!(i64, Value::BigInt(x) => *x),
        Kind::I128 => fill!(i128, Value::HugeInt(x) => *x),
        Kind::U8 => fill!(u8, Value::UTinyInt(x) => *x),
        Kind::U16 => fill!(u16, Value::USmallInt(x) => *x),
        Kind::U32 => fill!(u32, Value::UInt(x) => *x),
        Kind::U64 => fill!(u64, Value::UBigInt(x) => *x),
        Kind::U128 => fill!(u128, Value::UHugeInt(x) => *x),
        Kind::F32 => fill!(f32, Value::Float(x) => *x),
        Kind::F64 => fill!(f64, Value::Double(x) => *x),
        Kind::Blob => fill!(BlobValue, Value::Blob(b) => b.as_slice(), Value::Geometry(b) => b.as_slice()),
        Kind::Date => fill!(DateValue, Value::Date32(d) => DateValue(*d)),
        Kind::TimeUs => fill!(TimeValue, Value::Time64(u, t) => TimeValue(u.to_micros(*t))),
        Kind::TimeNs => fill!(TimeNsValue, Value::Time64(_, t) => TimeNsValue(*t)),
        Kind::TsS => fill!(TimestampSecValue, Value::Timestamp(_, t) => TimestampSecValue(*t)),
        Kind::TsMs => fill!(TimestampMsValue, Value::Timestamp(_, t) => TimestampMsValue(*t)),
        Kind::TsUs => fill!(TimestampValue, Value::Timestamp(_, t) => TimestampValue(*t)),
        Kind::TsNs => fill!(TimestampNsValue, Value::Timestamp(_, t) => TimestampNsValue(*t)),
        Kind::Interval => fill!(IntervalValue, Value::Interval { months, days, nanos } => IntervalValue { months: *months, days: *days, micros: *nanos / 1000 }),
        Kind::TsTz => fill!(TimestampTzValue, Value::Timestamp(u, t) => TimestampTzValue(u.to_micros(*t))),
        Kind::Uuid => {
            let mut v = chunk.vector_mut::<UuidValueRaw>(c).d()?;
            v.set_size(n).d()?;
            for (i, r) in rows.iter().enumerate() {
                match &r[c] {
                    Value::Null => v.write(i, None).d()?,
                    x => v.write(i, Some(uuid_raw(x).ok_or_else(|| anyhow!("value {x:?} does not fit column {c} (UUID)"))?)).d()?,
                }
            }
        }
        Kind::Decimal { width, scale } => {
            macro_rules! dec {
                ($t:ident, $int:ty) => {{
                    let mut v = chunk.vector_mut::<$t>(c).d()?;
                    v.set_size(n).d()?;
                    let limit = 10i128.pow(width as u32);
                    for (i, r) in rows.iter().enumerate() {
                        match &r[c] {
                            Value::Null => v.write(i, None).d()?,
                            x => {
                                let m = decimal_mantissa(x, scale).filter(|m| m.abs() < limit)
                                    .ok_or_else(|| anyhow!("value {x:?} does not fit column {c} (DECIMAL({width},{scale}))"))?;
                                v.write(i, Some($t(m as $int))).d()?
                            }
                        }
                    }
                }};
            }
            match width { 1..=4 => dec!(ShortDecimal, i16), 5..=9 => dec!(WordDecimal, i32), 10..=18 => dec!(LongDecimal, i64), _ => dec!(HugeDecimal, i128) }
        }
        Kind::Text => {
            let texts: Vec<Option<String>> = rows.iter().map(|r| match &r[c] { Value::Text(_) | Value::Enum(_) => None, v => value_text(v) }).collect();
            let mut v = chunk.vector_mut::<String>(c).d()?;
            v.set_size(n).d()?;
            for (i, r) in rows.iter().enumerate() {
                match (&r[c], &texts[i]) {
                    (Value::Text(s) | Value::Enum(s), _) => v.write(i, Some(s.as_str())).d()?,
                    (_, Some(t)) => v.write(i, Some(t.as_str())).d()?,
                    (_, None) => v.write(i, None).d()?,
                }
            }
        }
    }
    Ok(())
}

/// Arrow record batches from what duckdb-neo's `ArrowExporter` produced: its schema and arrays
/// (C data interface structs), imported into our `arrow` crate as `ArrowResult` does for query
/// results.
pub fn import_arrow(schema: neo::arrow::ArrowSchema, arrays: Vec<neo::arrow::ArrowArray>) -> Result<Vec<RecordBatch>> {
    // SAFETY: duckdb-neo's ArrowSchema / ArrowArray are the Arrow C data interface structs, as arrow's FFI types.
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute::<neo::ffi::ArrowSchema, FFI_ArrowSchema>(schema.into_raw()) };
    let schema = Arc::new(Schema::try_from(&ffi_schema).context("reading the exported Arrow schema")?);
    arrays.into_iter().map(|a| {
        let ffi_array: FFI_ArrowArray = unsafe { std::mem::transmute::<neo::ffi::ArrowArray, FFI_ArrowArray>(a.into_raw()) };
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }.context("importing an exported Arrow array")?;
        let sa = StructArray::from(data);
        let (_, columns, _) = sa.into_parts();
        RecordBatch::try_new(schema.clone(), columns).context("building a record batch from an exported Arrow array")
    }).collect()
}

// ------------------------------------------------------------------ Arrow streaming

/// A streaming query: its result schema is known up front, then the result is read either as
/// Arrow chunks ([`ArrowStream::next_chunk`]) or row by row ([`ArrowStream::rows`]) - one or the
/// other, not both.
pub struct ArrowStream<'c> {
    res: ArrowResult<'c>,
    _stmt: Statement<'c>,
}

/// Prepare and start `sql` as a streaming query (DuckDB produces chunks as they are read).
/// Errors carry `preparing <sql>` / `running <sql>` contexts (first 300 characters of the SQL).
pub fn stream<'c>(con: &'c Connection, sql: &str) -> Result<ArrowStream<'c>> {
    let stmt = con.prepare(sql).with_context(|| format!("preparing {}", sql.chars().take(300).collect::<String>()))?;
    let res = stmt.prep.execute_arrow(Parameters::None, Some(STREAM_BATCH)).d()
        .and_then(ArrowResult::new)
        .with_context(|| format!("running {}", sql.chars().take(300).collect::<String>()))?;
    Ok(ArrowStream { res, _stmt: stmt })
}

impl<'c> ArrowStream<'c> {
    /// The result schema.
    pub fn schema(&self) -> &SchemaRef { &self.res.schema }

    /// The next result chunk (columns in result order), `None` at the end.
    pub fn next_chunk(&mut self) -> Result<Option<StructArray>> {
        self.res.next_batch()
    }

    /// Read the (already running) result row by row instead.
    pub fn rows(&mut self) -> Rows<'_> {
        Rows::new(Box::new(&mut self.res))
    }
}

/// `sql` as a stream of Arrow record batches (see [`stream`]).
pub fn arrow_batches<'c>(con: &'c Connection, sql: &str) -> Result<impl Iterator<Item = Result<RecordBatch>> + 'c> {
    let mut s = stream(con, sql)?;
    Ok(std::iter::from_fn(move || match s.next_chunk() {
        Ok(Some(chunk)) => {
            let (fields, columns, _) = chunk.into_parts();
            Some(RecordBatch::try_new(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)), columns).map_err(anyhow::Error::from))
        }
        Ok(None) => None,
        Err(e) => Some(Err(e)),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neo_decimal_and_uuid_values() {
        let t = |s: &str| Value::Text(s.into());
        assert_eq!(decimal_mantissa(&t("12.5"), 2), Some(1250));
        assert_eq!(decimal_mantissa(&t("-0.505"), 2), Some(-51));
        assert_eq!(decimal_mantissa(&t("123."), 0), Some(123));
        assert_eq!(decimal_mantissa(&t("0.0000"), 4), Some(0));
        assert_eq!(decimal_mantissa(&t("+7"), 3), Some(7000));
        assert_eq!(decimal_mantissa(&Value::Double(0.125), 2), Some(13));
        assert_eq!(decimal_mantissa(&Value::Double(-2.5), 0), Some(-3));
        assert_eq!(decimal_mantissa(&Value::Decimal(Decimal::new(10, 3, 1234).unwrap()), 1), Some(12));
        assert_eq!(decimal_mantissa(&Value::BigInt(-4), 2), Some(-400));
        for bad in ["", ".", "1e5", "abc", "1.2.3", "99999999999999999999999999999999999999999"] { assert_eq!(decimal_mantissa(&t(bad), 2), None, "{bad}"); }
        let u = "6f1b5a2e-0c3d-4e5f-8a9b-0123456789ab";
        let raw = uuid_raw(&t(u)).unwrap();
        assert_eq!(uuid::Uuid::from_bytes(raw.to_bytes()).to_string(), u);
        assert_eq!(uuid_raw(&Value::Blob(uuid::Uuid::parse_str(u).unwrap().as_bytes().to_vec())), Some(raw));
        assert_eq!(uuid_raw(&t("nope")), None);
    }

    /// The kinds `bridge_scan` writes (DECIMAL widths, UUID, TIMESTAMPTZ, text) round-trip through a chunk.
    #[test]
    fn neo_write_column_scan_kinds() {
        let con = open_in_memory().unwrap();
        exec(&con, "SET TimeZone = 'UTC'; CREATE TABLE t (a DECIMAL(4,1), b DECIMAL(9,2), c DECIMAL(18,3), d DECIMAL(38,4), u UUID, z TIMESTAMPTZ, s VARCHAR, x UTINYINT)").unwrap();
        let names = ["DECIMAL(4,1)", "DECIMAL(9,2)", "DECIMAL(18,3)", "DECIMAL(38,4)", "UUID", "TIMESTAMP WITH TIME ZONE", "VARCHAR", "UTINYINT"];
        let types: Vec<LogicalType> = names.iter().map(|n| LogicalType::from_text(&con.con, n).unwrap()).collect();
        let kinds: Vec<Kind> = types.iter().map(|t| Kind::of_type(t).unwrap()).collect();
        assert_eq!(&kinds[..2], &[Kind::Decimal { width: 4, scale: 1 }, Kind::Decimal { width: 9, scale: 2 }]);
        assert_eq!(&kinds[4..], &[Kind::Uuid, Kind::TsTz, Kind::Text, Kind::U8]);
        assert_eq!(Kind::of_type(&LogicalType::from_text(&con.con, "INTEGER[]").unwrap()), None);
        let t = |s: &str| Value::Text(s.into());
        let rows = vec![
            vec![t("-999.9"), t("1234567.89"), Value::Double(1.0005), t("-12345678901234567890123456789012.3456"), t("6f1b5a2e-0c3d-4e5f-8a9b-0123456789ab"),
                 Value::Timestamp(TimeUnit::Microsecond, 1_000_001), Value::Int(7), Value::UTinyInt(255)],
            vec![Value::Null, Value::Null, Value::Null, Value::Null, Value::Null, Value::Null, Value::Null, Value::Null],
        ];
        let mut chunk = DataChunk::create(&con.con, &types, true).unwrap();
        for (c, k) in kinds.iter().enumerate() { write_column(&mut chunk, c, *k, &rows).unwrap(); }
        let mut app = ColumnDataCollection::new(&con.con, types.clone()).d().unwrap().to_append().d().unwrap();
        app.append(&mut chunk).d().unwrap();
        let stmt = con.con.parse("INSERT INTO t SELECT * FROM __w").d().unwrap().next().unwrap().d().unwrap().add_collection(app.to_normal(), "__w").d().unwrap();
        stmt.query(&con.con, Parameters::None).d().unwrap().drain().d().unwrap();
        let got: Vec<String> = query_rows(&con, "SELECT concat_ws('|', a, b, c, d, u, z, s, x) FROM t ORDER BY a NULLS LAST", |r| r.get(0)).unwrap();
        assert_eq!(got, ["-999.9|1234567.89|1.001|-12345678901234567890123456789012.3456|6f1b5a2e-0c3d-4e5f-8a9b-0123456789ab|1970-01-01 00:00:01.000001+00|7|255", ""]);
        // values that do not fit name the column and the value
        let e = write_column(&mut chunk, 0, kinds[0], &[vec![t("1000.0")]]).unwrap_err().to_string();
        assert!(e.contains("column 0") && e.contains("DECIMAL(4,1)") && e.contains("1000.0"), "{e}");
        let mut bad = rows[1].clone();
        bad[4] = t("x");
        let e = write_column(&mut chunk, 4, kinds[4], &[bad]).unwrap_err().to_string();
        assert!(e.contains("column 4 (UUID)"), "{e}");
    }
}
