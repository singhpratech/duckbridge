//! Columnar, pipelined DuckDB -> SQL Server bulk copy (spec R5.3, R5.5).
//!
//! A reader thread runs the SELECT on its own DuckDB connection (`core::conn::acquire`) as a
//! *streaming* query, takes each result chunk as Arrow arrays, converts it column by column with
//! the precompiled [`Converter`]s (one downcast per column per chunk, no per-cell `get_ref`), and
//! hands batches of `TokenRow`s to the async side through a bounded channel. The async side feeds
//! them to `INSERT BULK`, finalizing and restarting the request every `batch_rows` rows to bound
//! the server-side transaction. DuckDB reading/conversion and network I/O overlap.
use super::convert::Converter;
use super::MsClient;
use anyhow::{anyhow, bail, Context, Result};
use duckdb::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array, Decimal32Array, Decimal64Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalMonthDayNanoArray,
    LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use duckdb::arrow::buffer::NullBuffer;
use duckdb::arrow::datatypes::{DataType, IntervalUnit, TimeUnit as ArrowUnit};
use duckdb::types::{TimeUnit, ValueRef};
use duckdb::Connection;
use std::fmt;
use std::time::Instant;
use tiberius::{ColumnData, MetaDataColumn, SortOrder, SqlBulkCopyOption, SqlBulkCopyOptions, TokenRow, TypeInfo, VarLenType};

/// Upper bound for the rows in one channel message (memory: `prefetch_batches` x this x row size).
const MAX_CHUNK_ROWS: usize = 8_192;

pub struct BulkOptions {
    /// Rows per `INSERT BULK` request (finalize + restart bounds the server transaction).
    pub batch_rows: usize,
    /// Send identity values (`KEEP_IDENTITY`: the identity column stays in the column list).
    pub keep_identity: bool,
    /// `TABLOCK` (bulk-update lock; minimal logging into heaps / empty tables).
    pub table_lock: bool,
    /// Converted batches buffered between the DuckDB reader and the TDS writer (default 4).
    pub prefetch_batches: usize,
    /// `col [ASC|DESC], ...` (target column names from `columns`): the DuckDB query is sorted by
    /// these columns and `INSERT BULK ... ORDER (...)` tells SQL Server so. Only for keys whose
    /// order is the same in both engines (integers, dates); text collations may differ.
    pub order_by: Option<String>,
}

impl Default for BulkOptions {
    fn default() -> Self {
        BulkOptions { batch_rows: 100_000, keep_identity: false, table_lock: true, prefetch_batches: 4, order_by: None }
    }
}

/// A conversion failure in the reader: which column (index into `convs`) and which source row
/// (1-based). `bulk_load` turns it into a message with the table and column names.
#[derive(Debug)]
pub struct ConvertError {
    pub column: usize,
    pub row: i64,
    pub message: String,
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "column #{} row {}: {}", self.column + 1, self.row, self.message)
    }
}

impl std::error::Error for ConvertError {}

/// The writer went away (its error is the one to report).
#[derive(Debug)]
struct ReceiverGone;
impl fmt::Display for ReceiverGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "bulk writer stopped") }
}
impl std::error::Error for ReceiverGone {}

enum Msg {
    Rows(Vec<TokenRow<'static>>),
    Done(i64),
}

/// Reads `sql` from DuckDB on a reader thread (its own pooled connection via core::conn::acquire),
/// converts column by column, and streams TokenRows to SQL Server with a bounded channel. Returns
/// rows sent.
///
/// `columns` are the target column names in the order of the SELECT's columns (raw names; they
/// are bracket-quoted here unless already `[quoted]`). They must not include computed columns, nor
/// identity columns unless `keep_identity`. On error nothing of the current batch is committed
/// (earlier batches are), and `client` is left mid-request: drop it.
#[allow(clippy::too_many_arguments)]
pub async fn bulk_load(con: &Connection, client: &mut MsClient, table_ref: &str, columns: &[String], convs: &[Converter], sql: &str, o: &BulkOptions, progress: &dyn Fn(i64)) -> Result<i64> {
    if columns.len() != convs.len() {
        bail!("bulk load into {table_ref}: {} columns but {} converters", columns.len(), convs.len());
    }
    let started = Instant::now();
    let quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let order = parse_order_by(o.order_by.as_deref(), columns)?;
    let sql = if order.is_empty() {
        sql.to_string()
    } else {
        // match SQL Server's NULL placement, or the server rejects the ORDER hint
        let keys: Vec<String> = order.iter().map(|(i, desc)| format!("{} {}", i + 1, if *desc { "DESC NULLS LAST" } else { "ASC NULLS FIRST" })).collect();
        format!("SELECT * FROM ({sql}) AS _duckbridge_src ORDER BY {}", keys.join(", "))
    };
    let hint_names: Vec<(&str, SortOrder)> = order.iter().map(|(i, desc)| (quoted[*i].as_str(), if *desc { SortOrder::Descending } else { SortOrder::Ascending })).collect();
    let mut opts = SqlBulkCopyOptions::empty();
    if o.keep_identity { opts |= SqlBulkCopyOption::KeepIdentity; }
    if o.table_lock { opts |= SqlBulkCopyOption::TableLock; }
    let label = format!("INSERT BULK {table_ref} ({}){}{} -- source: {}", quoted.join(", "),
        if o.table_lock { " WITH (TABLOCK)" } else { "" },
        if hint_names.is_empty() { String::new() } else { format!(" ORDER ({})", hint_names.iter().map(|(c, s)| format!("{c} {}", if *s == SortOrder::Descending { "DESC" } else { "ASC" })).collect::<Vec<_>>().join(", ")) },
        sql.chars().take(500).collect::<String>());
    if crate::core::runlog::planning() {
        crate::core::runlog::stmt("target", table_ref, &label, 0, started);
        return Ok(0);
    }

    let col_refs: Vec<&str> = quoted.iter().map(String::as_str).collect();
    let meta = client.column_metadata(table_ref, &col_refs).await.with_context(|| format!("reading the column metadata of {table_ref}"))?;
    let fixups = wire_fixups(&meta, table_ref, columns, convs, o.keep_identity)?;

    let reader = crate::core::conn::acquire(con)?;
    let interrupt = reader.interrupt_handle();
    let (tx, rx) = tokio::sync::mpsc::channel::<Msg>(o.prefetch_batches.max(1));
    let thread_convs = convs.to_vec();
    let chunk = o.batch_rows.clamp(1, MAX_CHUNK_ROWS);
    let handle = std::thread::Builder::new().name("duckbridge-bulk-reader".into()).spawn(move || {
        let r = read_rows(&reader, &sql, &thread_convs, chunk, &mut |rows| tx.blocking_send(Msg::Rows(rows)).map_err(|_| anyhow::Error::new(ReceiverGone)));
        let r = match r {
            Ok(n) => tx.blocking_send(Msg::Done(n)).map(|_| n).map_err(|_| anyhow::Error::new(ReceiverGone)),
            Err(e) => Err(e),
        };
        (reader, r)
    }).context("starting the DuckDB reader thread")?;

    let sent = send_loop(client, rx, table_ref, &col_refs, opts, &hint_names, o.batch_rows.max(1), &fixups, progress).await;
    if sent.is_err() { interrupt.interrupt(); }
    let (reader, read) = handle.join().map_err(|_| anyhow!("DuckDB reader thread for {table_ref} panicked"))?;
    crate::core::conn::release(reader);

    let read = read.map_err(|e| describe_read_error(e, table_ref, columns, convs));
    match (read, sent) {
        (Ok(n), Ok(s)) => {
            if n != s { bail!("bulk load into {table_ref}: read {n} rows from DuckDB but sent {s}"); }
            crate::core::runlog::stmt("target", table_ref, &label, s, started);
            Ok(s)
        }
        (Err(e), Ok(_)) => Err(e),
        (Err(e), Err(se)) => if e.downcast_ref::<ReceiverGone>().is_some() { Err(se) } else { Err(e) },
        (Ok(_), Err(se)) => Err(se),
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_loop(client: &mut MsClient, mut rx: tokio::sync::mpsc::Receiver<Msg>, table_ref: &str, col_refs: &[&str], opts: SqlBulkCopyOptions,
                   hints: &[(&str, SortOrder)], batch_rows: usize, fixups: &[usize], progress: &dyn Fn(i64)) -> Result<i64> {
    let first = match rx.recv().await {
        Some(Msg::Rows(r)) => r,
        Some(Msg::Done(n)) => { progress(0); if n != 0 { bail!("DuckDB reader for {table_ref} counted {n} rows but sent none"); } return Ok(0); }
        None => bail!("DuckDB reader for {table_ref} stopped"),
    };
    let start = || format!("starting bulk insert into {table_ref}");
    let mut req = client.bulk_insert_with_options(table_ref, col_refs, opts, hints).await.with_context(start)?;
    let mut sent = 0i64;
    let mut in_req = 0usize;
    let mut next = Some(first);
    loop {
        let rows = match next.take() {
            Some(r) => r,
            None => match rx.recv().await {
                Some(Msg::Rows(r)) => r,
                Some(Msg::Done(n)) => {
                    if n != sent { bail!("DuckDB reader for {table_ref} counted {n} rows but {sent} arrived"); }
                    break;
                }
                // the reader failed: do not commit the open batch
                None => bail!("DuckDB reader for {table_ref} stopped"),
            },
        };
        for row in rows {
            if in_req >= batch_rows {
                req.finalize().await.with_context(|| format!("committing a bulk batch into {table_ref} (rows up to {sent})"))?;
                progress(sent);
                req = client.bulk_insert_with_options(table_ref, col_refs, opts, hints).await.with_context(start)?;
                in_req = 0;
            }
            let row = if fixups.is_empty() { row } else { fix_row(row, fixups) };
            req.send(row).await.with_context(|| format!("bulk insert into {table_ref}, row {}", sent + 1))?;
            sent += 1;
            in_req += 1;
        }
    }
    req.finalize().await.with_context(|| format!("committing the last bulk batch into {table_ref}"))?;
    progress(sent);
    Ok(sent)
}

/// Checks the target columns against tiberius' bulk path and returns the columns whose values
/// need a wire fix-up. tiberius 0.13 declares the `INSERT BULK` column types from the server
/// metadata but writes `time` / `datetimeoffset` without their scale and a nullable
/// `smalldatetime` (TDS DATETIMEN(4)) as `datetime`:
/// * `time(n)` / `datetimeoffset(n)` with n < 7 cannot be bulk loaded (the server rejects the
///   declared type): refused here with a clear message;
/// * `nvarchar(n)` / `nchar(n)` are declared with the byte length, so n in 2001..=4000 becomes
///   `nvarchar(max)` / an invalid `nchar`: refused;
/// * `xml` columns are not supported by tiberius' bulk encoder: refused;
/// * nullable `smalldatetime` takes `datetime` values: the minute-rounded value is sent as a
///   `datetime` (exact, the server stores it unchanged).
fn wire_fixups(meta: &[MetaDataColumn<'_>], table_ref: &str, columns: &[String], convs: &[Converter], keep_identity: bool) -> Result<Vec<usize>> {
    if meta.len() != convs.len() {
        bail!("{table_ref}: the server reports {} columns for the {} given", meta.len(), convs.len());
    }
    let mut fix = vec![];
    for (i, m) in meta.iter().enumerate() {
        let name = &columns[i];
        if m.base.is_identity() && !keep_identity {
            bail!("{table_ref}: column {name} is an identity column; leave it out of the column list or keep identity values");
        }
        if !m.base.is_updateable() && !m.base.is_identity() {
            bail!("{table_ref}: column {name} is computed or read-only and cannot be loaded");
        }
        if let TypeInfo::Xml { .. } = &m.base.ty {
            bail!("{table_ref}: column {name} is xml; the bulk path (tiberius 0.13) cannot load xml columns. Load into a staging table with nvarchar(max) and INSERT ... SELECT CAST(.. AS xml), or use --method insert");
        }
        if let TypeInfo::VarLenSized(ctx) = &m.base.ty {
            match ctx.r#type() {
                VarLenType::Timen | VarLenType::DatetimeOffsetn if ctx.len() != 7 => {
                    let t = if ctx.r#type() == VarLenType::Timen { "time" } else { "datetimeoffset" };
                    bail!("{table_ref}: column {name} is {t}({}); the bulk path (tiberius 0.13) only supports {t}(7). Load into a staging table with {t}(7) and INSERT ... SELECT, or use --method insert", ctx.len());
                }
                // nvarchar(n)/nchar(n) are declared with the byte length: nvarchar(2001..4000) becomes
                // nvarchar(max) and the server rejects it (error 4816)
                VarLenType::NVarchar | VarLenType::NChar if ctx.len() > 4000 && ctx.len() < 0xffff => {
                    let t = if ctx.r#type() == VarLenType::NVarchar { "nvarchar" } else { "nchar" };
                    bail!("{table_ref}: column {name} is {t}({}); the bulk path (tiberius 0.13) only supports {t}(1..2000) and nvarchar(max). Load into a staging table with nvarchar(max) and INSERT ... SELECT, or use --method insert", ctx.len() / 2);
                }
                VarLenType::Datetimen if ctx.len() == 4 && convs[i] == Converter::SmallDateTime => fix.push(i),
                _ => {}
            }
        }
    }
    Ok(fix)
}

fn fix_row(row: TokenRow<'static>, fix: &[usize]) -> TokenRow<'static> {
    let mut out = TokenRow::with_capacity(row.len());
    for (i, v) in row.into_iter().enumerate() {
        out.push(match v {
            ColumnData::SmallDateTime(s) if fix.contains(&i) => {
                ColumnData::DateTime(s.map(|s| tiberius::time::DateTime::new(s.days() as i32, s.seconds_fragments() as u32 * 60 * 300)))
            }
            v => v,
        });
    }
    out
}

fn describe_read_error(e: anyhow::Error, table_ref: &str, columns: &[String], convs: &[Converter]) -> anyhow::Error {
    if let Some(ce) = e.downcast_ref::<ConvertError>() {
        let name = columns.get(ce.column).map(String::as_str).unwrap_or("?");
        let ty = convs.get(ce.column).map(|c| c.to_string()).unwrap_or_default();
        return anyhow!("{table_ref}: column {name} ({ty}), source row {}: {}", ce.row, ce.message);
    }
    if e.downcast_ref::<ReceiverGone>().is_some() { return e; }
    e.context(format!("reading the DuckDB source for {table_ref}"))
}

fn quote_ident(c: &str) -> String {
    if c.len() >= 2 && c.starts_with('[') && c.ends_with(']') { c.to_string() } else { super::bracket(c) }
}

/// `a, [b c] DESC` -> [(index in columns, descending)].
fn parse_order_by(spec: Option<&str>, columns: &[String]) -> Result<Vec<(usize, bool)>> {
    let Some(spec) = spec.map(str::trim).filter(|s| !s.is_empty()) else { return Ok(vec![]) };
    let mut items = vec![];
    let mut cur = String::new();
    let mut in_br = false;
    for ch in spec.chars() {
        match ch {
            '[' => { in_br = true; cur.push(ch); }
            ']' => { in_br = false; cur.push(ch); }
            ',' if !in_br => items.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    items.push(cur);
    let unquote = |s: &str| -> String {
        let s = s.trim();
        if s.len() >= 2 && s.starts_with('[') && s.ends_with(']') { s[1..s.len() - 1].replace("]]", "]") }
        else if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') { s[1..s.len() - 1].replace("\"\"", "\"") }
        else { s.to_string() }
    };
    let mut out = vec![];
    for it in items {
        let it = it.trim();
        let upper = it.to_ascii_uppercase();
        let (name, desc) = if upper.ends_with(" DESC") { (&it[..it.len() - 5], true) } else if upper.ends_with(" ASC") { (&it[..it.len() - 4], false) } else { (it, false) };
        let name = unquote(name);
        let idx = columns.iter().position(|c| unquote(c).eq_ignore_ascii_case(&name))
            .ok_or_else(|| anyhow!("--order-by column {name:?} is not one of the loaded columns"))?;
        out.push((idx, desc));
    }
    Ok(out)
}

// ------------------------------------------------------------------------------------------
// reader

/// One result column of a chunk, downcast once.
enum Col<'a> {
    Bool(&'a BooleanArray),
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
    F32(&'a [f32]),
    F64(&'a [f64]),
    /// mantissas with their scale; `Decimal128(38,0)` is also HUGEINT (same value either way)
    Dec32(&'a [i32], u8),
    Dec64(&'a [i64], u8),
    Dec128(&'a [i128], u8),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Bin(&'a BinaryArray),
    LargeBin(&'a LargeBinaryArray),
    BinView(&'a BinaryViewArray),
    FixedBin(&'a FixedSizeBinaryArray),
    Date32(&'a [i32]),
    Time64(&'a [i64], TimeUnit),
    Ts(&'a [i64], TimeUnit),
    Interval(&'a IntervalMonthDayNanoArray),
    Null,
}

/// Arrow types with a typed fast path. Anything else (LIST, STRUCT, MAP, ENUM dictionaries, UNION)
/// makes the whole query use the row-based path.
fn has_fast_path(dt: &DataType) -> bool {
    matches!(dt,
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float32 | DataType::Float64
        | DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _)
        | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        | DataType::FixedSizeBinary(_) | DataType::Date32 | DataType::Time64(_) | DataType::Timestamp(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano))
        && !matches!(dt, DataType::Decimal32(_, s) | DataType::Decimal64(_, s) | DataType::Decimal128(_, s) if *s < 0)
}

macro_rules! down {
    ($a:expr, $t:ty) => {
        $a.as_any().downcast_ref::<$t>().ok_or_else(|| anyhow!("unexpected Arrow array for {}", $a.data_type()))?
    };
}

fn unit(u: &ArrowUnit) -> TimeUnit {
    match u {
        ArrowUnit::Second => TimeUnit::Second,
        ArrowUnit::Millisecond => TimeUnit::Millisecond,
        ArrowUnit::Microsecond => TimeUnit::Microsecond,
        ArrowUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn col_of(a: &ArrayRef) -> Result<Col<'_>> {
    Ok(match a.data_type() {
        DataType::Null => Col::Null,
        DataType::Boolean => Col::Bool(down!(a, BooleanArray)),
        DataType::Int8 => Col::I8(down!(a, Int8Array).values()),
        DataType::Int16 => Col::I16(down!(a, Int16Array).values()),
        DataType::Int32 => Col::I32(down!(a, Int32Array).values()),
        DataType::Int64 => Col::I64(down!(a, Int64Array).values()),
        DataType::UInt8 => Col::U8(down!(a, UInt8Array).values()),
        DataType::UInt16 => Col::U16(down!(a, UInt16Array).values()),
        DataType::UInt32 => Col::U32(down!(a, UInt32Array).values()),
        DataType::UInt64 => Col::U64(down!(a, UInt64Array).values()),
        DataType::Float32 => Col::F32(down!(a, Float32Array).values()),
        DataType::Float64 => Col::F64(down!(a, Float64Array).values()),
        DataType::Decimal32(_, s) => Col::Dec32(down!(a, Decimal32Array).values(), *s as u8),
        DataType::Decimal64(_, s) => Col::Dec64(down!(a, Decimal64Array).values(), *s as u8),
        DataType::Decimal128(_, s) => Col::Dec128(down!(a, Decimal128Array).values(), *s as u8),
        DataType::Utf8 => Col::Utf8(down!(a, StringArray)),
        DataType::LargeUtf8 => Col::LargeUtf8(down!(a, LargeStringArray)),
        DataType::Utf8View => Col::Utf8View(down!(a, StringViewArray)),
        DataType::Binary => Col::Bin(down!(a, BinaryArray)),
        DataType::LargeBinary => Col::LargeBin(down!(a, LargeBinaryArray)),
        DataType::BinaryView => Col::BinView(down!(a, BinaryViewArray)),
        DataType::FixedSizeBinary(_) => Col::FixedBin(down!(a, FixedSizeBinaryArray)),
        DataType::Date32 => Col::Date32(down!(a, Date32Array).values()),
        DataType::Time64(ArrowUnit::Microsecond) => Col::Time64(down!(a, Time64MicrosecondArray).values(), TimeUnit::Microsecond),
        DataType::Time64(ArrowUnit::Nanosecond) => Col::Time64(down!(a, Time64NanosecondArray).values(), TimeUnit::Nanosecond),
        DataType::Timestamp(u, _) => Col::Ts(match u {
            ArrowUnit::Second => down!(a, TimestampSecondArray).values(),
            ArrowUnit::Millisecond => down!(a, TimestampMillisecondArray).values(),
            ArrowUnit::Microsecond => down!(a, TimestampMicrosecondArray).values(),
            ArrowUnit::Nanosecond => down!(a, TimestampNanosecondArray).values(),
        }, unit(u)),
        DataType::Interval(IntervalUnit::MonthDayNano) => Col::Interval(down!(a, IntervalMonthDayNanoArray)),
        other => bail!("no columnar conversion for Arrow type {other}"),
    })
}

impl<'a> Col<'a> {
    #[inline(always)]
    fn value(&self, r: usize) -> ValueRef<'a> {
        match self {
            Col::Bool(a) => ValueRef::Boolean(a.value(r)),
            Col::I8(v) => ValueRef::TinyInt(v[r]),
            Col::I16(v) => ValueRef::SmallInt(v[r]),
            Col::I32(v) => ValueRef::Int(v[r]),
            Col::I64(v) => ValueRef::BigInt(v[r]),
            Col::U8(v) => ValueRef::UTinyInt(v[r]),
            Col::U16(v) => ValueRef::USmallInt(v[r]),
            Col::U32(v) => ValueRef::UInt(v[r]),
            Col::U64(v) => ValueRef::UBigInt(v[r]),
            Col::F32(v) => ValueRef::Float(v[r]),
            Col::F64(v) => ValueRef::Double(v[r]),
            Col::Dec32(..) | Col::Dec64(..) | Col::Dec128(..) => unreachable!("decimals use convert_mantissa"),
            Col::Utf8(a) => ValueRef::Text(a.value(r).as_bytes()),
            Col::LargeUtf8(a) => ValueRef::Text(a.value(r).as_bytes()),
            Col::Utf8View(a) => ValueRef::Text(a.value(r).as_bytes()),
            Col::Bin(a) => ValueRef::Blob(a.value(r)),
            Col::LargeBin(a) => ValueRef::Blob(a.value(r)),
            Col::BinView(a) => ValueRef::Blob(a.value(r)),
            Col::FixedBin(a) => ValueRef::Blob(a.value(r)),
            Col::Date32(v) => ValueRef::Date32(v[r]),
            Col::Time64(v, u) => ValueRef::Time64(*u, v[r]),
            Col::Ts(v, u) => ValueRef::Timestamp(*u, v[r]),
            Col::Interval(a) => { let x = a.value(r); ValueRef::Interval { months: x.months, days: x.days, nanos: x.nanoseconds } }
            Col::Null => ValueRef::Null,
        }
    }
}

#[inline(always)]
fn is_null(n: Option<&NullBuffer>, r: usize) -> bool {
    match n { Some(n) => n.is_null(r), None => false }
}

/// Convert one Arrow column of a chunk into `rows[..n]` (row r of the chunk -> rows[r]).
fn convert_column(a: &ArrayRef, conv: &Converter, rows: &mut [TokenRow<'static>], c: usize, first_row: i64) -> Result<()> {
    let col = col_of(a)?;
    let nulls = a.logical_nulls();
    let nulls = nulls.as_ref();
    let err = |r: usize, e: anyhow::Error| anyhow::Error::new(ConvertError { column: c, row: first_row + r as i64 + 1, message: format!("{e:#}") });
    macro_rules! mantissa_loop {
        ($v:expr, $s:expr) => {{
            for (r, row) in rows.iter_mut().enumerate() {
                let d = if is_null(nulls, r) { conv.null() } else { conv.convert_mantissa($v[r] as i128, $s).map_err(|e| err(r, e))? };
                row.push(d);
            }
        }};
    }
    macro_rules! str_loop {
        ($a:expr) => {{
            for (r, row) in rows.iter_mut().enumerate() {
                let d = if is_null(nulls, r) { conv.null() } else { conv.convert_str($a.value(r)).map_err(|e| err(r, e))? };
                row.push(d);
            }
        }};
    }
    match col {
        Col::Dec32(v, s) => mantissa_loop!(v, s),
        Col::Dec64(v, s) => mantissa_loop!(v, s),
        Col::Dec128(v, s) => mantissa_loop!(v, s),
        Col::Null => for row in rows.iter_mut() { row.push(conv.null()); },
        Col::Utf8(a) => str_loop!(a),
        Col::LargeUtf8(a) => str_loop!(a),
        Col::Utf8View(a) => str_loop!(a),
        col => {
            for (r, row) in rows.iter_mut().enumerate() {
                let v = if is_null(nulls, r) { ValueRef::Null } else { col.value(r) };
                row.push(conv.convert(v).map_err(|e| err(r, e))?);
            }
        }
    }
    Ok(())
}

/// Run `sql` as a streaming query on `con`, convert every row with `convs` (one per result column)
/// and pass the rows to `sink` in batches of `batch_rows` (the last one may be smaller). Columnar
/// over Arrow chunks when every result type has a fast path, row by row otherwise. Conversion
/// errors are [`ConvertError`]s. Returns the number of rows read.
pub(crate) fn read_rows(con: &Connection, sql: &str, convs: &[Converter], batch_rows: usize, sink: &mut dyn FnMut(Vec<TokenRow<'static>>) -> Result<()>) -> Result<i64> {
    let batch_rows = batch_rows.max(1);
    let mut stmt = con.prepare(sql).with_context(|| format!("preparing {}", sql.chars().take(300).collect::<String>()))?;
    let schema = stmt.stream_arrow([]).with_context(|| format!("running {}", sql.chars().take(300).collect::<String>()))?.get_schema();
    let fields = schema.fields();
    if fields.len() != convs.len() {
        bail!("the source query returns {} columns but {} converters were given", fields.len(), convs.len());
    }
    let ncols = convs.len();
    let mut total = 0i64;
    let mut pending: Vec<TokenRow<'static>> = Vec::with_capacity(batch_rows.min(MAX_CHUNK_ROWS) + 2048);
    if fields.iter().all(|f| has_fast_path(f.data_type())) {
        while let Some(chunk) = stmt.step()? {
            let n = chunk.len();
            if n == 0 { continue; }
            let base = pending.len();
            pending.extend((0..n).map(|_| TokenRow::with_capacity(ncols)));
            for (c, conv) in convs.iter().enumerate() {
                convert_column(chunk.column(c), conv, &mut pending[base..], c, total)?;
            }
            total += n as i64;
            while pending.len() >= batch_rows {
                let rest = pending.split_off(batch_rows);
                sink(std::mem::replace(&mut pending, rest))?;
            }
        }
    } else {
        let mut rows = stmt.raw_query();
        while let Some(row) = rows.next()? {
            let mut tr = TokenRow::with_capacity(ncols);
            for (c, conv) in convs.iter().enumerate() {
                let v = conv.convert(row.get_ref(c)?).map_err(|e| anyhow::Error::new(ConvertError { column: c, row: total + 1, message: format!("{e:#}") }))?;
                tr.push(v);
            }
            pending.push(tr);
            total += 1;
            if pending.len() >= batch_rows { sink(std::mem::take(&mut pending))?; }
        }
    }
    if !pending.is_empty() { sink(pending)?; }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mixed_table(con: &Connection, n: i64) {
        con.execute_batch(&format!(
            "CREATE TABLE t AS SELECT i AS id,
                CASE WHEN i % 7 = 0 THEN NULL ELSE 'name_' || i END AS name,
                CAST((i % 1000) * 0.01 AS DECIMAL(8,5)) AS amount,
                DATE '2024-01-01' + CAST(i % 366 AS INTEGER) AS d,
                TIMESTAMP '2024-01-01 00:00:00' + to_microseconds(i * 1001) AS ts,
                i % 2 = 0 AS flag,
                CAST(i AS DOUBLE) / 4 AS ratio,
                CAST(i % 200 AS UTINYINT) AS small,
                CAST(i AS HUGEINT) * 1000000000000 AS big
             FROM range({n}) r(i)"
        )).unwrap();
    }

    fn convs() -> Vec<Converter> {
        let f = |t: &str| Converter::for_sql_type(t, "").unwrap();
        vec![f("BIGINT"), f("NVARCHAR(40)"), f("DECIMAL(10,5)"), f("DATE"), f("DATETIME2(7)"), f("BIT"), f("FLOAT(53)"), f("TINYINT"), f("DECIMAL(38,0)")]
    }

    #[test]
    fn bulk_read_rows_mixed_types() {
        let con = Connection::open_in_memory().unwrap();
        mixed_table(&con, 10_000);
        let mut batches = 0;
        let mut seen = 0usize;
        let mut sample: Vec<TokenRow<'static>> = vec![];
        let n = read_rows(&con, "SELECT * FROM t ORDER BY id", &convs(), 3_000, &mut |rows| {
            assert!(rows.len() <= 3_000);
            for r in &rows { assert_eq!(r.len(), 9); }
            if seen == 0 { sample.extend(rows.iter().take(10).cloned()); }
            if seen == 9_000 { sample.extend(rows.iter().cloned()); }
            seen += rows.len();
            batches += 1;
            Ok(())
        }).unwrap();
        assert_eq!(n, 10_000);
        assert_eq!(seen, 10_000);
        assert_eq!(batches, 4);
        // row 0: id 0, name NULL (0 % 7 == 0)
        let r0 = &sample[0];
        assert!(matches!(r0.get(0), Some(ColumnData::I64(Some(0)))));
        assert!(matches!(r0.get(1), Some(ColumnData::String(None))));
        assert!(matches!(r0.get(5), Some(ColumnData::Bit(Some(true)))));
        // row 3
        let r3 = &sample[3];
        match r3.get(1) { Some(ColumnData::String(Some(s))) => assert_eq!(s, "name_3"), o => panic!("{o:?}") }
        match r3.get(2) { Some(ColumnData::Numeric(Some(x))) => { assert_eq!(x.scale(), 5); assert_eq!(x.value(), 3_000) } o => panic!("{o:?}") } // 0.03
        match r3.get(3) { Some(ColumnData::Date(Some(d))) => assert_eq!(d.days(), 738_885 + 3), o => panic!("{o:?}") }
        match r3.get(4) { Some(ColumnData::DateTime2(Some(d))) => { assert_eq!(d.time().increments(), 3 * 1001 * 10); assert_eq!(d.time().scale(), 7) } o => panic!("{o:?}") }
        assert!(matches!(r3.get(5), Some(ColumnData::Bit(Some(false)))));
        assert!(matches!(r3.get(6), Some(ColumnData::F64(Some(x))) if *x == 0.75));
        assert!(matches!(r3.get(7), Some(ColumnData::U8(Some(3)))));
        match r3.get(8) { Some(ColumnData::Numeric(Some(x))) => { assert_eq!(x.value(), 3_000_000_000_000); assert_eq!(x.scale(), 0) } o => panic!("{o:?}") }
        // last batch, row 9999
        let last = sample.last().unwrap();
        assert!(matches!(last.get(0), Some(ColumnData::I64(Some(9_999)))));
        match last.get(1) { Some(ColumnData::String(Some(s))) => assert_eq!(s, "name_9999"), o => panic!("{o:?}") }
    }

    #[test]
    fn bulk_read_rows_reports_column_and_row() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch("CREATE TABLE t AS SELECT i AS id, CAST(i AS INTEGER) AS v FROM range(5000) r(i)").unwrap();
        let cv = vec![Converter::BigInt, Converter::TinyInt];
        let e = read_rows(&con, "SELECT id, v FROM t ORDER BY id", &cv, 1000, &mut |_| Ok(())).unwrap_err();
        let ce = e.downcast_ref::<ConvertError>().expect("a ConvertError");
        assert_eq!((ce.column, ce.row), (1, 257));
        assert!(ce.message.contains("256"), "{}", ce.message);
        let msg = describe_read_error(e, "[dbo].[t]", &["id".into(), "v".into()], &cv).to_string();
        assert!(msg.contains("[dbo].[t]") && msg.contains("column v") && msg.contains("row 257"), "{msg}");
    }

    #[test]
    fn bulk_read_rows_row_fallback_and_sink_errors() {
        let con = Connection::open_in_memory().unwrap();
        // LIST has no columnar fast path: the row path converts it as text
        let cv = vec![Converter::Int, Converter::Text];
        let mut got = vec![];
        let n = read_rows(&con, "SELECT i::INTEGER, [i, i + 1] FROM range(3) r(i)", &cv, 2, &mut |rows| { got.extend(rows); Ok(()) }).unwrap();
        assert_eq!(n, 3);
        assert_eq!(got.len(), 3);
        assert!(matches!(got[2].get(0), Some(ColumnData::I32(Some(2)))));
        // a failing sink stops the reader
        let e = read_rows(&con, "SELECT i::INTEGER, 'x' FROM range(10000) r(i)", &cv, 100, &mut |_| bail!("sink full")).unwrap_err();
        assert!(e.to_string().contains("sink full"));
        // column count mismatch
        assert!(read_rows(&con, "SELECT 1", &cv, 10, &mut |_| Ok(())).is_err());
    }

    #[test]
    fn bulk_order_by_parsing() {
        let cols: Vec<String> = vec!["Id".into(), "Mkt Code".into(), "[x]".into()];
        assert_eq!(parse_order_by(None, &cols).unwrap(), vec![]);
        assert_eq!(parse_order_by(Some("id, [Mkt Code] DESC"), &cols).unwrap(), vec![(0, false), (1, true)]);
        assert_eq!(parse_order_by(Some("x asc"), &cols).unwrap(), vec![(2, false)]);
        assert!(parse_order_by(Some("nope"), &cols).is_err());
        assert_eq!(quote_ident("a]b"), "[a]]b]");
        assert_eq!(quote_ident("[x]"), "[x]");
    }

    /// End-to-end against a real SQL Server (ignored; needs DUCKBRIDGE_TEST_MSSQL=host:port:user:password and
    /// an existing database `_agent_c`):
    /// `DUCKBRIDGE_TEST_MSSQL=localhost:14319:sa:... cargo test --release --lib -- --ignored bulk_live --nocapture`
    #[test]
    #[ignore]
    fn bulk_live_sql_server() {
        let Ok(spec) = std::env::var("DUCKBRIDGE_TEST_MSSQL") else { eprintln!("DUCKBRIDGE_TEST_MSSQL not set; skipped"); return };
        let mut it = spec.splitn(4, ':');
        let (host, port, user, password) = (it.next().unwrap().to_string(), it.next().unwrap().parse().unwrap(), it.next().unwrap().to_string(), it.next().unwrap().to_string());
        let target = crate::mssql::MssqlTarget { host, port, user, password, database: "_agent_c".into(), trust_cert: true, encrypt: false, windows_auth: false };
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let con = Connection::open_in_memory().unwrap();
        let n = 25_000;
        con.execute_batch(&format!(
            "CREATE TABLE src AS SELECT (i + 1)::INTEGER AS id,
                CASE WHEN i % 11 = 0 THEN NULL ELSE 'v' || i END AS v,
                CASE WHEN i % 13 = 0 THEN NULL ELSE 'n\u{e9}\u{4e2d}' || i END AS nv,
                chr(65 + (i % 26)::INTEGER) || 'BC' AS c3,
                CAST((i % 100000) * 0.00001 AS DECIMAL(8,5)) AS amt,
                CAST(i * 1.1234567 AS DECIMAL(18,7)) AS big,
                CAST(i % 1000 AS DECIMAL(10,2)) / 4 AS money_v,
                DATE '1990-01-01' + (i % 20000)::INTEGER AS d,
                TIMESTAMP '1990-01-01' + to_microseconds(i::BIGINT * 3600123457) AS ts,
                i % 3 = 0 AS flag,
                (i % 256)::UTINYINT AS tiny,
                uuid() AS g,
                CASE WHEN i % 5 = 0 THEN NULL ELSE encode('b' || i) END AS bin,
                i / 3.0 AS f
             FROM range({n}) r(i)")).unwrap();
        // DuckDB-side reference values
        let (sum_amt, sum_big, nulls_nv, sum_len_nv): (String, String, i64, i64) = con.query_row(
            "SELECT CAST(sum(amt) AS VARCHAR), CAST(sum(big) AS VARCHAR), count(*) - count(nv), sum(length(nv)) FROM src", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap();
        rt.block_on(async {
            let mut c = target.connect("_agent_c").await.unwrap();
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_bulk;
                CREATE TABLE dbo.c_bulk (id INT IDENTITY(1,1) PRIMARY KEY, v VARCHAR(10) NULL, nv NVARCHAR(12) NULL, c3 CHAR(3) NOT NULL,
                  amt DECIMAL(8,5) NOT NULL, big DECIMAL(20,9) NOT NULL, money_v MONEY NULL, d DATE NOT NULL,
                  dt2 DATETIME2(7) NOT NULL, dt23 DATETIME2(3) NOT NULL, dtm DATETIME NOT NULL, sdt SMALLDATETIME NULL, t7 TIME(7) NOT NULL, dto DATETIMEOFFSET(7) NOT NULL,
                  flag BIT NOT NULL, tiny TINYINT NOT NULL, g UNIQUEIDENTIFIER NOT NULL, bin VARBINARY(16) NULL, f FLOAT NOT NULL)").await.unwrap();
            let cols: Vec<String> = ["id", "v", "nv", "c3", "amt", "big", "money_v", "d", "dt2", "dt23", "dtm", "sdt", "t7", "dto", "flag", "tiny", "g", "bin", "f"].iter().map(|s| s.to_string()).collect();
            let ty = |t: &str| Converter::for_sql_type(t, "").unwrap();
            let convs = vec![ty("INT"), ty("VARCHAR(10)"), ty("NVARCHAR(12)"), ty("CHAR(3)"), ty("DECIMAL(8,5)"), ty("DECIMAL(20,9)"), ty("MONEY"), ty("DATE"),
                             ty("DATETIME2(7)"), ty("DATETIME2(3)"), ty("DATETIME"), ty("SMALLDATETIME"), ty("TIME(7)"), ty("DATETIMEOFFSET(7)"),
                             ty("BIT"), ty("TINYINT"), ty("UNIQUEIDENTIFIER"), ty("VARBINARY(16)"), ty("FLOAT(53)")];
            let sql = "SELECT id, v, nv, c3, amt, big, money_v, d, ts, ts, ts, CASE WHEN ts < TIMESTAMP '2079-06-06' THEN ts END, ts::TIME, ts::TIMESTAMPTZ, flag, tiny, g::VARCHAR, bin, f FROM src";
            let o = BulkOptions { batch_rows: 4_000, keep_identity: true, table_lock: true, prefetch_batches: 4, order_by: Some("id".into()) };
            let calls = std::cell::Cell::new(0);
            let t0 = Instant::now();
            let sent = bulk_load(&con, &mut c, "[dbo].[c_bulk]", &cols, &convs, sql, &o, &|_| calls.set(calls.get() + 1)).await.unwrap();
            println!("bulk_live: {sent} rows in {:.2}s", t0.elapsed().as_secs_f64());
            assert_eq!(sent, n);
            assert!(calls.get() >= 6, "progress calls {}", calls.get());
            async fn q(c: &mut MsClient, sql: &str) -> String {
                let rows = c.simple_query(sql).await.unwrap().into_first_result().await.unwrap();
                rows.first().and_then(|r| r.get::<&str, _>(0)).unwrap_or("").to_string()
            }
            assert_eq!(q(&mut c, "SELECT CAST(COUNT(*) AS NVARCHAR(20)) FROM dbo.c_bulk").await, n.to_string());
            let canon = |s: String| s.trim_end_matches('0').trim_end_matches('.').to_string();
            assert_eq!(canon(q(&mut c, "SELECT CAST(SUM(amt) AS NVARCHAR(60)) FROM dbo.c_bulk").await), canon(sum_amt));
            assert_eq!(canon(q(&mut c, "SELECT CAST(SUM(big) AS NVARCHAR(60)) FROM dbo.c_bulk").await), canon(sum_big));
            assert_eq!(q(&mut c, "SELECT CAST(SUM(CASE WHEN nv IS NULL THEN 1 ELSE 0 END) AS NVARCHAR(20)) FROM dbo.c_bulk").await, nulls_nv.to_string());
            assert_eq!(q(&mut c, "SELECT CAST(SUM(CAST(LEN(nv) AS BIGINT)) AS NVARCHAR(20)) FROM dbo.c_bulk").await, sum_len_nv.to_string());
            // our rounding == SQL Server's own datetime2(7) -> datetime / smalldatetime / datetime2(3) / time(3) conversions
            assert_eq!(q(&mut c, "SELECT CAST(COUNT(*) AS NVARCHAR(20)) FROM dbo.c_bulk WHERE dtm <> CAST(dt2 AS DATETIME) OR dt23 <> CAST(dt2 AS DATETIME2(3))
                                 OR t7 <> CAST(dt2 AS TIME(7)) OR (sdt IS NOT NULL AND sdt <> CAST(dt2 AS SMALLDATETIME)) OR CAST(dto AS DATETIME2(7)) <> dt2
                                 OR DATEPART(TZOFFSET, dto) <> 0").await, "0");
            assert_eq!(q(&mut c, "SELECT CAST(COUNT(*) AS NVARCHAR(20)) FROM dbo.c_bulk WHERE sdt IS NULL").await,
                       con.query_row("SELECT count(*) FROM src WHERE ts >= TIMESTAMP '2079-06-06'", [], |r| r.get::<_, i64>(0)).unwrap().to_string());
            // one row, every column as text
            let id = 12_345;
            type Row7 = (Option<String>, Option<String>, String, String, String, String, String);
            let (v, nv, c3, amt, d, ts, g): Row7 = con.query_row(
                &format!("SELECT v, nv, c3, CAST(amt AS VARCHAR), CAST(d AS VARCHAR), strftime(ts, '%Y-%m-%d %H:%M:%S.%f'), g::VARCHAR FROM src WHERE id = {id}"), [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))).unwrap();
            let got = q(&mut c, &format!("SELECT CONCAT_WS('|', ISNULL(v, '~'), ISNULL(nv, N'~'), c3, CAST(amt AS NVARCHAR(20)), CONVERT(NVARCHAR(10), d, 23),
                CONVERT(NVARCHAR(27), dt2, 121), LOWER(CAST(g AS NVARCHAR(36))), CAST(tiny AS NVARCHAR(3)), CAST(flag AS NVARCHAR(1))) FROM dbo.c_bulk WHERE id = {id}")).await;
            let i = (id - 1) as i64;
            let want = format!("{}|{}|{c3}|{amt}|{d}|{ts}0|{g}|{}|{}", v.unwrap_or("~".into()), nv.unwrap_or("~".into()), i % 256, if i % 3 == 0 { 1 } else { 0 });
            assert_eq!(got, want);
            assert_eq!(q(&mut c, "SELECT CAST(COUNT(*) AS NVARCHAR(20)) FROM dbo.c_bulk WHERE f <> CAST(id - 1 AS FLOAT) / 3.0 OR (bin IS NOT NULL AND bin <> CAST('b' + CAST(id - 1 AS VARCHAR(10)) AS VARBINARY(16)))").await, "0");
            assert_eq!(q(&mut c, "SELECT CAST(SUM(CAST(money_v AS DECIMAL(19,4))) AS NVARCHAR(40)) FROM dbo.c_bulk").await,
                       con.query_row("SELECT CAST(CAST(sum(money_v) AS DECIMAL(19,4)) AS VARCHAR) FROM src", [], |r| r.get::<_, String>(0)).unwrap());

            // a conversion error: names table, column and row; the open batch is not committed
            con.execute_batch("CREATE TABLE bad AS SELECT i::INTEGER AS id, CASE WHEN i = 3000 THEN 'caf\u{e9}' ELSE 'ok' END AS v FROM range(5000) r(i)").unwrap();
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_bad; CREATE TABLE dbo.c_bad (id INT NOT NULL, v VARCHAR(10) NOT NULL)").await.unwrap();
            let mut c2 = target.connect("_agent_c").await.unwrap();
            let o = BulkOptions { batch_rows: 1_000, keep_identity: false, table_lock: false, prefetch_batches: 2, order_by: None };
            let e = bulk_load(&con, &mut c2, "dbo.c_bad", &["id".into(), "v".into()], &[Converter::Int, ty("VARCHAR(10)")], "SELECT id, v FROM bad ORDER BY id", &o, &|_| {}).await.unwrap_err().to_string();
            assert!(e.contains("dbo.c_bad") && e.contains("column v") && e.contains("row 3001") && e.contains("non-ASCII"), "{e}");
            drop(c2);
            let committed: i64 = q(&mut c, "SELECT CAST(COUNT(*) AS NVARCHAR(20)) FROM dbo.c_bad").await.parse().unwrap();
            assert_eq!(committed % 1000, 0, "only whole batches are committed ({committed})");
            assert!(committed < 3000);
            // empty source: nothing sent, no error
            let mut c3c = target.connect("_agent_c").await.unwrap();
            assert_eq!(bulk_load(&con, &mut c3c, "dbo.c_bad", &["id".into(), "v".into()], &[Converter::Int, ty("VARCHAR(10)")], "SELECT id, v FROM bad WHERE false", &o, &|_| {}).await.unwrap(), 0);
            // more target types: long and MAX strings/binaries, NOT NULL smalldatetime, xml, ntext, money limits
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_types; CREATE TABLE dbo.c_types (a NVARCHAR(2000) NULL, b VARCHAR(MAX) NULL,
                c NVARCHAR(MAX) NULL, d VARBINARY(MAX) NULL, e CHAR(10) NOT NULL, f NCHAR(4) NULL, g DECIMAL(38,10) NULL, h SMALLDATETIME NOT NULL,
                i DATETIME NULL, j REAL NULL, k SMALLINT NULL, l BIGINT NULL, m SMALLMONEY NULL, n NUMERIC(5,0) NULL, o BINARY(4) NULL, p NVARCHAR(MAX) NULL, q NTEXT NULL,
                r DATETIME2(0) NULL)").await.unwrap();
            con.execute_batch("CREATE TABLE ty AS SELECT repeat('\u{e9}', 1500 + i::INTEGER) AS a, repeat('x', 10000) AS b, repeat('\u{4e2d}', 5000) AS c,
                encode(repeat('z', 9000)) AS d, 'ab' AS e, 'n\u{e9}' AS f, CAST('1234567890123456789012345678.0123456789' AS DECIMAL(38,10)) AS g,
                TIMESTAMP '2001-02-03 04:05:29.999' AS h, TIMESTAMP '1753-01-01 00:00:00.001' AS i, 1.5::FLOAT AS j, (-i)::SMALLINT AS k,
                9007199254740993::BIGINT AS l, 214748.3647::DECIMAL(10,4) AS m, 99999 AS n, '\x01\x02'::BLOB AS o, '<r a=\"1\"/>' AS p, 'nt\u{e9}' AS q,
                TIMESTAMP '2020-12-31 23:59:59.5' AS r
                FROM range(100) r(i)").unwrap();
            let tcols: Vec<String> = "a b c d e f g h i j k l m n o p q r".split(' ').map(String::from).collect();
            let tconv: Vec<Converter> = ["NVARCHAR(2000)", "VARCHAR(MAX)", "NVARCHAR(MAX)", "VARBINARY(MAX)", "CHAR(10)", "NCHAR(4)", "DECIMAL(38,10)", "SMALLDATETIME",
                "DATETIME", "REAL", "SMALLINT", "BIGINT", "SMALLMONEY", "NUMERIC(5,0)", "BINARY(4)", "NVARCHAR(MAX)", "NTEXT", "DATETIME2(0)"].iter().map(|t| ty(t)).collect();
            let o = BulkOptions { batch_rows: 30, ..BulkOptions::default() };
            assert_eq!(bulk_load(&con, &mut c, "dbo.c_types", &tcols, &tconv, "SELECT * FROM ty", &o, &|_| {}).await.unwrap(), 100);
            let got = q(&mut c, "SELECT CONCAT_WS('|', SUM(CAST(LEN(a) AS BIGINT)), MIN(LEN(b)), MIN(LEN(c)), MIN(DATALENGTH(d)), MIN(e + '#'), MIN(f), CAST(MIN(g) AS NVARCHAR(50)),
                CONVERT(NVARCHAR(30), MIN(h), 121), CONVERT(NVARCHAR(30), MIN(i), 121), MIN(j), MIN(k), MIN(l), CONVERT(NVARCHAR(20), MIN(m), 2), MIN(n), CONVERT(NVARCHAR(20), MIN(o), 1),
                MIN(p), MIN(CAST(q AS NVARCHAR(10))), CONVERT(NVARCHAR(30), MIN(r), 121),
                SUM(CASE WHEN h = CAST(CAST('2001-02-03 04:05:29.999' AS DATETIME2) AS SMALLDATETIME) THEN 1 ELSE 0 END)) FROM dbo.c_types").await;
            assert_eq!(got, format!("{}|10000|5000|9000|ab        #|n\u{e9}  |1234567890123456789012345678.0123456789|2001-02-03 04:05:00.000|1753-01-01 00:00:00.000|1.5|-99|9007199254740993|214748.3647|99999|0x01020000|<r a=\"1\"/>|nt\u{e9}|2021-01-01 00:00:00|100", 150_000 + 4950));
            // tiberius limitation: time(n < 7) is refused up front with a clear message
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_t3; CREATE TABLE dbo.c_t3 (t TIME(3) NULL)").await.unwrap();
            let e = bulk_load(&con, &mut c, "dbo.c_t3", &["t".into()], &[ty("TIME(3)")], "SELECT CAST(ts AS TIME) FROM src", &o, &|_| {}).await.unwrap_err().to_string();
            assert!(e.contains("time(3)") && e.contains("staging"), "{e}");
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_nv; CREATE TABLE dbo.c_nv (t NVARCHAR(3000) NULL)").await.unwrap();
            let e = bulk_load(&con, &mut c, "dbo.c_nv", &["t".into()], &[ty("NVARCHAR(3000)")], "SELECT v FROM src", &o, &|_| {}).await.unwrap_err().to_string();
            assert!(e.contains("nvarchar(3000)") && e.contains("staging"), "{e}");
            crate::mssql::exec_batch(&mut c, "DROP TABLE IF EXISTS dbo.c_xml; CREATE TABLE dbo.c_xml (t XML NULL)").await.unwrap();
            let e = bulk_load(&con, &mut c, "dbo.c_xml", &["t".into()], &[ty("XML")], "SELECT '<a/>'", &o, &|_| {}).await.unwrap_err().to_string();
            assert!(e.contains("xml") && e.contains("staging"), "{e}");
            // identity column without keep_identity is refused up front
            let e = bulk_load(&con, &mut c, "dbo.c_bulk", &["id".into()], &[ty("INT")], "SELECT id FROM src", &o, &|_| {}).await.unwrap_err().to_string();
            assert!(e.contains("identity"), "{e}");
            crate::mssql::exec_batch(&mut c, "DROP TABLE dbo.c_bulk; DROP TABLE dbo.c_bad; DROP TABLE dbo.c_types; DROP TABLE dbo.c_t3; DROP TABLE dbo.c_nv; DROP TABLE dbo.c_xml").await.unwrap();
        });
    }

    /// End-to-end rows/s for one bulk session (same env as `bulk_live_sql_server`; rows:
    /// DUCKBRIDGE_BENCH_ROWS, default 1 M): `cargo test --release --lib -- --ignored bulk_live_throughput --nocapture`
    #[test]
    #[ignore]
    fn bulk_live_throughput() {
        let Ok(spec) = std::env::var("DUCKBRIDGE_TEST_MSSQL") else { eprintln!("DUCKBRIDGE_TEST_MSSQL not set; skipped"); return };
        let mut it = spec.splitn(4, ':');
        let (host, port, user, password) = (it.next().unwrap().to_string(), it.next().unwrap().parse().unwrap(), it.next().unwrap().to_string(), it.next().unwrap().to_string());
        let target = crate::mssql::MssqlTarget { host, port, user, password, database: "_agent_c".into(), trust_cert: true, encrypt: false, windows_auth: false };
        let rows: i64 = std::env::var("DUCKBRIDGE_BENCH_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        let con = Connection::open_in_memory().unwrap();
        let convs = crate::mssql::convert::tests::bench_table(&con, rows);
        let names: Vec<String> = "v1 v2 v3 v4 v5 v6 c1 c2 i1 i2 i3 i4 i5 amt d t1 t2".split(' ').map(String::from).collect();
        let ddl: Vec<String> = names.iter().zip(crate::mssql::convert::tests::bench_types()).map(|(n, t)| format!("{n} {t} NULL")).collect();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut c = target.connect("_agent_c").await.unwrap();
            crate::mssql::exec_batch(&mut c, &format!("DROP TABLE IF EXISTS dbo.c_tp; CREATE TABLE dbo.c_tp ({})", ddl.join(", "))).await.unwrap();
            for batch_rows in [100_000usize, 1_000_000] {
                crate::mssql::exec_batch(&mut c, "TRUNCATE TABLE dbo.c_tp").await.unwrap();
                let o = BulkOptions { batch_rows, ..BulkOptions::default() };
                let t0 = Instant::now();
                let n = bulk_load(&con, &mut c, "dbo.c_tp", &names, &convs, "SELECT * FROM b", &o, &|_| {}).await.unwrap();
                let secs = t0.elapsed().as_secs_f64();
                println!("bulk_live_throughput: {n} rows (17 columns, heap, TABLOCK, batch {batch_rows}) in {secs:.2} s = {:.0} rows/s", n as f64 / secs);
            }
            crate::mssql::exec_batch(&mut c, "DROP TABLE dbo.c_tp").await.unwrap();
        });
    }
}
