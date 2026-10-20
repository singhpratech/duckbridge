//! Precompiled per-column converters: DuckDB value -> TDS `ColumnData` (spec R5.1, R5.2, R3.4).
//!
//! A [`Converter`] is built once per column from the target type (`sys.types` metadata in
//! `--into-existing` mode, the emitted SQL type text in create mode). [`Converter::convert`] is the
//! hot loop: one `match` on the converter, no string work for non-string targets, DECIMAL straight
//! from the DuckDB mantissa, every integer range-checked (never wrapped), and every value that SQL
//! Server would silently change (non-ASCII into `varchar`, over-long strings, extra decimal
//! digits) rejected with an error that names the value.
//!
//! `varchar`/`char` code pages: the converter does not know the column's code page, so it accepts
//! ASCII only and rejects anything else ("non-ASCII value for varchar column; SQL Server would
//! substitute '?'"). tiberius additionally encodes `varchar` data with the target collation's code
//! page and fails on unmappable characters, so nothing reaches the server as `?` either way.
use super::target::TargetColumn;
use anyhow::{bail, Result};
use crate::core::duck::{Decimal, TimeUnit, ValueRef};
use std::borrow::Cow;
use std::fmt;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::ColumnData;

/// How one column's DuckDB values become TDS values. Built once per column, applied per value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Converter {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Real,
    Float,
    /// Also `money` (19,4) and `smallmoney` (10,4).
    Decimal { precision: u8, scale: u8 },
    /// `char`/`varchar` (`unicode: false`, ASCII only) and `nchar`/`nvarchar` (`unicode: true`,
    /// length counted in UTF-16 code units). `max_chars: None` = MAX.
    Char { max_chars: Option<usize>, unicode: bool, fixed: bool },
    Date,
    Time { scale: u8 },
    /// `datetime`: 1/300 s resolution, 1753-01-01 ..= 9999-12-31.
    DateTime,
    /// `smalldatetime`: minute resolution, 1900-01-01 ..= 2079-06-06.
    SmallDateTime,
    DateTime2 { scale: u8 },
    DateTimeOffset { scale: u8 },
    Guid,
    /// `binary`/`varbinary`/`image`; `max_len: None` = MAX.
    Binary { max_len: Option<usize> },
    /// Fallback: the value as text (NVARCHAR(MAX)-compatible), formatted like DuckDB prints it.
    Text,
}

/// Days from 0001-01-01 to 1970-01-01.
const UNIX_DAYS_CE: i64 = 719_162;
/// Days from 1900-01-01 to 1970-01-01.
const UNIX_DAYS_1900: i64 = 25_567;
/// 9999-12-31 as days from 0001-01-01.
const MAX_DAYS_CE: i64 = 3_652_058;
/// `datetime` range as days from 1900-01-01: 1753-01-01 ..= 9999-12-31.
const DATETIME_MIN_1900: i64 = -53_690;
const DATETIME_MAX_1900: i64 = 2_958_463;
/// `smalldatetime` upper bound: 2079-06-06 as days from 1900-01-01.
const SMALLDATETIME_MAX_1900: i64 = 65_535;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;
/// 1/300 s fragments per day.
const FRAGMENTS_PER_DAY: i64 = 86_400 * 300;

const fn pow10_table() -> [i128; 39] {
    let mut t = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
}
static POW10: [i128; 39] = pow10_table();

#[inline]
fn pow10_i64(n: u8) -> i64 {
    POW10[n as usize] as i64
}

impl fmt::Display for Converter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Converter::Bit => write!(f, "bit"),
            Converter::TinyInt => write!(f, "tinyint"),
            Converter::SmallInt => write!(f, "smallint"),
            Converter::Int => write!(f, "int"),
            Converter::BigInt => write!(f, "bigint"),
            Converter::Real => write!(f, "real"),
            Converter::Float => write!(f, "float"),
            Converter::Decimal { precision, scale } => write!(f, "decimal({precision},{scale})"),
            Converter::Char { max_chars, unicode, fixed } => {
                let n = if *unicode { "n" } else { "" };
                let v = if *fixed { "" } else { "var" };
                match max_chars {
                    Some(m) => write!(f, "{n}{v}char({m})"),
                    None => write!(f, "{n}{v}char(max)"),
                }
            }
            Converter::Date => write!(f, "date"),
            Converter::Time { scale } => write!(f, "time({scale})"),
            Converter::DateTime => write!(f, "datetime"),
            Converter::SmallDateTime => write!(f, "smalldatetime"),
            Converter::DateTime2 { scale } => write!(f, "datetime2({scale})"),
            Converter::DateTimeOffset { scale } => write!(f, "datetimeoffset({scale})"),
            Converter::Guid => write!(f, "uniqueidentifier"),
            Converter::Binary { max_len: Some(m) } => write!(f, "varbinary({m})"),
            Converter::Binary { max_len: None } => write!(f, "varbinary(max)"),
            Converter::Text => write!(f, "nvarchar(max)"),
        }
    }
}

fn check_scale(scale: u8, what: &str) -> Result<u8> {
    if scale > 7 { bail!("{what} scale {scale} is out of range (0..=7)"); }
    Ok(scale)
}

fn decimal(precision: u8, scale: u8) -> Result<Converter> {
    if !(1..=38).contains(&precision) || scale > precision {
        bail!("invalid decimal({precision},{scale})");
    }
    Ok(Converter::Decimal { precision, scale })
}

impl Converter {
    /// Converter for an existing target column (`--into-existing`), from its `sys.types` metadata.
    /// `duck_type` is only used in error messages: what arrives is decided by the SELECT expression.
    pub fn for_target(col: &TargetColumn, duck_type: &str) -> Result<Converter> {
        let len = |bytes: i16, per_char: usize| -> Option<usize> { if bytes < 0 { None } else { Some(bytes as usize / per_char) } };
        let t = col.type_name.to_ascii_lowercase();
        Ok(match t.as_str() {
            "bit" => Converter::Bit,
            "tinyint" => Converter::TinyInt,
            "smallint" => Converter::SmallInt,
            "int" => Converter::Int,
            "bigint" => Converter::BigInt,
            "real" => Converter::Real,
            // float(1..24) is stored as a 4-byte real and reported as `float` with max_length 4
            "float" => if col.max_length == 4 { Converter::Real } else { Converter::Float },
            "decimal" | "numeric" => decimal(col.precision, col.scale).map_err(|e| anyhow::anyhow!("column {} (DuckDB {duck_type}): {e}", col.name))?,
            "money" => Converter::Decimal { precision: 19, scale: 4 },
            "smallmoney" => Converter::Decimal { precision: 10, scale: 4 },
            "char" => Converter::Char { max_chars: len(col.max_length, 1), unicode: false, fixed: true },
            "varchar" => Converter::Char { max_chars: len(col.max_length, 1), unicode: false, fixed: false },
            "nchar" => Converter::Char { max_chars: len(col.max_length, 2), unicode: true, fixed: true },
            "nvarchar" | "sysname" => Converter::Char { max_chars: len(col.max_length, 2), unicode: true, fixed: false },
            "text" | "ntext" => Converter::Text,
            "date" => Converter::Date,
            "time" => Converter::Time { scale: check_scale(col.scale, "time")? },
            "datetime" => Converter::DateTime,
            "smalldatetime" => Converter::SmallDateTime,
            "datetime2" => Converter::DateTime2 { scale: check_scale(col.scale, "datetime2")? },
            "datetimeoffset" => Converter::DateTimeOffset { scale: check_scale(col.scale, "datetimeoffset")? },
            "uniqueidentifier" => Converter::Guid,
            "binary" | "varbinary" => Converter::Binary { max_len: len(col.max_length, 1) },
            "image" => Converter::Binary { max_len: None },
            _ => Converter::Text, // xml, json, sql_variant, ... : text
        })
    }

    /// Converter for a create-mode column from its SQL Server type text (`core::model::Column.sql_type`),
    /// e.g. `NVARCHAR(50)`, `DECIMAL(18,3)`, `DATETIME2(7)`, `VARBINARY(MAX)`, `FLOAT(53)`, `INT`.
    pub fn for_sql_type(sql_type: &str, duck_type: &str) -> Result<Converter> {
        let s = sql_type.trim();
        let (base, args) = match s.find('(') {
            Some(i) => {
                let close = s[i..].find(')').map(|j| i + j).unwrap_or(s.len());
                (s[..i].trim(), Some(s[i + 1..close].trim()))
            }
            None => (s.split_whitespace().next().unwrap_or(""), None),
        };
        let base = base.trim_matches(|c| c == '[' || c == ']').to_ascii_uppercase();
        let base = if base == "DOUBLE" && s.to_ascii_uppercase().contains("PRECISION") { "FLOAT".to_string() } else { base };
        let bad = || anyhow::anyhow!("cannot parse SQL Server type {sql_type:?} (DuckDB {duck_type})");
        let nums: Vec<&str> = args.map(|a| a.split(',').map(|x| x.trim()).collect()).unwrap_or_default();
        let num = |i: usize, default: u32| -> Result<u32> {
            match nums.get(i) { None => Ok(default), Some(x) => x.parse::<u32>().map_err(|_| bad()) }
        };
        // length argument: number or MAX
        let length = |default: usize| -> Result<Option<usize>> {
            match nums.first() {
                None => Ok(Some(default)),
                Some(x) if x.eq_ignore_ascii_case("max") => Ok(None),
                Some(x) => x.parse::<usize>().map(Some).map_err(|_| bad()),
            }
        };
        let scale = |what: &str| -> Result<u8> { let n = num(0, 7)?; if n > 7 { Err(bad()) } else { check_scale(n as u8, what) } };
        Ok(match base.as_str() {
            "BIT" => Converter::Bit,
            "TINYINT" => Converter::TinyInt,
            "SMALLINT" => Converter::SmallInt,
            "INT" | "INTEGER" => Converter::Int,
            "BIGINT" => Converter::BigInt,
            "REAL" => Converter::Real,
            "FLOAT" => if num(0, 53)? <= 24 { Converter::Real } else { Converter::Float },
            "DECIMAL" | "NUMERIC" | "DEC" => {
                let p = num(0, 18)?;
                let sc = num(1, 0)?;
                if p > 38 || sc > p { return Err(bad()); }
                decimal(p as u8, sc as u8)?
            }
            "MONEY" => Converter::Decimal { precision: 19, scale: 4 },
            "SMALLMONEY" => Converter::Decimal { precision: 10, scale: 4 },
            "CHAR" | "CHARACTER" => Converter::Char { max_chars: length(1)?, unicode: false, fixed: true },
            "VARCHAR" => Converter::Char { max_chars: length(1)?, unicode: false, fixed: false },
            "NCHAR" => Converter::Char { max_chars: length(1)?, unicode: true, fixed: true },
            "NVARCHAR" => Converter::Char { max_chars: length(1)?, unicode: true, fixed: false },
            "SYSNAME" => Converter::Char { max_chars: Some(128), unicode: true, fixed: false },
            "TEXT" | "NTEXT" => Converter::Text,
            "DATE" => Converter::Date,
            "TIME" => Converter::Time { scale: scale("time")? },
            "DATETIME" => Converter::DateTime,
            "SMALLDATETIME" => Converter::SmallDateTime,
            "DATETIME2" => Converter::DateTime2 { scale: scale("datetime2")? },
            "DATETIMEOFFSET" => Converter::DateTimeOffset { scale: scale("datetimeoffset")? },
            "UNIQUEIDENTIFIER" => Converter::Guid,
            "BINARY" | "VARBINARY" => Converter::Binary { max_len: length(1)? },
            "IMAGE" => Converter::Binary { max_len: None },
            "" => return Err(bad()),
            _ => Converter::Text, // JSON, VECTOR(n), SQL_VARIANT, ...
        })
    }

    /// Convert one DuckDB value. NULL becomes the typed `None` of the target's TDS type.
    #[inline]
    pub fn convert(&self, v: ValueRef<'_>) -> Result<ColumnData<'static>> {
        use ColumnData as C;
        if let ValueRef::Null = v {
            return Ok(self.null());
        }
        Ok(match self {
            Converter::Int => C::I32(Some(int_in_range(v, i32::MIN as i128, i32::MAX as i128, self)? as i32)),
            Converter::BigInt => C::I64(Some(int_in_range(v, i64::MIN as i128, i64::MAX as i128, self)? as i64)),
            Converter::SmallInt => C::I16(Some(int_in_range(v, i16::MIN as i128, i16::MAX as i128, self)? as i16)),
            Converter::TinyInt => C::U8(Some(int_in_range(v, 0, 255, self)? as u8)),
            Converter::Bit => C::Bit(Some(bit_of(v)?)),
            Converter::Real => match float_of(v, self)? { f if f.is_finite() => C::F32(Some(f as f32)), _ => C::F32(None) },
            Converter::Float => match float_of(v, self)? { f if f.is_finite() => C::F64(Some(f)), _ => C::F64(None) },
            Converter::Decimal { precision, scale } => C::Numeric(Some(decimal_of(v, *precision, *scale)?)),
            Converter::Char { max_chars, unicode, .. } => C::String(Some(Cow::Owned(string_of(v, *max_chars, *unicode, self)?))),
            Converter::Date => {
                let m = moment_of(v, self)?;
                C::Date(Some(Date::new(days_ce(m.day, self)?)))
            }
            Converter::Time { scale } => C::Time(Some(time_of(v, *scale, self)?)),
            Converter::DateTime2 { scale } => C::DateTime2(Some(dt2_of(moment_of(v, self)?, *scale, self)?)),
            Converter::DateTimeOffset { scale } => {
                let (m, offset) = match v {
                    ValueRef::Text(b) => {
                        let s = utf8(b);
                        let (local, off) = parse_ts_offset(&s)?;
                        // TDS carries the UTC instant plus the offset
                        let utc = local - chrono::Duration::minutes(off as i64);
                        (moment_from_naive(utc), off)
                    }
                    other => (moment_of(other, self)?, 0),
                };
                C::DateTimeOffset(Some(DateTimeOffset::new(dt2_of(m, *scale, self)?, offset)))
            }
            Converter::DateTime => C::DateTime(Some(datetime_of(moment_of(v, self)?, self)?)),
            Converter::SmallDateTime => C::SmallDateTime(Some(smalldatetime_of(moment_of(v, self)?, self)?)),
            Converter::Guid => C::Guid(Some(guid_of(v)?)),
            Converter::Binary { max_len } => {
                let b: &[u8] = match v {
                    ValueRef::Blob(b) | ValueRef::Geometry(b) | ValueRef::Text(b) => b,
                    other => bail!("cannot convert {} to {self}", describe(&other)),
                };
                if let Some(m) = max_len {
                    if b.len() > *m { bail!("binary length {} > max {m} for {self}", b.len()); }
                }
                C::Binary(Some(Cow::Owned(b.to_vec())))
            }
            Converter::Text => C::String(Some(Cow::Owned(text_of(v)))),
        })
    }

    /// Fast path for DECIMAL / HUGEINT Arrow columns: mantissa and scale straight from the array,
    /// without building a `duck::Decimal` (which re-validates the digit count).
    #[inline]
    pub(crate) fn convert_mantissa(&self, m: i128, s: u8) -> Result<ColumnData<'static>> {
        match self {
            Converter::Decimal { precision, scale } => Ok(ColumnData::Numeric(Some(Numeric::new_with_scale(rescale(m, s, *scale, *precision)?, *scale)))),
            _ => match Decimal::new(38, s, m) {
                Ok(d) => self.convert(ValueRef::Decimal(d)),
                Err(e) => bail!("invalid decimal mantissa {m} scale {s}: {e:?}"),
            },
        }
    }

    /// Fast path for Arrow string columns (already valid UTF-8: no re-validation).
    #[inline]
    pub(crate) fn convert_str(&self, v: &str) -> Result<ColumnData<'static>> {
        match self {
            Converter::Char { max_chars, unicode, .. } => Ok(ColumnData::String(Some(Cow::Owned(checked_string(v, *max_chars, *unicode, self)?)))),
            Converter::Text => Ok(ColumnData::String(Some(Cow::Owned(v.to_owned())))),
            _ => self.convert(ValueRef::Text(v.as_bytes())),
        }
    }

    /// The typed NULL for this target.
    #[inline]
    pub fn null(&self) -> ColumnData<'static> {
        use ColumnData as C;
        match self {
            Converter::Bit => C::Bit(None),
            Converter::TinyInt => C::U8(None),
            Converter::SmallInt => C::I16(None),
            Converter::Int => C::I32(None),
            Converter::BigInt => C::I64(None),
            Converter::Real => C::F32(None),
            Converter::Float => C::F64(None),
            Converter::Decimal { .. } => C::Numeric(None),
            Converter::Char { .. } | Converter::Text => C::String(None),
            Converter::Date => C::Date(None),
            Converter::Time { .. } => C::Time(None),
            Converter::DateTime => C::DateTime(None),
            Converter::SmallDateTime => C::SmallDateTime(None),
            Converter::DateTime2 { .. } => C::DateTime2(None),
            Converter::DateTimeOffset { .. } => C::DateTimeOffset(None),
            Converter::Guid => C::Guid(None),
            Converter::Binary { .. } => C::Binary(None),
        }
    }
}

// ---------------------------------------------------------------- numbers

/// Short value description for error messages (only built on the error path).
fn describe(v: &ValueRef<'_>) -> String {
    match v {
        ValueRef::Text(b) => {
            let s = utf8(b);
            let short: String = s.chars().take(60).collect();
            format!("'{short}'{}", if s.chars().count() > 60 { "..." } else { "" })
        }
        ValueRef::Blob(b) => format!("blob of {} bytes", b.len()),
        other => text_of(*other),
    }
}

#[inline]
fn int_of(v: ValueRef<'_>) -> Option<i128> {
    Some(match v {
        ValueRef::TinyInt(x) => x as i128,
        ValueRef::SmallInt(x) => x as i128,
        ValueRef::Int(x) => x as i128,
        ValueRef::BigInt(x) => x as i128,
        ValueRef::HugeInt(x) => x,
        ValueRef::UTinyInt(x) => x as i128,
        ValueRef::USmallInt(x) => x as i128,
        ValueRef::UInt(x) => x as i128,
        ValueRef::UBigInt(x) => x as i128,
        ValueRef::UHugeInt(x) => i128::try_from(x).ok()?,
        ValueRef::Boolean(b) => b as i128,
        _ => return None,
    })
}

/// Integer value for an integer target: exact integers only (a DECIMAL/float with a fractional
/// part or text that is not an integer is an error, never truncated), range-checked.
#[inline]
fn int_in_range(v: ValueRef<'_>, lo: i128, hi: i128, target: &Converter) -> Result<i128> {
    let x = match int_of(v) {
        Some(x) => x,
        None => match v {
            ValueRef::Decimal(d) => {
                let p = POW10[d.scale() as usize];
                if d.value() % p != 0 { bail!("value {d} has a fractional part; target is {target}"); }
                d.value() / p
            }
            ValueRef::Float(f) => float_to_int(f as f64, target)?,
            ValueRef::Double(f) => float_to_int(f, target)?,
            ValueRef::Text(b) => {
                let s = utf8(b);
                match s.trim().parse::<i128>() { Ok(x) => x, Err(_) => bail!("text '{s}' is not an integer; target is {target}") }
            }
            ValueRef::UHugeInt(x) => bail!("value {x} out of range for {target}"),
            other => bail!("cannot convert {} to {target}", describe(&other)),
        },
    };
    if x < lo || x > hi {
        bail!("value {x} out of range for {target} ({lo}..={hi})");
    }
    Ok(x)
}

fn float_to_int(f: f64, target: &Converter) -> Result<i128> {
    if !f.is_finite() || f.fract() != 0.0 || f.abs() > 1.0e38 { bail!("value {f:?} is not an integer; target is {target}"); }
    Ok(f as i128)
}

fn bit_of(v: ValueRef<'_>) -> Result<bool> {
    if let ValueRef::Boolean(b) = v { return Ok(b); }
    if let Some(x) = int_of(v) {
        return match x { 0 => Ok(false), 1 => Ok(true), _ => bail!("value {x} is not 0/1 for bit") };
    }
    match v {
        ValueRef::Text(b) => match utf8(b).trim().to_ascii_lowercase().as_str() {
            "0" | "false" | "f" => Ok(false),
            "1" | "true" | "t" => Ok(true),
            other => bail!("text '{other}' is not 0/1/true/false for bit"),
        },
        ValueRef::Decimal(d) if d.value() == 0 => Ok(false),
        ValueRef::Decimal(d) if d.value() == POW10[d.scale() as usize] => Ok(true),
        other => bail!("cannot convert {} to bit (0/1 only)", describe(&other)),
    }
}

#[inline]
fn float_of(v: ValueRef<'_>, target: &Converter) -> Result<f64> {
    Ok(match v {
        ValueRef::Double(f) => f,
        ValueRef::Float(f) => f as f64,
        ValueRef::Decimal(d) => d.value() as f64 / POW10[d.scale() as usize] as f64,
        ValueRef::Text(b) => {
            let s = utf8(b);
            match s.trim().parse::<f64>() { Ok(f) => f, Err(_) => bail!("text '{s}' is not a number; target is {target}") }
        }
        ValueRef::UHugeInt(x) => x as f64,
        other => match int_of(other) { Some(x) => x as f64, None => bail!("cannot convert {} to {target}", describe(&other)) },
    })
}

/// Exact DECIMAL: the DuckDB mantissa is rescaled by integer multiplication only. Extra source
/// digits that are all zero are dropped exactly; a real loss of digits is an error.
#[inline]
fn decimal_of(v: ValueRef<'_>, precision: u8, scale: u8) -> Result<Numeric> {
    let (m, s) = match v {
        ValueRef::Decimal(d) => (d.value(), d.scale()),
        ValueRef::Double(f) => return float_decimal(f, precision, scale),
        ValueRef::Float(f) => return float_decimal(f as f64, precision, scale),
        ValueRef::Text(b) => {
            let t = utf8(b);
            match parse_decimal_text(t.trim()) {
                Some(x) => x,
                None => bail!("text '{t}' is not a decimal number; target is decimal({precision},{scale})"),
            }
        }
        other => match int_of(other) {
            Some(x) => (x, 0),
            None => bail!("cannot convert {} to decimal({precision},{scale})", describe(&other)),
        },
    };
    let m = rescale(m, s, scale, precision)?;
    Ok(Numeric::new_with_scale(m, scale))
}

#[inline]
fn rescale(m: i128, s: u8, target: u8, precision: u8) -> Result<i128> {
    let out = if s == target {
        m
    } else if s < target {
        match m.checked_mul(POW10[(target - s) as usize]) {
            Some(x) => x,
            None => bail!("value {} exceeds decimal({precision},{target})", fmt_mantissa(m, s)),
        }
    } else {
        let p = POW10[(s - target) as usize];
        if m % p != 0 {
            bail!("value {}: scale {s} > target scale {target}; use --allow-rescale", fmt_mantissa(m, s));
        }
        m / p
    };
    if out.unsigned_abs() >= POW10[precision as usize] as u128 {
        bail!("value {} exceeds decimal({precision},{target})", fmt_mantissa(m, s));
    }
    Ok(out)
}

/// float/double into DECIMAL: the shortest decimal text that round-trips the float, rounded half
/// away from zero to the target scale (a float has no exact decimal scale; this is SQL Server's
/// own float -> decimal behaviour).
fn float_decimal(f: f64, precision: u8, scale: u8) -> Result<Numeric> {
    if !f.is_finite() { bail!("value {f:?} cannot be stored in decimal({precision},{scale})"); }
    let text = format!("{f}");
    let (m, sc) = parse_decimal_text(&text).ok_or_else(|| anyhow::anyhow!("value {f:?} exceeds decimal({precision},{scale})"))?;
    let m = if sc > scale {
        let p = POW10[(sc - scale) as usize];
        let half = p / 2;
        if m >= 0 { (m + half) / p } else { (m - half) / p }
    } else {
        m
    };
    let m = rescale(m, sc.min(scale), scale, precision)?;
    Ok(Numeric::new_with_scale(m, scale))
}

/// `[-+]digits[.digits]` (or with an exponent, via rust_decimal) -> (mantissa, scale). Exact.
fn parse_decimal_text(s: &str) -> Option<(i128, u8)> {
    let b = s.as_bytes();
    let (neg, mut i) = match b.first() { Some(b'-') => (true, 1), Some(b'+') => (false, 1), _ => (false, 0) };
    let mut m: i128 = 0;
    let mut scale: u8 = 0;
    let mut digits = 0;
    let mut dot = false;
    while i < b.len() {
        let c = b[i];
        if c == b'.' && !dot { dot = true; i += 1; continue; }
        if !c.is_ascii_digit() {
            // exponent or anything else: let rust_decimal try (28 significant digits)
            let d = s.parse::<rust_decimal::Decimal>().ok().or_else(|| rust_decimal::Decimal::from_scientific(s).ok())?;
            return Some((d.mantissa(), d.scale() as u8));
        }
        m = m.checked_mul(10)?.checked_add((c - b'0') as i128)?;
        if dot { scale += 1; if scale > 38 { return None; } }
        digits += 1;
        i += 1;
    }
    if digits == 0 { return None; }
    Some((if neg { -m } else { m }, scale))
}

fn fmt_mantissa(m: i128, s: u8) -> String {
    match Decimal::new(38, s, m) {
        Ok(d) => d.to_string(),
        Err(_) => format!("{m}e-{s}"),
    }
}

// ---------------------------------------------------------------- strings

#[inline]
fn utf8(b: &[u8]) -> Cow<'_, str> {
    match std::str::from_utf8(b) { Ok(s) => Cow::Borrowed(s), Err(_) => String::from_utf8_lossy(b) }
}

#[inline]
fn utf16_len(s: &str) -> usize {
    s.chars().map(char::len_utf16).sum()
}

/// String for a char/varchar/nchar/nvarchar target: ASCII check for non-unicode targets, length in
/// characters (UTF-16 units for unicode targets). Over-long values whose excess is only trailing
/// spaces are cut (SQL Server does the same without an error); anything else over the limit fails.
#[inline]
fn string_of(v: ValueRef<'_>, max_chars: Option<usize>, unicode: bool, target: &Converter) -> Result<String> {
    let owned;
    let s: &str = match v {
        ValueRef::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => s,
            Err(_) => { owned = String::from_utf8_lossy(b).into_owned(); &owned }
        },
        other => { owned = text_of(other); &owned }
    };
    checked_string(s, max_chars, unicode, target)
}

#[inline]
fn checked_string(s: &str, max_chars: Option<usize>, unicode: bool, target: &Converter) -> Result<String> {
    if !unicode && !s.is_ascii() {
        bail!("non-ASCII value {} for varchar column; SQL Server would substitute '?' (use an nvarchar column)", describe(&ValueRef::Text(s.as_bytes())));
    }
    if let Some(max) = max_chars {
        // UTF-8 bytes >= UTF-16 units >= chars, so the byte length is a cheap upper bound
        if s.len() > max {
            let n = if unicode { utf16_len(s) } else { s.len() };
            if n > max {
                let trimmed = s.trim_end_matches(' ');
                let tn = if unicode { utf16_len(trimmed) } else { trimmed.len() };
                if tn > max {
                    bail!("length {n} > max {max} for {target}: {}", describe(&ValueRef::Text(s.as_bytes())));
                }
                // keep as many of the trailing spaces as fit (each is one byte / one unit)
                return Ok(s[..trimmed.len() + (max - tn)].to_owned());
            }
        }
    }
    Ok(s.to_owned())
}

/// DuckDB-style text for any value (the `Text` fallback and non-string sources into string targets).
pub(crate) fn text_of(v: ValueRef<'_>) -> String {
    use crate::core::script::{date_from_days, naive_ts, time_from_units};
    match v {
        ValueRef::Null => String::new(),
        ValueRef::Text(b) => utf8(b).into_owned(),
        ValueRef::Boolean(b) => if b { "true".into() } else { "false".into() },
        ValueRef::TinyInt(x) => x.to_string(),
        ValueRef::SmallInt(x) => x.to_string(),
        ValueRef::Int(x) => x.to_string(),
        ValueRef::BigInt(x) => x.to_string(),
        ValueRef::HugeInt(x) => x.to_string(),
        ValueRef::UHugeInt(x) => x.to_string(),
        ValueRef::UTinyInt(x) => x.to_string(),
        ValueRef::USmallInt(x) => x.to_string(),
        ValueRef::UInt(x) => x.to_string(),
        ValueRef::UBigInt(x) => x.to_string(),
        ValueRef::Float(f) => format!("{f:?}"),
        ValueRef::Double(f) => format!("{f:?}"),
        ValueRef::Decimal(d) => d.to_string(),
        ValueRef::Timestamp(u, ts) => naive_ts(u, ts).format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        ValueRef::Date32(d) => date_from_days(d).to_string(),
        ValueRef::Time64(u, t) => time_from_units(u, t),
        ValueRef::Interval { months, days, nanos } => format!("{months} months {days} days {} microseconds", nanos / 1000),
        ValueRef::Blob(b) | ValueRef::Geometry(b) => hex::encode(b),
        ValueRef::Enum(..) => v.as_str().map(|s| s.to_owned()).unwrap_or_else(|_| format!("{v:?}")),
        other => format!("{other:?}"),
    }
}

fn guid_of(v: ValueRef<'_>) -> Result<uuid::Uuid> {
    match v {
        ValueRef::Text(b) => {
            let s = utf8(b);
            uuid::Uuid::parse_str(s.trim()).map_err(|e| anyhow::anyhow!("text '{s}' is not a uuid: {e}"))
        }
        ValueRef::Blob(b) if b.len() == 16 => Ok(uuid::Uuid::from_slice(b)?),
        // DuckDB's internal UUID layout: a HUGEINT with the top bit flipped
        ValueRef::HugeInt(x) => Ok(uuid::Uuid::from_u128((x as u128) ^ (1u128 << 127))),
        other => bail!("cannot convert {} to uniqueidentifier", describe(&other)),
    }
}

// ---------------------------------------------------------------- date / time

/// A point in time as days since 1970-01-01 plus nanoseconds since midnight (0..NANOS_PER_DAY).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Moment {
    day: i64,
    nanos: i64,
}

#[inline]
fn moment_from_ts(unit: TimeUnit, v: i64) -> Moment {
    let (per_day, mul) = match unit {
        TimeUnit::Second => (86_400i64, 1_000_000_000i64),
        TimeUnit::Millisecond => (86_400_000, 1_000_000),
        TimeUnit::Microsecond => (86_400_000_000, 1_000),
        TimeUnit::Nanosecond => (NANOS_PER_DAY, 1),
    };
    Moment { day: v.div_euclid(per_day), nanos: v.rem_euclid(per_day) * mul }
}

fn moment_from_naive(dt: chrono::NaiveDateTime) -> Moment {
    use chrono::Timelike;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let nanos = dt.num_seconds_from_midnight() as i64 * 1_000_000_000 + (dt.nanosecond() as i64).min(999_999_999);
    Moment { day: (dt.date() - epoch).num_days(), nanos }
}

#[inline]
fn moment_of(v: ValueRef<'_>, target: &Converter) -> Result<Moment> {
    match v {
        ValueRef::Timestamp(u, t) => Ok(moment_from_ts(u, t)),
        ValueRef::Date32(d) => Ok(Moment { day: d as i64, nanos: 0 }),
        ValueRef::Text(b) => {
            let s = crate::core::script::trim_ns(&utf8(b));
            Ok(moment_from_naive(parse_naive_ts(&s)?))
        }
        other => bail!("cannot convert {} to {target}", describe(&other)),
    }
}

fn moment_text(m: Moment) -> String {
    let d = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().checked_add_signed(chrono::Duration::days(m.day));
    match d {
        Some(d) => format!("{} {}", d, crate::core::script::time_from_units(TimeUnit::Nanosecond, m.nanos)),
        None => format!("{} days from 1970-01-01", m.day),
    }
}

#[inline]
fn days_ce(day: i64, target: &Converter) -> Result<u32> {
    let d = day + UNIX_DAYS_CE;
    if !(0..=MAX_DAYS_CE).contains(&d) {
        bail!("date {} is outside 0001-01-01..9999-12-31 for {target}", moment_text(Moment { day, nanos: 0 }));
    }
    Ok(d as u32)
}

/// Nanoseconds since midnight -> increments of 10^-scale s, rounded half up; `true` when the
/// rounding carried into the next day.
#[inline]
fn tod_units(nanos: i64, scale: u8) -> (u64, bool) {
    let div = pow10_i64(9 - scale);
    let u = (nanos + div / 2) / div;
    let per_day = 86_400 * pow10_i64(scale);
    if u >= per_day { ((u - per_day) as u64, true) } else { (u as u64, false) }
}

#[inline]
fn dt2_of(m: Moment, scale: u8, target: &Converter) -> Result<DateTime2> {
    let (units, carry) = tod_units(m.nanos, scale);
    let days = days_ce(m.day + carry as i64, target)?;
    Ok(DateTime2::new(Date::new(days), Time::new(units, scale)))
}

fn time_of(v: ValueRef<'_>, scale: u8, target: &Converter) -> Result<Time> {
    let nanos = match v {
        ValueRef::Time64(u, t) => match u {
            TimeUnit::Second => t.saturating_mul(1_000_000_000),
            TimeUnit::Millisecond => t.saturating_mul(1_000_000),
            TimeUnit::Microsecond => t.saturating_mul(1_000),
            TimeUnit::Nanosecond => t,
        },
        ValueRef::Timestamp(u, t) => moment_from_ts(u, t).nanos,
        ValueRef::Text(b) => {
            use chrono::Timelike;
            let t = parse_naive_time(&utf8(b))?;
            t.num_seconds_from_midnight() as i64 * 1_000_000_000 + (t.nanosecond() as i64).min(999_999_999)
        }
        other => bail!("cannot convert {} to {target}", describe(&other)),
    };
    if !(0..NANOS_PER_DAY).contains(&nanos) {
        bail!("time value of {nanos} ns is outside 00:00:00..24:00:00 for {target}");
    }
    let (units, carry) = tod_units(nanos, scale);
    if carry {
        bail!("time {} rounds to 24:00:00 at {target}", crate::core::script::time_from_units(TimeUnit::Nanosecond, nanos));
    }
    Ok(Time::new(units, scale))
}

/// Round to the nearest 1/300 s (SQL Server's `datetime` resolution) -> (day, fragments).
#[inline]
fn fragments(m: Moment) -> (i64, i64) {
    let f = (m.nanos * 3 + 5_000_000) / 10_000_000; // nanos * 300 / 1e9, rounded
    if f >= FRAGMENTS_PER_DAY { (m.day + 1, f - FRAGMENTS_PER_DAY) } else { (m.day, f) }
}

#[inline]
fn datetime_of(m: Moment, target: &Converter) -> Result<DateTime> {
    let (day, frag) = fragments(m);
    let d = day + UNIX_DAYS_1900;
    if !(DATETIME_MIN_1900..=DATETIME_MAX_1900).contains(&d) {
        bail!("{} is outside 1753-01-01..9999-12-31 for {target}", moment_text(m));
    }
    Ok(DateTime::new(d as i32, frag as u32))
}

#[inline]
fn smalldatetime_of(m: Moment, target: &Converter) -> Result<SmallDateTime> {
    // as SQL Server's datetime2 -> smalldatetime: nearest minute, 30.000 s rounds up
    let mut day = m.day;
    let mut minutes = (m.nanos + 30_000_000_000) / 60_000_000_000;
    if minutes >= 1440 { minutes -= 1440; day += 1; }
    let d = day + UNIX_DAYS_1900;
    if !(0..=SMALLDATETIME_MAX_1900).contains(&d) {
        bail!("{} is outside 1900-01-01..2079-06-06 for {target}", moment_text(m));
    }
    Ok(SmallDateTime::new(d as u16, minutes as u16))
}

fn parse_naive_ts(s: &str) -> Result<chrono::NaiveDateTime> {
    let s = s.trim();
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(s, f) { return Ok(d); }
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") { return Ok(d.and_hms_opt(0, 0, 0).unwrap()); }
    bail!("cannot parse timestamp {s:?}")
}

fn parse_naive_time(s: &str) -> Result<chrono::NaiveTime> {
    let s = s.trim();
    for f in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(t) = chrono::NaiveTime::parse_from_str(s, f) { return Ok(t); }
    }
    bail!("cannot parse time {s:?}")
}

/// "2024-01-01 10:00:00.123456+02:00" / "+02" / "Z" -> (local wall-clock time, offset minutes).
fn parse_ts_offset(s: &str) -> Result<(chrono::NaiveDateTime, i16)> {
    let s = crate::core::script::trim_ns(s.trim());
    let s = s.as_str();
    let off = |dt: chrono::DateTime<chrono::FixedOffset>| (dt.naive_local(), (dt.offset().local_minus_utc() / 60) as i16);
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) { return Ok(off(dt)); }
    for f in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f %:z", "%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%:z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, f) { return Ok(off(dt)); }
    }
    Ok((parse_naive_ts(s.trim_end_matches('Z'))?, 0))
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::core::duck::{self, Connection, Decimal as DDec};

    fn dec(width: u8, scale: u8, v: i128) -> ValueRef<'static> {
        ValueRef::Decimal(DDec::new(width, scale, v).unwrap())
    }
    fn num(c: &ColumnData<'_>) -> (i128, u8) {
        match c { ColumnData::Numeric(Some(n)) => (n.value(), n.scale()), other => panic!("not numeric: {other:?}") }
    }
    fn s(c: &ColumnData<'_>) -> String {
        match c { ColumnData::String(Some(s)) => s.to_string(), other => panic!("not string: {other:?}") }
    }
    fn tcol(type_name: &str, max_length: i16, precision: u8, scale: u8) -> TargetColumn {
        TargetColumn { name: "c".into(), column_id: 1, type_name: type_name.into(), max_length, precision, scale, collation: None, nullable: true, has_default: false, is_identity: false, is_computed: false }
    }
    fn us(y: i32, mo: u32, d: u32, h: u32, mi: u32, sec: u32, micro: u32) -> ValueRef<'static> {
        let dt = chrono::NaiveDate::from_ymd_opt(y, mo, d).unwrap().and_hms_micro_opt(h, mi, sec, micro).unwrap();
        ValueRef::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
    }
    fn days_1900(y: i32, m: u32, d: u32) -> i64 {
        (chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap() - chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()).num_days()
    }
    fn days_from_ce(y: i32, m: u32, d: u32) -> u32 {
        use chrono::Datelike;
        (chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap().num_days_from_ce() - 1) as u32
    }

    #[test]
    fn convert_constants_match_chrono() {
        assert_eq!(UNIX_DAYS_CE, days_from_ce(1970, 1, 1) as i64);
        assert_eq!(MAX_DAYS_CE, days_from_ce(9999, 12, 31) as i64);
        assert_eq!(UNIX_DAYS_1900, days_1900(1970, 1, 1));
        assert_eq!(DATETIME_MIN_1900, days_1900(1753, 1, 1));
        assert_eq!(DATETIME_MAX_1900, days_1900(9999, 12, 31));
        assert_eq!(SMALLDATETIME_MAX_1900, days_1900(2079, 6, 6));
    }

    #[test]
    fn convert_for_sql_type_parses() {
        let f = |t: &str| Converter::for_sql_type(t, "X").unwrap();
        assert_eq!(f("NVARCHAR(50)"), Converter::Char { max_chars: Some(50), unicode: true, fixed: false });
        assert_eq!(f("nvarchar(max)"), Converter::Char { max_chars: None, unicode: true, fixed: false });
        assert_eq!(f("VARCHAR(10)"), Converter::Char { max_chars: Some(10), unicode: false, fixed: false });
        assert_eq!(f("CHAR(3)"), Converter::Char { max_chars: Some(3), unicode: false, fixed: true });
        assert_eq!(f("DECIMAL(18,3)"), Converter::Decimal { precision: 18, scale: 3 });
        assert_eq!(f("decimal(18, 3)"), Converter::Decimal { precision: 18, scale: 3 });
        assert_eq!(f("NUMERIC(10)"), Converter::Decimal { precision: 10, scale: 0 });
        assert_eq!(f("DATETIME2(7)"), Converter::DateTime2 { scale: 7 });
        assert_eq!(f("DATETIME2"), Converter::DateTime2 { scale: 7 });
        assert_eq!(f("TIME(3)"), Converter::Time { scale: 3 });
        assert_eq!(f("DATETIMEOFFSET(7)"), Converter::DateTimeOffset { scale: 7 });
        assert_eq!(f("VARBINARY(MAX)"), Converter::Binary { max_len: None });
        assert_eq!(f("VARBINARY(900)"), Converter::Binary { max_len: Some(900) });
        assert_eq!(f("FLOAT(53)"), Converter::Float);
        assert_eq!(f("FLOAT(24)"), Converter::Real);
        assert_eq!(f("REAL"), Converter::Real);
        assert_eq!(f("INT"), Converter::Int);
        assert_eq!(f("BIT"), Converter::Bit);
        assert_eq!(f("MONEY"), Converter::Decimal { precision: 19, scale: 4 });
        assert_eq!(f("UNIQUEIDENTIFIER"), Converter::Guid);
        assert_eq!(f("json"), Converter::Text);
        assert_eq!(f("VECTOR(3)"), Converter::Text);
        assert_eq!(f("XML"), Converter::Text);
        assert!(Converter::for_sql_type("DECIMAL(39,2)", "X").is_err());
        assert!(Converter::for_sql_type("DECIMAL(5,6)", "X").is_err());
        assert!(Converter::for_sql_type("DATETIME2(8)", "X").is_err());
        assert!(Converter::for_sql_type("VARCHAR(abc)", "X").is_err());
    }

    #[test]
    fn convert_for_target_maps_sys_types() {
        let f = |c: TargetColumn| Converter::for_target(&c, "X").unwrap();
        assert_eq!(f(tcol("nvarchar", 100, 0, 0)), Converter::Char { max_chars: Some(50), unicode: true, fixed: false });
        assert_eq!(f(tcol("nvarchar", -1, 0, 0)), Converter::Char { max_chars: None, unicode: true, fixed: false });
        assert_eq!(f(tcol("varchar", 20, 0, 0)), Converter::Char { max_chars: Some(20), unicode: false, fixed: false });
        assert_eq!(f(tcol("nchar", 6, 0, 0)), Converter::Char { max_chars: Some(3), unicode: true, fixed: true });
        assert_eq!(f(tcol("char", 3, 0, 0)), Converter::Char { max_chars: Some(3), unicode: false, fixed: true });
        assert_eq!(f(tcol("decimal", 9, 8, 5)), Converter::Decimal { precision: 8, scale: 5 });
        assert_eq!(f(tcol("money", 8, 19, 4)), Converter::Decimal { precision: 19, scale: 4 });
        assert_eq!(f(tcol("smallmoney", 4, 10, 4)), Converter::Decimal { precision: 10, scale: 4 });
        assert_eq!(f(tcol("float", 8, 53, 0)), Converter::Float);
        assert_eq!(f(tcol("float", 4, 24, 0)), Converter::Real);
        assert_eq!(f(tcol("datetime2", 8, 27, 7)), Converter::DateTime2 { scale: 7 });
        assert_eq!(f(tcol("time", 5, 16, 3)), Converter::Time { scale: 3 });
        assert_eq!(f(tcol("datetimeoffset", 10, 34, 7)), Converter::DateTimeOffset { scale: 7 });
        assert_eq!(f(tcol("datetime", 8, 23, 3)), Converter::DateTime);
        assert_eq!(f(tcol("smalldatetime", 4, 16, 0)), Converter::SmallDateTime);
        assert_eq!(f(tcol("varbinary", -1, 0, 0)), Converter::Binary { max_len: None });
        assert_eq!(f(tcol("binary", 16, 0, 0)), Converter::Binary { max_len: Some(16) });
        assert_eq!(f(tcol("text", 16, 0, 0)), Converter::Text);
        assert_eq!(f(tcol("ntext", 16, 0, 0)), Converter::Text);
        assert_eq!(f(tcol("json", -1, 0, 0)), Converter::Text);
        assert_eq!(f(tcol("xml", -1, 0, 0)), Converter::Text);
        assert_eq!(f(tcol("uniqueidentifier", 16, 0, 0)), Converter::Guid);
        assert!(Converter::for_target(&tcol("decimal", 17, 0, 0), "X").is_err());
    }

    #[test]
    fn convert_nulls_are_typed() {
        let cases: Vec<(Converter, fn(&ColumnData<'_>) -> bool)> = vec![
            (Converter::Bit, |c| matches!(c, ColumnData::Bit(None))),
            (Converter::TinyInt, |c| matches!(c, ColumnData::U8(None))),
            (Converter::SmallInt, |c| matches!(c, ColumnData::I16(None))),
            (Converter::Int, |c| matches!(c, ColumnData::I32(None))),
            (Converter::BigInt, |c| matches!(c, ColumnData::I64(None))),
            (Converter::Real, |c| matches!(c, ColumnData::F32(None))),
            (Converter::Float, |c| matches!(c, ColumnData::F64(None))),
            (Converter::Decimal { precision: 18, scale: 2 }, |c| matches!(c, ColumnData::Numeric(None))),
            (Converter::Char { max_chars: Some(3), unicode: false, fixed: true }, |c| matches!(c, ColumnData::String(None))),
            (Converter::Text, |c| matches!(c, ColumnData::String(None))),
            (Converter::Date, |c| matches!(c, ColumnData::Date(None))),
            (Converter::Time { scale: 7 }, |c| matches!(c, ColumnData::Time(None))),
            (Converter::DateTime, |c| matches!(c, ColumnData::DateTime(None))),
            (Converter::SmallDateTime, |c| matches!(c, ColumnData::SmallDateTime(None))),
            (Converter::DateTime2 { scale: 7 }, |c| matches!(c, ColumnData::DateTime2(None))),
            (Converter::DateTimeOffset { scale: 7 }, |c| matches!(c, ColumnData::DateTimeOffset(None))),
            (Converter::Guid, |c| matches!(c, ColumnData::Guid(None))),
            (Converter::Binary { max_len: None }, |c| matches!(c, ColumnData::Binary(None))),
        ];
        for (conv, ok) in cases {
            let c = conv.convert(ValueRef::Null).unwrap();
            assert!(ok(&c), "{conv}: {c:?}");
        }
    }

    #[test]
    fn convert_integers_range_checked() {
        let t = Converter::TinyInt;
        assert!(matches!(t.convert(ValueRef::Int(0)).unwrap(), ColumnData::U8(Some(0))));
        assert!(matches!(t.convert(ValueRef::Int(255)).unwrap(), ColumnData::U8(Some(255))));
        let e = t.convert(ValueRef::Int(256)).unwrap_err().to_string();
        assert!(e.contains("256") && e.contains("tinyint"), "{e}");
        assert!(t.convert(ValueRef::SmallInt(-1)).is_err());
        assert!(t.convert(ValueRef::TinyInt(-1)).is_err());
        assert!(matches!(Converter::SmallInt.convert(ValueRef::Int(32767)).unwrap(), ColumnData::I16(Some(32767))));
        assert!(Converter::SmallInt.convert(ValueRef::Int(32768)).is_err());
        assert!(matches!(Converter::Int.convert(ValueRef::BigInt(i32::MIN as i64)).unwrap(), ColumnData::I32(Some(i32::MIN))));
        assert!(Converter::Int.convert(ValueRef::BigInt(i32::MAX as i64 + 1)).is_err());
        assert!(matches!(Converter::BigInt.convert(ValueRef::HugeInt(i64::MAX as i128)).unwrap(), ColumnData::I64(Some(i64::MAX))));
        assert!(Converter::BigInt.convert(ValueRef::HugeInt(i64::MAX as i128 + 1)).is_err());
        assert!(Converter::BigInt.convert(ValueRef::UBigInt(u64::MAX)).is_err());
        assert!(Converter::BigInt.convert(ValueRef::UHugeInt(u128::MAX)).is_err());
        assert!(matches!(Converter::Int.convert(dec(10, 2, 4200)).unwrap(), ColumnData::I32(Some(42))));
        assert!(Converter::Int.convert(dec(10, 2, 4201)).is_err());
        assert!(matches!(Converter::Int.convert(ValueRef::Text(b" 17 ")).unwrap(), ColumnData::I32(Some(17))));
        assert!(Converter::Int.convert(ValueRef::Double(1.5)).is_err());
        assert!(matches!(Converter::Bit.convert(ValueRef::Boolean(true)).unwrap(), ColumnData::Bit(Some(true))));
        assert!(matches!(Converter::Bit.convert(ValueRef::Int(0)).unwrap(), ColumnData::Bit(Some(false))));
        assert!(matches!(Converter::Bit.convert(ValueRef::Int(1)).unwrap(), ColumnData::Bit(Some(true))));
        assert!(Converter::Bit.convert(ValueRef::Int(2)).is_err());
    }

    #[test]
    fn convert_decimal_from_mantissa() {
        let max38 = POW10[38] - 1;
        // decimal(38,s) max, both signs, any scale
        for sc in [0u8, 2, 10, 38] {
            let c = Converter::Decimal { precision: 38, scale: sc };
            assert_eq!(num(&c.convert(dec(38, sc, max38)).unwrap()), (max38, sc));
            assert_eq!(num(&c.convert(dec(38, sc, -max38)).unwrap()), (-max38, sc));
        }
        // rescale up is exact integer multiplication
        let c = Converter::Decimal { precision: 18, scale: 5 };
        assert_eq!(num(&c.convert(dec(8, 2, 12345)).unwrap()), (12_345_000, 5));
        assert_eq!(num(&c.convert(dec(8, 2, -12345)).unwrap()), (-12_345_000, 5));
        // rescale down: zero digits drop exactly, real digits are an error
        let c2 = Converter::Decimal { precision: 10, scale: 2 };
        assert_eq!(num(&c2.convert(dec(18, 5, 123_400_000)).unwrap()), (123_400, 2));
        let e = c2.convert(dec(18, 5, 123_456_789)).unwrap_err().to_string();
        assert!(e.contains("scale 5 > target scale 2") && e.contains("--allow-rescale"), "{e}");
        // precision overflow
        assert!(Converter::Decimal { precision: 8, scale: 5 }.convert(dec(10, 5, 1_000_000_000)).is_err());
        assert_eq!(num(&Converter::Decimal { precision: 8, scale: 5 }.convert(dec(8, 5, 99_999_999)).unwrap()), (99_999_999, 5));
        // rescale up that overflows decimal(38)
        assert!(Converter::Decimal { precision: 38, scale: 2 }.convert(dec(38, 0, max38)).is_err());
        // integers, hugeint
        assert_eq!(num(&Converter::Decimal { precision: 18, scale: 3 }.convert(ValueRef::Int(-7)).unwrap()), (-7000, 3));
        assert_eq!(num(&Converter::Decimal { precision: 38, scale: 0 }.convert(ValueRef::HugeInt(max38)).unwrap()), (max38, 0));
        assert!(Converter::Decimal { precision: 38, scale: 0 }.convert(ValueRef::HugeInt(max38 + 1)).is_err());
        // text, exactly
        assert_eq!(num(&Converter::Decimal { precision: 38, scale: 4 }.convert(ValueRef::Text(b"-12.5")).unwrap()), (-125_000, 4));
        assert_eq!(num(&Converter::Decimal { precision: 38, scale: 38 }.convert(ValueRef::Text(b"0.99999999999999999999999999999999999999")).unwrap()), (max38, 38));
        assert!(Converter::Decimal { precision: 10, scale: 1 }.convert(ValueRef::Text(b"1.25")).is_err());
        // double: shortest repr, rounded to the target scale
        assert_eq!(num(&Converter::Decimal { precision: 10, scale: 2 }.convert(ValueRef::Double(0.1)).unwrap()), (10, 2));
        assert_eq!(num(&Converter::Decimal { precision: 10, scale: 1 }.convert(ValueRef::Double(-1.25)).unwrap()), (-13, 1));
        // money keeps scale 4
        assert_eq!(num(&Converter::for_sql_type("MONEY", "DECIMAL(10,2)").unwrap().convert(dec(10, 2, 199)).unwrap()), (19_900, 4));
    }

    #[test]
    fn convert_negative_scale_handling() {
        // DuckDB has no negative scales; negative values at every scale boundary are exact and
        // the sign survives rescaling in both directions.
        let c = Converter::Decimal { precision: 5, scale: 0 };
        assert_eq!(num(&c.convert(dec(5, 0, -99_999)).unwrap()), (-99_999, 0));
        assert!(c.convert(dec(6, 0, -100_000)).is_err());
        assert!(c.convert(dec(6, 1, -5)).is_err()); // -0.5 into scale 0
        assert_eq!(num(&c.convert(dec(6, 1, -50)).unwrap()), (-5, 0));
        assert_eq!(num(&Converter::Decimal { precision: 38, scale: 37 }.convert(dec(2, 1, -1)).unwrap()), (-POW10[36], 37));
    }

    #[test]
    fn convert_varchar_lengths_and_ascii() {
        let v = Converter::Char { max_chars: Some(5), unicode: false, fixed: false };
        assert_eq!(s(&v.convert(ValueRef::Text(b"abcde")).unwrap()), "abcde");
        let e = v.convert(ValueRef::Text(b"abcdef")).unwrap_err().to_string();
        assert!(e.contains("length 6 > max 5"), "{e}");
        // trailing spaces beyond the limit are cut, like SQL Server does
        assert_eq!(s(&v.convert(ValueRef::Text(b"abc     ")).unwrap()), "abc  ");
        let e = v.convert(ValueRef::Text("caf\u{e9}".as_bytes())).unwrap_err().to_string();
        assert!(e.contains("non-ASCII") && e.contains("'?'"), "{e}");
        // char(3)
        let c3 = Converter::Char { max_chars: Some(3), unicode: false, fixed: true };
        assert_eq!(s(&c3.convert(ValueRef::Text(b"USA")).unwrap()), "USA");
        assert!(c3.convert(ValueRef::Text(b"USAX")).is_err());
        // varchar(max): no limit, still ASCII only
        let vm = Converter::Char { max_chars: None, unicode: false, fixed: false };
        assert_eq!(s(&vm.convert(ValueRef::Text("x".repeat(100_000).as_bytes())).unwrap()).len(), 100_000);
        assert!(vm.convert(ValueRef::Text("\u{e9}".as_bytes())).is_err());
        // numbers into varchar use DuckDB's text
        assert_eq!(s(&v.convert(ValueRef::Int(42)).unwrap()), "42");
    }

    #[test]
    fn convert_nvarchar_counts_utf16_units() {
        let n2 = Converter::Char { max_chars: Some(2), unicode: true, fixed: false };
        assert_eq!(s(&n2.convert(ValueRef::Text("\u{e9}\u{e8}".as_bytes())).unwrap()), "\u{e9}\u{e8}"); // 4 bytes, 2 units
        assert_eq!(s(&n2.convert(ValueRef::Text("\u{1F600}".as_bytes())).unwrap()), "\u{1F600}"); // 1 emoji = 2 units
        let e = n2.convert(ValueRef::Text("a\u{1F600}".as_bytes())).unwrap_err().to_string();
        assert!(e.contains("length 3 > max 2"), "{e}");
        let n1 = Converter::Char { max_chars: Some(1), unicode: true, fixed: false };
        assert!(n1.convert(ValueRef::Text("\u{1F600}".as_bytes())).is_err());
        assert_eq!(s(&n1.convert(ValueRef::Text("\u{4e2d}".as_bytes())).unwrap()), "\u{4e2d}");
    }

    #[test]
    fn convert_datetime_rounds_to_300ths() {
        let c = Converter::DateTime;
        let frag = |v| match c.convert(v).unwrap() { ColumnData::DateTime(Some(d)) => (d.days(), d.seconds_fragments()), o => panic!("{o:?}") };
        let d2024 = days_1900(2024, 1, 1) as i32;
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 0, 0)), (d2024, 0));
        // nearest 1/300 s: .001 -> 0 (.000), .002 -> 1 (.003), .0049 -> 1, .005 -> 2 (.007)
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 0, 1_000)), (d2024, 0));
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 0, 2_000)), (d2024, 1));
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 0, 4_900)), (d2024, 1));
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 0, 5_000)), (d2024, 2));
        assert_eq!(frag(us(2024, 1, 1, 0, 0, 1, 0)), (d2024, 300));
        // 23:59:59.999 rounds into the next day
        assert_eq!(frag(us(2024, 1, 1, 23, 59, 59, 999_000)), (d2024 + 1, 0));
        // range
        assert_eq!(frag(us(1753, 1, 1, 0, 0, 0, 0)).0, DATETIME_MIN_1900 as i32);
        assert!(c.convert(us(1752, 12, 31, 23, 59, 59, 0)).is_err());
        // date and text sources
        assert_eq!(frag(ValueRef::Date32(19_723)), (d2024, 0));
        assert_eq!(frag(ValueRef::Text(b"2024-01-01 00:00:01.5")), (d2024, 450));
    }

    #[test]
    fn convert_smalldatetime_rounds_to_minutes() {
        let c = Converter::SmallDateTime;
        let v = |x| match c.convert(x).unwrap() { ColumnData::SmallDateTime(Some(d)) => (d.days(), d.seconds_fragments()), o => panic!("{o:?}") };
        let d = days_1900(2024, 1, 1) as u16;
        assert_eq!(v(us(2024, 1, 1, 10, 30, 29, 999_999)), (d, 630));
        assert_eq!(v(us(2024, 1, 1, 10, 30, 30, 0)), (d, 631));
        assert_eq!(v(us(2024, 1, 1, 23, 59, 30, 0)), (d + 1, 0));
        assert!(c.convert(us(1899, 12, 31, 0, 0, 0, 0)).is_err());
        assert!(c.convert(us(2079, 6, 7, 0, 0, 0, 0)).is_err());
    }

    #[test]
    fn convert_datetime2_time_scales() {
        let ts = us(2024, 2, 29, 12, 34, 56, 123_456);
        let tod = (12 * 3600 + 34 * 60 + 56) as u64;
        let c7 = Converter::DateTime2 { scale: 7 };
        match c7.convert(ts).unwrap() {
            ColumnData::DateTime2(Some(d)) => {
                assert_eq!(d.time().scale(), 7);
                assert_eq!(d.time().increments(), tod * 10_000_000 + 1_234_560);
                assert_eq!(d.date().days(), days_from_ce(2024, 2, 29));
            }
            o => panic!("{o:?}"),
        }
        // scale 3: rounded, sent at the column's scale
        match (Converter::DateTime2 { scale: 3 }).convert(ts).unwrap() {
            ColumnData::DateTime2(Some(d)) => { assert_eq!(d.time().scale(), 3); assert_eq!(d.time().increments(), tod * 1000 + 123); }
            o => panic!("{o:?}"),
        }
        // scale 0 carry into the next day
        match (Converter::DateTime2 { scale: 0 }).convert(us(2024, 12, 31, 23, 59, 59, 600_000)).unwrap() {
            ColumnData::DateTime2(Some(d)) => { assert_eq!(d.time().increments(), 0); assert_eq!(d.date().days(), days_from_ce(2025, 1, 1)); }
            o => panic!("{o:?}"),
        }
        // pre-1970 and out of range
        match c7.convert(us(1, 1, 1, 0, 0, 0, 1)).unwrap() {
            ColumnData::DateTime2(Some(d)) => { assert_eq!(d.date().days(), 0); assert_eq!(d.time().increments(), 10); }
            o => panic!("{o:?}"),
        }
        assert!(c7.convert(ValueRef::Date32(3_000_000)).is_err());
        assert!(c7.convert(ValueRef::Text(b"2024-01-01 10:00:00.123456789")).is_ok());
        // time
        match (Converter::Time { scale: 7 }).convert(ValueRef::Time64(TimeUnit::Microsecond, 3_600_000_001)).unwrap() {
            ColumnData::Time(Some(t)) => { assert_eq!(t.scale(), 7); assert_eq!(t.increments(), 36_000_000_010); }
            o => panic!("{o:?}"),
        }
        match (Converter::Time { scale: 2 }).convert(ValueRef::Text(b"10:00:00.125")).unwrap() {
            ColumnData::Time(Some(t)) => { assert_eq!(t.scale(), 2); assert_eq!(t.increments(), 36_000 * 100 + 13); }
            o => panic!("{o:?}"),
        }
        assert!((Converter::Time { scale: 0 }).convert(ValueRef::Time64(TimeUnit::Microsecond, 86_399_600_000)).is_err());
        // date
        match Converter::Date.convert(ValueRef::Date32(0)).unwrap() { ColumnData::Date(Some(d)) => assert_eq!(d.days(), UNIX_DAYS_CE as u32), o => panic!("{o:?}") }
        assert!(Converter::Date.convert(ValueRef::Date32(i32::MAX)).is_err());
        assert!(Converter::Date.convert(ValueRef::Date32(-719_163)).is_err()); // year 0
    }

    #[test]
    fn convert_datetimeoffset_sends_utc() {
        let c = Converter::DateTimeOffset { scale: 7 };
        match c.convert(ValueRef::Text(b"2024-01-01 10:00:00+02:00")).unwrap() {
            ColumnData::DateTimeOffset(Some(d)) => {
                assert_eq!(d.offset(), 120);
                assert_eq!(d.datetime2().time().increments(), 8 * 3600 * 10_000_000); // 08:00 UTC
            }
            o => panic!("{o:?}"),
        }
        match c.convert(us(2024, 1, 1, 10, 0, 0, 0)).unwrap() {
            ColumnData::DateTimeOffset(Some(d)) => { assert_eq!(d.offset(), 0); assert_eq!(d.datetime2().time().increments(), 10 * 3600 * 10_000_000); }
            o => panic!("{o:?}"),
        }
    }

    /// The spec's row shape (R5.6): 6 varchar, 2 char(3), 5 int, 1 decimal(8,5), 1 date, 2 datetime2.
    pub(crate) fn bench_table(con: &Connection, rows: i64) -> Vec<Converter> {
        duck::exec(con, &format!(
            "CREATE TABLE b AS SELECT
                'CUST' || lpad((i % 100000)::VARCHAR, 7, '0') AS v1, 'product-' || (i % 5000) AS v2, 'region_' || (i % 50) AS v3,
                'order line description ' || i AS v4, md5(i::VARCHAR) AS v5, CASE WHEN i % 10 = 0 THEN NULL ELSE 'note' END AS v6,
                'USA' AS c1, chr(65 + (i % 26)::INTEGER) || 'BC' AS c2,
                i::INTEGER AS i1, (i % 1000)::INTEGER AS i2, (i % 7)::INTEGER AS i3, (i * 31 % 100000)::INTEGER AS i4, (-i)::INTEGER AS i5,
                CAST((i % 10000000) * 0.00001 AS DECIMAL(8,5)) AS amt,
                DATE '2020-01-01' + (i % 2000)::INTEGER AS d,
                TIMESTAMP '2020-01-01' + to_microseconds(i * 997) AS t1, TIMESTAMP '2021-06-01 12:00:00' + to_microseconds(i * 13) AS t2
             FROM range({rows}) r(i)")).unwrap();
        bench_types().iter().map(|t| Converter::for_sql_type(t, "").unwrap()).collect()
    }

    pub(crate) fn bench_types() -> [&'static str; 17] {
        ["VARCHAR(20)", "VARCHAR(30)", "VARCHAR(20)", "VARCHAR(60)", "VARCHAR(32)", "VARCHAR(10)", "CHAR(3)", "CHAR(3)",
         "INT", "INT", "INT", "INT", "INT", "DECIMAL(8,5)", "DATE", "DATETIME2(7)", "DATETIME2(7)"]
    }

    /// Conversion throughput (DuckDB streaming read + Arrow -> TokenRow, no network):
    /// `cargo test --release --lib -- --ignored bench --nocapture` (rows: DUCKBRIDGE_BENCH_ROWS, default 2 M)
    #[test]
    #[ignore]
    fn bench_convert() {
        let rows: i64 = std::env::var("DUCKBRIDGE_BENCH_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(2_000_000);
        let con = duck::open_in_memory().unwrap();
        let convs = bench_table(&con, rows);
        {
            // baseline: the DuckDB streaming read alone
            let t0 = std::time::Instant::now();
            let mut stream = duck::stream(&con, "SELECT * FROM b").unwrap();
            let mut n = 0usize;
            while let Some(c) = stream.next_chunk().unwrap() { n += duck::arrow::array::Array::len(&c); }
            let secs = t0.elapsed().as_secs_f64();
            println!("bench_convert (DuckDB read only): {n} rows in {secs:.3} s = {:.0} rows/s", n as f64 / secs);
        }
        {
            let (label, sql) = ("all columns", "SELECT * FROM b");
            let t0 = std::time::Instant::now();
            let mut seen = 0usize;
            let n = crate::mssql::bulk::read_rows(&con, sql, &convs, 16_384, &mut |r| { seen += r.len(); drop(r); Ok(()) }).unwrap();
            let secs = t0.elapsed().as_secs_f64();
            assert_eq!(n as usize, seen);
            println!("bench_convert ({label}, 17 columns, rows dropped on the reader thread): {n} rows in {secs:.3} s = {:.0} rows/s", n as f64 / secs);
            // as in bulk_load: the reader hands batches through a bounded channel to a consumer
            // thread (there tiberius encodes and drops them; here they are only dropped)
            let t0 = std::time::Instant::now();
            let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<tiberius::TokenRow<'static>>>(4);
            let consumer = std::thread::spawn(move || rx.iter().map(|b| b.len()).sum::<usize>());
            let n = crate::mssql::bulk::read_rows(&con, sql, &convs, 16_384, &mut |r| { tx.send(r).map_err(|e| anyhow::anyhow!("{e}")) }).unwrap();
            drop(tx);
            assert_eq!(consumer.join().unwrap(), n as usize);
            let secs = t0.elapsed().as_secs_f64();
            println!("bench_convert ({label}, 17 columns, pipelined to a consumer thread): {n} rows in {secs:.3} s = {:.0} rows/s", n as f64 / secs);
        }
    }

    #[test]
    fn convert_guid_binary_text() {
        let g = "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8";
        match Converter::Guid.convert(ValueRef::Text(g.as_bytes())).unwrap() { ColumnData::Guid(Some(u)) => assert_eq!(u.to_string(), g), o => panic!("{o:?}") }
        assert!(Converter::Guid.convert(ValueRef::Text(b"nope")).is_err());
        let b = Converter::Binary { max_len: Some(3) };
        assert!(matches!(b.convert(ValueRef::Blob(&[1, 2, 3])).unwrap(), ColumnData::Binary(Some(_))));
        assert!(b.convert(ValueRef::Blob(&[1, 2, 3, 4])).is_err());
        assert_eq!(s(&Converter::Text.convert(dec(10, 3, -1500)).unwrap()), "-1.500");
        assert_eq!(s(&Converter::Text.convert(ValueRef::Boolean(true)).unwrap()), "true");
        assert_eq!(s(&Converter::Text.convert(ValueRef::Date32(0)).unwrap()), "1970-01-01");
        assert_eq!(s(&Converter::Text.convert(ValueRef::Blob(&[0xab, 0x01])).unwrap()), "ab01");
        assert!(matches!(Converter::Real.convert(ValueRef::Double(f64::NAN)).unwrap(), ColumnData::F32(None)));
        assert!(matches!(Converter::Float.convert(ValueRef::Int(3)).unwrap(), ColumnData::F64(Some(x)) if x == 3.0));
    }
}
