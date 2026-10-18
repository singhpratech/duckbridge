//! DuckDB -> SQL Server 2019 / PostgreSQL type mapping. Every lossy decision is recorded
//! on the column as a warning so users see the risk before committing.
use super::model::{Column, Conv};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Mssql,
    Postgres,
}

/// SQL Server version features used by the mapper (2019 baseline).
pub fn mssql_version() -> u16 { MSSQL_VERSION.load(std::sync::atomic::Ordering::Relaxed) }
pub fn set_mssql_version(v: u16) { MSSQL_VERSION.store(v, std::sync::atomic::Ordering::Relaxed); }
static MSSQL_VERSION: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(2019);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringMode {
    /// NVARCHAR(n) sized from measured data, rounded up to a bucket.
    Fit,
    /// Always NVARCHAR(MAX) (keys are still sized).
    Max,
}

const BUCKETS: [usize; 7] = [50, 100, 255, 500, 1000, 2000, 4000];

fn bucket(n: usize) -> Option<usize> {
    BUCKETS.iter().copied().find(|b| n <= *b)
}

/// Split `DECIMAL(18,3)` -> ("DECIMAL", "18,3"); `INTEGER[]` -> ("LIST", "INTEGER"); `INTEGER[3]` -> ("ARRAY", ..).
pub fn parse_type(t: &str) -> (String, String) {
    let t = t.trim();
    if let Some(inner) = t.strip_suffix("[]") {
        return ("LIST".into(), inner.into());
    }
    if t.ends_with(']') {
        if let Some(i) = t.rfind('[') {
            if t[i + 1..t.len() - 1].chars().all(|c| c.is_ascii_digit()) {
                return ("ARRAY".into(), t[..i].into());
            }
        }
    }
    if let Some(i) = t.find('(') {
        if t.ends_with(')') {
            return (t[..i].trim().to_uppercase(), t[i + 1..t.len() - 1].to_string());
        }
    }
    (t.to_uppercase(), String::new())
}

pub fn quote_duck(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn map_column(col: &mut Column, is_key: bool, mode: StringMode, target: Target) {
    let q = quote_duck(&col.name);
    col.select_expr = q.clone();
    col.conv = Conv::Identity;
    let (base, arg) = parse_type(&col.duck_type);
    if target == Target::Postgres {
        map_pg(col, &base, &arg, &q);
        return;
    }
    let w = &mut col.warnings;
    let (ty, conv): (String, Conv) = match base.as_str() {
        "BOOLEAN" => ("BIT".into(), Conv::Bool),
        "TINYINT" => ("SMALLINT".into(), Conv::Identity), // DuckDB TINYINT is signed
        "UTINYINT" => ("TINYINT".into(), Conv::Identity),
        "SMALLINT" => ("SMALLINT".into(), Conv::Identity),
        "USMALLINT" | "INTEGER" => (if base == "INTEGER" { "INT" } else { "INT" }.into(), Conv::Identity),
        "UINTEGER" | "BIGINT" => ("BIGINT".into(), Conv::Identity),
        "UBIGINT" => ("DECIMAL(20,0)".into(), Conv::BigDecimal),
        "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => {
            // max_len is measured as digits without sign; 38 digits is SQL Server's limit
            let digits = col.max_len.unwrap_or(0);
            if digits > 38 {
                w.push(format!("{base} has values beyond 38 digits (max {digits} chars); stored as NVARCHAR text to avoid loss"));
                col.select_expr = format!("CAST({q} AS VARCHAR)");
                (format!("NVARCHAR({})", bucket(digits).unwrap_or(4000)), Conv::Text)
            } else {
                w.push(format!("{base} mapped to DECIMAL(38,0)"));
                ("DECIMAL(38,0)".into(), Conv::BigDecimal)
            }
        }
        "FLOAT" => ("REAL".into(), Conv::Float),
        "DOUBLE" => ("FLOAT(53)".into(), Conv::Float),
        "DECIMAL" => {
            let mut it = arg.split(',').map(|s| s.trim().parse::<u32>().unwrap_or(18));
            let p = it.next().unwrap_or(18).min(38);
            let s = it.next().unwrap_or(3);
            (format!("DECIMAL({p},{s})"), Conv::Identity)
        }
        "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => (nvarchar(col.max_len, is_key, mode, &mut col.warnings), Conv::Text),
        "BLOB" | "BYTEA" => {
            if is_key {
                w.push("BLOB used in a key: sized VARBINARY(900)".into());
                ("VARBINARY(900)".into(), Conv::Bytes)
            } else {
                ("VARBINARY(MAX)".into(), Conv::Bytes)
            }
        }
        "DATE" => ("DATE".into(), Conv::Date),
        "TIME" => ("TIME(7)".into(), Conv::Time),
        "TIME WITH TIME ZONE" => {
            w.push("TIME WITH TIME ZONE: offset dropped (SQL Server has no TIMETZ)".into());
            col.select_expr = format!("CAST({q} AS TIME)");
            ("TIME(7)".into(), Conv::Time)
        }
        "TIMESTAMP" => ("DATETIME2(7)".into(), Conv::DateTime),
        "TIMESTAMP_S" => ("DATETIME2(7)".into(), Conv::DateTime),
        "TIMESTAMP_MS" => ("DATETIME2(7)".into(), Conv::DateTime),
        "TIMESTAMP_NS" => {
            w.push("TIMESTAMP_NS truncated to 7 fractional digits (DATETIME2(7))".into());
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("DATETIME2(7)".into(), Conv::TsNsText)
        }
        "TIMESTAMP WITH TIME ZONE" => ("DATETIMEOFFSET(7)".into(), Conv::DateTimeTz),
        "INTERVAL" => {
            w.push("INTERVAL stored as text (SQL Server has no interval type)".into());
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("NVARCHAR(64)".into(), Conv::Text)
        }
        "UUID" => ("UNIQUEIDENTIFIER".into(), Conv::Uuid),
        "JSON" => {
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            (if mssql_version() >= 2025 { "json" } else { "NVARCHAR(MAX)" }.into(), Conv::Text)
        }
        "VARIANT" => {
            w.push("VARIANT serialised to JSON text".into());
            col.select_expr = format!("to_json({q})");
            (if mssql_version() >= 2025 { "json" } else { "NVARCHAR(MAX)" }.into(), Conv::Text)
        }
        "GEOMETRY" => {
            w.push("GEOMETRY stored as WKT text (NVARCHAR); convert with geometry::STGeomFromText on SQL Server".into());
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("NVARCHAR(MAX)".into(), Conv::Text)
        }
        "ARRAY" if mssql_version() >= 2025 && arg.to_uppercase().trim() == "FLOAT" || (base == "ARRAY" && mssql_version() >= 2025 && arg.to_uppercase().trim() == "DOUBLE") => {
            // FLOAT[n] -> VECTOR(n) on SQL Server 2025
            let n: usize = col.duck_type.rsplit('[').next().unwrap_or("").trim_end_matches(']').parse().unwrap_or(0);
            if (1..=1998).contains(&n) {
                col.select_expr = format!("to_json({q})");
                (format!("VECTOR({n})"), Conv::Text)
            } else {
                w.push("fixed-size array serialised to JSON text (VECTOR supports 1..1998 float32 dims)".into());
                col.select_expr = format!("to_json({q})");
                ("NVARCHAR(MAX)".into(), Conv::Text)
            }
        }
        "LIST" | "ARRAY" | "STRUCT" | "MAP" | "UNION" => {
            w.push(format!("{base} serialised to JSON text{}", if mssql_version() >= 2025 { " (json type)" } else { "" }));
            col.select_expr = format!("to_json({q})");
            (if mssql_version() >= 2025 { "json" } else { "NVARCHAR(MAX)" }.into(), Conv::Text)
        }
        "ENUM" => {
            let vals = enum_values(&arg);
            let n = vals.iter().map(|v| v.chars().count()).max().unwrap_or(1).max(1);
            col.enum_values = Some(vals);
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            (format!("NVARCHAR({})", bucket(n).map(|b| b.to_string()).unwrap_or("MAX".into())), Conv::Text)
        }
        "BIT" => {
            w.push("BIT string stored as '0'/'1' text".into());
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("VARCHAR(MAX)".into(), Conv::Text)
        }
        _ => {
            w.push(format!("unknown DuckDB type {:?} stored as NVARCHAR(MAX)", col.duck_type));
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("NVARCHAR(MAX)".into(), Conv::Text)
        }
    };
    col.sql_type = ty;
    col.conv = conv;
    if is_key && col.sql_type.ends_with("(MAX)") {
        col.sql_type = col.sql_type.replace("(MAX)", "(450)");
        col.warnings.push("MAX type used in key/index: sized to 450 characters".into());
    }
}

fn nvarchar(max_len: Option<usize>, is_key: bool, mode: StringMode, w: &mut Vec<String>) -> String {
    if is_key {
        let n = max_len.unwrap_or(1).max(1);
        return match bucket(n) {
            Some(b) if b <= 450 => format!("NVARCHAR({b})"),
            _ => {
                w.push("string key column longer than 450 chars; sized NVARCHAR(450), may fail".into());
                "NVARCHAR(450)".into()
            }
        };
    }
    match (mode, max_len) {
        (StringMode::Max, _) | (_, None) => "NVARCHAR(MAX)".into(),
        (StringMode::Fit, Some(n)) => bucket(n.max(1)).map(|b| format!("NVARCHAR({b})")).unwrap_or("NVARCHAR(MAX)".into()),
    }
}

pub fn enum_values(arg: &str) -> Vec<String> {
    // ENUM('a', 'b''c')
    let mut out = Vec::new();
    let chars: Vec<char> = arg.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\'' {
            let mut s = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                s.push(chars[i]);
                i += 1;
            }
            out.push(s);
        }
        i += 1;
    }
    out
}

fn map_pg(col: &mut Column, base: &str, arg: &str, q: &str) {
    let (ty, conv): (String, Conv) = match base {
        "BOOLEAN" => ("boolean".into(), Conv::Bool),
        "TINYINT" | "UTINYINT" | "SMALLINT" => ("smallint".into(), Conv::Identity),
        "USMALLINT" | "INTEGER" => ("integer".into(), Conv::Identity),
        "UINTEGER" | "BIGINT" => ("bigint".into(), Conv::Identity),
        "UBIGINT" => ("numeric(20,0)".into(), Conv::BigDecimal),
        "HUGEINT" | "UHUGEINT" => ("numeric(39,0)".into(), Conv::BigDecimal),
        "VARINT" | "BIGNUM" => ("numeric".into(), Conv::BigDecimal),
        "VARIANT" => { col.select_expr = format!("to_json({q})"); ("jsonb".into(), Conv::Text) }
        "GEOMETRY" => { col.warnings.push("GEOMETRY stored as WKT text".into()); col.select_expr = format!("CAST({q} AS VARCHAR)"); ("text".into(), Conv::Text) }
        "FLOAT" => ("real".into(), Conv::Float),
        "DOUBLE" => ("double precision".into(), Conv::Float),
        "DECIMAL" => (if arg.is_empty() { "numeric(18,3)".into() } else { format!("numeric({arg})") }, Conv::Identity),
        "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => ("text".into(), Conv::Text),
        "BLOB" | "BYTEA" => ("bytea".into(), Conv::Bytes),
        "DATE" => ("date".into(), Conv::Date),
        "TIME" => ("time(6)".into(), Conv::Time),
        "TIME WITH TIME ZONE" => {
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("time(6) with time zone".into(), Conv::Text)
        }
        "TIMESTAMP" => ("timestamp(6)".into(), Conv::DateTime),
        "TIMESTAMP_S" => ("timestamp(0)".into(), Conv::DateTime),
        "TIMESTAMP_MS" => ("timestamp(3)".into(), Conv::DateTime),
        "TIMESTAMP_NS" => {
            col.warnings.push("TIMESTAMP_NS truncated to microseconds".into());
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("timestamp(6)".into(), Conv::TsNsText)
        }
        "TIMESTAMP WITH TIME ZONE" => ("timestamp(6) with time zone".into(), Conv::DateTimeTz),
        "INTERVAL" => {
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("interval".into(), Conv::Text)
        }
        "UUID" => ("uuid".into(), Conv::Uuid),
        "JSON" => {
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("jsonb".into(), Conv::Text)
        }
        "LIST" | "ARRAY" | "STRUCT" | "MAP" | "UNION" => {
            col.warnings.push(format!("{base} serialised to jsonb"));
            col.select_expr = format!("to_json({q})");
            ("jsonb".into(), Conv::Text)
        }
        "ENUM" => {
            col.enum_values = Some(enum_values(arg));
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("text".into(), Conv::Text)
        }
        "BIT" => {
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("bit varying".into(), Conv::Text)
        }
        _ => {
            col.warnings.push(format!("unknown DuckDB type {:?} stored as text", col.duck_type));
            col.select_expr = format!("CAST({q} AS VARCHAR)");
            ("text".into(), Conv::Text)
        }
    };
    col.sql_type = ty;
    col.conv = conv;
}

/// SQL Server system type -> DuckDB type (used by the importer).
pub fn mssql_to_duck(type_name: &str, max_length: i16, precision: u8, scale: u8) -> (String, Option<String>) {
    let t = type_name.to_lowercase();
    let w = |s: &str| Some(s.to_string());
    match t.as_str() {
        "bit" => ("BOOLEAN".into(), None),
        "tinyint" => ("UTINYINT".into(), None),
        "smallint" => ("SMALLINT".into(), None),
        "int" => ("INTEGER".into(), None),
        "bigint" => ("BIGINT".into(), None),
        "decimal" | "numeric" => (format!("DECIMAL({precision},{scale})"), None),
        "money" => ("DECIMAL(19,4)".into(), None),
        "smallmoney" => ("DECIMAL(10,4)".into(), None),
        "float" => ("DOUBLE".into(), None),
        "real" => ("FLOAT".into(), None),
        "date" => ("DATE".into(), None),
        "time" => ("TIME".into(), if scale > 6 { w("time(7) has 100ns precision; DuckDB TIME keeps microseconds") } else { None }),
        "json" => ("JSON".into(), None),
        "vector" => ("FLOAT[]".into(), w("vector converted to FLOAT[] via its JSON text form")),
        "datetime" | "smalldatetime" => ("TIMESTAMP".into(), None),
        "datetime2" => ("TIMESTAMP".into(), if scale > 6 { w("datetime2(7) has 100ns precision; DuckDB TIMESTAMP keeps microseconds (digit 7 truncated)") } else { None }),
        "datetimeoffset" => ("TIMESTAMP WITH TIME ZONE".into(), w("datetimeoffset converted to a UTC instant; the original offset is not stored")),
        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext" | "sysname" => ("VARCHAR".into(), None),
        "xml" => ("VARCHAR".into(), w("xml stored as text")),
        "binary" | "varbinary" | "image" => ("BLOB".into(), None),
        "timestamp" | "rowversion" => ("BLOB".into(), w("rowversion copied as 8-byte BLOB; not regenerated")),
        "uniqueidentifier" => ("UUID".into(), None),
        "hierarchyid" | "geography" | "geometry" => ("VARCHAR".into(), w(&format!("{t} converted with .ToString()"))),
        "sql_variant" => ("VARCHAR".into(), w("sql_variant converted to text")),
        _ => {
            let _ = max_length;
            ("VARCHAR".into(), w(&format!("unknown SQL Server type {t}; stored as VARCHAR")))
        }
    }
}
