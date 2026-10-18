//! Data-type and date-part name translation between dialects.
use super::engine::{as_num, fmt, t};
use super::tokens::*;

/// T-SQL type (name + optional args) -> DuckDB type tokens.
pub fn tsql_type_to_duck(name: &str, args: &[Vec<Tok>]) -> Option<Vec<Tok>> {
    let n = name.to_uppercase();
    let arg_text = || args.iter().map(|a| join(trim(a))).collect::<Vec<_>>().join(",");
    Some(match n.as_str() {
        "INT" | "INTEGER" => t("INTEGER"),
        "BIT" => t("BOOLEAN"),
        "TINYINT" => t("UTINYINT"),
        "SMALLINT" => t("SMALLINT"),
        "BIGINT" => t("BIGINT"),
        "DECIMAL" | "NUMERIC" | "DEC" => if args.is_empty() { t("DECIMAL(18,0)") } else { t(&format!("DECIMAL({})", arg_text())) },
        "MONEY" => t("DECIMAL(19,4)"),
        "SMALLMONEY" => t("DECIMAL(10,4)"),
        "FLOAT" => t("DOUBLE"),
        "REAL" => t("FLOAT"),
        "DATE" => t("DATE"),
        "TIME" => t("TIME"),
        "DATETIME" | "DATETIME2" | "SMALLDATETIME" => t("TIMESTAMP"),
        "DATETIMEOFFSET" => t("TIMESTAMPTZ"),
        "CHAR" | "VARCHAR" | "NCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "SYSNAME" | "XML" | "SQL_VARIANT" => t("VARCHAR"),
        "JSON" => t("JSON"),
        "VECTOR" => t("FLOAT[]"),
        "BINARY" | "VARBINARY" | "IMAGE" | "ROWVERSION" | "TIMESTAMP" => t("BLOB"),
        "UNIQUEIDENTIFIER" => t("UUID"),
        "HIERARCHYID" | "GEOGRAPHY" | "GEOMETRY" => t("VARCHAR"),
        _ => return None,
    })
}

pub fn is_tsql_type(name: &str) -> bool {
    tsql_type_to_duck(name, &[]).is_some()
}

/// DuckDB type -> T-SQL type tokens (+ optional warning).
pub fn duck_type_to_tsql(name: &str, args: &[Vec<Tok>]) -> Option<(Vec<Tok>, Option<String>)> {
    duck_type_to_tsql_v(name, args, 2019)
}

pub fn duck_type_to_tsql_v(name: &str, args: &[Vec<Tok>], version: u16) -> Option<(Vec<Tok>, Option<String>)> {
    let n = name.to_uppercase();
    let arg_text = || args.iter().map(|a| join(trim(a))).collect::<Vec<_>>().join(",");
    let ok = |s: &str| Some((t(s), None));
    match n.as_str() {
        "BOOLEAN" | "BOOL" | "LOGICAL" => ok("BIT"),
        "TINYINT" | "INT1" => ok("SMALLINT"),
        "UTINYINT" => ok("TINYINT"),
        "SMALLINT" | "INT2" | "SHORT" => ok("SMALLINT"),
        "USMALLINT" => ok("INT"),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => ok("INT"),
        "UINTEGER" => ok("BIGINT"),
        "BIGINT" | "INT8" | "LONG" => ok("BIGINT"),
        "UBIGINT" => ok("DECIMAL(20,0)"),
        "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => Some((t("DECIMAL(38,0)"), Some(format!("{n} mapped to DECIMAL(38,0); values beyond 38 digits overflow")))),
        "FLOAT" | "FLOAT4" | "REAL" => ok("REAL"),
        "DOUBLE" | "FLOAT8" => ok("FLOAT"),
        "DECIMAL" | "NUMERIC" => if args.is_empty() { ok("DECIMAL(18,3)") } else { Some((t(&format!("DECIMAL({})", arg_text())), None)) },
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" | "NVARCHAR" => {
            if let Some(n) = args.first().and_then(|a| as_num(a)) { Some((t(&format!("NVARCHAR({n})")), None)) } else { ok("NVARCHAR(MAX)") }
        }
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => ok("VARBINARY(MAX)"),
        "DATE" => ok("DATE"),
        "TIME" => ok("TIME(7)"),
        "TIMESTAMP" | "DATETIME" => ok("DATETIME2(7)"),
        "TIMESTAMP_S" => ok("DATETIME2(7)"),
        "TIMESTAMP_MS" => ok("DATETIME2(7)"),
        "TIMESTAMP_NS" => ok("DATETIME2(7)"),
        "TIMESTAMPTZ" => ok("DATETIMEOFFSET(7)"),
        "UUID" => ok("UNIQUEIDENTIFIER"),
        "JSON" => if version >= 2025 { ok("json") } else { ok("NVARCHAR(MAX)") },
        "VARIANT" => Some((t(if version >= 2025 { "json" } else { "NVARCHAR(MAX)" }), Some("VARIANT mapped to JSON text".into()))),
        "GEOMETRY" => Some((t("NVARCHAR(MAX)"), Some("GEOMETRY mapped to WKT text; use geometry::STGeomFromText on SQL Server".into()))),
        "INTERVAL" => Some((t("NVARCHAR(64)"), Some("INTERVAL has no SQL Server equivalent; mapped to NVARCHAR(64)".into()))),
        "BIT" => Some((t("VARCHAR(MAX)"), Some("DuckDB BIT string mapped to VARCHAR(MAX)".into()))),
        _ => None,
    }
}

pub fn duck_type_to_pg(name: &str, args: &[Vec<Tok>]) -> Option<(Vec<Tok>, Option<String>)> {
    let n = name.to_uppercase();
    let ok = |s: &str| Some((t(s), None));
    match n.as_str() {
        "TINYINT" | "UTINYINT" | "INT1" => ok("smallint"),
        "USMALLINT" => ok("integer"),
        "UINTEGER" => ok("bigint"),
        "UBIGINT" => ok("numeric(20,0)"),
        "HUGEINT" | "UHUGEINT" => ok("numeric(39,0)"),
        "VARINT" | "BIGNUM" => ok("numeric"),
        "VARIANT" => Some((t("jsonb"), Some("VARIANT mapped to jsonb".into()))),
        "FLOAT" | "FLOAT4" => ok("real"),
        "DOUBLE" | "FLOAT8" => ok("double precision"),
        "STRING" | "TEXT" => ok("text"),
        "VARCHAR" => if args.is_empty() { ok("text") } else { None },
        "BLOB" | "BYTEA" => ok("bytea"),
        "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS" => Some((t("timestamp"), Some(format!("{n} mapped to timestamp (microseconds)")))),
        "TIMESTAMPTZ" => ok("timestamp with time zone"),
        "JSON" => ok("jsonb"),
        "STRUCT" | "MAP" | "UNION" => Some((t("jsonb"), Some(format!("{n} mapped to jsonb")))),
        _ => None,
    }
}

pub fn pg_type_to_duck(name: &str, _args: &[Vec<Tok>]) -> Option<(Vec<Tok>, Option<String>)> {
    let n = name.to_uppercase();
    let ok = |s: &str| Some((t(s), None));
    match n.as_str() {
        "SERIAL" | "SERIAL4" => Some((t("INTEGER"), Some("serial mapped to INTEGER; add a sequence default".into()))),
        "BIGSERIAL" | "SERIAL8" => Some((t("BIGINT"), Some("bigserial mapped to BIGINT; add a sequence default".into()))),
        "SMALLSERIAL" => Some((t("SMALLINT"), Some("smallserial mapped to SMALLINT".into()))),
        "BYTEA" => ok("BLOB"),
        "JSONB" => ok("JSON"),
        "MONEY" => ok("DECIMAL(19,2)"),
        "INET" | "CIDR" | "MACADDR" | "TSVECTOR" | "XML" | "CITEXT" | "NAME" => Some((t("VARCHAR"), Some(format!("{n} mapped to VARCHAR")))),
        _ => None,
    }
}

/// Normalise a T-SQL date part (dd, mm, yyyy, hh, mi, ...) to the canonical DuckDB / standard name.
pub fn datepart(part: &str) -> Option<&'static str> {
    Some(match part.to_lowercase().as_str() {
        "year" | "yy" | "yyyy" | "years" => "year",
        "quarter" | "qq" | "q" => "quarter",
        "month" | "mm" | "m" | "months" => "month",
        "dayofyear" | "dy" | "y" | "doy" => "dayofyear",
        "day" | "dd" | "d" | "days" => "day",
        "week" | "wk" | "ww" | "weeks" => "week",
        "iso_week" | "isowk" | "isoww" => "isoweek",
        "weekday" | "dw" | "w" | "dayofweek" | "isodow" => "dayofweek",
        "hour" | "hh" | "hours" => "hour",
        "minute" | "mi" | "n" | "minutes" => "minute",
        "second" | "ss" | "s" | "seconds" => "second",
        "millisecond" | "ms" | "milliseconds" => "millisecond",
        "microsecond" | "mcs" | "us" | "microseconds" => "microsecond",
        "nanosecond" | "ns" => "nanosecond",
        "tzoffset" | "tz" => "timezone",
        _ => return None,
    })
}

/// DuckDB canonical part -> T-SQL DATEPART/DATEADD keyword.
pub fn tsql_part(part: &str) -> &'static str {
    match part {
        "year" => "YEAR", "quarter" => "QUARTER", "month" => "MONTH", "dayofyear" => "DAYOFYEAR", "day" => "DAY",
        "week" => "WEEK", "isoweek" => "ISO_WEEK", "dayofweek" => "WEEKDAY", "hour" => "HOUR", "minute" => "MINUTE",
        "second" => "SECOND", "millisecond" => "MILLISECOND", "microsecond" => "MICROSECOND", "nanosecond" => "NANOSECOND",
        "timezone" => "TZOFFSET", _ => "DAY",
    }
}

/// DuckDB `to_xxx()` interval helper for a canonical part, applied to `n` tokens.
pub fn duck_interval(part: &str, n: &[Tok]) -> Option<Vec<Tok>> {
    Some(match part {
        "year" => fmt("to_years(CAST({} AS BIGINT))", &[n]),
        "quarter" => fmt("to_months(3 * CAST({} AS BIGINT))", &[n]),
        "month" => fmt("to_months(CAST({} AS BIGINT))", &[n]),
        "week" => fmt("to_weeks(CAST({} AS BIGINT))", &[n]),
        "day" | "dayofyear" | "dayofweek" => fmt("to_days(CAST({} AS BIGINT))", &[n]),
        "hour" => fmt("to_hours(CAST({} AS BIGINT))", &[n]),
        "minute" => fmt("to_minutes(CAST({} AS BIGINT))", &[n]),
        "second" => fmt("to_seconds(CAST({} AS BIGINT))", &[n]),
        "millisecond" => fmt("to_milliseconds(CAST({} AS BIGINT))", &[n]),
        "microsecond" => fmt("to_microseconds(CAST({} AS BIGINT))", &[n]),
        _ => return None,
    })
}

/// .NET-style date format (yyyy-MM-dd HH:mm:ss) -> strftime format.
pub fn dotnet_to_strftime(f: &str) -> String {
    let mut out = String::new();
    let cs: Vec<char> = f.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        let mut n = 0;
        while i + n < cs.len() && cs[i + n] == c { n += 1; }
        let rep = match (c, n) {
            ('y', 4) | ('y', 3) => Some("%Y"), ('y', 2) | ('y', 1) => Some("%y"),
            ('M', 4) => Some("%B"), ('M', 3) => Some("%b"), ('M', 2) => Some("%m"), ('M', 1) => Some("%-m"),
            ('d', 4) => Some("%A"), ('d', 3) => Some("%a"), ('d', 2) => Some("%d"), ('d', 1) => Some("%-d"),
            ('H', 2) => Some("%H"), ('H', 1) => Some("%-H"), ('h', 2) => Some("%I"), ('h', 1) => Some("%-I"),
            ('m', 2) => Some("%M"), ('m', 1) => Some("%-M"), ('s', 2) => Some("%S"), ('s', 1) => Some("%-S"),
            ('f', 3) => Some("%g"), ('f', 6) => Some("%f"), ('f', 7) | ('f', 9) => Some("%n"),
            ('t', 2) => Some("%p"), ('z', _) => Some("%z"),
            _ => None,
        };
        match rep {
            Some(r) => { out.push_str(r); i += n; }
            None => { if c == '%' { out.push_str("%%"); } else { out.push(c); } i += 1; }
        }
    }
    out
}

/// strftime format -> .NET-style FORMAT() string. Second value is true when a specifier had no equivalent.
pub fn strftime_to_dotnet(f: &str) -> (String, bool) {
    let mut out = String::new();
    let mut lossy = false;
    let cs: Vec<char> = f.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        if cs[i] == '%' && i + 1 < cs.len() {
            let (spec, adv) = if cs[i + 1] == '-' && i + 2 < cs.len() { (cs[i + 2], 3) } else { (cs[i + 1], 2) };
            let rep = match spec {
                'Y' => "yyyy", 'y' => "yy", 'm' => "MM", 'd' => "dd", 'H' => "HH", 'I' => "hh", 'M' => "mm", 'S' => "ss",
                'f' => "ffffff", 'g' => "fff", 'n' => "fffffff", 'p' => "tt", 'B' => "MMMM", 'b' => "MMM", 'A' => "dddd", 'a' => "ddd",
                'z' => "zzz", '%' => "%", _ => { lossy = true; "" }
            };
            out.push_str(rep);
            i += adv;
        } else {
            let c = cs[i];
            if c.is_ascii_alphabetic() { out.push('\\'); }
            out.push(c);
            i += 1;
        }
    }
    (out, lossy)
}

/// T-SQL CONVERT style number -> strftime format.
pub fn convert_style(style: &str) -> Option<&'static str> {
    Some(match style {
        "101" => "%m/%d/%Y", "102" => "%Y.%m.%d", "103" => "%d/%m/%Y", "104" => "%d.%m.%Y", "105" => "%d-%m-%Y",
        "110" => "%m-%d-%Y", "111" => "%Y/%m/%d", "112" => "%Y%m%d", "120" => "%Y-%m-%d %H:%M:%S",
        "121" => "%Y-%m-%d %H:%M:%S.%g", "126" => "%Y-%m-%dT%H:%M:%S.%g", "127" => "%Y-%m-%dT%H:%M:%S.%gZ",
        "23" => "%Y-%m-%d", "1" => "%m/%d/%y", "3" => "%d/%m/%y", "8" | "108" => "%H:%M:%S", "14" | "114" => "%H:%M:%S.%g",
        "20" => "%Y-%m-%d %H:%M:%S", "21" | "25" => "%Y-%m-%d %H:%M:%S.%g",
        _ => return None,
    })
}
