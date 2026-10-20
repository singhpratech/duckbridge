//! Verification after a load (spec R9): row counts, per-column checksums, row diffs and index
//! completeness.
//!
//! Both engines render every value in the same **canonical text form** (see [`Canon`]), hash its
//! UTF-8 bytes with MD5, read the first 8 bytes as an unsigned 64-bit integer and sum those per
//! column as `DECIMAL(38,0)` (SQL Server) / `HUGEINT` (DuckDB). The sums are additive, so a
//! partitioned table is checksummed in one scan per engine (grouped by partition) and the groups
//! are combined in Rust.
//!
//! SQL Server side: `HASHBYTES('MD5', …)` of `varchar` text. ASCII-only canonical forms (numbers,
//! dates, GUIDs, hex) hash the same bytes in any code page; string columns are converted to UTF-8
//! bytes through `nvarchar(max)` and the `Latin1_General_100_BIN2_UTF8` collation, which needs
//! SQL Server 2019 or later. DuckDB side: `md5()` of the VARCHAR (UTF-8) text.
use crate::core::model::{Column, Table};
use crate::core::runlog::{self, Failure};
use crate::core::typemap::quote_duck;
use crate::mssql::target::{self, TableMapping, TargetColumn, TargetTable};
use crate::mssql::{bracket, nstr, MsClient};
use anyhow::{Context, Result};
use crate::core::duck::Connection;
use futures_util::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

/// Text that stands for NULL in the canonical form (U+0001 followed by `NULL`).
pub const NULL_SENTINEL: &str = "\u{1}NULL";
/// Column separator inside a canonical row (U+001F, unit separator).
const SEP: char = '\u{1f}';
/// Rows fetched per side for a row diff; bigger tables/partitions are sampled by key hash bucket.
pub const MAX_DIFF_ROWS: i64 = 1_000_000;
/// Partition-column values looked up on SQL Server to split the DuckDB side by partition.
const MAX_PARTITION_VALUES: usize = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyLevel {
    /// Row counts per table and partition (always part of every level).
    Counts,
    /// Counts plus per-column checksums.
    Checksum,
    /// Checksums plus, for each mismatching table/partition, up to `sample` differing rows each way.
    Except { sample: usize },
}

impl VerifyLevel {
    /// `counts`, `checksum`, `except` (10 sample rows) or `except:<n>`.
    pub fn parse(s: &str) -> Result<VerifyLevel> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "counts" | "count" => Ok(VerifyLevel::Counts),
            "checksum" | "checksums" => Ok(VerifyLevel::Checksum),
            "except" | "diff" => Ok(VerifyLevel::Except { sample: 10 }),
            _ => {
                if let Some(n) = s.strip_prefix("except:") {
                    let sample = n.trim().parse::<usize>().with_context(|| format!("--verify except:<n>: bad sample size {n:?}"))?;
                    return Ok(VerifyLevel::Except { sample });
                }
                anyhow::bail!("unknown --verify level {s:?} (expected counts, checksum or except)")
            }
        }
    }

    /// Apply `--sample n` to `except` (no effect on the other levels).
    pub fn with_sample(self, n: usize) -> VerifyLevel {
        match self { VerifyLevel::Except { .. } => VerifyLevel::Except { sample: n }, l => l }
    }

    fn checksums(self) -> bool { !matches!(self, VerifyLevel::Counts) }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyResult {
    pub table: String,
    pub partition: Option<i32>,
    pub ok: bool,
    pub detail: String,
}

impl std::fmt::Display for VerifyResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", if self.ok { "OK  " } else { "FAIL" }, self.table)?;
        if let Some(p) = self.partition { write!(f, " partition {p}")?; }
        write!(f, ": {}", self.detail)
    }
}

/// `Err(Failure::Check)` (exit 1) listing every failed result; `Ok` when all passed.
pub fn check_results(results: &[VerifyResult]) -> Result<()> {
    let bad: Vec<String> = results.iter().filter(|r| !r.ok).map(|r| r.to_string()).collect();
    if bad.is_empty() { return Ok(()); }
    Err(Failure::Check(format!("verification failed ({} problem(s)):\n{}", bad.len(), bad.join("\n"))).into())
}

// ---- canonical text form ---------------------------------------------------------------------

/// Canonical text rule per SQL Server target type (the target type decides, whatever the DuckDB
/// type is: a DOUBLE loaded into `decimal(10,2)` is rendered as `CAST(x AS DECIMAL(10,2))`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Canon {
    /// tinyint/smallint/int/bigint: plain digits.
    Int,
    /// bit: `0` / `1`.
    Bit,
    /// decimal/numeric/money/smallmoney: plain text at the target scale, no exponent.
    Decimal { precision: u8, scale: u8 },
    /// char/nchar: right-trimmed (spaces).
    Char,
    /// varchar/nvarchar/text/ntext/sysname: as is.
    Str,
    /// date: `yyyy-mm-dd`.
    Date,
    /// datetime2(n): `yyyy-mm-dd hh:mi:ss[.f…]` with n digits (rounded half up like SQL Server).
    DateTime2 { scale: u8 },
    /// datetime: 3 digits after rounding to 1/300 s.
    DateTime,
    /// smalldatetime: rounded half up to the minute, `:00` seconds.
    SmallDateTime,
    /// time(n): `hh:mi:ss[.f…]`.
    Time { scale: u8 },
    /// datetimeoffset(n): the UTC instant as datetime2(n) text.
    DateTimeOffset { scale: u8 },
    /// uniqueidentifier: lower-case text.
    Guid,
    /// binary/varbinary/image: lower-case hex; `binary(n)` zero-padded to n bytes.
    Binary { fixed: Option<usize> },
    /// float/real/xml/sql_variant/spatial/hierarchyid: only NULL vs non-NULL is compared.
    NullOnly,
}

impl Canon {
    pub fn of(t: &TargetColumn) -> Canon {
        match t.type_name.to_ascii_lowercase().as_str() {
            "tinyint" | "smallint" | "int" | "bigint" => Canon::Int,
            "bit" => Canon::Bit,
            "decimal" | "numeric" => Canon::Decimal { precision: t.precision.clamp(1, 38), scale: t.scale },
            "money" => Canon::Decimal { precision: 19, scale: 4 },
            "smallmoney" => Canon::Decimal { precision: 10, scale: 4 },
            "char" | "nchar" => Canon::Char,
            "varchar" | "nvarchar" | "text" | "ntext" | "sysname" => Canon::Str,
            "date" => Canon::Date,
            "datetime2" => Canon::DateTime2 { scale: t.scale.min(7) },
            "datetime" => Canon::DateTime,
            "smalldatetime" => Canon::SmallDateTime,
            "time" => Canon::Time { scale: t.scale.min(7) },
            "datetimeoffset" => Canon::DateTimeOffset { scale: t.scale.min(7) },
            "uniqueidentifier" => Canon::Guid,
            "binary" => Canon::Binary { fixed: (t.max_length > 0).then_some(t.max_length as usize) },
            "varbinary" | "image" => Canon::Binary { fixed: None },
            _ => Canon::NullOnly,
        }
    }

    /// String columns hash their UTF-8 bytes (SQL Server 2019+ UTF-8 collation on the target side).
    pub fn is_string(self) -> bool { matches!(self, Canon::Char | Canon::Str) }

    /// Part of the canonical row text for `except` (NULL-only columns are left out).
    pub fn in_row_text(self) -> bool { self != Canon::NullOnly }
}

fn duck_str(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// `floor((v + half) / k) * k` in exact integer arithmetic (also for negative v).
fn duck_round_to(v: &str, k: i64, half: i64) -> String {
    if k <= 1 { return v.to_string(); }
    let a = format!("({v} + {half})");
    format!("({a} - ((({a} % {k}) + {k}) % {k}))")
}

/// `floor(a / k)` in exact integer arithmetic.
fn duck_floor_div(a: &str, k: i64) -> String {
    format!("(({a} - ((({a} % {k}) + {k}) % {k})) // {k})")
}

fn duck_is_ns(dt: &str) -> bool { dt.starts_with("TIMESTAMP_NS") || dt.starts_with("TIMESTAMP(9)") }

/// Epoch microseconds of a DuckDB value (UTC for TIMESTAMPTZ).
fn duck_epoch_us(x: &str, dt: &str) -> String {
    if dt.starts_with("TIMESTAMP") || dt == "DATE" { format!("epoch_us({x})") } else { format!("epoch_us(CAST({x} AS TIMESTAMP))") }
}

/// Timestamp text with `n` fractional digits, rounded half up at the n-th digit. `fmt_base` is the
/// strftime format without fraction and `base_len` its rendered length.
fn duck_ts_text(x: &str, dt: &str, n: u8, fmt_base: &str, base_len: usize) -> String {
    let n = n as usize;
    let (epoch, make, frac, digits) = if duck_is_ns(dt) { (format!("epoch_ns({x})"), "make_timestamp_ns", "%n", 9usize) } else { (duck_epoch_us(x, dt), "make_timestamp", "%f", 6usize) };
    if n >= digits {
        let pad = "0".repeat(n - digits);
        let s = format!("strftime({make}({epoch}), '{fmt_base}.{frac}')");
        return if pad.is_empty() { s } else { format!("({s} || '{pad}')") };
    }
    let k = 10i64.pow((digits - n) as u32);
    let r = duck_round_to(&epoch, k, k / 2);
    let len = if n == 0 { base_len } else { base_len + 1 + n };
    format!("substr(strftime({make}({r}), '{fmt_base}.{frac}'), 1, {len})")
}

/// DuckDB VARCHAR expression of the canonical text (NULL stays NULL).
pub fn duck_canon(kind: Canon, col: &Column) -> String {
    let x = quote_duck(&col.name);
    let dt = col.duck_type.trim().to_ascii_uppercase();
    match kind {
        Canon::Int => format!("CAST(CAST({x} AS BIGINT) AS VARCHAR)"),
        Canon::Bit => format!("CAST(CAST(CAST({x} AS BOOLEAN) AS INTEGER) AS VARCHAR)"),
        Canon::Decimal { precision, scale } => format!("CAST(CAST({x} AS DECIMAL({precision},{scale})) AS VARCHAR)"),
        Canon::Char => format!("rtrim(CAST({x} AS VARCHAR))"),
        Canon::Str => if dt == "VARCHAR" { x } else { format!("CAST({x} AS VARCHAR)") },
        Canon::Date => {
            if dt == "DATE" { format!("strftime({x}, '%Y-%m-%d')") }
            else if dt.starts_with("TIMESTAMP") { format!("strftime(make_timestamp(epoch_us({x})), '%Y-%m-%d')") }
            else { format!("strftime(CAST({x} AS DATE), '%Y-%m-%d')") }
        }
        Canon::DateTime2 { scale } | Canon::DateTimeOffset { scale } => duck_ts_text(&x, &dt, scale, "%Y-%m-%d %H:%M:%S", 19),
        Canon::DateTime => {
            // SQL Server datetime ticks are 1/300 s: round to the nearest tick, render milliseconds
            let us = if duck_is_ns(&dt) { format!("epoch_us({x})") } else { duck_epoch_us(&x, &dt) };
            let ticks = duck_floor_div(&format!("({us} * 3 + 5000)"), 10000);
            let ms = duck_floor_div(&format!("({ticks} * 10 + 1)"), 3);
            format!("substr(strftime(make_timestamp({ms} * 1000), '%Y-%m-%d %H:%M:%S.%f'), 1, 23)")
        }
        Canon::SmallDateTime => {
            // half up at 30 s, as SQL Server converts datetime2 to smalldatetime
            let us = if duck_is_ns(&dt) { format!("epoch_us({x})") } else { duck_epoch_us(&x, &dt) };
            format!("strftime(make_timestamp({}), '%Y-%m-%d %H:%M:%S')", duck_round_to(&us, 60_000_000, 30_000_000))
        }
        Canon::Time { scale } => {
            let ts = format!("(DATE '1970-01-01' + CAST({x} AS TIME))");
            duck_ts_text(&ts, "TIMESTAMP", scale, "%H:%M:%S", 8)
        }
        Canon::Guid => format!("lower(CAST({x} AS VARCHAR))"),
        Canon::Binary { fixed } => {
            let h = if dt == "BLOB" { format!("lower(hex({x}))") } else { format!("lower(hex(CAST({x} AS BLOB)))") };
            match fixed { Some(n) => format!("rpad({h}, {}, '0')", n * 2), None => h }
        }
        Canon::NullOnly => format!("CASE WHEN {x} IS NULL THEN NULL ELSE 'v' END"),
    }
}

/// SQL Server expression of the canonical text (varchar for ASCII forms, nvarchar for strings).
pub fn mssql_canon(kind: Canon, col: &TargetColumn) -> String {
    let x = bracket(&col.name);
    match kind {
        Canon::Int => format!("CONVERT(varchar(20), {x})"),
        Canon::Bit => format!("CONVERT(varchar(1), {x})"),
        Canon::Decimal { precision, scale } => format!("CONVERT(varchar(50), CAST({x} AS decimal({precision},{scale})))"),
        Canon::Char => format!("RTRIM(CAST({x} AS nvarchar(max)))"),
        Canon::Str => format!("CAST({x} AS nvarchar(max))"),
        Canon::Date => format!("CONVERT(varchar(10), {x}, 23)"),
        Canon::DateTime2 { .. } => format!("CONVERT(varchar(27), {x}, 121)"),
        Canon::DateTime => format!("CONVERT(varchar(23), {x}, 121)"),
        Canon::SmallDateTime => format!("CONVERT(varchar(19), {x}, 120)"),
        Canon::Time { .. } => format!("CONVERT(varchar(16), {x})"),
        Canon::DateTimeOffset { scale } => format!("CONVERT(varchar(27), CAST(SWITCHOFFSET({x}, 0) AS datetime2({scale})), 121)"),
        Canon::Guid => format!("LOWER(CONVERT(varchar(36), {x}))"),
        Canon::Binary { .. } => format!("LOWER(CONVERT(varchar(max), CAST({x} AS varbinary(max)), 2))"),
        Canon::NullOnly => format!("CASE WHEN {x} IS NULL THEN NULL ELSE 'v' END"),
    }
}

/// DuckDB: canonical text with the NULL sentinel.
fn duck_canon_nn(kind: Canon, col: &Column) -> String { format!("COALESCE({}, chr(1) || 'NULL')", duck_canon(kind, col)) }

/// SQL Server: the bytes that are hashed (UTF-8 for strings).
fn mssql_hash_input(kind: Canon, col: &TargetColumn) -> String {
    let e = mssql_canon(kind, col);
    if kind.is_string() {
        format!("CONVERT(varchar(max), COALESCE({e}, NCHAR(1) + N'NULL') COLLATE Latin1_General_100_BIN2_UTF8)")
    } else {
        format!("COALESCE({e}, CHAR(1) + 'NULL')")
    }
}

/// SQL Server: canonical text with the NULL sentinel as nvarchar(max) (row text for `except`).
fn mssql_canon_text(kind: Canon, col: &TargetColumn) -> String {
    format!("COALESCE(CAST({} AS nvarchar(max)), NCHAR(1) + N'NULL')", mssql_canon(kind, col))
}

/// `decimal(10,2)`, `varchar(50)`, `nvarchar(max)`, `datetime2(7)`… for CASTs on SQL Server.
pub fn mssql_type_text(c: &TargetColumn) -> String {
    let n = c.type_name.to_ascii_lowercase();
    let len = |div: i16| if c.max_length < 0 { "max".to_string() } else { (c.max_length / div).max(1).to_string() };
    match n.as_str() {
        "char" | "varchar" | "binary" | "varbinary" => format!("{n}({})", len(1)),
        "nchar" | "nvarchar" => format!("{n}({})", len(2)),
        "decimal" | "numeric" => format!("{n}({},{})", c.precision, c.scale),
        "datetime2" | "time" | "datetimeoffset" => format!("{n}({})", c.scale),
        _ => n,
    }
}

// ---- checksum generators ---------------------------------------------------------------------

/// `"schema"."table"` of a DuckDB table.
pub fn duck_source(duck: &Table) -> String { format!("{}.{}", quote_duck(&duck.schema), quote_duck(&duck.name)) }

fn duck_checksum_query(src: &str, m: &TableMapping, pred: Option<&str>, group: Option<&str>) -> String {
    let mut inner: Vec<String> = vec![];
    if let Some(g) = group { inner.push(format!("{g} AS g")); }
    for (i, cm) in m.columns.iter().enumerate() {
        inner.push(format!("md5({}) AS h{}", duck_canon_nn(Canon::of(&cm.target), &cm.duck), i + 1));
    }
    if inner.is_empty() { inner.push("1 AS one".into()); }
    let mut outer: Vec<String> = vec![];
    if group.is_some() { outer.push("g".into()); }
    outer.push("CAST(count(*) AS VARCHAR) AS n".into());
    for i in 1..=m.columns.len() {
        outer.push(format!("CAST(COALESCE(SUM(CAST(CAST('0x' || substr(h{i}, 1, 16) AS UBIGINT) AS HUGEINT)), 0) AS VARCHAR) AS c{i}"));
    }
    let wh = pred.map(|p| format!(" WHERE {p}")).unwrap_or_default();
    let gb = if group.is_some() { " GROUP BY g" } else { "" };
    format!("SELECT {} FROM (SELECT {} FROM {src}{wh}) q{gb}", outer.join(", "), inner.join(", "))
}

/// DuckDB checksum query: one row `n, c1, c2, …` (all VARCHAR) over the mapped columns in mapping
/// order. `partition_predicate` restricts the rows (e.g. `"Mkt" = 'ABC'`).
pub fn duck_checksum_sql(duck: &Table, m: &TableMapping, partition_predicate: Option<&str>) -> String {
    duck_checksum_query(&duck_source(duck), m, partition_predicate, None)
}

/// [`duck_checksum_sql`] over any FROM item (a Parquet scan, a subquery in parentheses with alias).
pub fn duck_checksum_sql_from(src: &str, m: &TableMapping, partition_predicate: Option<&str>) -> String {
    duck_checksum_query(src, m, partition_predicate, None)
}

fn mssql_partition_expr(t: &TargetTable) -> Option<String> {
    t.partitioning.as_ref().map(|p| format!("$PARTITION.{}({})", bracket(&p.function), bracket(&p.column)))
}

fn mssql_checksum_query(t: &TargetTable, m: &TableMapping, partition_number: Option<i32>, grouped: bool) -> String {
    let pexpr = mssql_partition_expr(t);
    let grouped = grouped && pexpr.is_some();
    let mut inner: Vec<String> = vec![];
    if grouped { if let Some(p) = &pexpr { inner.push(format!("{p} AS p")); } }
    for (i, cm) in m.columns.iter().enumerate() {
        inner.push(format!("HASHBYTES('MD5', {}) AS h{}", mssql_hash_input(Canon::of(&cm.target), &cm.target), i + 1));
    }
    if inner.is_empty() { inner.push("1 AS one".into()); }
    let mut outer: Vec<String> = vec![];
    if grouped { outer.push("CONVERT(varchar(11), p) AS p".into()); }
    outer.push("CONVERT(varchar(20), COUNT_BIG(*)) AS n".into());
    for i in 1..=m.columns.len() {
        outer.push(format!(
            "CONVERT(varchar(40), COALESCE(SUM(CAST(CAST(SUBSTRING(h{i}, 1, 8) AS bigint) AS decimal(38,0)) + CASE WHEN SUBSTRING(h{i}, 1, 1) >= 0x80 THEN CAST(18446744073709551616 AS decimal(38,0)) ELSE 0 END), 0)) AS c{i}"));
    }
    let wh = match (partition_number, &pexpr) { (Some(n), Some(p)) => format!(" WHERE {p} = {n}"), _ => String::new() };
    let gb = if grouped { " GROUP BY p" } else { "" };
    format!("SELECT {} FROM (SELECT {} FROM {}.{}{wh}) q{gb}", outer.join(", "), inner.join(", "), bracket(&t.schema), bracket(&t.name))
}

/// SQL Server checksum query: one row `n, c1, c2, …` (all varchar) in the same column order as
/// [`duck_checksum_sql`]. `partition_number` adds `WHERE $PARTITION.fn(col) = n`.
pub fn mssql_checksum_sql(t: &TargetTable, m: &TableMapping, partition_number: Option<i32>) -> String {
    mssql_checksum_query(t, m, partition_number, false)
}

/// Canonical row text over the row-text columns (DuckDB).
fn duck_row_text(m: &TableMapping, only: Option<&[usize]>) -> String {
    let parts: Vec<String> = m.columns.iter().enumerate()
        .filter(|(i, cm)| Canon::of(&cm.target).in_row_text() && only.map(|o| o.contains(i)).unwrap_or(true))
        .map(|(_, cm)| duck_canon_nn(Canon::of(&cm.target), &cm.duck)).collect();
    if parts.is_empty() { "''".into() } else { parts.join(" || chr(31) || ") }
}

fn mssql_row_text(m: &TableMapping, only: Option<&[usize]>) -> String {
    let parts: Vec<String> = m.columns.iter().enumerate()
        .filter(|(i, cm)| Canon::of(&cm.target).in_row_text() && only.map(|o| o.contains(i)).unwrap_or(true))
        .map(|(_, cm)| mssql_canon_text(Canon::of(&cm.target), &cm.target)).collect();
    if parts.is_empty() { "N''".into() } else { parts.join(" + NCHAR(31) + ") }
}

// ---- execution helpers -----------------------------------------------------------------------

fn duck_rows(con: &Connection, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let t0 = Instant::now();
    let mut st = con.prepare(sql).with_context(|| format!("DuckDB: {}", sql.chars().take(300).collect::<String>()))?;
    let mut rows = st.query([])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        let n = r.as_ref().column_count();
        let mut v = Vec::with_capacity(n);
        for i in 0..n { v.push(r.get::<_, Option<String>>(i)?); }
        out.push(v);
    }
    runlog::stmt("duckdb", "verify", sql, out.len() as i64, t0);
    Ok(out)
}

async fn ms_rows(c: &mut MsClient, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let t0 = Instant::now();
    let rows = c.simple_query(sql).await.with_context(|| format!("SQL Server: {}", sql.chars().take(300).collect::<String>()))?.into_first_result().await?;
    let out: Vec<Vec<Option<String>>> = rows.iter().map(|r| (0..r.len()).map(|i| r.try_get::<&str, _>(i).ok().flatten().map(|s| s.to_string())).collect()).collect();
    runlog::stmt("target", "verify", sql, out.len() as i64, t0);
    Ok(out)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Sums { rows: i64, cols: Vec<i128> }

impl Sums {
    fn parse(row: &[Option<String>]) -> Result<Sums> {
        let num = |s: &Option<String>| -> Result<i128> { Ok(s.as_deref().unwrap_or("0").trim().parse::<i128>()?) };
        let rows = num(row.first().ok_or_else(|| anyhow::anyhow!("empty checksum row"))?)? as i64;
        Ok(Sums { rows, cols: row[1..].iter().map(num).collect::<Result<_>>()? })
    }
    fn add(&mut self, o: &Sums) {
        self.rows += o.rows;
        if self.cols.len() < o.cols.len() { self.cols.resize(o.cols.len(), 0); }
        for (a, b) in self.cols.iter_mut().zip(&o.cols) { *a += *b; }
    }
}

/// Partition-column values of the DuckDB side mapped to target partition numbers.
struct PartMap {
    canon: String,
    is_null_col: String,
    /// canonical value (None = NULL) -> partition number
    by_value: Vec<(Option<String>, i32)>,
    counts: BTreeMap<i32, i64>,
}

impl PartMap {
    fn predicate(&self, pn: i32) -> String {
        let vals: Vec<String> = self.by_value.iter().filter_map(|(v, p)| if *p == pn { v.as_deref().map(duck_str) } else { None }).collect();
        let null = self.by_value.iter().any(|(v, p)| *p == pn && v.is_none());
        let mut parts = vec![];
        if !vals.is_empty() { parts.push(format!("{} IN ({})", self.canon, vals.join(", "))); }
        if null { parts.push(format!("{} IS NULL", self.is_null_col)); }
        if parts.is_empty() { "false".into() } else { format!("({})", parts.join(" OR ")) }
    }
    fn partition_of(&self, v: &Option<String>) -> Option<i32> { self.by_value.iter().find(|(x, _)| x == v).map(|(_, p)| *p) }
}

async fn partition_map(con: &Connection, c: &mut MsClient, src: &str, t: &TargetTable, m: &TableMapping, notes: &mut Vec<String>) -> Result<Option<PartMap>> {
    let Some(p) = &t.partitioning else { return Ok(None) };
    let Some(cm) = m.columns.iter().find(|cm| cm.target.name.eq_ignore_ascii_case(&p.column)) else {
        notes.push(format!("partition column {} is not loaded from DuckDB; per-partition checks skipped", p.column));
        return Ok(None);
    };
    let kind = Canon::of(&cm.target);
    if matches!(kind, Canon::NullOnly | Canon::Binary { .. }) {
        notes.push(format!("partition column {} ({}) cannot be mapped by value; per-partition checks skipped", p.column, cm.target.type_name));
        return Ok(None);
    }
    let canon = duck_canon(kind, &cm.duck);
    let rows = duck_rows(con, &format!("SELECT {canon} AS v, CAST(count(*) AS VARCHAR) AS n FROM {src} GROUP BY 1 LIMIT {}", MAX_PARTITION_VALUES + 1))?;
    if rows.len() > MAX_PARTITION_VALUES {
        notes.push(format!("more than {MAX_PARTITION_VALUES} distinct values in partition column {}; per-partition checks skipped", p.column));
        return Ok(None);
    }
    let ty = mssql_type_text(&cm.target);
    let mut by_value = vec![];
    let mut counts = BTreeMap::new();
    for chunk in rows.chunks(1000) {
        let vals: Vec<String> = chunk.iter().enumerate().map(|(i, r)| format!("({i}, {})", r[0].as_deref().map(nstr).unwrap_or_else(|| "NULL".into()))).collect();
        let sql = format!("SELECT CONVERT(varchar(11), i), CONVERT(varchar(11), $PARTITION.{}(CAST(v AS {ty}))) FROM (VALUES {}) x(i, v)", bracket(&p.function), vals.join(", "));
        for r in ms_rows(c, &sql).await? {
            let i: usize = r[0].as_deref().unwrap_or("0").parse()?;
            let pn: i32 = r[1].as_deref().unwrap_or("0").parse()?;
            let n: i64 = chunk[i][1].as_deref().unwrap_or("0").parse()?;
            by_value.push((chunk[i][0].clone(), pn));
            *counts.entry(pn).or_insert(0) += n;
        }
    }
    Ok(Some(PartMap { canon, is_null_col: quote_duck(&cm.duck.name), by_value, counts }))
}

async fn server_major(c: &mut MsClient) -> Result<i32> {
    let r = ms_rows(c, "SELECT CONVERT(varchar(10), SERVERPROPERTY('ProductMajorVersion'))").await?;
    Ok(r.first().and_then(|r| r[0].as_deref()).and_then(|s| s.trim().parse().ok()).unwrap_or(0))
}

fn diff_columns(m: &TableMapping, a: &Sums, b: &Sums) -> Vec<String> {
    m.columns.iter().enumerate().filter(|(i, _)| a.cols.get(*i) != b.cols.get(*i)).map(|(_, cm)| cm.target.name.clone()).collect()
}

// ---- verify_table ----------------------------------------------------------------------------

/// Verify one loaded table against its DuckDB source at `level`. Async because it talks to SQL
/// Server; DuckDB queries run on `con` (the future is not `Send`, use a current-thread runtime).
pub async fn verify_table(con: &Connection, c: &mut MsClient, duck: &Table, target: &TargetTable, m: &TableMapping, level: VerifyLevel) -> Result<Vec<VerifyResult>> {
    verify_table_from(con, c, &duck_source(duck), target, m, level).await
}

/// [`verify_table`] with any DuckDB FROM item as the source (e.g. a Parquet scan).
pub async fn verify_table_from(con: &Connection, c: &mut MsClient, src: &str, target: &TargetTable, m: &TableMapping, level: VerifyLevel) -> Result<Vec<VerifyResult>> {
    let table = target.full_name();
    let mut out = vec![];
    let mut notes = vec![];
    let res = |partition: Option<i32>, ok: bool, detail: String| VerifyResult { table: table.clone(), partition, ok, detail };

    // ---- counts
    let pmap = partition_map(con, c, src, target, m, &mut notes).await?;
    let target_counts: BTreeMap<i32, i64> = target::partition_row_counts(c, target).await?.into_iter().collect();
    let target_total: i64 = target_counts.values().sum();
    let duck_total: i64 = match &pmap {
        Some(pm) => pm.counts.values().sum(),
        None => duck_rows(con, &format!("SELECT CAST(count(*) AS VARCHAR) FROM {src}"))?.first().and_then(|r| r[0].as_deref()).unwrap_or("0").parse()?,
    };
    if let Some(pm) = &pmap {
        let pns: BTreeSet<i32> = target_counts.iter().filter(|(_, n)| **n > 0).map(|(p, _)| *p).chain(pm.counts.keys().copied()).collect();
        for pn in pns {
            let (d, t) = (pm.counts.get(&pn).copied().unwrap_or(0), target_counts.get(&pn).copied().unwrap_or(0));
            out.push(res(Some(pn), d == t, format!("counts: duckdb={d} target={t}")));
        }
    }
    let mut detail = format!("counts: duckdb={duck_total} target={target_total}");
    for n in &notes { detail.push_str(&format!(" ({n})")); }
    out.push(res(None, duck_total == target_total, detail));
    if !level.checksums() { return Ok(out); }

    // ---- checksums
    if m.columns.iter().any(|cm| Canon::of(&cm.target).is_string()) {
        let major = server_major(c).await?;
        if major > 0 && major < 15 {
            return Err(Failure::Run(format!("--verify checksum on {table}: string columns need SQL Server 2019 or later (UTF-8 collation); this server is version {major}. Use --verify counts")).into());
        }
    }
    let null_only: Vec<&str> = m.columns.iter().filter(|cm| Canon::of(&cm.target) == Canon::NullOnly).map(|cm| cm.target.name.as_str()).collect();
    let null_note = if null_only.is_empty() { String::new() } else { format!("; NULL pattern only for {}", null_only.join(", ")) };
    // (scope, duck sums, target sums)
    let mut scopes: Vec<(Option<i32>, Sums, Sums)> = vec![];
    if let Some(pm) = &pmap {
        let mut dsum: BTreeMap<i32, Sums> = BTreeMap::new();
        for r in duck_rows(con, &duck_checksum_query(src, m, None, Some(&pm.canon)))? {
            let pn = pm.partition_of(&r[0]).ok_or_else(|| anyhow::anyhow!("partition value {:?} of {table} has no partition number", r[0]))?;
            dsum.entry(pn).or_default().add(&Sums::parse(&r[1..])?);
        }
        let mut tsum: BTreeMap<i32, Sums> = BTreeMap::new();
        for r in ms_rows(c, &mssql_checksum_query(target, m, None, true)).await? {
            let pn: i32 = r[0].as_deref().unwrap_or("0").parse()?;
            tsum.entry(pn).or_default().add(&Sums::parse(&r[1..])?);
        }
        let pns: BTreeSet<i32> = dsum.keys().chain(tsum.keys()).copied().collect();
        let empty = Sums { rows: 0, cols: vec![0; m.columns.len()] };
        for pn in pns {
            let d = dsum.get(&pn).cloned().unwrap_or_else(|| empty.clone());
            let t = tsum.get(&pn).cloned().unwrap_or_else(|| empty.clone());
            scopes.push((Some(pn), d, t));
        }
    } else {
        let d = Sums::parse(duck_rows(con, &duck_checksum_query(src, m, None, None))?.first().ok_or_else(|| anyhow::anyhow!("no checksum row from DuckDB"))?)?;
        let t = Sums::parse(ms_rows(c, &mssql_checksum_sql(target, m, None)).await?.first().ok_or_else(|| anyhow::anyhow!("no checksum row from SQL Server"))?)?;
        scopes.push((None, d, t));
    }
    let mut failing: Vec<(Option<i32>, i64)> = vec![];
    for (scope, d, t) in &scopes {
        let cols = diff_columns(m, d, t);
        let ok = cols.is_empty() && d.rows == t.rows;
        let detail = if ok {
            format!("checksum: {} rows, {} column(s) match{null_note}", d.rows, m.columns.len())
        } else {
            let mut s = format!("checksum mismatch: rows duckdb={} target={}", d.rows, t.rows);
            if !cols.is_empty() { s.push_str(&format!("; columns differ: {}", cols.join(", "))); }
            s
        };
        if !ok { failing.push((*scope, d.rows.max(t.rows))); }
        out.push(res(*scope, ok, detail));
    }
    let VerifyLevel::Except { sample } = level else { return Ok(out) };

    // ---- row diff for the failing scopes
    let key_idx: Vec<usize> = {
        let key = target.pk().or_else(|| target.clustered()).map(|i| i.columns.iter().filter(|c| !c.included).map(|c| c.name.clone()).collect::<Vec<_>>()).unwrap_or_default();
        let idx: Vec<usize> = key.iter().filter_map(|k| m.columns.iter().position(|cm| cm.target.name.eq_ignore_ascii_case(k))).collect();
        if idx.len() == key.len() { idx } else { vec![] }
    };
    for (scope, rows) in failing {
        let dpred = match (scope, &pmap) { (Some(pn), Some(pm)) => Some(pm.predicate(pn)), _ => None };
        let tpred = match (scope, mssql_partition_expr(target)) { (Some(pn), Some(p)) => Some(format!("{p} = {pn}")), _ => None };
        let diff = row_diff(con, c, src, target, m, Filters { duck: dpred, target: tpred }, rows, &key_idx, sample).await?;
        out.push(res(scope, false, diff));
    }
    Ok(out)
}

struct Filters { duck: Option<String>, target: Option<String> }

#[allow(clippy::too_many_arguments)]
async fn row_diff(con: &Connection, c: &mut MsClient, src: &str, t: &TargetTable, m: &TableMapping, f: Filters, rows: i64, key_idx: &[usize], sample: usize) -> Result<String> {
    let mut dconds: Vec<String> = f.duck.into_iter().collect();
    let mut tconds: Vec<String> = f.target.into_iter().collect();
    let mut sampled = String::new();
    if rows > MAX_DIFF_ROWS {
        let modulus = (rows + MAX_DIFF_ROWS - 1) / MAX_DIFF_ROWS;
        let only = if key_idx.is_empty() { None } else { Some(key_idx) };
        dconds.push(format!("CAST('0x' || substr(md5({}), 1, 8) AS BIGINT) % {modulus} = 0", duck_row_text(m, only)));
        tconds.push(format!("CAST(SUBSTRING(HASHBYTES('MD5', CONVERT(varchar(max), ({}) COLLATE Latin1_General_100_BIN2_UTF8)), 1, 4) AS bigint) % {modulus} = 0", mssql_row_text(m, only)));
        sampled = format!(" (sampled 1/{modulus} of {rows} rows by {} hash)", if key_idx.is_empty() { "row" } else { "key" });
    }
    let wh = |c: &[String]| if c.is_empty() { String::new() } else { format!(" WHERE {}", c.join(" AND ")) };
    let mut bag: HashMap<String, i64> = HashMap::new();
    for r in duck_rows(con, &format!("SELECT {} FROM {src}{}", duck_row_text(m, None), wh(&dconds)))? {
        *bag.entry(r.into_iter().next().flatten().unwrap_or_default()).or_insert(0) += 1;
    }
    let sql = format!("SELECT {} FROM {}.{}{}", mssql_row_text(m, None), bracket(&t.schema), bracket(&t.name), wh(&tconds));
    let t0 = Instant::now();
    let mut n = 0i64;
    {
        let mut stream = c.simple_query(&sql).await.with_context(|| format!("SQL Server: {}", sql.chars().take(300).collect::<String>()))?.into_row_stream();
        while let Some(row) = stream.try_next().await? {
            let s = row.try_get::<&str, _>(0).ok().flatten().unwrap_or("").to_string();
            *bag.entry(s).or_insert(0) -= 1;
            n += 1;
        }
    }
    runlog::stmt("target", "verify", &sql, n, t0);
    Ok(format_diff(m, &bag, sample, &sampled))
}

fn format_diff(m: &TableMapping, bag: &HashMap<String, i64>, sample: usize, sampled: &str) -> String {
    let show = |s: &str| s.replace(NULL_SENTINEL, "NULL").replace(SEP, " | ");
    let mut only_duck: Vec<(&String, i64)> = bag.iter().filter(|(_, v)| **v > 0).map(|(k, v)| (k, *v)).collect();
    let mut only_target: Vec<(&String, i64)> = bag.iter().filter(|(_, v)| **v < 0).map(|(k, v)| (k, -*v)).collect();
    only_duck.sort();
    only_target.sort();
    let fmt = |v: &[(&String, i64)]| -> String {
        let total: i64 = v.iter().map(|(_, n)| n).sum();
        let shown: Vec<String> = v.iter().take(sample).map(|(k, n)| if *n > 1 { format!("[{}] x{n}", show(k)) } else { format!("[{}]", show(k)) }).collect();
        format!("{total} row(s){}", if shown.is_empty() { String::new() } else { format!(": {}", shown.join("; ")) })
    };
    let cols: Vec<&str> = m.columns.iter().filter(|cm| Canon::of(&cm.target).in_row_text()).map(|cm| cm.target.name.as_str()).collect();
    format!("except{sampled}: columns ({}); only in duckdb {}; only in target {}", cols.join(" | "), fmt(&only_duck), fmt(&only_target))
}

// ---- verify_indexes --------------------------------------------------------------------------

/// Re-read `before`'s table and check that every index captured before the load exists with the
/// same definition, is enabled, and that the PK, check constraints and foreign keys are trusted.
pub async fn verify_indexes(c: &mut MsClient, before: &TargetTable) -> Result<Vec<VerifyResult>> {
    let after = target::read_target_tables(c, &[(before.schema.clone(), before.name.clone())]).await?
        .into_iter().next().ok_or_else(|| anyhow::anyhow!("table {} not found after the load", before.full_name()))?;
    let fq = nstr(&format!("{}.{}", bracket(&before.schema), bracket(&before.name)));
    let disabled: BTreeSet<String> = ms_rows(c, &format!("SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID({fq}) AND index_id > 0 AND is_disabled = 1")).await?
        .into_iter().filter_map(|r| r.into_iter().next().flatten()).map(|s| s.to_lowercase()).collect();
    let bad_fks = ms_rows(c, &format!("SELECT name, CASE WHEN is_disabled = 1 THEN 'disabled' ELSE 'not trusted' END FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID({fq}) AND (is_disabled = 1 OR is_not_trusted = 1)")).await?;
    Ok(compare_indexes(before, &after, &disabled, &bad_fks))
}

fn index_def(i: &target::TargetIndex) -> String {
    let cols: Vec<String> = i.columns.iter().filter(|c| !c.included).map(|c| format!("{}{}", c.name, if c.descending { " DESC" } else { "" })).collect();
    let inc: Vec<&str> = i.columns.iter().filter(|c| c.included).map(|c| c.name.as_str()).collect();
    let mut s = format!("{}{}{} ({})", if i.is_primary_key { "PRIMARY KEY " } else { "" }, if i.is_unique { "UNIQUE " } else { "" }, i.type_desc, cols.join(", "));
    if !inc.is_empty() { s.push_str(&format!(" INCLUDE ({})", inc.join(", "))); }
    if let Some(f) = &i.filter { s.push_str(&format!(" WHERE {f}")); }
    s.push_str(&format!(" COMPRESSION {} FILLFACTOR {} ON {}", i.data_compression, i.fill_factor, i.data_space));
    s
}

fn compare_indexes(before: &TargetTable, after: &TargetTable, disabled: &BTreeSet<String>, bad_fks: &[Vec<Option<String>>]) -> Vec<VerifyResult> {
    let table = before.full_name();
    let mut out = vec![];
    let res = |ok: bool, detail: String| VerifyResult { table: table.clone(), partition: None, ok, detail };
    for b in before.indexes.iter().filter(|i| !i.type_desc.eq_ignore_ascii_case("HEAP")) {
        let Some(a) = after.indexes.iter().find(|a| a.name.eq_ignore_ascii_case(&b.name)) else {
            out.push(res(false, format!("index {} missing (expected {})", b.name, index_def(b))));
            continue;
        };
        let (db, da) = (index_def(b), index_def(a));
        let mut problems = vec![];
        if db.to_lowercase() != da.to_lowercase() { problems.push(format!("definition differs: before {db}; after {da}")); }
        if b.is_unique_constraint != a.is_unique_constraint { problems.push("unique constraint flag differs".to_string()); }
        if disabled.contains(&a.name.to_lowercase()) { problems.push("is disabled".to_string()); }
        if problems.is_empty() {
            out.push(res(true, format!("index {}: {da}", a.name)));
        } else {
            out.push(res(false, format!("index {}: {}", a.name, problems.join("; "))));
        }
    }
    if before.pk().is_some() && after.pk().is_none() { out.push(res(false, "primary key missing".into())); }
    for a in after.indexes.iter().filter(|a| !a.type_desc.eq_ignore_ascii_case("HEAP") && !before.indexes.iter().any(|b| b.name.eq_ignore_ascii_case(&a.name))) {
        out.push(res(true, format!("extra index {} (not present before the load): {}", a.name, index_def(a))));
    }
    if before.is_heap() != after.is_heap() {
        out.push(res(false, format!("table was {} before the load and is {} now", if before.is_heap() { "a heap" } else { "clustered" }, if after.is_heap() { "a heap" } else { "clustered" })));
    }
    for b in &before.checks {
        match after.checks.iter().find(|a| a.name.eq_ignore_ascii_case(&b.name)) {
            None => out.push(res(false, format!("check constraint {} missing", b.name))),
            Some(a) if a.is_not_trusted => out.push(res(false, format!("check constraint {} is not trusted (is_not_trusted = 1); re-enable WITH CHECK", a.name))),
            Some(a) => out.push(res(true, format!("check constraint {} trusted", a.name))),
        }
    }
    for a in after.checks.iter().filter(|a| a.is_not_trusted && !before.checks.iter().any(|b| b.name.eq_ignore_ascii_case(&a.name))) {
        out.push(res(false, format!("check constraint {} is not trusted", a.name)));
    }
    for r in bad_fks {
        out.push(res(false, format!("foreign key {} is {}", r[0].as_deref().unwrap_or("?"), r.get(1).and_then(|s| s.as_deref()).unwrap_or("not trusted"))));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::duck;
    use crate::core::model::Conv;
    use crate::mssql::target::{ColumnMapping, TargetCheck, TargetIndex, TargetIndexColumn, TargetPartitioning};

    fn tcol(name: &str, id: i32, ty: &str, max_length: i16, precision: u8, scale: u8) -> TargetColumn {
        TargetColumn { name: name.into(), column_id: id, type_name: ty.into(), max_length, precision, scale, collation: None, nullable: true, has_default: false, is_identity: false, is_computed: false }
    }
    fn dcol(name: &str, duck_type: &str) -> Column {
        Column { name: name.into(), duck_type: duck_type.into(), nullable: true, default: None, comment: None, sql_type: String::new(), select_expr: quote_duck(name), conv: Conv::Identity, max_len: None, identity: None, enum_values: None, warnings: vec![] }
    }
    type ColSpec<'a> = (&'a str, &'a str, &'a str, i16, u8, u8);
    fn table(cols: &[ColSpec], partitioning: Option<TargetPartitioning>) -> (Table, TargetTable, TableMapping) {
        let tcols: Vec<TargetColumn> = cols.iter().enumerate().map(|(i, c)| tcol(c.0, i as i32 + 1, c.2, c.3, c.4, c.5)).collect();
        let dcols: Vec<Column> = cols.iter().map(|c| dcol(c.0, c.1)).collect();
        let t = TargetTable { schema: "dbo".into(), name: "g_t".into(), object_id: 1, columns: tcols.clone(), indexes: vec![], partitioning, checks: vec![], row_count: 0, modify_date: String::new() };
        let m = TableMapping { table: t.clone(), columns: dcols.iter().zip(&tcols).enumerate().map(|(i, (d, tc))| ColumnMapping { duck_index: i, duck: d.clone(), target: tc.clone() }).collect(), omitted_target_columns: vec![], notes: vec![] };
        let d = Table { schema: "main".into(), name: "g_t".into(), columns: dcols, constraints: vec![], indexes: vec![], row_count: 0, comment: None };
        (d, t, m)
    }

    /// The parity fixture: (name, DuckDB type, SQL Server type, max_length, precision, scale). The
    /// same rows exist on SQL Server as `_agent_g.dbo.g_t` (setup script in docs/handoff-G.md).
    const FIXTURE: &[ColSpec<'static>] = &[
        ("id", "INTEGER", "int", 4, 10, 0),
        ("d", "DECIMAL(10,2)", "decimal", 9, 10, 2),
        ("vc", "VARCHAR", "varchar", 50, 0, 0),
        ("nv", "VARCHAR", "nvarchar", 100, 0, 0),
        ("dt", "DATE", "date", 3, 10, 0),
        ("ts", "TIMESTAMP", "datetime2", 8, 27, 7),
        ("b", "BOOLEAN", "bit", 1, 1, 0),
        ("g", "UUID", "uniqueidentifier", 16, 0, 0),
        ("ch", "VARCHAR", "char", 5, 0, 0),
        ("m", "DECIMAL(19,4)", "money", 8, 19, 4),
        ("f", "DOUBLE", "float", 8, 53, 0),
        ("dtm", "TIMESTAMP", "datetime", 8, 23, 3),
        ("ts3", "TIMESTAMP", "datetime2", 7, 23, 3),
        ("dto", "TIMESTAMP WITH TIME ZONE", "datetimeoffset", 10, 30, 3),
        ("tm", "TIME", "time", 4, 12, 3),
        ("bn", "BLOB", "varbinary", 8, 0, 0),
        ("bf", "BLOB", "binary", 4, 0, 0),
        ("sdt", "TIMESTAMP", "smalldatetime", 4, 16, 0),
    ];

    fn fixture_duck() -> Connection {
        let con = duck::open_in_memory().unwrap();
        duck::exec(&con, r#"
            CREATE TABLE g_t (id INTEGER, d DECIMAL(10,2), vc VARCHAR, nv VARCHAR, dt DATE, ts TIMESTAMP, b BOOLEAN, g UUID, ch VARCHAR,
                              m DECIMAL(19,4), f DOUBLE, dtm TIMESTAMP, ts3 TIMESTAMP, dto TIMESTAMP WITH TIME ZONE, tm TIME, bn BLOB, bf BLOB, sdt TIMESTAMP);
            INSERT INTO g_t VALUES
              (1, 12.50, 'Müller café', '日本語 😀', DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.123456', true, '6F9619FF-8B86-D011-B42D-00C04FC964FF', 'ab', 12.3456, 1.5,
               TIMESTAMP '2024-01-02 03:04:05.0017', TIMESTAMP '2024-01-02 03:04:05.1235', '2024-01-02 03:04:05.123456+02'::TIMESTAMPTZ, TIME '03:04:05.5', '\xAB\x01'::BLOB, '\xAB\x01'::BLOB, TIMESTAMP '2024-01-02 03:04:29.999'),
              (2, -0.50, 'plain', 'Ünïcødé', DATE '0001-01-01', TIMESTAMP '1969-12-31 23:59:59.5', false, '00000000-0000-0000-0000-000000000001', 'xyz  ', -1, -2.25,
               TIMESTAMP '1999-12-31 23:59:59.9989', TIMESTAMP '1999-12-31 23:59:59.9996', '2000-01-01 00:30:00-05'::TIMESTAMPTZ, TIME '23:59:59.9994', '\x00'::BLOB, '\x01\x02\x03\x04'::BLOB, TIMESTAMP '2024-01-02 03:04:29.998'),
              (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
              (4, 0.00, '', '', DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59.999999', true, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '', 922337203685477.5807, 0,
               TIMESTAMP '2024-02-29 12:00:00', TIMESTAMP '2024-02-29 12:00:00', '2024-02-29 12:00:00+00'::TIMESTAMPTZ, TIME '00:00:00', ''::BLOB, '\xFF'::BLOB, TIMESTAMP '2024-02-29 12:00:00'),
              (5, 99999999.99, 'it''s', 'quote '' and ''', DATE '2024-02-29', TIMESTAMP '2024-02-29 00:00:00.000001', false, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', ' lead', -922337203685477.5808, 1e300,
               TIMESTAMP '2024-02-29 00:00:00.003', TIMESTAMP '2024-02-29 00:00:00.0005', '2024-02-29 00:00:00.0005+14'::TIMESTAMPTZ, TIME '12:00:00.0005', '\x7F\x80'::BLOB, '\x00'::BLOB, TIMESTAMP '1900-01-01 00:00:30');
        "#).unwrap();
        con
    }

    #[test]
    fn level_parse() {
        assert_eq!(VerifyLevel::parse("counts").unwrap(), VerifyLevel::Counts);
        assert_eq!(VerifyLevel::parse("Checksum").unwrap(), VerifyLevel::Checksum);
        assert_eq!(VerifyLevel::parse("except").unwrap(), VerifyLevel::Except { sample: 10 });
        assert_eq!(VerifyLevel::parse("except:3").unwrap(), VerifyLevel::Except { sample: 3 });
        assert_eq!(VerifyLevel::parse("except").unwrap().with_sample(7), VerifyLevel::Except { sample: 7 });
        assert_eq!(VerifyLevel::Counts.with_sample(7), VerifyLevel::Counts);
        assert!(VerifyLevel::parse("full").is_err());
    }

    #[test]
    fn mssql_generator_text() {
        let (_, t, m) = table(&[("id", "INTEGER", "int", 4, 10, 0), ("s", "VARCHAR", "nvarchar", -1, 0, 0)], Some(TargetPartitioning {
            scheme: "ps".into(), function: "pf".into(), column: "id".into(), range_right: true, boundaries: vec!["10".into()], filegroups: vec!["PRIMARY".into(); 2] }));
        let agg = |i: usize| format!("CONVERT(varchar(40), COALESCE(SUM(CAST(CAST(SUBSTRING(h{i}, 1, 8) AS bigint) AS decimal(38,0)) + CASE WHEN SUBSTRING(h{i}, 1, 1) >= 0x80 THEN CAST(18446744073709551616 AS decimal(38,0)) ELSE 0 END), 0)) AS c{i}");
        assert_eq!(mssql_checksum_sql(&t, &m, Some(2)), format!(
            "SELECT CONVERT(varchar(20), COUNT_BIG(*)) AS n, {}, {} FROM (SELECT HASHBYTES('MD5', COALESCE(CONVERT(varchar(20), [id]), CHAR(1) + 'NULL')) AS h1, \
             HASHBYTES('MD5', CONVERT(varchar(max), COALESCE(CAST([s] AS nvarchar(max)), NCHAR(1) + N'NULL') COLLATE Latin1_General_100_BIN2_UTF8)) AS h2 \
             FROM [dbo].[g_t] WHERE $PARTITION.[pf]([id]) = 2) q", agg(1), agg(2)));
        let g = mssql_checksum_query(&t, &m, None, true);
        assert!(g.starts_with("SELECT CONVERT(varchar(11), p) AS p, CONVERT(varchar(20), COUNT_BIG(*)) AS n,"), "{g}");
        assert!(g.contains("FROM (SELECT $PARTITION.[pf]([id]) AS p, HASHBYTES"), "{g}");
        assert!(g.ends_with("FROM [dbo].[g_t]) q GROUP BY p"), "{g}");
    }

    #[test]
    fn duck_generator_text() {
        let (d, _, m) = table(&[("id", "INTEGER", "int", 4, 10, 0), ("s", "VARCHAR", "nvarchar", -1, 0, 0)], None);
        assert_eq!(duck_checksum_sql(&d, &m, Some("\"id\" > 1")),
            "SELECT CAST(count(*) AS VARCHAR) AS n, \
             CAST(COALESCE(SUM(CAST(CAST('0x' || substr(h1, 1, 16) AS UBIGINT) AS HUGEINT)), 0) AS VARCHAR) AS c1, \
             CAST(COALESCE(SUM(CAST(CAST('0x' || substr(h2, 1, 16) AS UBIGINT) AS HUGEINT)), 0) AS VARCHAR) AS c2 \
             FROM (SELECT md5(COALESCE(CAST(CAST(\"id\" AS BIGINT) AS VARCHAR), chr(1) || 'NULL')) AS h1, md5(COALESCE(\"s\", chr(1) || 'NULL')) AS h2 \
             FROM \"main\".\"g_t\" WHERE \"id\" > 1) q");
    }

    #[test]
    fn type_text() {
        assert_eq!(mssql_type_text(&tcol("a", 1, "nvarchar", 20, 0, 0)), "nvarchar(10)");
        assert_eq!(mssql_type_text(&tcol("a", 1, "varchar", -1, 0, 0)), "varchar(max)");
        assert_eq!(mssql_type_text(&tcol("a", 1, "decimal", 9, 10, 2)), "decimal(10,2)");
        assert_eq!(mssql_type_text(&tcol("a", 1, "datetime2", 8, 27, 7)), "datetime2(7)");
        assert_eq!(mssql_type_text(&tcol("a", 1, "int", 4, 10, 0)), "int");
    }

    /// Prints the SQL Server checksum and canonical-text queries for the fixture (used to produce
    /// the expected values below on the SQL Server container; see docs/handoff-G.md).
    #[test]
    #[ignore]
    fn print_fixture_mssql_sql() {
        let (_, t, m) = table(FIXTURE, None);
        println!("{};", mssql_checksum_sql(&t, &m, None));
        let exprs: Vec<String> = m.columns.iter().map(|cm| format!("COALESCE(CAST({} AS nvarchar(max)), N'<NULL>')", mssql_canon(Canon::of(&cm.target), &cm.target))).collect();
        println!("SELECT {} FROM [dbo].[g_t] ORDER BY id;", exprs.join(" + N'~' + "));
        let part = TargetPartitioning { scheme: "g_ps".into(), function: "g_pf".into(), column: "id".into(), range_right: true, boundaries: vec!["3".into()], filegroups: vec!["PRIMARY".into(); 2] };
        let (_, tp, mp) = table(FIXTURE, Some(part));
        println!("{};", mssql_checksum_query(&tp, &mp, None, true));
        println!("{};", mssql_checksum_sql(&tp, &mp, Some(2)));
        println!("SELECT {} FROM [dbo].[g_t] WHERE CAST(SUBSTRING(HASHBYTES('MD5', CONVERT(varchar(max), ({}) COLLATE Latin1_General_100_BIN2_UTF8)), 1, 4) AS bigint) % 2 = 0;", mssql_row_text(&m, None), mssql_row_text(&m, Some(&[0])));
    }

    /// Canonical text of each fixture column in DuckDB equals SQL Server's rendering of the same
    /// values (strings taken from the SQL Server 2019 container).
    #[test]
    fn canonical_forms_match_sql_server_rendering() {
        let con = fixture_duck();
        let (_, _, m) = table(FIXTURE, None);
        let exprs: Vec<String> = m.columns.iter().map(|cm| format!("COALESCE({}, '<NULL>')", duck_canon(Canon::of(&cm.target), &cm.duck))).collect();
        let rows = duck_rows(&con, &format!("SELECT {} FROM g_t ORDER BY id", exprs.join(" || '~' || "))).unwrap();
        let got: Vec<String> = rows.into_iter().map(|r| r[0].clone().unwrap()).collect();
        assert_eq!(got, EXPECTED_CANON);
    }

    const EXPECTED_CANON: [&str; 5] = [
        "1~12.50~Müller café~日本語 😀~2024-01-02~2024-01-02 03:04:05.1234560~1~6f9619ff-8b86-d011-b42d-00c04fc964ff~ab~12.3456~v~2024-01-02 03:04:05.003~2024-01-02 03:04:05.124~2024-01-02 01:04:05.123~03:04:05.500~ab01~ab010000~2024-01-02 03:04:00",
        "2~-0.50~plain~Ünïcødé~0001-01-01~1969-12-31 23:59:59.5000000~0~00000000-0000-0000-0000-000000000001~xyz~-1.0000~v~2000-01-01 00:00:00.000~2000-01-01 00:00:00.000~2000-01-01 05:30:00.000~23:59:59.999~00~01020304~2024-01-02 03:04:00",
        "3~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>~<NULL>",
        "4~0.00~~~9999-12-31~9999-12-31 23:59:59.9999990~1~ffffffff-ffff-ffff-ffff-ffffffffffff~~922337203685477.5807~v~2024-02-29 12:00:00.000~2024-02-29 12:00:00.000~2024-02-29 12:00:00.000~00:00:00.000~~ff000000~2024-02-29 12:00:00",
        "5~99999999.99~it's~quote ' and '~2024-02-29~2024-02-29 00:00:00.0000010~0~a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11~ lead~-922337203685477.5808~v~2024-02-29 00:00:00.003~2024-02-29 00:00:00.001~2024-02-28 10:00:00.001~12:00:00.001~7f80~00000000~1900-01-01 00:01:00",
    ];

    /// Checksums computed by `mssql_checksum_sql` on SQL Server 2019 for the same five rows
    /// (`_agent_g.dbo.g_t`); DuckDB must produce identical numbers.
    #[test]
    fn checksum_parity_with_sql_server() {
        let con = fixture_duck();
        let (d, _, m) = table(FIXTURE, None);
        let sql = duck_checksum_sql(&d, &m, None).replace("\"main\".", "");
        let got: Vec<String> = duck_rows(&con, &sql).unwrap().remove(0).into_iter().map(|v| v.unwrap()).collect();
        assert_eq!(got, EXPECTED_MSSQL);
        let one = duck_rows(&con, &duck_checksum_sql(&d, &m, Some("\"id\" = 3")).replace("\"main\".", "")).unwrap();
        assert_eq!(one[0][0].as_deref(), Some("1"));
    }

    /// Output of the printed `mssql_checksum_sql` on SQL Server 2019 (15.0.4480), `_agent_g.dbo.g_t`.
    const EXPECTED_MSSQL: [&str; 19] = [
        "5",
        "74295512783200206917",
        "45876661665826066732",
        "60414949016600510693",
        "54194216387296621526",
        "43867012952289789925",
        "49868226104557701117",
        "66800574519201567007",
        "49749956071241538812",
        "45585296777261282911",
        "54566252500759508010",
        "54094479386827124113",
        "42266492496468287357",
        "49324210310116105774",
        "51075526882793546348",
        "38625143229243284176",
        "62987943009005012115",
        "40856984016762251674",
        "31483152893399827718",
    ];

    #[test]
    fn grouped_sums_add_up() {
        let con = fixture_duck();
        let (_, _, m) = table(FIXTURE, None);
        let whole = Sums::parse(&duck_rows(&con, &duck_checksum_query("g_t", &m, None, None)).unwrap()[0]).unwrap();
        let mut total = Sums::default();
        for r in duck_rows(&con, &duck_checksum_query("g_t", &m, None, Some("CAST(\"id\" % 2 AS VARCHAR)"))).unwrap() { total.add(&Sums::parse(&r[1..]).unwrap()); }
        assert_eq!(whole, total);
    }

    #[test]
    fn row_text_diff_and_partition_predicate() {
        let con = fixture_duck();
        let (_, _, m) = table(&FIXTURE[..3], None);
        let rows = duck_rows(&con, &format!("SELECT {} FROM g_t WHERE id IN (1, 3) ORDER BY id", duck_row_text(&m, None))).unwrap();
        assert_eq!(rows[0][0].as_deref(), Some("1\u{1f}12.50\u{1f}Müller café"));
        assert_eq!(rows[1][0].as_deref(), Some("3\u{1f}\u{1}NULL\u{1f}\u{1}NULL"));
        let mut bag = HashMap::new();
        bag.insert("1\u{1f}12.50\u{1f}a".to_string(), 1);
        bag.insert("1\u{1f}12.51\u{1f}a".to_string(), -1);
        bag.insert("same".to_string(), 0);
        bag.insert("3\u{1f}\u{1}NULL\u{1f}x".to_string(), 2);
        assert_eq!(format_diff(&m, &bag, 5, ""), "except: columns (id | d | vc); only in duckdb 3 row(s): [1 | 12.50 | a]; [3 | NULL | x] x2; only in target 1 row(s): [1 | 12.51 | a]");
        assert_eq!(format_diff(&m, &bag, 0, ""), "except: columns (id | d | vc); only in duckdb 3 row(s); only in target 1 row(s)");
        let pm = PartMap { canon: "CAST(\"id\" AS VARCHAR)".into(), is_null_col: "\"id\"".into(), by_value: vec![(Some("1".into()), 1), (Some("o'k".into()), 1), (None, 1), (Some("2".into()), 2)], counts: BTreeMap::new() };
        assert_eq!(pm.predicate(1), "(CAST(\"id\" AS VARCHAR) IN ('1', 'o''k') OR \"id\" IS NULL)");
        assert_eq!(pm.predicate(2), "(CAST(\"id\" AS VARCHAR) IN ('2'))");
        assert_eq!(pm.predicate(3), "false");
        assert_eq!(pm.partition_of(&None), Some(1));
    }

    #[test]
    fn index_comparison() {
        let ix = |name: &str, desc: bool, comp: &str| TargetIndex { name: name.into(), index_id: 1, type_desc: "CLUSTERED".into(), is_primary_key: true, is_unique: true, is_unique_constraint: false,
            columns: vec![TargetIndexColumn { name: "id".into(), descending: desc, included: false, partition_ordinal: 0 }], filter: None, data_compression: comp.into(), fill_factor: 0, data_space: "PRIMARY".into(), is_partitioned: false };
        let (_, mut before, _) = table(&FIXTURE[..1], None);
        before.indexes = vec![ix("PK_g", false, "PAGE")];
        before.checks = vec![TargetCheck { name: "CK_a".into(), definition: "([id]>(0))".into(), is_not_trusted: false }];
        let mut after = before.clone();
        assert!(compare_indexes(&before, &after, &BTreeSet::new(), &[]).iter().all(|r| r.ok));
        after.indexes = vec![ix("PK_g", false, "NONE")];
        after.checks[0].is_not_trusted = true;
        let r = compare_indexes(&before, &after, &["pk_g".to_string()].into_iter().collect(), &[vec![Some("FK_x".into()), Some("not trusted".into())]]);
        let bad: Vec<&str> = r.iter().filter(|r| !r.ok).map(|r| r.detail.as_str()).collect();
        assert_eq!(bad.len(), 3, "{bad:?}");
        assert!(bad[0].contains("COMPRESSION PAGE") && bad[0].contains("COMPRESSION NONE") && bad[0].contains("is disabled"));
        assert!(bad[1].contains("CK_a is not trusted"));
        assert!(bad[2].contains("FK_x"));
        let mut desc = before.clone();
        desc.indexes = vec![ix("PK_g", true, "PAGE")];
        assert!(compare_indexes(&before, &desc, &BTreeSet::new(), &[]).iter().any(|r| !r.ok && r.detail.contains("id DESC")));
        after.indexes.clear();
        let r = compare_indexes(&before, &after, &BTreeSet::new(), &[]);
        assert!(r.iter().any(|r| !r.ok && r.detail.contains("index PK_g missing")));
        assert!(r.iter().any(|r| !r.ok && r.detail.contains("primary key missing")));
        assert_eq!(runlog::exit_code(&check_results(&r).unwrap_err()), 1);
    }
}
