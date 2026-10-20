//! Partition SWITCH loading and heap-then-index support (spec R4, work stream D; see docs/plan-0.1.1.md).
//!
//! `switch` strategy, per job (one partition value):
//!
//! 1. [`prepare_staging`]: `[schema].[<table>__stg_<value>]` from the target's own metadata
//!    (`target::staging_ddl`: same columns, clustered key, compression, `CHECK (col = value)`, on the
//!    partition's filegroup). A leftover staging table of the same name (from a killed run) is dropped
//!    first; only names containing `__stg_` are ever dropped here.
//! 2. The caller bulk loads the job's rows into [`Staging::table_ref`] (`TABLOCK`, one batch is fine:
//!    the staging table is invisible to readers).
//! 3. [`switch_in`]: builds the target's nonclustered indexes on the staging table (SWITCH needs
//!    them), then the staging row count (`sys.dm_db_partition_stats`) must equal the job's DuckDB count;
//!    the target partition must be empty unless `--replace-partitions` (then the old partition is
//!    switched out to `<table>__old_<value>` in the same transaction); `ALTER TABLE .. SWITCH TO ..
//!    PARTITION n`.
//! 4. [`drop_staging`], then the caller writes the job's done marker.
//!
//! On Standard Edition `CREATE INDEX` is single-threaded, so `switch` (each partition's clustered
//! index built by its own session) is the only way to build a large table's index in parallel.
use super::jobs::{safe_part, short_hash, Job};
use super::target::{self, TargetTable};
use super::{bracket, exec_batch_as, nstr, MsClient};
use crate::core::runlog::{self, Failure};
use anyhow::{bail, Context, Result};

/// Longest SQL Server identifier.
const MAX_IDENT: usize = 128;

#[derive(Debug, Clone)]
pub struct Staging {
    /// `<table>__stg_<value>`.
    pub name: String,
    pub schema: String,
    pub partition_number: i32,
    /// T-SQL literal of the partition value as used in the CHECK constraint and `$PARTITION`.
    pub partition_literal: String,
    /// The DDL the staging table was created with (quoted in SWITCH error messages).
    pub ddl: Vec<String>,
    /// Nonclustered indexes of the target, built on the staging table after the load (SWITCH needs them, Msg 4947).
    pub nc_ddl: Vec<String>,
    /// Set by [`finish_staging`]; `switch_in` builds the nonclustered indexes itself when it is false.
    pub nc_built: bool,
    /// Job id, for the run log.
    pub job: String,
}

impl Staging {
    /// `[schema].[name]`, the bulk-load target.
    pub fn table_ref(&self) -> String { format!("{}.{}", bracket(&self.schema), bracket(&self.name)) }
}

/// `<table>__<kind>_<value>`, only `[A-Za-z0-9_]`, at most 128 characters. Lossy sanitising or
/// truncation appends a hash of the original, so two values never share a staging table.
fn object_name(table: &str, kind: &str, value: &str) -> String {
    let prefix = name_prefix(table, kind);
    let v = safe_part(value, false);
    let room = MAX_IDENT - prefix.len();
    if v.len() <= room { return format!("{prefix}{v}"); }
    format!("{prefix}{}_{}", &v[..room - 9], short_hash(value))
}

/// `<table>__<kind>_` with the table part capped at 100 characters.
fn name_prefix(table: &str, kind: &str) -> String {
    let t = safe_part(table, false);
    let t = if t.len() > 100 { format!("{}_{}", &t[..91], short_hash(table)) } else { t };
    format!("{t}__{kind}_")
}

/// Staging table name for one partition value (the raw value, not its T-SQL literal).
pub fn staging_name(table: &str, value: &str) -> String { object_name(table, "stg", value) }

/// Name of the table that keeps a replaced partition until `--drop-old`.
pub fn old_name(table: &str, value: &str) -> String { object_name(table, "old", value) }

/// The raw value of a T-SQL literal (`N'O''Brien'` -> `O'Brien`, `42` -> `42`).
pub fn literal_value(lit: &str) -> String {
    let s = lit.trim();
    let s = s.strip_prefix('N').filter(|r| r.starts_with('\'')).unwrap_or(s);
    match s.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')) {
        Some(inner) => inner.replace("''", "'"),
        None => s.to_string(),
    }
}

/// Render the job's literal for the target's partition column: `N'..'` loses the `N` for
/// `char`/`varchar` columns, so the CHECK constraint compares without an implicit conversion
/// (which would stop SQL Server from proving the constraint matches the partition, error 4972).
pub fn adapt_literal(lit: &str, t: &TargetTable) -> String {
    let Some(p) = &t.partitioning else { return lit.to_string() };
    let unicode = t.column(&p.column).map(|c| matches!(c.type_name.as_str(), "nchar" | "nvarchar" | "ntext" | "sysname")).unwrap_or(true);
    let s = lit.trim();
    if !unicode && s.starts_with("N'") { s[1..].to_string() } else { s.to_string() }
}

fn ensure_disposable_name(name: &str, markers: &[&str]) -> Result<()> {
    if !markers.iter().any(|m| name.contains(m)) {
        return Err(Failure::Guard(format!("refusing to drop table {name:?}: only staging tables whose name contains {} are dropped automatically", markers.join(" or "))).into());
    }
    Ok(())
}

fn obj(schema: &str, name: &str) -> String { format!("{}.{}", bracket(schema), bracket(name)) }

fn drop_if_exists_sql(schema: &str, name: &str) -> String {
    let o = obj(schema, name);
    format!("IF OBJECT_ID({}, N'U') IS NOT NULL DROP TABLE {o}", nstr(&o))
}

async fn scalar_i64(c: &mut MsClient, sql: &str) -> Result<Option<i64>> {
    let rows = c.simple_query(sql).await.with_context(|| format!("querying: {sql}"))?.into_first_result().await?;
    let Some(r) = rows.first() else { return Ok(None) };
    if let Ok(v) = r.try_get::<i64, _>(0) { return Ok(v); }
    if let Ok(v) = r.try_get::<i32, _>(0) { return Ok(v.map(i64::from)); }
    Ok(r.try_get::<i16, _>(0).ok().flatten().map(i64::from))
}

/// `SELECT $PARTITION.<function>(<literal>)` (from `target::partition_number_sql`).
pub fn partition_number_query(t: &TargetTable, literal: &str) -> Result<String> {
    target::partition_number_sql(t, literal).with_context(|| format!("{} is not partitioned", t.full_name()))
}

/// Rows of one table (all partitions) or one partition, from `sys.dm_db_partition_stats` (heap or clustered index).
async fn stats_rows(c: &mut MsClient, schema: &str, name: &str, partition: Option<i32>) -> Result<i64> {
    let o = obj(schema, name);
    let part = partition.map(|n| format!(" AND partition_number = {n}")).unwrap_or_default();
    let sql = format!("SELECT CAST(ISNULL(SUM(row_count), 0) AS BIGINT) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID({}) AND index_id IN (0, 1){part}", nstr(&o));
    Ok(scalar_i64(c, &sql).await?.unwrap_or(0))
}

async fn table_exists(c: &mut MsClient, schema: &str, name: &str) -> Result<bool> {
    let o = obj(schema, name);
    Ok(scalar_i64(c, &format!("SELECT CAST(CASE WHEN OBJECT_ID({}, N'U') IS NULL THEN 0 ELSE 1 END AS BIGINT)", nstr(&o))).await?.unwrap_or(0) == 1)
}

/// Create the staging table for one `switch` job (dropping a leftover one of the same name first).
pub async fn prepare_staging(c: &mut MsClient, t: &TargetTable, job: &Job) -> Result<Staging> {
    let Some(pv) = job.partition_value.as_deref() else {
        bail!("job {}: the switch strategy needs a partition value (split the table with column:<partition column> or files:<dir>/<col>=*/; NULL keys and hash/range jobs cannot be switched); use --strategy {}=heap-then-index or serial", job.id, t.name);
    };
    let Some(p) = &t.partitioning else {
        return Err(Failure::Check(format!("{}: the switch strategy needs a partitioned target table; use --strategy {}=heap-then-index or serial", t.full_name(), t.name)).into());
    };
    if let Some(col) = &job.partition_column {
        if !col.eq_ignore_ascii_case(&p.column) {
            return Err(Failure::Check(format!("job {}: split column {col} is not the partition column {} of {} (partition function {})", job.id, p.column, t.full_name(), p.function)).into());
        }
    }
    let literal = adapt_literal(pv, t);
    let partition_number = scalar_i64(c, &partition_number_query(t, &literal)?).await
        .with_context(|| format!("job {}: finding the partition of {literal} in {}", job.id, p.function))?
        .with_context(|| format!("job {}: $PARTITION.{}({literal}) returned no row", job.id, p.function))? as i32;
    let name = staging_name(&t.name, &literal_value(pv));
    ensure_disposable_name(&name, &["__stg_"])?;
    exec_batch_as(c, "target", &job.id, &drop_if_exists_sql(&t.schema, &name)).await
        .with_context(|| format!("job {}: dropping leftover staging table {name}", job.id))?;
    let ddl = target::staging_ddl_for_partition(t, &name, &literal, partition_number).with_context(|| format!("job {}: generating staging DDL for {name}", job.id))?;
    let nc_ddl = target::staging_nonclustered_ddl(t, &name, Some(&literal), Some(partition_number)).with_context(|| format!("job {}: generating staging index DDL for {name}", job.id))?;
    for s in &ddl {
        exec_batch_as(c, "target", &job.id, s).await.with_context(|| format!("job {}: creating staging table {name}", job.id))?;
    }
    runlog::note(&format!("job {}: staging table {}.{name} for partition {partition_number} ({literal})", job.id, t.schema));
    Ok(Staging { name, schema: t.schema.clone(), partition_number, partition_literal: literal, ddl, nc_ddl, nc_built: false, job: job.id.clone() })
}

async fn build_staging_indexes(c: &mut MsClient, s: &Staging) -> Result<()> {
    if s.nc_ddl.is_empty() { return Ok(()); }
    // filtered indexes need it (Msg 1934)
    exec_batch_as(c, "target", &s.job, "SET QUOTED_IDENTIFIER ON").await?;
    for ddl in &s.nc_ddl {
        exec_batch_as(c, "target", &s.job, ddl).await.with_context(|| format!("job {}: indexing staging table {}", s.job, s.table_ref()))?;
    }
    Ok(())
}

/// After the bulk load, before [`switch_in`]: build the target's nonclustered indexes on the staging
/// table (`target::staging_nonclustered_ddl`; SWITCH needs identical indexes, Msg 4947), so the load
/// itself goes into a table with only the clustered key. Optional: `switch_in` does it when this was
/// not called.
pub async fn finish_staging(c: &mut MsClient, t: &TargetTable, s: &mut Staging) -> Result<()> {
    debug_assert!(s.name.contains("__stg_") && s.schema == t.schema);
    if !s.nc_built { build_staging_indexes(c, s).await?; }
    s.nc_built = true;
    Ok(())
}

/// Switch a loaded staging table into its target partition.
pub async fn switch_in(c: &mut MsClient, t: &TargetTable, s: &Staging, expected_rows: i64, replace_partitions: bool) -> Result<()> {
    let tgt = obj(&t.schema, &t.name);
    let stg = s.table_ref();
    let n = s.partition_number;
    let switch_sql = format!("ALTER TABLE {stg} SWITCH TO {tgt} PARTITION {n}");
    // (under --plan exec_batch_as only records these)
    if !s.nc_built { build_staging_indexes(c, s).await?; }
    if runlog::planning() {
        // the staging table only exists on paper: record the statements, skip the counts
        if replace_partitions {
            let old = old_name(&t.name, &literal_value(&s.partition_literal));
            exec_batch_as(c, "target", &s.job, &format!("-- if partition {n} is not empty: ALTER TABLE {tgt} SWITCH PARTITION {n} TO {}", obj(&t.schema, &old))).await?;
        }
        return exec_batch_as(c, "target", &s.job, &switch_sql).await;
    }
    let staged = stats_rows(c, &s.schema, &s.name, None).await?;
    if staged != expected_rows {
        return Err(Failure::Check(format!("job {}: staging table {stg} has {staged} rows but DuckDB produced {expected_rows}; not switching", s.job)).into());
    }
    let existing = stats_rows(c, &t.schema, &t.name, Some(n)).await?;
    let mut batch = switch_sql.clone();
    if existing > 0 {
        if !replace_partitions {
            return Err(Failure::Check(format!("job {}: partition {n} ({}) of {tgt} already holds {existing} rows; pass --replace-partitions to switch the old rows out to a __old_ table", s.job, s.partition_literal)).into());
        }
        let old = old_name(&t.name, &literal_value(&s.partition_literal));
        ensure_disposable_name(&old, &["__old_"])?;
        if table_exists(c, &t.schema, &old).await? {
            return Err(Failure::Check(format!("job {}: {} already exists (kept from an earlier --replace-partitions); drop it with --drop-old first", s.job, obj(&t.schema, &old))).into());
        }
        let old_ddl = target::staging_ddl_for_partition(t, &old, &s.partition_literal, n)?;
        let old_nc = target::staging_nonclustered_ddl(t, &old, Some(&s.partition_literal), Some(n))?;
        for ddl in old_ddl.iter().chain(old_nc.iter()) {
            exec_batch_as(c, "target", &s.job, ddl).await.with_context(|| format!("job {}: creating {old}", s.job))?;
        }
        // switching OUT into a table whose CHECK is narrower than the partition range fails with 4972
        // (a RANGE RIGHT partition 'BBB' <= mkt < 'CCC' is wider than mkt = 'BBB'): the holding table needs none
        drop_check_constraints(c, &t.schema, &old, &s.job).await?;
        // one transaction: readers see either the old or the new partition, never an empty one
        batch = format!("SET XACT_ABORT ON;\nBEGIN TRANSACTION;\nALTER TABLE {tgt} SWITCH PARTITION {n} TO {};\n{switch_sql};\nCOMMIT TRANSACTION;", obj(&t.schema, &old));
        runlog::note(&format!("job {}: replacing {existing} rows of partition {n}; the old rows go to {old}", s.job));
    }
    if let Err(e) = exec_batch_as(c, "target", &s.job, &batch).await {
        let _ = c.simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await;
        let detail = switch_error_detail(t, s, server_error_code(&e));
        return Err(e.context(detail));
    }
    runlog::note(&format!("job {}: switched {staged} rows into partition {n} of {tgt}", s.job));
    Ok(())
}

async fn drop_check_constraints(c: &mut MsClient, schema: &str, name: &str, job: &str) -> Result<()> {
    ensure_disposable_name(name, &["__old_"])?;
    let o = obj(schema, name);
    let rows = c.simple_query(&format!("SELECT name FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID({})", nstr(&o))).await?.into_first_result().await?;
    let names: Vec<String> = rows.iter().filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string())).collect();
    for ck in names {
        exec_batch_as(c, "target", job, &format!("ALTER TABLE {o} DROP CONSTRAINT {}", bracket(&ck))).await?;
    }
    Ok(())
}

fn server_error_code(e: &anyhow::Error) -> Option<u32> {
    e.chain().find_map(|x| match x.downcast_ref::<tiberius::error::Error>() { Some(tiberius::error::Error::Server(te)) => Some(te.code()), _ => None })
}

/// Explain a failed SWITCH: the staging DDL next to the target's partition, key and compression.
fn switch_error_detail(t: &TargetTable, s: &Staging, code: Option<u32>) -> String {
    let n = s.partition_number;
    let mut m = format!("SWITCH of {} into partition {n} of {} failed", s.table_ref(), obj(&t.schema, &t.name));
    if let Some(code) = code { m.push_str(&format!(" (error {code})")); }
    m.push_str(". ");
    m.push_str(match code {
        Some(4972) => "The CHECK constraint on the staging table does not prove that every row falls into the target partition: the partition column must be NOT NULL in the staging table and the CHECK must use the same type/collation as the partition function (no implicit conversion), inside the partition's boundaries.",
        Some(4904) => "The target partition is not empty (another session loaded it after the check); rerun with --replace-partitions to switch the old rows out.",
        Some(4947) => "The staging table's indexes differ from the target (every index of the target, including nonclustered aligned indexes, must exist on the staging table with the same key, compression and settings).",
        Some(4939) => "The staging table is on a different filegroup than the target partition.",
        _ => "Check that the staging table matches the target exactly: CHECK constraint on the partition column within the boundaries, same clustered key, same indexes, same DATA_COMPRESSION, same filegroup as the partition.",
    });
    if let Some(p) = &t.partitioning {
        let (lo, hi) = boundaries_of(p, n);
        m.push_str(&format!("\nTarget: partition function {} on column {} ({}), partition {n} covers {lo} .. {hi}, filegroup {}.",
            p.function, p.column, if p.range_right { "RANGE RIGHT" } else { "RANGE LEFT" },
            p.filegroups.get((n - 1).max(0) as usize).map(|s| s.as_str()).unwrap_or("?")));
    }
    if let Some(ci) = t.clustered() {
        m.push_str(&format!("\nTarget clustered index {} ({}) DATA_COMPRESSION = {}.", ci.name,
            ci.columns.iter().filter(|c| !c.included).map(|c| format!("{}{}", c.name, if c.descending { " DESC" } else { "" })).collect::<Vec<_>>().join(", "), ci.data_compression));
    }
    let others: Vec<String> = t.indexes.iter().filter(|i| i.type_desc.eq_ignore_ascii_case("NONCLUSTERED")).map(|i| i.name.clone()).collect();
    if !others.is_empty() { m.push_str(&format!("\nTarget nonclustered indexes (must exist on the staging table too): {}.", others.join(", "))); }
    m.push_str(&format!("\nStaging DDL:\n{}", s.ddl.join("\n")));
    m
}

/// Lower and upper boundary of partition `n` (1-based) as text, `-inf` / `+inf` at the ends.
fn boundaries_of(p: &target::TargetPartitioning, n: i32) -> (String, String) {
    let i = (n - 1).max(0) as usize;
    let lo = if i == 0 { "-inf".to_string() } else { p.boundaries.get(i - 1).cloned().unwrap_or_else(|| "?".into()) };
    let hi = p.boundaries.get(i).cloned().unwrap_or_else(|| "+inf".into());
    if p.range_right { (format!(">= {lo}"), format!("< {hi}")) } else { (format!("> {lo}"), format!("<= {hi}")) }
}

/// Drop a staging (or `__old_`) table.
pub async fn drop_staging(c: &mut MsClient, s: &Staging) -> Result<()> {
    ensure_disposable_name(&s.name, &["__stg_", "__old_"])?;
    exec_batch_as(c, "target", &s.job, &drop_if_exists_sql(&s.schema, &s.name)).await
}

/// `--drop-old`: drop every `<table>__old_*` table kept by `--replace-partitions`. Returns their names.
pub async fn drop_old_tables(c: &mut MsClient, schema: &str, table: &str) -> Result<Vec<String>> {
    let prefix = name_prefix(table, "old");
    let like = prefix.replace('[', "[[]").replace('_', "[_]").replace('%', "[%]");
    let sql = format!("SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = {} AND t.name LIKE {} ORDER BY t.name", nstr(schema), nstr(&format!("{like}%")));
    let rows = c.simple_query(&sql).await.context("listing __old_ tables")?.into_first_result().await?;
    let names: Vec<String> = rows.iter().filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string())).collect();
    for n in &names {
        ensure_disposable_name(n, &["__old_"])?;
        exec_batch_as(c, "target", "drop-old", &drop_if_exists_sql(schema, n)).await?;
    }
    Ok(names)
}

/// heap-then-index: capture the CREATE statements of every index of `t` (from its metadata), then
/// drop them (nonclustered first, then the PK/clustered index). Returns the DDL for
/// [`recreate_indexes`]; it is also written to the run log so a killed run can be finished by hand.
pub async fn capture_and_drop_indexes(c: &mut MsClient, t: &TargetTable) -> Result<Vec<String>> {
    let ddl = target::index_ddl(t, &obj(&t.schema, &t.name));
    let drops = target::drop_index_ddl(t);
    let real_indexes = t.indexes.iter().filter(|i| !i.type_desc.eq_ignore_ascii_case("HEAP")).count();
    if real_indexes > 0 && (ddl.is_empty() || drops.is_empty()) {
        bail!("{}: has {real_indexes} index(es) but no index DDL could be generated; not dropping anything", t.full_name());
    }
    for s in &ddl { runlog::note(&format!("{}: index DDL captured before drop: {s}", t.full_name())); }
    let job = format!("{}#indexes", t.full_name());
    for s in &drops {
        exec_batch_as(c, "target", &job, s).await.with_context(|| format!("dropping indexes of {}", t.full_name()))?;
    }
    Ok(ddl)
}

/// Recreate the indexes captured by [`capture_and_drop_indexes`], in order (clustered first).
pub async fn recreate_indexes(c: &mut MsClient, ddl: &[String], job: &str) -> Result<()> {
    // filtered indexes need it (Msg 1934)
    if !ddl.is_empty() { exec_batch_as(c, "target", job, "SET QUOTED_IDENTIFIER ON").await?; }
    for s in ddl {
        exec_batch_as(c, "target", job, s).await.with_context(|| format!("recreating index: {s}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mssql::target::*;

    #[test]
    fn staging_names() {
        assert_eq!(staging_name("kpi", "BBB"), "kpi__stg_BBB");
        assert_eq!(old_name("kpi", "BBB"), "kpi__old_BBB");
        assert_eq!(staging_name("kpi", "2024"), "kpi__stg_2024");
        let a = staging_name("kpi", "A-B");
        let b = staging_name("kpi", "A_B");
        assert!(a.starts_with("kpi__stg_A_B_") && b == "kpi__stg_A_B" && a != b, "{a} {b}");
        let all_ok = |s: &str| s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        for (t, v) in [("kpi", "O'Brien & co"), ("my table", "x"), (&"t".repeat(300) as &str, &"v".repeat(300) as &str), ("kpi", &"é".repeat(200) as &str), ("kpi", "")] {
            let n = staging_name(t, v);
            assert!(n.len() <= 128 && all_ok(&n) && n.contains("__stg_"), "{n}");
        }
        assert_ne!(staging_name("kpi", &format!("{}a", "v".repeat(200))), staging_name("kpi", &format!("{}b", "v".repeat(200))));
        assert!(staging_name(&"t".repeat(300), "v").starts_with(&name_prefix(&"t".repeat(300), "stg")));
    }

    #[test]
    fn literal_values() {
        assert_eq!(literal_value("N'O''Brien'"), "O'Brien");
        assert_eq!(literal_value("'2024-01-31'"), "2024-01-31");
        assert_eq!(literal_value("42"), "42");
        assert_eq!(literal_value("N'N'"), "N");
    }

    fn table(col_type: &str) -> TargetTable {
        TargetTable {
            schema: "dbo".into(), name: "t".into(), object_id: 1,
            columns: vec![TargetColumn { name: "mkt".into(), column_id: 1, type_name: col_type.into(), max_length: 3, precision: 0, scale: 0, collation: None, nullable: false, has_default: false, is_identity: false, is_computed: false }],
            indexes: vec![], checks: vec![], row_count: 0, modify_date: String::new(),
            partitioning: Some(TargetPartitioning { scheme: "ps".into(), function: "pf".into(), column: "mkt".into(), range_right: true, boundaries: vec!["'AAA'".into(), "'BBB'".into()], filegroups: vec!["PRIMARY".into(); 3] }),
        }
    }

    #[test]
    fn literals_follow_the_target_type() {
        assert_eq!(adapt_literal("N'BBB'", &table("char")), "'BBB'");
        assert_eq!(adapt_literal("N'BBB'", &table("varchar")), "'BBB'");
        assert_eq!(adapt_literal("N'BBB'", &table("nvarchar")), "N'BBB'");
        assert_eq!(adapt_literal("42", &table("int")), "42");
        assert_eq!(partition_number_query(&table("char"), "'BBB'").unwrap(), "SELECT $PARTITION.[pf]('BBB')");
        let p = table("char").partitioning.unwrap();
        assert_eq!(boundaries_of(&p, 1), (">= -inf".to_string(), "< 'AAA'".to_string()));
        assert_eq!(boundaries_of(&p, 3), (">= 'BBB'".to_string(), "< +inf".to_string()));
    }

    #[test]
    fn only_staging_names_are_dropped() {
        assert!(ensure_disposable_name("kpi__stg_AAA", &["__stg_"]).is_ok());
        let e = ensure_disposable_name("kpi", &["__stg_", "__old_"]).unwrap_err();
        assert_eq!(runlog::exit_code(&e), 3);
        assert_eq!(drop_if_exists_sql("dbo", "kpi__stg_A"), "IF OBJECT_ID(N'[dbo].[kpi__stg_A]', N'U') IS NOT NULL DROP TABLE [dbo].[kpi__stg_A]");
    }

    /// Live round trip against a disposable database (never run by default):
    /// `DUCKBRIDGE_TEST_SWITCH=host:port:user:password:database cargo test --release switch_live -- --ignored`
    /// The database must exist and hold `dbo.t` partitioned as in docs/handoff-D.md.
    #[test]
    #[ignore]
    fn switch_live() {
        let Ok(spec) = std::env::var("DUCKBRIDGE_TEST_SWITCH") else { return };
        let parts: Vec<&str> = spec.splitn(5, ':').collect();
        let (host, port, user, password, db) = (parts[0], parts[1].parse::<u16>().unwrap(), parts[2], parts[3], parts[4]);
        assert!(db.starts_with('_'), "use a disposable database");
        let target = crate::mssql::MssqlTarget { host: host.into(), port, user: user.into(), password: password.into(), database: db.into(), trust_cert: true, encrypt: false, windows_auth: false };
        let rt = crate::mssql::runtime().unwrap();
        rt.block_on(async {
            let mut c = target.connect(db).await.unwrap();
            exec_batch_as(&mut c, "target", "test", "TRUNCATE TABLE dbo.t").await.unwrap();
            drop_old_tables(&mut c, "dbo", "t").await.unwrap();
            let t = read_target_tables(&mut c, &[("dbo".into(), "t".into())]).await.unwrap().remove(0);
            assert!(t.partitioning.is_some());

            let con = duckdb::Connection::open_in_memory().unwrap();
            con.execute_batch("CREATE TABLE t (mkt VARCHAR NOT NULL, id INTEGER NOT NULL, v VARCHAR, PRIMARY KEY (mkt, id));
                INSERT INTO t SELECT CASE WHEN i % 4 = 0 THEN 'AAA' WHEN i % 4 = 1 THEN 'BBB' ELSE 'CCC' END, i, 'v' || i FROM range(200) r(i);").unwrap();
            let cat = crate::core::catalog::read_catalog(&con, "test", &Default::default()).unwrap();
            let splits = crate::mssql::jobs::parse_splits(&["t=column:mkt".into()]).unwrap();
            let sts = crate::mssql::jobs::parse_strategies(&["t=switch".into()]).unwrap();
            let plan = crate::mssql::jobs::plan_jobs(&con, &cat, &splits, &sts, crate::mssql::jobs::Strategy::Serial, 1_000_000, 4).unwrap();
            assert_eq!(plan.jobs.len(), 3);
            let values = |sql: &str| -> String {
                con.query_row(&format!("SELECT string_agg(format('(''{{}}'', {{}}, ''{{}}'')', mkt, id, v), ', ') FROM ({sql})"), [], |r| r.get(0)).unwrap()
            };
            async fn load(c: &mut MsClient, t: &TargetTable, job: &Job, vals: &str, expected: i64, replace: bool) -> Result<Staging> {
                let mut s = prepare_staging(c, t, job).await?;
                exec_batch_as(c, "target", &job.id, &format!("INSERT INTO {} (mkt, id, v) VALUES {vals}", s.table_ref())).await?;
                if replace { finish_staging(c, t, &mut s).await?; } // both paths: explicit and inside switch_in
                let r = switch_in(c, t, &s, expected, replace).await;
                drop_staging(c, &s).await?;
                r.map(|_| s)
            }
            for j in &plan.jobs {
                let s = load(&mut c, &t, j, &values(&j.source_sql), j.est_rows, false).await.unwrap();
                assert!(s.name.starts_with("t__stg_"));
            }
            let counts = partition_row_counts(&mut c, &t).await.unwrap();
            assert_eq!(counts.iter().map(|(_, n)| n).sum::<i64>(), 200, "{counts:?}");
            assert_eq!(counts.iter().find(|(p, _)| *p == 3).map(|(_, n)| *n), Some(50), "{counts:?}"); // BBB
            let bbb = plan.jobs.iter().find(|j| j.label == "t:BBB").unwrap();
            // a second load of a finished partition is refused, a wrong count too
            let e = load(&mut c, &t, bbb, &values(&bbb.source_sql), bbb.est_rows, false).await.unwrap_err();
            assert!(format!("{e:#}").contains("already holds 50 rows"), "{e:#}");
            let e = load(&mut c, &t, bbb, &values(&bbb.source_sql), bbb.est_rows + 1, true).await.unwrap_err();
            assert!(format!("{e:#}").contains("has 50 rows but DuckDB produced 51"), "{e:#}");
            // replace: old rows go to t__old_BBB in the same transaction
            load(&mut c, &t, bbb, &values(&format!("{} AND id < 100", bbb.source_sql)), 25, true).await.unwrap();
            let counts = partition_row_counts(&mut c, &t).await.unwrap();
            assert_eq!(counts.iter().find(|(p, _)| *p == 3).map(|(_, n)| *n), Some(25), "{counts:?}");
            assert_eq!(stats_rows(&mut c, "dbo", "t__old_BBB", None).await.unwrap(), 50);
            let e = load(&mut c, &t, bbb, &values(&bbb.source_sql), bbb.est_rows, true).await.unwrap_err();
            assert!(format!("{e:#}").contains("--drop-old"), "{e:#}");
            assert_eq!(drop_old_tables(&mut c, "dbo", "t").await.unwrap(), vec!["t__old_BBB".to_string()]);
            // a leftover staging table (killed run) is dropped and recreated
            let s = prepare_staging(&mut c, &t, bbb).await.unwrap();
            exec_batch_as(&mut c, "target", "test", &format!("INSERT INTO {} (mkt, id, v) VALUES ('BBB', 1, 'x')", s.table_ref())).await.unwrap();
            let s2 = prepare_staging(&mut c, &t, bbb).await.unwrap();
            assert_eq!(stats_rows(&mut c, "dbo", &s2.name, None).await.unwrap(), 0);
            drop_staging(&mut c, &s2).await.unwrap();
            assert!(!table_exists(&mut c, "dbo", &s2.name).await.unwrap());

            // heap-then-index on the same table: capture, drop, recreate
            let ddl = capture_and_drop_indexes(&mut c, &t).await.unwrap();
            assert!(!ddl.is_empty());
            let after = read_target_tables(&mut c, &[("dbo".into(), "t".into())]).await.unwrap().remove(0);
            assert!(after.is_heap());
            recreate_indexes(&mut c, &ddl, "test").await.unwrap();
            let again = read_target_tables(&mut c, &[("dbo".into(), "t".into())]).await.unwrap().remove(0);
            assert_eq!(again.pk().map(|i| (i.name.clone(), i.data_compression.clone())), t.pk().map(|i| (i.name.clone(), i.data_compression.clone())));
            exec_batch_as(&mut c, "target", "test", "TRUNCATE TABLE dbo.t").await.unwrap();
        });
    }

    #[test]
    fn switch_error_explains_the_mismatch() {
        let t = table("char");
        let s = Staging { name: "t__stg_BBB".into(), schema: "dbo".into(), partition_number: 3, partition_literal: "'BBB'".into(), ddl: vec!["CREATE TABLE [dbo].[t__stg_BBB] (...)".into()], nc_ddl: vec![], nc_built: false, job: "dbo.t#BBB".into() };
        let m = switch_error_detail(&t, &s, Some(4972));
        assert!(m.contains("error 4972") && m.contains("CHECK constraint") && m.contains("partition 3 covers >= 'BBB' .. < +inf") && m.contains("CREATE TABLE [dbo].[t__stg_BBB]"), "{m}");
    }
}
