//! Target-table metadata and pre-flight checks for `--into-existing` loads (spec R2, R3).
//!
//! The target DDL is the contract: DuckBridge reads `sys.columns`, `sys.types`, `sys.indexes`,
//! `sys.index_columns`, `sys.partitions`, `sys.partition_schemes`/`functions` and
//! `sys.check_constraints`, maps DuckDB columns onto the target by name, and checks every value that
//! could lose information **before** any data moves. Nothing in this module issues DDL.
//!
//! The types below are the shared contract (see docs/plan-0.1.1.md); work stream B implemented the
//! functions. DDL helpers (`index_ddl`, `staging_ddl`, …) only build statement text; callers send it
//! through `mssql::exec_batch`.
use super::{bracket, nstr, MsClient};
use crate::core::model::{Column, Table};
use crate::core::runlog::Failure;
use crate::core::typemap::{parse_type, quote_duck};
use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashMap;
use tiberius::Row;

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetColumn {
    pub name: String,
    pub column_id: i32,
    /// `sys.types.name`, lowercase (`varchar`, `nvarchar`, `decimal`, `datetime2`, …).
    pub type_name: String,
    /// Bytes; -1 for MAX. `nvarchar(n)` reports 2n.
    pub max_length: i16,
    pub precision: u8,
    pub scale: u8,
    pub collation: Option<String>,
    pub nullable: bool,
    pub has_default: bool,
    pub is_identity: bool,
    pub is_computed: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetIndexColumn {
    pub name: String,
    pub descending: bool,
    pub included: bool,
    pub partition_ordinal: i32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetIndex {
    pub name: String,
    pub index_id: i32,
    /// `CLUSTERED`, `NONCLUSTERED`, `HEAP`.
    pub type_desc: String,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_unique_constraint: bool,
    pub columns: Vec<TargetIndexColumn>,
    pub filter: Option<String>,
    /// `NONE`, `ROW`, `PAGE` (of partition 1).
    pub data_compression: String,
    pub fill_factor: u8,
    /// Filegroup name or partition scheme name.
    pub data_space: String,
    pub is_partitioned: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetPartitioning {
    pub scheme: String,
    pub function: String,
    pub column: String,
    pub range_right: bool,
    /// Boundary values as T-SQL literals, in order.
    pub boundaries: Vec<String>,
    /// Filegroup per partition number (index 0 = partition 1).
    pub filegroups: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetCheck {
    pub name: String,
    pub definition: String,
    pub is_not_trusted: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetTable {
    pub schema: String,
    pub name: String,
    pub object_id: i32,
    pub columns: Vec<TargetColumn>,
    pub indexes: Vec<TargetIndex>,
    pub partitioning: Option<TargetPartitioning>,
    pub checks: Vec<TargetCheck>,
    pub row_count: i64,
    pub modify_date: String,
}

impl TargetTable {
    pub fn full_name(&self) -> String { format!("{}.{}", self.schema, self.name) }
    pub fn clustered(&self) -> Option<&TargetIndex> { self.indexes.iter().find(|i| i.type_desc.eq_ignore_ascii_case("CLUSTERED")) }
    pub fn pk(&self) -> Option<&TargetIndex> { self.indexes.iter().find(|i| i.is_primary_key) }
    pub fn is_heap(&self) -> bool { self.clustered().is_none() }
    /// Case-insensitive column lookup (SQL Server identifiers are CI by default).
    pub fn column(&self, name: &str) -> Option<&TargetColumn> { self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)) }
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    pub duck_index: usize,
    pub duck: Column,
    pub target: TargetColumn,
}

#[derive(Debug, Clone)]
pub struct TableMapping {
    pub table: TargetTable,
    /// In target `column_id` order; this is the bulk column list.
    pub columns: Vec<ColumnMapping>,
    /// Nullable / defaulted target columns with no DuckDB counterpart.
    pub omitted_target_columns: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkCheck { None, Full, Hash }

#[derive(Debug, Clone)]
pub struct PreflightOptions {
    pub check_pk: PkCheck,
    pub allow_rescale: bool,
    pub collation_check: bool,
    pub samples: usize,
}

impl Default for PreflightOptions {
    fn default() -> Self { PreflightOptions { check_pk: PkCheck::None, allow_rescale: false, collation_check: true, samples: 5 } }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Finding {
    pub table: String,
    pub column: Option<String>,
    /// LENGTH, CODEPAGE, NOT_NULL, RANGE, SCALE, PRECISION, BIT, DATE_RANGE, DATETIME_PRECISION, PK_DUP, CI_COLLISION, EXTRA_COLUMN, MISSING_COLUMN, TYPE
    pub code: &'static str,
    pub message: String,
    pub samples: Vec<String>,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}{}: {}", self.code, self.table, self.column.as_ref().map(|c| format!(".{c}")).unwrap_or_default(), self.message)?;
        if !self.samples.is_empty() { write!(f, " (samples: {})", self.samples.join(", "))?; }
        Ok(())
    }
}

// ---- functions implemented by work stream B -------------------------------------------------

/// Everything except `DATETIME_PRECISION` (and other findings whose message starts with
/// `warning:`) stops the load.
pub fn is_blocking(f: &Finding) -> bool {
    f.code != "DATETIME_PRECISION" && !f.message.starts_with("warning:")
}

// ---- metadata reader ------------------------------------------------------------------------

fn cell_str(r: &Row, i: usize) -> Result<Option<String>> {
    Ok(r.try_get::<&str, _>(i).with_context(|| format!("metadata column {i}"))?.map(|s| s.to_string()))
}
fn cell_string(r: &Row, i: usize) -> Result<String> { Ok(cell_str(r, i)?.unwrap_or_default()) }
fn cell_i32(r: &Row, i: usize) -> Result<i32> { Ok(r.try_get::<i32, _>(i).with_context(|| format!("metadata column {i}"))?.unwrap_or(0)) }
fn cell_i64(r: &Row, i: usize) -> Result<i64> { Ok(r.try_get::<i64, _>(i).with_context(|| format!("metadata column {i}"))?.unwrap_or(0)) }
fn cell_i16(r: &Row, i: usize) -> Result<i16> { Ok(r.try_get::<i16, _>(i).with_context(|| format!("metadata column {i}"))?.unwrap_or(0)) }
fn cell_u8(r: &Row, i: usize) -> Result<u8> { Ok(r.try_get::<u8, _>(i).with_context(|| format!("metadata column {i}"))?.unwrap_or(0)) }
fn cell_bool(r: &Row, i: usize) -> Result<bool> { Ok(r.try_get::<bool, _>(i).with_context(|| format!("metadata column {i}"))?.unwrap_or(false)) }

async fn meta_query(c: &mut MsClient, sql: &str) -> Result<Vec<Row>> {
    let head: String = sql.chars().take(160).collect();
    let rows = c.simple_query(sql).await.with_context(|| format!("reading target metadata: {head}"))?.into_first_result().await.with_context(|| format!("reading target metadata: {head}"))?;
    Ok(rows)
}

fn two_part(schema: &str, name: &str) -> String { format!("{}.{}", bracket(schema), bracket(name)) }

fn id_list<T: std::fmt::Display>(ids: impl IntoIterator<Item = T>) -> String {
    let v: Vec<String> = ids.into_iter().map(|i| i.to_string()).collect();
    if v.is_empty() { "NULL".into() } else { v.join(", ") }
}

/// `sql_variant` boundary value as text, by base type (dates in ISO 8601, floats with 17 digits).
const RANGE_VALUE_TEXT: &str = "CASE CAST(SQL_VARIANT_PROPERTY(prv.value, 'BaseType') AS nvarchar(128)) \
     WHEN 'date' THEN CONVERT(nvarchar(4000), CAST(prv.value AS date), 23) \
     WHEN 'datetime' THEN CONVERT(nvarchar(4000), CAST(prv.value AS datetime), 126) \
     WHEN 'smalldatetime' THEN CONVERT(nvarchar(4000), CAST(prv.value AS smalldatetime), 126) \
     WHEN 'datetime2' THEN CONVERT(nvarchar(4000), CAST(prv.value AS datetime2(7)), 126) \
     WHEN 'datetimeoffset' THEN CONVERT(nvarchar(4000), CAST(prv.value AS datetimeoffset(7)), 127) \
     WHEN 'float' THEN CONVERT(nvarchar(4000), CAST(prv.value AS float), 3) \
     WHEN 'real' THEN CONVERT(nvarchar(4000), CAST(prv.value AS float), 3) \
     WHEN 'money' THEN CONVERT(nvarchar(4000), CAST(prv.value AS money), 2) \
     WHEN 'smallmoney' THEN CONVERT(nvarchar(4000), CAST(prv.value AS money), 2) \
     WHEN 'binary' THEN CONVERT(nvarchar(4000), CAST(prv.value AS varbinary(8000)), 1) \
     WHEN 'varbinary' THEN CONVERT(nvarchar(4000), CAST(prv.value AS varbinary(8000)), 1) \
     ELSE CAST(prv.value AS nvarchar(4000)) END";

/// Render a boundary value as a T-SQL literal of its base type.
fn tsql_literal(base_type: Option<&str>, text: Option<&str>) -> String {
    let (Some(base), Some(v)) = (base_type, text) else { return "NULL".into() };
    let quoted = |prefix: &str| format!("{prefix}'{}'", v.replace('\'', "''"));
    match base.to_ascii_lowercase().as_str() {
        "char" | "varchar" | "text" => quoted(""),
        "nchar" | "nvarchar" | "ntext" | "sysname" => quoted("N"),
        "tinyint" | "smallint" | "int" | "bigint" | "decimal" | "numeric" | "float" | "real" | "money" | "smallmoney" | "bit" | "binary" | "varbinary" => v.to_string(),
        _ => quoted(""),
    }
}

/// Read metadata for the given `(schema, table)` pairs. Missing tables are an error naming them.
/// One query per catalog view (per chunk of 500 tables), filtered to the requested object ids.
pub async fn read_target_tables(c: &mut MsClient, names: &[(String, String)]) -> Result<Vec<TargetTable>> {
    let mut out = Vec::with_capacity(names.len());
    for chunk in names.chunks(500) {
        out.extend(read_chunk(c, chunk).await?);
    }
    Ok(out)
}

async fn read_chunk(c: &mut MsClient, names: &[(String, String)]) -> Result<Vec<TargetTable>> {
    // 1. resolve names -> object ids, schema/name in catalog spelling, modify_date
    let values: Vec<String> = names.iter().enumerate().map(|(i, (s, t))| format!("({i}, OBJECT_ID({}))", nstr(&two_part(s, t)))).collect();
    let rows = meta_query(c, &format!(
        "SELECT v.i, o.object_id, s.name, o.name, CONVERT(varchar(30), o.modify_date, 126) \
         FROM (VALUES {}) v(i, id) LEFT JOIN sys.objects o ON o.object_id = v.id AND o.type = 'U' \
         LEFT JOIN sys.schemas s ON s.schema_id = o.schema_id ORDER BY v.i", values.join(", "))).await?;
    let mut tables: Vec<TargetTable> = Vec::with_capacity(names.len());
    let mut missing = vec![];
    for r in &rows {
        let i = cell_i32(r, 0)? as usize;
        let oid: Option<i32> = r.try_get::<i32, _>(1)?;
        match oid {
            None => missing.push(format!("{}.{}", names[i].0, names[i].1)),
            Some(oid) => tables.push(TargetTable {
                schema: cell_string(r, 2)?, name: cell_string(r, 3)?, object_id: oid, columns: vec![], indexes: vec![],
                partitioning: None, checks: vec![], row_count: 0, modify_date: cell_string(r, 4)?,
            }),
        }
    }
    if !missing.is_empty() {
        return Err(Failure::Check(format!("target table(s) not found: {}", missing.join(", "))).into());
    }
    let pos: HashMap<i32, Vec<usize>> = tables.iter().enumerate().fold(HashMap::new(), |mut m, (i, t)| { m.entry(t.object_id).or_default().push(i); m });
    let ids = id_list(pos.keys());
    let each = |tables: &mut Vec<TargetTable>, oid: i32, f: &mut dyn FnMut(&mut TargetTable)| {
        if let Some(ix) = pos.get(&oid) { for &i in ix { f(&mut tables[i]); } }
    };

    // 2. columns (base system type, so alias types and sysname resolve to their storage type)
    let rows = meta_query(c, &format!(
        "SELECT c.object_id, c.column_id, c.name, LOWER(ty.name), c.max_length, c.precision, c.scale, c.collation_name, c.is_nullable, \
                CAST(CASE WHEN c.default_object_id <> 0 THEN 1 ELSE 0 END AS bit), c.is_identity, \
                CAST(CASE WHEN c.is_computed = 1 OR c.generated_always_type <> 0 THEN 1 ELSE 0 END AS bit) \
         FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = CASE WHEN c.system_type_id = 240 THEN c.user_type_id ELSE c.system_type_id END \
         WHERE c.object_id IN ({ids}) ORDER BY c.object_id, c.column_id")).await?;
    for r in &rows {
        let col = TargetColumn {
            name: cell_string(r, 2)?, column_id: cell_i32(r, 1)?, type_name: cell_string(r, 3)?, max_length: cell_i16(r, 4)?,
            precision: cell_u8(r, 5)?, scale: cell_u8(r, 6)?, collation: cell_str(r, 7)?, nullable: cell_bool(r, 8)?,
            has_default: cell_bool(r, 9)?, is_identity: cell_bool(r, 10)?, is_computed: cell_bool(r, 11)?,
        };
        each(&mut tables, cell_i32(r, 0)?, &mut |t| t.columns.push(col.clone()));
    }

    // 3. indexes: heap / rowstore / columnstore (XML and spatial indexes are not captured)
    let rows = meta_query(c, &format!(
        "SELECT i.object_id, i.index_id, ISNULL(i.name, N''), i.type_desc, i.is_primary_key, i.is_unique, i.is_unique_constraint, \
                i.has_filter, i.filter_definition, i.fill_factor, ISNULL(ds.name, N''), CAST(CASE WHEN ds.type = 'PS' THEN 1 ELSE 0 END AS bit), \
                ISNULL(p.data_compression_desc, N'NONE') \
         FROM sys.indexes i LEFT JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id \
         LEFT JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id AND p.partition_number = 1 \
         WHERE i.object_id IN ({ids}) AND i.type IN (0, 1, 2, 5, 6) AND i.is_hypothetical = 0 ORDER BY i.object_id, i.index_id")).await?;
    for r in &rows {
        let has_filter = cell_bool(r, 7)?;
        let ix = TargetIndex {
            name: cell_string(r, 2)?, index_id: cell_i32(r, 1)?, type_desc: cell_string(r, 3)?, is_primary_key: cell_bool(r, 4)?,
            is_unique: cell_bool(r, 5)?, is_unique_constraint: cell_bool(r, 6)?, columns: vec![],
            filter: if has_filter { cell_str(r, 8)? } else { None }, fill_factor: cell_u8(r, 9)?, data_space: cell_string(r, 10)?,
            is_partitioned: cell_bool(r, 11)?, data_compression: cell_string(r, 12)?,
        };
        each(&mut tables, cell_i32(r, 0)?, &mut |t| t.indexes.push(ix.clone()));
    }

    // 4. index columns: keys in key order, then included columns. A column that is in the index only
    //    because it is the partitioning column (key_ordinal 0, not included) is not listed.
    let rows = meta_query(c, &format!(
        "SELECT ic.object_id, ic.index_id, c.name, ic.is_descending_key, ic.is_included_column, ic.partition_ordinal, ic.key_ordinal \
         FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         WHERE ic.object_id IN ({ids}) \
         ORDER BY ic.object_id, ic.index_id, CASE WHEN ic.key_ordinal = 0 THEN 1 ELSE 0 END, ic.key_ordinal, ic.index_column_id")).await?;
    let mut part_col: HashMap<i32, String> = HashMap::new();
    for r in &rows {
        let (oid, index_id) = (cell_i32(r, 0)?, cell_i32(r, 1)?);
        let col = TargetIndexColumn { name: cell_string(r, 2)?, descending: cell_bool(r, 3)?, included: cell_bool(r, 4)?, partition_ordinal: cell_u8(r, 5)? as i32 };
        let key_ordinal = cell_u8(r, 6)?;
        if col.partition_ordinal == 1 && (index_id == 0 || index_id == 1) { part_col.insert(oid, col.name.clone()); }
        if key_ordinal == 0 && !col.included { continue; }
        each(&mut tables, oid, &mut |t| { if let Some(ix) = t.indexes.iter_mut().find(|i| i.index_id == index_id) { ix.columns.push(col.clone()); } });
    }

    // 5. partitioning of the base (heap / clustered) index
    let rows = meta_query(c, &format!(
        "SELECT i.object_id, ps.data_space_id, ps.name, pf.function_id, pf.name, pf.boundary_value_on_right \
         FROM sys.indexes i JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id \
         JOIN sys.partition_functions pf ON pf.function_id = ps.function_id \
         WHERE i.object_id IN ({ids}) AND i.index_id IN (0, 1)")).await?;
    let mut parts: Vec<(i32, i32, i32, TargetPartitioning)> = vec![]; // (object_id, scheme id, function id, ..)
    for r in &rows {
        let oid = cell_i32(r, 0)?;
        parts.push((oid, cell_i32(r, 1)?, cell_i32(r, 3)?, TargetPartitioning {
            scheme: cell_string(r, 2)?, function: cell_string(r, 4)?, column: part_col.get(&oid).cloned().unwrap_or_default(),
            range_right: cell_bool(r, 5)?, boundaries: vec![], filegroups: vec![],
        }));
    }
    if !parts.is_empty() {
        let fids = id_list(parts.iter().map(|p| p.2).collect::<std::collections::BTreeSet<_>>());
        let rows = meta_query(c, &format!(
            "SELECT prv.function_id, prv.boundary_id, CAST(SQL_VARIANT_PROPERTY(prv.value, 'BaseType') AS nvarchar(128)), {RANGE_VALUE_TEXT} \
             FROM sys.partition_range_values prv WHERE prv.function_id IN ({fids}) ORDER BY prv.function_id, prv.boundary_id")).await?;
        let mut bounds: HashMap<i32, Vec<String>> = HashMap::new();
        for r in &rows {
            let base = cell_str(r, 2)?;
            let text = cell_str(r, 3)?;
            bounds.entry(cell_i32(r, 0)?).or_default().push(tsql_literal(base.as_deref(), text.as_deref()));
        }
        let sids = id_list(parts.iter().map(|p| p.1).collect::<std::collections::BTreeSet<_>>());
        let rows = meta_query(c, &format!(
            "SELECT dds.partition_scheme_id, dds.destination_id, ds.name FROM sys.destination_data_spaces dds \
             JOIN sys.data_spaces ds ON ds.data_space_id = dds.data_space_id WHERE dds.partition_scheme_id IN ({sids}) \
             ORDER BY dds.partition_scheme_id, dds.destination_id")).await?;
        let mut fgs: HashMap<i32, Vec<String>> = HashMap::new();
        for r in &rows { fgs.entry(cell_i32(r, 0)?).or_default().push(cell_string(r, 2)?); }
        for (oid, sid, fid, mut p) in parts {
            p.boundaries = bounds.get(&fid).cloned().unwrap_or_default();
            // destination_data_spaces also lists the NEXT USED filegroup: keep one per partition
            p.filegroups = fgs.get(&sid).cloned().unwrap_or_default().into_iter().take(p.boundaries.len() + 1).collect();
            each(&mut tables, oid, &mut |t| t.partitioning = Some(p.clone()));
        }
    }

    // 6. check constraints
    let rows = meta_query(c, &format!(
        "SELECT parent_object_id, name, definition, is_not_trusted FROM sys.check_constraints WHERE parent_object_id IN ({ids}) ORDER BY parent_object_id, name")).await?;
    for r in &rows {
        let ck = TargetCheck { name: cell_string(r, 1)?, definition: cell_string(r, 2)?, is_not_trusted: cell_bool(r, 3)? };
        each(&mut tables, cell_i32(r, 0)?, &mut |t| t.checks.push(ck.clone()));
    }

    // 7. row counts (dm_db_partition_stats needs VIEW DATABASE STATE; fall back to sys.partitions)
    let sql = format!("SELECT object_id, SUM(row_count) FROM sys.dm_db_partition_stats WHERE object_id IN ({ids}) AND index_id IN (0, 1) GROUP BY object_id");
    let rows = match meta_query(c, &sql).await {
        Ok(r) => r,
        Err(_) => meta_query(c, &format!("SELECT object_id, SUM(rows) FROM sys.partitions WHERE object_id IN ({ids}) AND index_id IN (0, 1) GROUP BY object_id")).await?,
    };
    for r in &rows {
        let n = cell_i64(r, 1)?;
        each(&mut tables, cell_i32(r, 0)?, &mut |t| t.row_count = n);
    }
    Ok(tables)
}

/// `(partition_number, rows)` from `sys.dm_db_partition_stats` for index 0/1.
pub async fn partition_row_counts(c: &mut MsClient, t: &TargetTable) -> Result<Vec<(i32, i64)>> {
    let oid = format!("OBJECT_ID({})", nstr(&two_part(&t.schema, &t.name)));
    let sql = format!("SELECT partition_number, SUM(row_count) FROM sys.dm_db_partition_stats WHERE object_id = {oid} AND index_id IN (0, 1) GROUP BY partition_number ORDER BY partition_number");
    let rows = match meta_query(c, &sql).await {
        Ok(r) => r,
        Err(_) => meta_query(c, &format!("SELECT partition_number, SUM(rows) FROM sys.partitions WHERE object_id = {oid} AND index_id IN (0, 1) GROUP BY partition_number ORDER BY partition_number")).await?,
    };
    rows.iter().map(|r| Ok((cell_i32(r, 0)?, cell_i64(r, 1)?))).collect()
}

// ---- column mapping ---------------------------------------------------------------------------

/// Columns the server fills itself: computed, `GENERATED ALWAYS` and `rowversion`.
fn server_generated(c: &TargetColumn) -> bool {
    c.is_computed || c.type_name == "timestamp" || c.type_name == "rowversion"
}

/// Map DuckDB columns onto the target by name (case-insensitive), in target column order.
pub fn map_columns(duck: &Table, target: &TargetTable, ignore_extra: bool) -> Result<TableMapping> {
    let mut used = vec![false; duck.columns.len()];
    let mut columns = vec![];
    let mut omitted = vec![];
    let mut notes = vec![];
    let mut missing = vec![];
    let mut tcols: Vec<&TargetColumn> = target.columns.iter().collect();
    tcols.sort_by_key(|c| c.column_id);
    for tc in tcols {
        let found = duck.columns.iter().enumerate().position(|(i, d)| !used[i] && d.name.eq_ignore_ascii_case(&tc.name));
        if server_generated(tc) {
            if let Some(i) = found {
                used[i] = true;
                notes.push(format!("{}: target column is computed/rowversion; DuckDB column {} is not loaded", tc.name, duck.columns[i].name));
            }
            omitted.push(tc.name.clone());
            continue;
        }
        match found {
            Some(i) => {
                used[i] = true;
                if tc.is_identity { notes.push(format!("{}: identity column loaded from DuckDB (needs KEEPIDENTITY)", tc.name)); }
                columns.push(ColumnMapping { duck_index: i, duck: duck.columns[i].clone(), target: tc.clone() });
            }
            None if tc.nullable || tc.has_default || tc.is_identity => {
                let why = if tc.is_identity { "identity" } else if tc.has_default { "default" } else { "NULL" };
                notes.push(format!("{}: no DuckDB column; left out of the load ({why})", tc.name));
                omitted.push(tc.name.clone());
            }
            None => missing.push(tc.name.clone()),
        }
    }
    let extra: Vec<String> = duck.columns.iter().enumerate().filter(|(i, _)| !used[*i]).map(|(_, c)| c.name.clone()).collect();
    let mut errs = vec![];
    if !extra.is_empty() {
        if ignore_extra {
            notes.push(format!("DuckDB columns not in the target, skipped: {}", extra.join(", ")));
        } else {
            errs.push(format!("EXTRA_COLUMN: DuckDB columns not in the target: {} (use --ignore-extra-columns)", extra.join(", ")));
        }
    }
    if !missing.is_empty() {
        errs.push(format!("MISSING_COLUMN: NOT NULL target columns without a default have no DuckDB column: {}", missing.join(", ")));
    }
    if columns.is_empty() && errs.is_empty() {
        errs.push("no DuckDB column matches a loadable target column".into());
    }
    if !errs.is_empty() {
        return Err(Failure::Check(format!("{} (DuckDB {}.{}): {}", target.full_name(), duck.schema, duck.name, errs.join("; "))).into());
    }
    Ok(TableMapping { table: target.clone(), columns, omitted_target_columns: omitted, notes })
}

// ---- pre-flight -------------------------------------------------------------------------------

/// DuckDB source type, as far as the checks care.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Src {
    Bool,
    /// Integer with its value range (`None` = unbounded).
    Int { lo: Option<i128>, hi: Option<i128> },
    Dec { p: u32, s: u32 },
    Float,
    Str,
    Date,
    Ts { ns: bool, tz: bool },
    Time,
    Blob,
    Uuid,
    Other,
}

fn src_kind(duck_type: &str) -> Src {
    let (base, arg) = parse_type(duck_type);
    let int = |lo: i128, hi: i128| Src::Int { lo: Some(lo), hi: Some(hi) };
    match base.as_str() {
        "BOOLEAN" | "BOOL" | "LOGICAL" => Src::Bool,
        "TINYINT" | "INT1" => int(i8::MIN as i128, i8::MAX as i128),
        "SMALLINT" | "INT2" | "SHORT" => int(i16::MIN as i128, i16::MAX as i128),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => int(i32::MIN as i128, i32::MAX as i128),
        "BIGINT" | "INT8" | "LONG" => int(i64::MIN as i128, i64::MAX as i128),
        "HUGEINT" | "INT128" => int(i128::MIN, i128::MAX),
        "UTINYINT" => int(0, u8::MAX as i128),
        "USMALLINT" => int(0, u16::MAX as i128),
        "UINTEGER" => int(0, u32::MAX as i128),
        "UBIGINT" => int(0, u64::MAX as i128),
        "UHUGEINT" => Src::Int { lo: Some(0), hi: None },
        "VARINT" | "BIGNUM" => Src::Int { lo: None, hi: None },
        "DECIMAL" | "NUMERIC" => {
            let mut it = arg.split(',').map(|s| s.trim().parse::<u32>().ok());
            let p = it.next().flatten().unwrap_or(18);
            let s = it.next().flatten().unwrap_or(if arg.trim().is_empty() { 3 } else { 0 });
            Src::Dec { p, s }
        }
        "FLOAT" | "FLOAT4" | "REAL" | "DOUBLE" | "FLOAT8" => Src::Float,
        "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" | "ENUM" | "JSON" => Src::Str,
        "DATE" => Src::Date,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_US" => Src::Ts { ns: false, tz: false },
        "TIMESTAMP_NS" => Src::Ts { ns: true, tz: false },
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => Src::Ts { ns: false, tz: true },
        "TIME" | "TIME WITH TIME ZONE" | "TIMETZ" => Src::Time,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => Src::Blob,
        "UUID" => Src::Uuid,
        _ => Src::Other,
    }
}

/// SQL Server target type, as far as the checks care.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Tgt {
    /// `max`: characters (non-unicode: bytes = characters unless UTF-8); `None` = MAX / text.
    Str { unicode: bool, max: Option<i64> },
    Int { lo: i128, hi: i128 },
    Bit,
    /// `range`: exact limits for money types.
    Dec { p: u32, s: u32, range: Option<(&'static str, &'static str)> },
    Float { real: bool },
    Date,
    DateTime2 { n: u32 },
    DateTimeOffset { n: u32 },
    DateTime,
    SmallDateTime,
    Time { n: u32 },
    Binary { max: Option<i64> },
    Uuid,
    /// xml, sql_variant, CLR types, json, vector, …: passed through, the server validates.
    Pass,
}

fn tgt_kind(c: &TargetColumn) -> Tgt {
    let len = |l: i16, div: i64| if l < 0 { None } else { Some(l as i64 / div) };
    match c.type_name.as_str() {
        "char" | "varchar" => Tgt::Str { unicode: false, max: len(c.max_length, 1) },
        "text" => Tgt::Str { unicode: false, max: None },
        "nchar" | "nvarchar" | "sysname" => Tgt::Str { unicode: true, max: len(c.max_length, 2) },
        "ntext" => Tgt::Str { unicode: true, max: None },
        "tinyint" => Tgt::Int { lo: 0, hi: 255 },
        "smallint" => Tgt::Int { lo: i16::MIN as i128, hi: i16::MAX as i128 },
        "int" => Tgt::Int { lo: i32::MIN as i128, hi: i32::MAX as i128 },
        "bigint" => Tgt::Int { lo: i64::MIN as i128, hi: i64::MAX as i128 },
        "bit" => Tgt::Bit,
        "decimal" | "numeric" => Tgt::Dec { p: c.precision as u32, s: c.scale as u32, range: None },
        "money" => Tgt::Dec { p: 19, s: 4, range: Some(("-922337203685477.5808", "922337203685477.5807")) },
        "smallmoney" => Tgt::Dec { p: 10, s: 4, range: Some(("-214748.3648", "214748.3647")) },
        "float" => Tgt::Float { real: c.precision < 25 },
        "real" => Tgt::Float { real: true },
        "date" => Tgt::Date,
        "datetime2" => Tgt::DateTime2 { n: c.scale as u32 },
        "datetimeoffset" => Tgt::DateTimeOffset { n: c.scale as u32 },
        "datetime" => Tgt::DateTime,
        "smalldatetime" => Tgt::SmallDateTime,
        "time" => Tgt::Time { n: c.scale as u32 },
        "binary" | "varbinary" => Tgt::Binary { max: len(c.max_length, 1) },
        "image" => Tgt::Binary { max: None },
        "uniqueidentifier" => Tgt::Uuid,
        _ => Tgt::Pass,
    }
}

/// SQL Server type text from catalog metadata: `varchar(10)`, `nvarchar(max)`, `decimal(18,2)`, `datetime2(7)`.
pub fn render_type(c: &TargetColumn) -> String {
    let t = c.type_name.as_str();
    let n = |div: i16| if c.max_length < 0 { "max".to_string() } else { (c.max_length / div).to_string() };
    match t {
        "char" | "varchar" | "binary" | "varbinary" => format!("{t}({})", n(1)),
        "nchar" | "nvarchar" => format!("{t}({})", n(2)),
        "sysname" => "nvarchar(128)".into(),
        "decimal" | "numeric" => format!("{t}({},{})", c.precision, c.scale),
        "datetime2" | "datetimeoffset" | "time" => format!("{t}({})", c.scale),
        "float" => if c.precision == 53 || c.precision == 0 { "float".into() } else { format!("float({})", c.precision) },
        "timestamp" => "rowversion".into(),
        _ => t.to_string(),
    }
}

fn is_string_type(t: &str) -> bool { matches!(t, "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "sysname") }

/// Windows code page 1252 collations (most Western European ones).
fn collation_is_cp1252(coll: &str) -> bool {
    let u = coll.to_ascii_uppercase();
    if u.starts_with("SQL_") { return u.contains("_CP1_"); }
    ["LATIN1_GENERAL_", "MODERN_SPANISH_", "TRADITIONAL_SPANISH_", "FRENCH_", "GERMAN_PHONEBOOK_", "FINNISH_SWEDISH_", "DANISH_NORWEGIAN_", "ICELANDIC_"]
        .iter().any(|p| u.starts_with(p))
}
fn collation_is_utf8(coll: Option<&str>) -> bool { coll.map(|c| c.to_ascii_uppercase().contains("_UTF8")).unwrap_or(false) }
fn collation_is_ci(coll: Option<&str>) -> bool { coll.map(|c| c.to_ascii_uppercase().contains("_CI")).unwrap_or(false) }
fn collation_is_ai(coll: Option<&str>) -> bool { coll.map(|c| c.to_ascii_uppercase().contains("_AI")).unwrap_or(false) }

/// RE2 class of characters that are NOT in Windows-1252.
const NOT_CP1252: &str = "[^\\x{0}-\\x{7F}\\x{A0}-\\x{FF}\\x{20AC}\\x{201A}\\x{192}\\x{201E}\\x{2026}\\x{2020}\\x{2021}\\x{2C6}\\x{2030}\\x{160}\\x{2039}\\x{152}\\x{17D}\\x{2018}\\x{2019}\\x{201C}\\x{201D}\\x{2022}\\x{2013}\\x{2014}\\x{2DC}\\x{2122}\\x{161}\\x{203A}\\x{153}\\x{17E}\\x{178}]";

fn pow10(n: u32) -> String { format!("1{}", "0".repeat(n as usize)) }
fn i128_fits(v: Option<i128>, lo: i128, hi: i128) -> bool { v.map(|v| v >= lo && v <= hi).unwrap_or(false) }

/// One row-level check: `count(*) FILTER (WHERE pred)` in the aggregate query, plus an optional
/// statistic for the message. `{count}` and `{stat}` in `msg` are replaced by the results.
#[derive(Debug, Clone)]
struct Check {
    column: Option<String>,
    code: &'static str,
    pred: String,
    stat: Option<String>,
    msg: String,
    /// Report even when no row violates (type-level findings such as SCALE).
    always: bool,
    /// Expression shown as sample when the target has no (mapped) primary key.
    sample: String,
}

struct Plan {
    from: String,
    checks: Vec<Check>,
    statics: Vec<Finding>,
    /// Text expression of the target PK (all PK columns mapped), used for samples.
    key_text: Option<String>,
}

/// DuckDB expressions for a mapped column: (raw quoted column, select expression).
fn exprs(cm: &ColumnMapping) -> (String, String) {
    let raw = quote_duck(&cm.duck.name);
    let sel = if cm.duck.select_expr.trim().is_empty() { raw.clone() } else { cm.duck.select_expr.clone() };
    (raw, sel)
}

/// Value expression used by typed checks: the native column for typed sources (the select
/// expression may be a text rendering), the select expression for strings and others.
fn value_expr(cm: &ColumnMapping) -> String {
    let (raw, sel) = exprs(cm);
    match src_kind(&cm.duck.duck_type) { Src::Str | Src::Other => sel, _ => raw }
}

/// Text that will be sent for a string target.
fn text_expr(cm: &ColumnMapping) -> String {
    let (_, sel) = exprs(cm);
    match src_kind(&cm.duck.duck_type) { Src::Str => sel, _ => format!("CAST({sel} AS VARCHAR)") }
}

/// Key expression used for duplicate / collation checks (SQL Server ignores trailing spaces).
fn key_norm(cm: &ColumnMapping, fold_case: bool) -> String {
    let t = &cm.target;
    if !is_string_type(&t.type_name) { return value_expr(cm); }
    let mut e = format!("rtrim({}, ' ')", text_expr(cm));
    if fold_case && collation_is_ci(t.collation.as_deref()) { e = format!("lower({e})"); }
    if fold_case && collation_is_ai(t.collation.as_deref()) { e = format!("strip_accents({e})"); }
    e
}

fn tuple_text(parts: &[String], quote: bool) -> String {
    let p: Vec<String> = parts.iter().map(|e| {
        let v = format!("CAST({e} AS VARCHAR)");
        if quote { format!("coalesce('''' || {v} || '''', 'NULL')") } else { format!("coalesce({v}, 'NULL')") }
    }).collect();
    if p.len() == 1 { p[0].clone() } else { format!("concat('(', {}, ')')", p.join(", ', ', ")) }
}

/// Mapped columns of an index's key, or `None` when a key column is not loaded.
fn mapped_keys<'a>(m: &'a TableMapping, ix: &TargetIndex) -> Option<Vec<&'a ColumnMapping>> {
    ix.columns.iter().filter(|c| !c.included).map(|k| m.columns.iter().find(|c| c.target.name.eq_ignore_ascii_case(&k.name))).collect()
}

fn build_plan(duck: &Table, m: &TableMapping, o: &PreflightOptions) -> Plan {
    let table = m.table.full_name();
    let mut checks: Vec<Check> = vec![];
    let mut statics: Vec<Finding> = vec![];
    for cm in &m.columns {
        column_checks(&table, cm, o, &mut checks, &mut statics);
    }
    // mapping-level notes as non-blocking findings, so reports list them
    let mapped: std::collections::HashSet<usize> = m.columns.iter().map(|c| c.duck_index).collect();
    for (i, dc) in duck.columns.iter().enumerate() {
        if !mapped.contains(&i) {
            statics.push(Finding { table: table.clone(), column: Some(dc.name.clone()), code: "EXTRA_COLUMN", message: "warning: DuckDB column has no loadable target column; not loaded".into(), samples: vec![] });
        }
    }
    for name in &m.omitted_target_columns {
        statics.push(Finding { table: table.clone(), column: Some(name.clone()), code: "MISSING_COLUMN", message: "warning: target column not loaded (NULL, default, identity or computed)".into(), samples: vec![] });
    }
    let key_text = m.table.pk().and_then(|pk| mapped_keys(m, pk)).filter(|k| !k.is_empty()).map(|k| {
        let parts: Vec<String> = k.iter().map(|c| exprs(c).1).collect();
        tuple_text(&parts, false)
    });
    Plan { from: format!("{}.{}", quote_duck(&duck.schema), quote_duck(&duck.name)), checks, statics, key_text }
}

fn column_checks(table: &str, cm: &ColumnMapping, o: &PreflightOptions, checks: &mut Vec<Check>, statics: &mut Vec<Finding>) {
    let tc = &cm.target;
    let col = Some(tc.name.clone());
    let tname = render_type(tc);
    let src = src_kind(&cm.duck.duck_type);
    let tgt = tgt_kind(tc);
    let v = value_expr(cm);
    let (raw, _) = exprs(cm);
    let mut add = |code: &'static str, pred: String, stat: Option<String>, msg: String, always: bool, sample: &str| {
        checks.push(Check { column: col.clone(), code, pred, stat, msg, always, sample: sample.to_string() });
    };
    let mut type_err = |why: &str| statics.push(Finding {
        table: table.to_string(), column: Some(tc.name.clone()), code: "TYPE",
        message: format!("DuckDB {} cannot be loaded into {tname}{why}", cm.duck.duck_type), samples: vec![],
    });

    if !tc.nullable && cm.duck.nullable {
        add("NOT_NULL", format!("{raw} IS NULL"), None, format!("{{count}} NULL values for NOT NULL {tname}"), false, &raw);
    }
    let minmax = |e: &str| format!("concat(CAST(min({e}) AS VARCHAR), ' .. ', CAST(max({e}) AS VARCHAR))");
    match tgt {
        Tgt::Pass => {}
        Tgt::Str { unicode, max } => {
            if src == Src::Blob { return type_err(" (binary data into a string column)"); }
            let tx = text_expr(cm);
            let utf8 = collation_is_utf8(tc.collation.as_deref());
            if let Some(n) = max {
                if unicode {
                    let units = format!("CASE WHEN strlen({tx}) = length({tx}) THEN length({tx}) ELSE length({tx}) + length(regexp_replace({tx}, '[^\\x{{10000}}-\\x{{10FFFF}}]', '', 'g')) END");
                    add("LENGTH", format!("{units} > {n}"), Some(format!("max({units})")), format!("max length {{stat}} > {n} UTF-16 code units of {tname} ({{count}} values)"), false, &tx);
                } else if utf8 {
                    add("LENGTH", format!("strlen({tx}) > {n}"), Some(format!("max(strlen({tx}))")), format!("max length {{stat}} bytes > {n} of {tname} (UTF-8 collation counts bytes; {{count}} values)"), false, &tx);
                } else {
                    add("LENGTH", format!("length({tx}) > {n}"), Some(format!("max(length({tx}))")), format!("max length {{stat}} > {n} of {tname} ({{count}} values)"), false, &tx);
                }
            }
            if !unicode && !utf8 && matches!(src, Src::Str | Src::Other) {
                let coll = tc.collation.clone().unwrap_or_else(|| "(none)".into());
                if collation_is_cp1252(&coll) {
                    add("CODEPAGE", format!("strlen({tx}) <> length({tx}) AND regexp_matches({tx}, '{NOT_CP1252}')"), None,
                        format!("{{count}} values have characters outside code page 1252 of collation {coll}; SQL Server would store them as '?' (use nvarchar or a _UTF8 collation)"), false, &tx);
                } else {
                    add("CODEPAGE", format!("strlen({tx}) <> length({tx})"), None,
                        format!("{{count}} values have non-ASCII characters and collation {coll} is not UTF-8; characters outside its code page would become '?'"), false, &tx);
                }
            }
        }
        Tgt::Int { lo, hi } => match src {
            Src::Bool => {}
            Src::Int { lo: slo, hi: shi } => {
                if !(i128_fits(slo, lo, hi) && i128_fits(shi, lo, hi)) {
                    add("RANGE", format!("{v} < {lo} OR {v} > {hi}"), Some(minmax(&v)), format!("{{count}} values outside {tname} range {lo}..{hi} (source {{stat}})"), false, &v);
                }
            }
            Src::Dec { p, s } => {
                let max_int = 10i128.checked_pow(p.saturating_sub(s)).unwrap_or(i128::MAX);
                if !(max_int <= hi && -max_int >= lo) {
                    add("RANGE", format!("{v} < {lo} OR {v} > {hi}"), Some(minmax(&v)), format!("{{count}} values outside {tname} range {lo}..{hi} (source {{stat}})"), false, &v);
                }
                if s > 0 && !o.allow_rescale {
                    add("SCALE", format!("{v} <> trunc({v})"), None, format!("{{count}} values have a fractional part that {tname} would drop (use --allow-rescale)"), false, &v);
                }
            }
            Src::Float => {
                add("RANGE", format!("NOT isfinite({v}) OR {v} < {lo} OR {v} > {hi}"), Some(minmax(&v)), format!("{{count}} values are NaN/Infinity or outside {tname} range {lo}..{hi} (source {{stat}})"), false, &v);
                if !o.allow_rescale {
                    add("SCALE", format!("isfinite({v}) AND {v} <> trunc({v})"), None, format!("{{count}} values have a fractional part that {tname} would drop (use --allow-rescale)"), false, &v);
                }
            }
            Src::Str => {
                let re = "'^\\s*[+-]?[0-9]+\\s*$'";
                let iv = format!("TRY_CAST(trim({v}) AS HUGEINT)");
                add("TYPE", format!("{v} IS NOT NULL AND NOT regexp_matches({v}, {re})"), None, format!("{{count}} values are not integers ({tname})"), false, &v);
                add("RANGE", format!("regexp_matches({v}, {re}) AND ({iv} IS NULL OR {iv} < {lo} OR {iv} > {hi})"), None, format!("{{count}} values outside {tname} range {lo}..{hi}"), false, &v);
            }
            _ => type_err(""),
        },
        Tgt::Bit => match src {
            Src::Bool => {}
            Src::Int { .. } | Src::Dec { .. } | Src::Float => add("BIT", format!("{v} NOT IN (0, 1)"), Some(minmax(&v)), "{count} values other than 0/1 for bit (source {stat})".into(), false, &v),
            Src::Str => add("BIT", format!("lower(trim({v})) NOT IN ('0', '1', 'true', 'false')"), None, "{count} values are not 0/1/true/false for bit".into(), false, &v),
            _ => type_err(""),
        },
        Tgt::Dec { p, s, range } => {
            let int_digits = p.saturating_sub(s);
            let numeric = matches!(src, Src::Int { .. } | Src::Dec { .. } | Src::Float);
            match src {
                Src::Bool => {}
                Src::Int { lo, hi } => {
                    let limit = 10i128.checked_pow(int_digits);
                    let fits = limit.map(|l| i128_fits(lo, -l + 1, l - 1) && i128_fits(hi, -l + 1, l - 1)).unwrap_or(true);
                    if !fits {
                        let l = pow10(int_digits);
                        add("PRECISION", format!("{v} >= {l} OR {v} <= -{l}"), Some(minmax(&v)), format!("{{count}} values do not fit {tname} (|value| must be < 10^{int_digits}; source {{stat}})"), false, &v);
                    }
                }
                Src::Dec { p: sp, s: ss } => {
                    let rounding = ss > s;
                    if rounding && !o.allow_rescale {
                        add("SCALE", format!("{v} <> round({v}, {s})"), None,
                            format!("source scale {ss} > target scale {s} ({} into {tname}); {{count}} values would be rounded (use --allow-rescale)", cm.duck.duck_type), true, &v);
                    }
                    let src_int = sp.saturating_sub(ss);
                    if src_int > int_digits || (rounding && src_int == int_digits) {
                        let e = if rounding { format!("abs(round({v}, {s}))") } else { format!("abs({v})") };
                        add("PRECISION", format!("{e} >= CAST('{}' AS DECIMAL({}, 0))", pow10(int_digits), (int_digits + 1).min(38)), Some(format!("max({e})")),
                            format!("max abs value {{stat}} does not fit {tname} (must be < 10^{int_digits}; {{count}} values)"), false, &v);
                    }
                }
                Src::Float => {
                    let e = format!("abs(round({v}, {s}))");
                    add("PRECISION", format!("NOT isfinite({v}) OR {e} >= 1e{int_digits}"), Some(format!("max(abs({v}))")),
                        format!("max abs value {{stat}} does not fit {tname} or is NaN/Infinity (must be < 10^{int_digits}; {{count}} values)"), false, &v);
                    if !o.allow_rescale {
                        add("SCALE", format!("isfinite({v}) AND {v} <> round({v}, {s})"), None, format!("{{count}} values have more than {s} decimals and would be rounded (use --allow-rescale)"), false, &v);
                    }
                }
                Src::Str => add("TYPE", format!("{v} IS NOT NULL AND TRY_CAST(trim({v}) AS DECIMAL({p},{s})) IS NULL"), None, format!("{{count}} values are not numbers that fit {tname}"), false, &v),
                _ => type_err(""),
            }
            if let (Some((lo, hi)), true) = (range, numeric) {
                add("RANGE", format!("{v} < {lo} OR {v} > {hi}"), Some(minmax(&v)), format!("{{count}} values outside {tname} range {lo}..{hi} (source {{stat}})"), false, &v);
            }
        }
        Tgt::Float { real } => match src {
            Src::Bool | Src::Int { .. } | Src::Dec { .. } => {}
            Src::Float => {
                let big = if real { format!(" OR abs({v}) > 3.4028234663852886e38") } else { String::new() };
                add("RANGE", format!("NOT isfinite({v}){big}"), None, format!("{{count}} values are NaN/Infinity or outside the {tname} range"), false, &v);
            }
            Src::Str => add("TYPE", format!("{v} IS NOT NULL AND TRY_CAST(trim({v}) AS DOUBLE) IS NULL"), None, format!("{{count}} values are not numbers ({tname})"), false, &v),
            _ => type_err(""),
        },
        Tgt::Date | Tgt::DateTime2 { .. } | Tgt::DateTimeOffset { .. } | Tgt::DateTime | Tgt::SmallDateTime => {
            let (dlo, dhi, tlo, thi) = match tgt {
                Tgt::DateTime => ("1753-01-01", "9999-12-31", "1753-01-01 00:00:00", "9999-12-31 23:59:59.998"),
                Tgt::SmallDateTime => ("1900-01-01", "2079-06-06", "1900-01-01 00:00:00", "2079-06-06 23:59:29.998"),
                _ => ("0001-01-01", "9999-12-31", "0001-01-01 00:00:00", "9999-12-31 23:59:59.999999"),
            };
            let range_msg = format!("{{count}} values outside the {tname} range {dlo} .. {dhi} (source {{stat}})");
            let ts_range = |add: &mut dyn FnMut(&'static str, String, Option<String>, String, bool, &str), e: &str, tz: bool| {
                let (ty, sfx) = if tz { ("TIMESTAMPTZ", "+00") } else { ("TIMESTAMP", "") };
                add("DATE_RANGE", format!("{e} < {ty} '{tlo}{sfx}' OR {e} > {ty} '{thi}{sfx}'"), Some(minmax(e)), range_msg.clone(), false, e);
            };
            match src {
                Src::Date => add("DATE_RANGE", format!("{v} < DATE '{dlo}' OR {v} > DATE '{dhi}'"), Some(minmax(&v)), range_msg.clone(), false, &v),
                Src::Ts { ns, tz } => {
                    let e = if ns { format!("CAST({v} AS TIMESTAMP)") } else { v.clone() };
                    ts_range(&mut add, &e, tz);
                    let us = format!("date_part('microseconds', {e})");
                    match tgt {
                        Tgt::Date => add("DATETIME_PRECISION", format!("CAST({e} AS TIME) <> TIME '00:00:00'"), None, format!("warning: {{count}} values have a time of day that {tname} drops"), false, &e),
                        Tgt::DateTime2 { n } | Tgt::DateTimeOffset { n } => {
                            if ns && n < 9 {
                                add("DATETIME_PRECISION", format!("epoch_ns({v}) % {} <> 0", pow10(9 - n)), None, format!("warning: {{count}} values have more fractional digits than {tname} keeps; they are rounded"), false, &v);
                            } else if !ns && n < 6 {
                                add("DATETIME_PRECISION", format!("{us} % {} <> 0", pow10(6 - n)), None, format!("warning: {{count}} values have more fractional digits than {tname} keeps; they are rounded"), false, &e);
                            }
                        }
                        Tgt::DateTime => {
                            let nsx = if ns { format!(" OR epoch_ns({v}) % 1000 <> 0") } else { String::new() };
                            add("DATETIME_PRECISION", format!("{us} % 1000 <> 0 OR ({us} // 1000) % 10 NOT IN (0, 3, 7){nsx}"), None,
                                "warning: {count} values are rounded to datetime's 1/300 s precision (.000/.003/.007); use datetime2 for exact values".into(), false, &e);
                        }
                        Tgt::SmallDateTime => add("DATETIME_PRECISION", format!("{us} <> 0"), None, "warning: {count} values have seconds; smalldatetime rounds to the minute".into(), false, &e),
                        _ => {}
                    }
                }
                Src::Str => {
                    let tz = matches!(tgt, Tgt::DateTimeOffset { .. });
                    let cast = format!("TRY_CAST({v} AS {})", if tz { "TIMESTAMPTZ" } else { "TIMESTAMP" });
                    add("TYPE", format!("{v} IS NOT NULL AND {cast} IS NULL"), None, format!("{{count}} values are not valid {tname} values"), false, &v);
                    ts_range(&mut add, &cast, tz);
                }
                _ => type_err(""),
            }
        }
        Tgt::Time { n } => match src {
            Src::Time => {
                if n < 6 {
                    add("DATETIME_PRECISION", format!("date_part('microseconds', {v}) % {} <> 0", pow10(6 - n)), None, format!("warning: {{count}} values have more fractional digits than {tname} keeps; they are rounded"), false, &v);
                }
            }
            Src::Str => add("TYPE", format!("{v} IS NOT NULL AND TRY_CAST({v} AS TIME) IS NULL"), None, format!("{{count}} values are not valid {tname} values"), false, &v),
            _ => type_err(""),
        },
        Tgt::Binary { max } => match src {
            Src::Blob => if let Some(n) = max {
                add("LENGTH", format!("octet_length({v}) > {n}"), Some(format!("max(octet_length({v}))")), format!("max length {{stat}} bytes > {n} of {tname} ({{count}} values)"), false, &v);
            },
            Src::Str => if let Some(n) = max {
                add("LENGTH", format!("strlen({v}) > {n}"), Some(format!("max(strlen({v}))")), format!("max length {{stat}} bytes > {n} of {tname} ({{count}} values)"), false, &v);
            },
            Src::Uuid => {}
            _ => type_err(""),
        },
        Tgt::Uuid => match src {
            Src::Uuid => {}
            Src::Str => add("TYPE", format!("{v} IS NOT NULL AND TRY_CAST(trim({v}) AS UUID) IS NULL"), None, "{count} values are not valid uniqueidentifier values".into(), false, &v),
            Src::Blob => add("TYPE", format!("octet_length({v}) <> 16"), None, "{count} values are not 16 bytes long (uniqueidentifier)".into(), false, &v),
            _ => type_err(""),
        },
    }
}

fn aggregate_sql(p: &Plan) -> String {
    let mut sel = vec!["count(*)".to_string()];
    for c in &p.checks {
        sel.push(format!("count(*) FILTER (WHERE {})", c.pred));
        if let Some(s) = &c.stat { sel.push(format!("CAST({s} AS VARCHAR)")); }
    }
    format!("SELECT {} FROM {}", sel.join(",\n       "), p.from)
}

/// The single aggregate query that `preflight` runs for a table (exposed for tests and `--plan`).
pub fn preflight_sql(duck: &Table, m: &TableMapping, o: &PreflightOptions) -> String {
    aggregate_sql(&build_plan(duck, m, o))
}

fn query_strings(con: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut st = con.prepare(sql).with_context(|| format!("preparing pre-flight query: {sql}"))?;
    let mut rows = st.query([])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        let v: Option<String> = r.get(0)?;
        out.push(v.unwrap_or_else(|| "NULL".into()));
    }
    Ok(out)
}

/// One aggregate query per table: lengths, code page, NULLs, ranges, scale; optional PK and CI checks.
/// Samples (up to `o.samples`, PK values when the PK is loaded) come from one small query per finding.
/// `PkCheck::Hash` runs the same GROUP BY as `Full` (DuckDB's aggregate spills to disk under its
/// memory limit); per-job hashing is the caller's business (call this per job table / view).
pub fn preflight(con: &Connection, duck: &Table, m: &TableMapping, o: &PreflightOptions) -> Result<Vec<Finding>> {
    let plan = build_plan(duck, m, o);
    let table = m.table.full_name();
    let mut findings = plan.statics.clone();
    let sql = aggregate_sql(&plan);
    let results: Vec<(i64, Option<String>)> = {
        let mut st = con.prepare(&sql).with_context(|| format!("preparing pre-flight query for {table}: {sql}"))?;
        let mut rows = st.query([]).with_context(|| format!("running pre-flight query for {table}"))?;
        let r = rows.next()?.context("pre-flight query returned no row")?;
        let mut out = vec![];
        let mut i = 1;
        for c in &plan.checks {
            let n: Option<i64> = r.get(i)?;
            i += 1;
            let stat: Option<String> = if c.stat.is_some() { let s = r.get(i)?; i += 1; s } else { None };
            out.push((n.unwrap_or(0), stat));
        }
        out
    };
    let limit = o.samples;
    for (c, (n, stat)) in plan.checks.iter().zip(results) {
        if n == 0 && !c.always { continue; }
        let mut f = Finding {
            table: table.clone(), column: c.column.clone(), code: c.code,
            message: c.msg.replace("{count}", &n.to_string()).replace("{stat}", stat.as_deref().unwrap_or("NULL")), samples: vec![],
        };
        if n > 0 && limit > 0 && is_blocking(&f) {
            let what = plan.key_text.clone().unwrap_or_else(|| format!("left(CAST({} AS VARCHAR), 60)", c.sample));
            f.samples = query_strings(con, &format!("SELECT {what} FROM {} WHERE {} LIMIT {limit}", plan.from, c.pred))?;
        }
        findings.push(f);
    }
    // primary key duplicates (trailing spaces ignored for string keys, as SQL Server does)
    if o.check_pk != PkCheck::None {
        if let Some(pk) = m.table.pk() {
            if let Some(keys) = mapped_keys(m, pk).filter(|k| !k.is_empty()) {
                let norm: Vec<String> = keys.iter().map(|c| key_norm(c, false)).collect();
                let text = tuple_text(&keys.iter().map(|c| exprs(c).1).collect::<Vec<_>>(), false);
                let sql = format!("SELECT min({text}) || ' x' || CAST(count(*) AS VARCHAR) FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT {}", plan.from, norm.join(", "), limit.max(1));
                let dups = query_strings(con, &sql)?;
                if !dups.is_empty() {
                    let cols: Vec<&str> = keys.iter().map(|c| c.target.name.as_str()).collect();
                    findings.push(Finding { table: table.clone(), column: None, code: "PK_DUP", message: format!("duplicate primary key {} values ({})", pk.name, cols.join(", ")), samples: dups.into_iter().take(limit).collect() });
                }
            }
        }
    }
    // case / accent / trailing-space collisions in PK and unique indexes with a CI collation
    if o.collation_check {
        for ix in m.table.indexes.iter().filter(|i| (i.is_primary_key || i.is_unique) && i.filter.is_none() && !i.type_desc.contains("COLUMNSTORE")) {
            let Some(keys) = mapped_keys(m, ix).filter(|k| !k.is_empty()) else { continue };
            if !keys.iter().any(|c| is_string_type(&c.target.type_name) && collation_is_ci(c.target.collation.as_deref())) { continue; }
            let norm: Vec<String> = keys.iter().map(|c| key_norm(c, true)).collect();
            let raw = tuple_text(&keys.iter().map(|c| exprs(c).1).collect::<Vec<_>>(), true);
            let sql = format!("SELECT string_agg(DISTINCT {raw}, ' / ') FROM {} GROUP BY {} HAVING count(DISTINCT {raw}) > 1 LIMIT {}", plan.from, norm.join(", "), limit.max(1));
            let hits = query_strings(con, &sql)?;
            if !hits.is_empty() {
                let cols: Vec<&str> = keys.iter().map(|c| c.target.name.as_str()).collect();
                findings.push(Finding {
                    table: table.clone(), column: None, code: "CI_COLLISION",
                    message: format!("{} ({}) has values that differ only in case, accents or trailing spaces; the target collation treats them as duplicates", ix.name, cols.join(", ")),
                    samples: hits.into_iter().take(limit).collect(),
                });
            }
        }
    }
    Ok(findings)
}

// ---- DDL from metadata ------------------------------------------------------------------------

fn is_columnstore(ix: &TargetIndex) -> bool { ix.type_desc.to_ascii_uppercase().contains("COLUMNSTORE") }
fn is_clustered_kind(ix: &TargetIndex) -> bool { ix.type_desc.to_ascii_uppercase().starts_with("CLUSTERED") }
fn is_heap(ix: &TargetIndex) -> bool { ix.type_desc.eq_ignore_ascii_case("HEAP") }

fn key_list(ix: &TargetIndex) -> String {
    ix.columns.iter().filter(|c| !c.included).map(|c| format!("{} {}", bracket(&c.name), if c.descending { "DESC" } else { "ASC" })).collect::<Vec<_>>().join(", ")
}
fn include_list(ix: &TargetIndex) -> Vec<String> { ix.columns.iter().filter(|c| c.included).map(|c| bracket(&c.name)).collect() }

fn with_clause(ix: &TargetIndex) -> String {
    let mut o = vec![];
    let comp = ix.data_compression.to_ascii_uppercase();
    if !comp.is_empty() && comp != "NONE" { o.push(format!("DATA_COMPRESSION = {comp}")); }
    if ix.fill_factor > 0 && !is_columnstore(ix) { o.push(format!("FILLFACTOR = {}", ix.fill_factor)); }
    if o.is_empty() { String::new() } else { format!(" WITH ({})", o.join(", ")) }
}

/// `ON [scheme]([col])` or `ON [filegroup]` of an index in place.
fn on_clause(t: &TargetTable, ix: &TargetIndex) -> String {
    if ix.data_space.is_empty() { return String::new(); }
    if ix.is_partitioned {
        let col = ix.columns.iter().find(|c| c.partition_ordinal == 1).map(|c| c.name.clone())
            .or_else(|| t.partitioning.as_ref().map(|p| p.column.clone())).unwrap_or_default();
        format!(" ON {}({})", bracket(&ix.data_space), bracket(&col))
    } else {
        format!(" ON {}", bracket(&ix.data_space))
    }
}

/// One index as DDL on `on_table`; `name` overrides the constraint name, `on` the ON clause.
fn index_stmt(ix: &TargetIndex, on_table: &str, name: &str, on: &str) -> Option<String> {
    if is_heap(ix) { return None; }
    let kind = if is_clustered_kind(ix) { "CLUSTERED" } else { "NONCLUSTERED" };
    let with = with_clause(ix);
    let n = bracket(name);
    if is_columnstore(ix) {
        let cols = if is_clustered_kind(ix) { String::new() } else { format!(" ({})", ix.columns.iter().map(|c| bracket(&c.name)).collect::<Vec<_>>().join(", ")) };
        let filter = ix.filter.as_ref().map(|f| format!(" WHERE {f}")).unwrap_or_default();
        return Some(format!("CREATE {kind} COLUMNSTORE INDEX {n} ON {on_table}{cols}{filter}{with}{on}"));
    }
    let keys = key_list(ix);
    if ix.is_primary_key {
        return Some(format!("ALTER TABLE {on_table} ADD CONSTRAINT {n} PRIMARY KEY {kind} ({keys}){with}{on}"));
    }
    if ix.is_unique_constraint {
        return Some(format!("ALTER TABLE {on_table} ADD CONSTRAINT {n} UNIQUE {kind} ({keys}){with}{on}"));
    }
    let inc = include_list(ix);
    let inc = if inc.is_empty() { String::new() } else { format!(" INCLUDE ({})", inc.join(", ")) };
    let filter = ix.filter.as_ref().map(|f| format!(" WHERE {f}")).unwrap_or_default();
    let uq = if ix.is_unique { "UNIQUE " } else { "" };
    Some(format!("CREATE {uq}{kind} INDEX {n} ON {on_table} ({keys}){inc}{filter}{with}{on}"))
}

fn clustered_first(t: &TargetTable) -> Vec<&TargetIndex> {
    let mut v: Vec<&TargetIndex> = t.indexes.iter().filter(|i| !is_heap(i)).collect();
    v.sort_by_key(|i| (!is_clustered_kind(i), i.index_id));
    v
}

/// Recreate every index of `t` on `on_table` from the captured metadata: clustered first, then the
/// rest in index_id order. Filtered indexes need `QUOTED_IDENTIFIER ON` in the session.
pub fn index_ddl(t: &TargetTable, on_table: &str) -> Vec<String> {
    clustered_first(t).into_iter().filter_map(|ix| index_stmt(ix, on_table, &ix.name, &on_clause(t, ix))).collect()
}

/// Drop statements for nonclustered indexes and the PK/clustered index, in a safe order:
/// nonclustered indexes, nonclustered unique constraints, the clustered (non-PK) index, the PK last.
pub fn drop_index_ddl(t: &TargetTable) -> Vec<String> {
    let tr = two_part(&t.schema, &t.name);
    let drop = |ix: &TargetIndex| if ix.is_primary_key || ix.is_unique_constraint {
        format!("ALTER TABLE {tr} DROP CONSTRAINT {}", bracket(&ix.name))
    } else {
        format!("DROP INDEX {} ON {tr}", bracket(&ix.name))
    };
    let live: Vec<&TargetIndex> = t.indexes.iter().filter(|i| !is_heap(i)).collect();
    let mut out = vec![];
    out.extend(live.iter().filter(|i| !is_clustered_kind(i) && !i.is_primary_key && !i.is_unique_constraint).map(|i| drop(i)));
    out.extend(live.iter().filter(|i| !is_clustered_kind(i) && !i.is_primary_key && i.is_unique_constraint).map(|i| drop(i)));
    out.extend(live.iter().filter(|i| is_clustered_kind(i) && !i.is_primary_key).map(|i| drop(i)));
    out.extend(live.iter().filter(|i| i.is_primary_key).map(|i| drop(i)));
    out
}

/// `SELECT $PARTITION.[fn](<literal>)`: the partition number a value goes to.
pub fn partition_number_sql(t: &TargetTable, literal: &str) -> Option<String> {
    t.partitioning.as_ref().map(|p| format!("SELECT $PARTITION.{}({literal})", bracket(&p.function)))
}

fn sql_name(s: String) -> String { s.chars().take(128).collect() }

/// Normalised ISO date/time text for comparisons (`2024-01-01` = `2024-01-01T00:00:00.000`).
fn iso_key(lit: &str) -> Option<String> {
    let s = lit.trim().trim_start_matches('N');
    let s = s.strip_prefix('\'')?.strip_suffix('\'')?;
    let re = regex::Regex::new(r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,7}))?)?$").ok()?;
    let c = re.captures(s)?;
    let g = |i: usize, d: &str| c.get(i).map(|m| m.as_str().to_string()).unwrap_or_else(|| d.to_string());
    Some(format!("{}T{}:{}:{}.{:0<7}", g(1, ""), g(2, "00"), g(3, "00"), g(4, "00"), g(5, "0")))
}

/// Partition number of a literal when it can be decided without the server's collation
/// (numbers and ISO dates); `None` otherwise.
fn resolve_partition(p: &TargetPartitioning, literal: &str) -> Option<i32> {
    #[derive(PartialEq, PartialOrd)]
    enum K { N(f64), S(String) }
    let key = |l: &str| -> Option<K> {
        if let Ok(v) = l.trim().parse::<f64>() { return Some(K::N(v)); }
        iso_key(l).map(K::S)
    };
    let v = key(literal)?;
    let mut n = 1;
    for b in &p.boundaries {
        let b = key(b)?;
        if std::mem::discriminant(&b) != std::mem::discriminant(&v) { return None; }
        if (p.range_right && b <= v) || (!p.range_right && b < v) { n += 1; }
    }
    Some(n)
}

/// Where a staging table lives: (ON clause, partition number) for the requested partition.
fn staging_space(t: &TargetTable, base: Option<&TargetIndex>, partition: Option<(&str, Option<i32>)>) -> Result<(String, Option<i32>)> {
    let Some(p) = &t.partitioning else {
        let ds = base.map(|i| i.data_space.clone()).unwrap_or_default();
        return Ok((if ds.is_empty() { String::new() } else { format!(" ON {}", bracket(&ds)) }, None));
    };
    let Some((lit, pn)) = partition else {
        return Ok((format!(" ON {}({})", bracket(&p.scheme), bracket(&p.column)), None));
    };
    let pn = pn.or_else(|| resolve_partition(p, lit));
    let fg = match pn {
        Some(n) => p.filegroups.get((n - 1).max(0) as usize).cloned()
            .with_context(|| format!("{}: partition {n} is out of range ({} filegroups)", t.full_name(), p.filegroups.len()))?,
        None => {
            let first = p.filegroups.first().cloned().unwrap_or_else(|| "PRIMARY".into());
            if p.filegroups.iter().any(|f| *f != first) {
                anyhow::bail!("{}: cannot tell which filegroup holds partition value {lit} without the server; run partition_number_sql and use staging_ddl_for_partition", t.full_name());
            }
            first
        }
    };
    Ok((format!(" ON {}", bracket(&fg)), pn))
}

fn staging_build(t: &TargetTable, staging: &str, partition: Option<(&str, Option<i32>)>) -> Result<Vec<String>> {
    if let Some(c) = t.columns.iter().find(|c| c.is_computed) {
        anyhow::bail!("{}: column {} is computed or GENERATED ALWAYS; staging tables for such targets are not supported", t.full_name(), c.name);
    }
    let base = t.indexes.iter().find(|i| is_clustered_kind(i)).or_else(|| t.indexes.iter().find(|i| is_heap(i)));
    let (on, pn) = staging_space(t, base, partition)?;
    let st = two_part(&t.schema, staging);
    let mut cols: Vec<&TargetColumn> = t.columns.iter().collect();
    cols.sort_by_key(|c| c.column_id);
    let defs: Vec<String> = cols.iter().map(|c| {
        let coll = match (&c.collation, is_string_type(&c.type_name)) { (Some(x), true) => format!(" COLLATE {x}"), _ => String::new() };
        format!("    {} {}{coll} {}", bracket(&c.name), render_type(c), if c.nullable { "NULL" } else { "NOT NULL" })
    }).collect();
    let heap_with = match base { Some(b) if is_heap(b) => with_clause(b), _ => String::new() };
    let mut create = format!("CREATE TABLE {st} (\n{}\n){heap_with}{on}", defs.join(",\n"));
    if let (Some(p), Some((lit, _))) = (&t.partitioning, partition) {
        let n = pn.map(|n| n.to_string()).unwrap_or_else(|| "?".into());
        create = format!("-- partition {n} of {}: SELECT $PARTITION.{}({lit})\n{create}", t.full_name(), bracket(&p.function));
    }
    let mut out = vec![create];
    if let Some(b) = base.filter(|b| !is_heap(b)) {
        let name = if b.is_primary_key { sql_name(format!("PK_{staging}")) } else if b.is_unique_constraint { sql_name(format!("UQ_{staging}_{}", b.index_id)) } else { b.name.clone() };
        out.extend(index_stmt(b, &st, &name, &on));
    }
    for (i, ck) in t.checks.iter().enumerate() {
        out.push(format!("ALTER TABLE {st} ADD CONSTRAINT {} CHECK {}", bracket(&sql_name(format!("CK_{staging}_{}", i + 1))), ck.definition));
    }
    if let (Some(p), Some((lit, _))) = (&t.partitioning, partition) {
        let col = t.column(&p.column).with_context(|| format!("{}: partition column {} not found", t.full_name(), p.column))?;
        let nn = if col.nullable { format!(" AND {} IS NOT NULL", bracket(&col.name)) } else { String::new() };
        out.push(format!("ALTER TABLE {st} ADD CONSTRAINT {} CHECK ({} = {lit}{nn})", bracket(&sql_name(format!("CK_{staging}_partition"))), bracket(&col.name)));
    }
    Ok(out)
}

/// Staging table DDL: same columns, clustered key and compression as `t`, a CHECK on the partition
/// column when `partition_value` is given, ON the matching filegroup.
///
/// * `partition_value` is a T-SQL literal (`'AAA'`, `42`, `'2024-01-01'`); the CHECK is
///   `[col] = <literal>` (plus `IS NOT NULL` for a nullable column), which SQL Server accepts for
///   `SWITCH` because it implies the partition's range. The caller must put exactly one value per
///   staging table / partition.
/// * The filegroup is resolved locally for numeric and ISO-date boundaries or when every partition
///   is on the same filegroup; otherwise this fails and the caller uses [`partition_number_sql`] +
///   [`staging_ddl_for_partition`].
/// * Identity columns become plain columns; CHECK constraints of the target are copied (SWITCH
///   requires them, Msg 4971). Nonclustered indexes are NOT included: create them with
///   [`staging_nonclustered_ddl`] after loading and before `SWITCH` (Msg 4947 otherwise).
/// * Without `partition_value` on a partitioned target the staging table is partitioned the same way.
pub fn staging_ddl(t: &TargetTable, staging: &str, partition_value: Option<&str>) -> Result<Vec<String>> {
    staging_build(t, staging, partition_value.map(|v| (v, None)))
}

/// [`staging_ddl`] for a partition whose number the caller resolved on the server.
pub fn staging_ddl_for_partition(t: &TargetTable, staging: &str, literal: &str, partition_number: i32) -> Result<Vec<String>> {
    staging_build(t, staging, Some((literal, Some(partition_number))))
}

/// Nonclustered indexes (and nonclustered PK / unique constraints) of `t` on the staging table,
/// on the same filegroup as [`staging_ddl`] chose. Run after loading, before `SWITCH`.
pub fn staging_nonclustered_ddl(t: &TargetTable, staging: &str, partition_value: Option<&str>, partition_number: Option<i32>) -> Result<Vec<String>> {
    let base = t.indexes.iter().find(|i| is_clustered_kind(i)).or_else(|| t.indexes.iter().find(|i| is_heap(i)));
    let (on, _) = staging_space(t, base, partition_value.map(|v| (v, partition_number)))?;
    let st = two_part(&t.schema, staging);
    Ok(clustered_first(t).into_iter().filter(|i| !is_clustered_kind(i)).filter_map(|ix| {
        let name = if ix.is_primary_key { sql_name(format!("PK_{staging}")) } else if ix.is_unique_constraint { sql_name(format!("UQ_{staging}_{}", ix.index_id)) } else { ix.name.clone() };
        index_stmt(ix, &st, &name, &on)
    }).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::Conv;

    fn tcol(id: i32, name: &str, ty: &str, len: i16, p: u8, s: u8, coll: Option<&str>, nullable: bool) -> TargetColumn {
        TargetColumn { name: name.into(), column_id: id, type_name: ty.into(), max_length: len, precision: p, scale: s, collation: coll.map(|c| c.into()), nullable, has_default: false, is_identity: false, is_computed: false }
    }
    fn icol(name: &str, desc: bool, inc: bool, po: i32) -> TargetIndexColumn { TargetIndexColumn { name: name.into(), descending: desc, included: inc, partition_ordinal: po } }
    fn dcol(name: &str, ty: &str, nullable: bool) -> Column {
        Column { name: name.into(), duck_type: ty.into(), nullable, default: None, comment: None, sql_type: String::new(), select_expr: String::new(), conv: Conv::Identity, max_len: None, identity: None, enum_values: None, warnings: vec![] }
    }
    fn dtable(name: &str, cols: Vec<Column>) -> Table {
        Table { schema: "main".into(), name: name.into(), columns: cols, constraints: vec![], indexes: vec![], row_count: 0, comment: None }
    }
    const CI: &str = "SQL_Latin1_General_CP1_CI_AS";

    /// Mirrors the e2e fixture `dbo.sales` (partitioned on mkt char(3), PAGE PK, filtered NC index).
    fn sales() -> TargetTable {
        let mut rid = tcol(7, "rid", "int", 4, 10, 0, None, false);
        rid.is_identity = true;
        let mut dflt = tcol(8, "dflt", "int", 4, 10, 0, None, false);
        dflt.has_default = true;
        TargetTable {
            schema: "dbo".into(), name: "sales".into(), object_id: 1, row_count: 3, modify_date: "2026-01-01T00:00:00".into(),
            columns: vec![
                tcol(1, "mkt", "char", 3, 0, 0, Some(CI), false),
                tcol(2, "id", "int", 4, 10, 0, None, false),
                tcol(3, "code", "varchar", 10, 0, 0, Some(CI), false),
                tcol(4, "amount", "decimal", 9, 18, 2, None, true),
                tcol(5, "note", "nvarchar", -1, 0, 0, Some(CI), true),
                tcol(6, "ts", "datetime", 8, 23, 3, None, true),
                rid, dflt,
            ],
            indexes: vec![
                TargetIndex { name: "PK_sales".into(), index_id: 1, type_desc: "CLUSTERED".into(), is_primary_key: true, is_unique: true, is_unique_constraint: false,
                    columns: vec![icol("mkt", false, false, 1), icol("id", true, false, 0), icol("code", false, false, 0)], filter: None, data_compression: "PAGE".into(), fill_factor: 90, data_space: "ps".into(), is_partitioned: true },
                TargetIndex { name: "ix_sales_code".into(), index_id: 2, type_desc: "NONCLUSTERED".into(), is_primary_key: false, is_unique: false, is_unique_constraint: false,
                    columns: vec![icol("code", false, false, 0), icol("amount", false, true, 0)], filter: Some("([amount] IS NOT NULL)".into()), data_compression: "ROW".into(), fill_factor: 0, data_space: "ps".into(), is_partitioned: true },
                TargetIndex { name: "UQ_sales_rid".into(), index_id: 3, type_desc: "NONCLUSTERED".into(), is_primary_key: false, is_unique: true, is_unique_constraint: true,
                    columns: vec![icol("rid", false, false, 0), icol("mkt", false, false, 1)], filter: None, data_compression: "NONE".into(), fill_factor: 0, data_space: "ps".into(), is_partitioned: true },
            ],
            partitioning: Some(TargetPartitioning { scheme: "ps".into(), function: "pf".into(), column: "mkt".into(), range_right: true, boundaries: vec!["'AAA'".into(), "'BBB'".into()], filegroups: vec!["PRIMARY".into(); 3] }),
            checks: vec![TargetCheck { name: "CK_sales_amount".into(), definition: "([amount]>=(0))".into(), is_not_trusted: false }],
        }
    }

    fn sales_duck() -> Table {
        dtable("sales", vec![dcol("MKT", "VARCHAR", true), dcol("id", "INTEGER", true), dcol("code", "VARCHAR", true), dcol("amount", "DECIMAL(18,2)", true), dcol("note", "VARCHAR", true), dcol("ts", "TIMESTAMP", true)])
    }

    #[test]
    fn target_map_columns_by_name_in_target_order() {
        let mut d = sales_duck();
        d.columns.reverse();
        let m = map_columns(&d, &sales(), false).unwrap();
        let names: Vec<&str> = m.columns.iter().map(|c| c.target.name.as_str()).collect();
        assert_eq!(names, ["mkt", "id", "code", "amount", "note", "ts"]);
        assert_eq!(m.columns[0].duck.name, "MKT");
        assert_eq!(m.columns[0].duck_index, 5);
        assert_eq!(m.omitted_target_columns, ["rid", "dflt"]);
        assert!(m.notes.iter().any(|n| n.contains("rid") && n.contains("identity")));
    }

    #[test]
    fn target_map_columns_extra_and_missing() {
        let mut d = sales_duck();
        d.columns.push(dcol("extra", "INTEGER", true));
        let e = map_columns(&d, &sales(), false).unwrap_err();
        assert!(e.to_string().contains("EXTRA_COLUMN") && e.to_string().contains("extra"), "{e}");
        assert_eq!(crate::core::runlog::exit_code(&e), 1);
        let m = map_columns(&d, &sales(), true).unwrap();
        assert!(m.notes.iter().any(|n| n.contains("extra")));
        // a NOT NULL column without default is required
        let mut d = sales_duck();
        d.columns.retain(|c| c.name != "code");
        let e = map_columns(&d, &sales(), false).unwrap_err();
        assert!(e.to_string().contains("MISSING_COLUMN") && e.to_string().contains("code"), "{e}");
        // computed columns are never loaded, even when DuckDB has them
        let mut t = sales();
        let mut c = tcol(9, "calc", "int", 4, 10, 0, None, true);
        c.is_computed = true;
        t.columns.push(c);
        let mut d = sales_duck();
        d.columns.push(dcol("calc", "INTEGER", true));
        let m = map_columns(&d, &t, false).unwrap();
        assert!(m.columns.iter().all(|c| c.target.name != "calc"));
        assert!(m.omitted_target_columns.contains(&"calc".to_string()));
    }

    #[test]
    fn target_preflight_sql_is_one_aggregate() {
        let t = sales();
        let d = sales_duck();
        let m = map_columns(&d, &t, false).unwrap();
        let sql = preflight_sql(&d, &m, &PreflightOptions::default());
        assert!(sql.starts_with("SELECT count(*)"));
        assert!(sql.ends_with("FROM \"main\".\"sales\""));
        assert_eq!(sql.matches("SELECT").count(), 1);
        assert!(sql.contains("count(*) FILTER (WHERE length(\"MKT\") > 3)"), "{sql}");
        assert!(sql.contains("max(length(\"code\"))"));
        assert!(sql.contains("\"MKT\" IS NULL"));
        assert!(sql.contains("regexp_matches(\"code\""));
        assert!(sql.contains("TIMESTAMP '1753-01-01 00:00:00'"));
        // DECIMAL(18,2) into decimal(18,2): nothing to check
        assert!(!sql.contains("round(\"amount\""));
    }

    fn duck_con(ddl: &str) -> Connection {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(ddl).unwrap();
        con
    }

    fn codes(f: &[Finding]) -> Vec<&'static str> { f.iter().filter(|f| is_blocking(f)).map(|f| f.code).collect() }

    #[test]
    fn target_preflight_length_codepage_null_samples() {
        let con = duck_con("CREATE TABLE sales (MKT VARCHAR, id INTEGER, code VARCHAR, amount DECIMAL(18,2), note VARCHAR, ts TIMESTAMP);
            INSERT INTO sales VALUES ('AAA', 1, 'x', 1.5, 'n', '2020-01-01'), ('ABCD', 2, 'caf\u{e9}', 2, NULL, NULL), ('BBB', 3, 'z\u{4e2d}', NULL, NULL, NULL), (NULL, 4, 'q', NULL, NULL, '2020-01-01 00:00:00.001');");
        let t = sales();
        let d = sales_duck();
        let m = map_columns(&d, &t, false).unwrap();
        let f = preflight(&con, &d, &m, &PreflightOptions::default()).unwrap();
        let len = f.iter().find(|f| f.code == "LENGTH").unwrap();
        assert_eq!(len.column.as_deref(), Some("mkt"));
        assert!(len.message.contains("max length 4 > 3"), "{}", len.message);
        assert_eq!(len.samples, ["(ABCD, 2, caf\u{e9})"]);
        // é is in code page 1252, 中 is not
        let cp = f.iter().find(|f| f.code == "CODEPAGE").unwrap();
        assert!(cp.message.starts_with("1 values"), "{}", cp.message);
        assert_eq!(cp.samples, ["(BBB, 3, z\u{4e2d})"]);
        assert!(f.iter().any(|f| f.code == "NOT_NULL" && f.column.as_deref() == Some("mkt")));
        let dt = f.iter().find(|f| f.code == "DATETIME_PRECISION").unwrap();
        assert!(!is_blocking(dt) && dt.message.starts_with("warning:"));
        assert!(f.iter().any(|f| f.code == "MISSING_COLUMN" && !is_blocking(f)));
        assert_eq!(codes(&f), ["NOT_NULL", "LENGTH", "CODEPAGE"]);
    }

    #[test]
    fn target_preflight_numeric_ranges() {
        let con = duck_con("CREATE TABLE n (t INTEGER, b INTEGER, d DECIMAL(10,4), p DECIMAL(10,2), big BIGINT, s VARCHAR, f DOUBLE, ts TIMESTAMP);
            INSERT INTO n VALUES (256, 2, 1.2345, 12345678.12, 3000000000, '12a', 1.5, '1700-01-01'), (0, 0, 1.0, 1.0, 1, '7', 2.0, '2000-01-01');");
        let t = TargetTable { schema: "dbo".into(), name: "n".into(), object_id: 2, indexes: vec![], partitioning: None, checks: vec![], row_count: 0, modify_date: String::new(),
            columns: vec![tcol(1, "t", "tinyint", 1, 3, 0, None, true), tcol(2, "b", "bit", 1, 1, 0, None, true), tcol(3, "d", "decimal", 9, 10, 2, None, true),
                tcol(4, "p", "decimal", 5, 9, 2, None, true), tcol(5, "big", "int", 4, 10, 0, None, true), tcol(6, "s", "int", 4, 10, 0, None, true),
                tcol(7, "f", "int", 4, 10, 0, None, true), tcol(8, "ts", "datetime", 8, 23, 3, None, true)] };
        let d = dtable("n", vec![dcol("t", "INTEGER", true), dcol("b", "INTEGER", true), dcol("d", "DECIMAL(10,4)", true), dcol("p", "DECIMAL(10,2)", true),
            dcol("big", "BIGINT", true), dcol("s", "VARCHAR", true), dcol("f", "DOUBLE", true), dcol("ts", "TIMESTAMP", true)]);
        let m = map_columns(&d, &t, false).unwrap();
        let f = preflight(&con, &d, &m, &PreflightOptions::default()).unwrap();
        let by = |col: &str, code: &str| f.iter().find(|x| x.column.as_deref() == Some(col) && x.code == code).unwrap_or_else(|| panic!("{col} {code}: {f:?}")).clone();
        assert_eq!(by("t", "RANGE").samples, ["256"]);
        assert_eq!(by("b", "BIT").samples, ["2"]);
        assert!(by("d", "SCALE").message.contains("source scale 4 > target scale 2"));
        assert!(by("p", "PRECISION").message.contains("12345678.12"), "{:?}", by("p", "PRECISION"));
        assert_eq!(by("big", "RANGE").samples, ["3000000000"]);
        assert_eq!(by("s", "TYPE").samples, ["12a"]);
        assert_eq!(by("f", "SCALE").samples, ["1.5"]);
        assert!(by("ts", "DATE_RANGE").message.contains("1700-01-01"));
        // --allow-rescale silences SCALE
        let f2 = preflight(&con, &d, &m, &PreflightOptions { allow_rescale: true, ..Default::default() }).unwrap();
        assert!(!f2.iter().any(|x| x.code == "SCALE"));
        // static incompatibility
        let mut t2 = t.clone();
        t2.columns[0].type_name = "int".into();
        let d2 = dtable("n", vec![dcol("t", "BLOB", true)]);
        let m2 = map_columns(&d2, &TargetTable { columns: vec![t2.columns[0].clone()], ..t2 }, false).unwrap();
        // TYPE is decided from the types alone (no query touches the column)
        let f3 = preflight(&con, &d2, &m2, &PreflightOptions::default()).unwrap();
        assert!(f3.iter().any(|x| x.code == "TYPE" && x.message.contains("BLOB")), "{f3:?}");
    }

    #[test]
    fn target_preflight_nvarchar_utf16_units() {
        let con = duck_con("CREATE TABLE u (k INTEGER, s VARCHAR); INSERT INTO u VALUES (1, 'ab\u{1F600}'), (2, 'abc'), (3, '\u{e9}\u{e9}\u{e9}');");
        let t = TargetTable { schema: "dbo".into(), name: "u".into(), object_id: 3, partitioning: None, checks: vec![], row_count: 0, modify_date: String::new(),
            columns: vec![tcol(1, "k", "int", 4, 10, 0, None, false), tcol(2, "s", "nvarchar", 6, 0, 0, Some(CI), true)],
            indexes: vec![TargetIndex { name: "PK_u".into(), index_id: 1, type_desc: "CLUSTERED".into(), is_primary_key: true, is_unique: true, is_unique_constraint: false, columns: vec![icol("k", false, false, 0)], filter: None, data_compression: "NONE".into(), fill_factor: 0, data_space: "PRIMARY".into(), is_partitioned: false }] };
        let d = dtable("u", vec![dcol("k", "INTEGER", false), dcol("s", "VARCHAR", true)]);
        let m = map_columns(&d, &t, false).unwrap();
        let f = preflight(&con, &d, &m, &PreflightOptions::default()).unwrap();
        // nvarchar(3): the emoji takes 2 UTF-16 units -> 4 > 3; 'ééé' is 3 units
        let len = f.iter().find(|f| f.code == "LENGTH").unwrap();
        assert!(len.message.contains("max length 4 > 3"), "{}", len.message);
        assert_eq!(len.samples, ["1"]);
        assert!(!f.iter().any(|f| f.code == "NOT_NULL"));
    }

    #[test]
    fn target_preflight_pk_dup_and_ci_collision() {
        let con = duck_con("CREATE TABLE sales (MKT VARCHAR, id INTEGER, code VARCHAR, amount DECIMAL(18,2), note VARCHAR, ts TIMESTAMP);
            INSERT INTO sales VALUES ('AAA', 1, 'x', 1, NULL, NULL), ('AAA', 1, 'x ', 2, NULL, NULL), ('AAA', 2, 'y', 1, NULL, NULL), ('aaa', 2, 'Y', 1, NULL, NULL), ('BBB', 3, 'z', 1, NULL, NULL);");
        let t = sales();
        let d = sales_duck();
        let m = map_columns(&d, &t, false).unwrap();
        let o = PreflightOptions { check_pk: PkCheck::Full, ..Default::default() };
        let f = preflight(&con, &d, &m, &o).unwrap();
        let dup = f.iter().find(|f| f.code == "PK_DUP").unwrap();
        assert_eq!(dup.samples.len(), 1, "{:?}", dup.samples);
        assert!(dup.samples[0].starts_with("(AAA, 1, x") && dup.samples[0].ends_with(" x2"), "{:?}", dup.samples);
        let ci = f.iter().find(|f| f.code == "CI_COLLISION").unwrap();
        assert!(ci.message.contains("PK_sales"));
        assert_eq!(ci.samples.len(), 2, "{:?}", ci.samples);
        assert!(ci.samples.iter().any(|s| s.contains("'aaa'") && s.contains("'AAA'")), "{:?}", ci.samples);
        // no PK check requested, no collation check: no key findings
        let f = preflight(&con, &d, &m, &PreflightOptions { collation_check: false, ..Default::default() }).unwrap();
        assert!(!f.iter().any(|f| f.code == "PK_DUP" || f.code == "CI_COLLISION"));
    }

    #[test]
    fn target_index_ddl_from_metadata() {
        let t = sales();
        let ddl = index_ddl(&t, "[dbo].[sales_new]");
        assert_eq!(ddl, [
            "ALTER TABLE [dbo].[sales_new] ADD CONSTRAINT [PK_sales] PRIMARY KEY CLUSTERED ([mkt] ASC, [id] DESC, [code] ASC) WITH (DATA_COMPRESSION = PAGE, FILLFACTOR = 90) ON [ps]([mkt])",
            "CREATE NONCLUSTERED INDEX [ix_sales_code] ON [dbo].[sales_new] ([code] ASC) INCLUDE ([amount]) WHERE ([amount] IS NOT NULL) WITH (DATA_COMPRESSION = ROW) ON [ps]([mkt])",
            "ALTER TABLE [dbo].[sales_new] ADD CONSTRAINT [UQ_sales_rid] UNIQUE NONCLUSTERED ([rid] ASC, [mkt] ASC) ON [ps]([mkt])",
        ]);
        // a clustered non-PK index comes first even with a higher index_id; heaps are skipped
        let mut t2 = sales();
        t2.indexes[0].type_desc = "NONCLUSTERED".into();
        t2.indexes.push(TargetIndex { name: "cx".into(), index_id: 9, type_desc: "CLUSTERED".into(), is_primary_key: false, is_unique: false, is_unique_constraint: false, columns: vec![icol("ts", false, false, 0)], filter: None, data_compression: "NONE".into(), fill_factor: 0, data_space: "PRIMARY".into(), is_partitioned: false });
        let ddl = index_ddl(&t2, "[dbo].[sales]");
        assert_eq!(ddl[0], "CREATE CLUSTERED INDEX [cx] ON [dbo].[sales] ([ts] ASC) ON [PRIMARY]");
        assert_eq!(ddl.len(), 4);
    }

    #[test]
    fn target_drop_index_ddl_order() {
        let mut t = sales();
        t.indexes[0].type_desc = "NONCLUSTERED".into();
        t.indexes.push(TargetIndex { name: "cx".into(), index_id: 9, type_desc: "CLUSTERED".into(), is_primary_key: false, is_unique: false, is_unique_constraint: false, columns: vec![icol("ts", false, false, 0)], filter: None, data_compression: "NONE".into(), fill_factor: 0, data_space: "PRIMARY".into(), is_partitioned: false });
        assert_eq!(drop_index_ddl(&t), [
            "DROP INDEX [ix_sales_code] ON [dbo].[sales]",
            "ALTER TABLE [dbo].[sales] DROP CONSTRAINT [UQ_sales_rid]",
            "DROP INDEX [cx] ON [dbo].[sales]",
            "ALTER TABLE [dbo].[sales] DROP CONSTRAINT [PK_sales]",
        ]);
    }

    #[test]
    fn target_staging_ddl_for_partition() {
        let t = sales();
        let ddl = staging_ddl(&t, "sales__stg_AAA", Some("'AAA'")).unwrap();
        assert_eq!(ddl.len(), 4, "{ddl:#?}");
        assert!(ddl[0].starts_with("-- partition ? of dbo.sales: SELECT $PARTITION.[pf]('AAA')\nCREATE TABLE [dbo].[sales__stg_AAA] (\n"), "{}", ddl[0]);
        assert!(ddl[0].contains("    [mkt] char(3) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n"));
        assert!(ddl[0].contains("    [amount] decimal(18,2) NULL,\n"));
        assert!(ddl[0].contains("    [note] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,\n"));
        assert!(ddl[0].contains("    [rid] int NOT NULL,\n"));
        assert!(ddl[0].ends_with("\n) ON [PRIMARY]"));
        assert_eq!(ddl[1], "ALTER TABLE [dbo].[sales__stg_AAA] ADD CONSTRAINT [PK_sales__stg_AAA] PRIMARY KEY CLUSTERED ([mkt] ASC, [id] DESC, [code] ASC) WITH (DATA_COMPRESSION = PAGE, FILLFACTOR = 90) ON [PRIMARY]");
        assert_eq!(ddl[2], "ALTER TABLE [dbo].[sales__stg_AAA] ADD CONSTRAINT [CK_sales__stg_AAA_1] CHECK ([amount]>=(0))");
        assert_eq!(ddl[3], "ALTER TABLE [dbo].[sales__stg_AAA] ADD CONSTRAINT [CK_sales__stg_AAA_partition] CHECK ([mkt] = 'AAA')");
        let nc = staging_nonclustered_ddl(&t, "sales__stg_AAA", Some("'AAA'"), None).unwrap();
        assert_eq!(nc, [
            "CREATE NONCLUSTERED INDEX [ix_sales_code] ON [dbo].[sales__stg_AAA] ([code] ASC) INCLUDE ([amount]) WHERE ([amount] IS NOT NULL) WITH (DATA_COMPRESSION = ROW) ON [PRIMARY]",
            "ALTER TABLE [dbo].[sales__stg_AAA] ADD CONSTRAINT [UQ_sales__stg_AAA_3] UNIQUE NONCLUSTERED ([rid] ASC, [mkt] ASC) ON [PRIMARY]",
        ]);
        assert_eq!(partition_number_sql(&t, "'AAA'").unwrap(), "SELECT $PARTITION.[pf]('AAA')");
    }

    #[test]
    fn target_staging_filegroups_and_heaps() {
        // string boundaries + different filegroups: needs the partition number from the server
        let mut t = sales();
        t.partitioning.as_mut().unwrap().filegroups = vec!["FG1".into(), "FG2".into(), "FG3".into()];
        assert!(staging_ddl(&t, "s", Some("'AAA'")).is_err());
        let ddl = staging_ddl_for_partition(&t, "s", "'AAA'", 2).unwrap();
        assert!(ddl[0].ends_with(") ON [FG2]") && ddl[1].ends_with("ON [FG2]"));
        // numeric RANGE LEFT boundaries resolve locally; nullable partition column gets IS NOT NULL
        let heap = TargetTable { schema: "dbo".into(), name: "h".into(), object_id: 4, row_count: 0, modify_date: String::new(), checks: vec![],
            columns: vec![tcol(1, "k", "int", 4, 10, 0, None, true), tcol(2, "v", "varchar", 5, 0, 0, Some(CI), true)],
            indexes: vec![TargetIndex { name: String::new(), index_id: 0, type_desc: "HEAP".into(), is_primary_key: false, is_unique: false, is_unique_constraint: false, columns: vec![], filter: None, data_compression: "PAGE".into(), fill_factor: 0, data_space: "psi".into(), is_partitioned: true }],
            partitioning: Some(TargetPartitioning { scheme: "psi".into(), function: "pfi".into(), column: "k".into(), range_right: false, boundaries: vec!["10".into(), "20".into()], filegroups: vec!["A".into(), "B".into(), "C".into()] }) };
        let ddl = staging_ddl(&heap, "h_stg", Some("15")).unwrap();
        assert!(ddl[0].starts_with("-- partition 2 "));
        assert!(ddl[0].ends_with(") WITH (DATA_COMPRESSION = PAGE) ON [B]"), "{}", ddl[0]);
        assert_eq!(ddl[1], "ALTER TABLE [dbo].[h_stg] ADD CONSTRAINT [CK_h_stg_partition] CHECK ([k] = 15 AND [k] IS NOT NULL)");
        assert!(staging_ddl(&heap, "h_stg", Some("10")).unwrap()[0].ends_with("ON [A]"));
        assert!(staging_ddl(&heap, "h_stg", Some("21")).unwrap()[0].ends_with("ON [C]"));
        // no partition value: staging partitioned like the target
        assert!(staging_ddl(&heap, "h_stg", None).unwrap()[0].ends_with("ON [psi]([k])"));
        assert!(index_ddl(&heap, "[dbo].[h]").is_empty());
        assert!(drop_index_ddl(&heap).is_empty());
        // computed columns are refused
        let mut c = heap.clone();
        c.columns[1].is_computed = true;
        assert!(staging_ddl(&c, "x", None).is_err());
    }

    #[test]
    fn target_literals_and_types() {
        assert_eq!(tsql_literal(Some("char"), Some("O'B")), "'O''B'");
        assert_eq!(tsql_literal(Some("nvarchar"), Some("x")), "N'x'");
        assert_eq!(tsql_literal(Some("int"), Some("42")), "42");
        assert_eq!(tsql_literal(Some("date"), Some("2024-01-01")), "'2024-01-01'");
        assert_eq!(tsql_literal(None, None), "NULL");
        assert_eq!(iso_key("'2024-01-01'"), iso_key("'2024-01-01T00:00:00'"));
        assert!(iso_key("'2024-01-01'") < iso_key("'2024-01-01T00:00:00.003'"));
        let c = |ty: &str, len: i16, p: u8, s: u8| render_type(&tcol(1, "c", ty, len, p, s, None, true));
        assert_eq!(c("varchar", 10, 0, 0), "varchar(10)");
        assert_eq!(c("nvarchar", -1, 0, 0), "nvarchar(max)");
        assert_eq!(c("nchar", 6, 0, 0), "nchar(3)");
        assert_eq!(c("decimal", 9, 18, 2), "decimal(18,2)");
        assert_eq!(c("datetime2", 8, 27, 7), "datetime2(7)");
        assert_eq!(c("float", 8, 53, 0), "float");
        assert_eq!(c("timestamp", 8, 0, 0), "rowversion");
        assert!(is_blocking(&Finding { table: "t".into(), column: None, code: "LENGTH", message: "x".into(), samples: vec![] }));
    }

    /// Reads the e2e fixtures from a live server. Set DUCKBRIDGE_TEST_MSSQL="host,port,user,password,database".
    #[test]
    #[ignore]
    fn target_read_live_metadata() {
        let Ok(spec) = std::env::var("DUCKBRIDGE_TEST_MSSQL") else { return };
        let p: Vec<&str> = spec.splitn(5, ',').collect();
        let tgt = super::super::MssqlTarget { host: p[0].into(), port: p[1].parse().unwrap(), user: p[2].into(), password: p[3].into(), database: p[4].into(), trust_cert: true, encrypt: false, windows_auth: false };
        let rt = super::super::runtime().unwrap();
        rt.block_on(async {
            let mut c = tgt.connect(p[4]).await.unwrap();
            let ts = read_target_tables(&mut c, &[("dbo".into(), "sales".into()), ("dbo".into(), "h".into()), ("dbo".into(), "cs".into())]).await.unwrap();
            println!("{}", serde_json::to_string_pretty(&ts).unwrap());
            let s = &ts[0];
            assert_eq!(s.columns.len(), 8);
            assert_eq!(s.pk().unwrap().columns.len(), 3);
            assert_eq!(s.indexes.iter().find(|i| i.name == "ix_sales_code").unwrap().columns.len(), 2);
            let p = s.partitioning.as_ref().unwrap();
            assert_eq!((p.column.as_str(), p.range_right, p.boundaries.clone(), p.filegroups.len()), ("mkt", true, vec!["'AAA'".to_string(), "'BBB'".to_string()], 3));
            println!("{:#?}", index_ddl(s, "[dbo].[sales]"));
            println!("{:#?}", staging_ddl(s, "sales__stg_t", Some("'BBB'")).unwrap());
            println!("{:#?}", partition_row_counts(&mut c, s).await.unwrap());
            assert!(read_target_tables(&mut c, &[("dbo".into(), "nope".into())]).await.unwrap_err().to_string().contains("dbo.nope"));
            // a failed statement leaves the session usable (the row-count fallback relies on it)
            assert!(c.simple_query("SELECT * FROM dbo.no_such_table").await.is_err());
            if std::env::var("DUCKBRIDGE_TEST_MSSQL_WRITE").is_err() { return; }
            // write mode (disposable database only): staging + SWITCH, then drop / recreate the indexes
            use super::super::exec_batch;
            let c = &mut c;
            exec_batch(c, "DROP TABLE IF EXISTS dbo.sales__stg_t").await.unwrap();
            exec_batch(c, "TRUNCATE TABLE dbo.sales WITH (PARTITIONS (3))").await.unwrap();
            for st in staging_ddl(s, "sales__stg_t", Some("'BBB'")).unwrap() { exec_batch(c, &st).await.unwrap(); }
            exec_batch(c, "INSERT INTO dbo.sales__stg_t VALUES ('BBB', 9, 'w', 1, NULL, NULL, 99, 0)").await.unwrap();
            for st in staging_nonclustered_ddl(s, "sales__stg_t", Some("'BBB'"), None).unwrap() { exec_batch(c, &st).await.unwrap(); }
            let row = c.simple_query(partition_number_sql(s, "'BBB'").unwrap()).await.unwrap().into_row().await.unwrap().unwrap();
            let pn: i32 = row.get(0).unwrap();
            assert_eq!(pn, 3);
            exec_batch(c, &format!("ALTER TABLE dbo.sales__stg_t SWITCH TO dbo.sales PARTITION {pn}")).await.unwrap();
            assert_eq!(partition_row_counts(c, s).await.unwrap()[2], (3, 1));
            exec_batch(c, "DROP TABLE dbo.sales__stg_t").await.unwrap();
            for st in drop_index_ddl(s) { exec_batch(c, &st).await.unwrap(); }
            for st in index_ddl(s, "[dbo].[sales]") { exec_batch(c, &st).await.unwrap(); }
            let again = read_target_tables(c, &[("dbo".into(), "sales".into())]).await.unwrap();
            let shape = |t: &TargetTable| serde_json::to_string(&t.indexes.iter().map(|i| (&i.name, &i.type_desc, &i.columns, &i.filter, &i.data_compression, i.fill_factor, &i.data_space)).collect::<Vec<_>>()).unwrap();
            assert_eq!(shape(&again[0]), shape(s));
        });
    }
}
