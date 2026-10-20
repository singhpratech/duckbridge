//! Target-table metadata and pre-flight checks for `--into-existing` loads (spec R2, R3).
//!
//! The target DDL is the contract: DuckBridge reads `sys.columns`, `sys.types`, `sys.indexes`,
//! `sys.index_columns`, `sys.partitions`, `sys.partition_schemes`/`functions` and
//! `sys.check_constraints`, maps DuckDB columns onto the target by name, and checks every value that
//! could lose information **before** any data moves. Nothing in this module issues DDL.
//!
//! The types below are the shared contract (see docs/plan-0.1.1.md); the functions are implemented
//! by work stream B.
use super::MsClient;
use crate::core::model::{Column, Table};
use anyhow::Result;
use duckdb::Connection;

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

/// Read metadata for the given `(schema, table)` pairs. Missing tables are an error naming them.
pub async fn read_target_tables(_c: &mut MsClient, _names: &[(String, String)]) -> Result<Vec<TargetTable>> {
    anyhow::bail!("target metadata reader not implemented yet (work stream B)")
}

/// `(partition_number, rows)` from `sys.dm_db_partition_stats` for index 0/1.
pub async fn partition_row_counts(_c: &mut MsClient, _t: &TargetTable) -> Result<Vec<(i32, i64)>> {
    anyhow::bail!("partition_row_counts not implemented yet (work stream B)")
}

/// Map DuckDB columns onto the target by name (case-insensitive), in target column order.
pub fn map_columns(_duck: &Table, _target: &TargetTable, _ignore_extra: bool) -> Result<TableMapping> {
    anyhow::bail!("map_columns not implemented yet (work stream B)")
}

/// One aggregate query per table: lengths, code page, NULLs, ranges, scale; optional PK and CI checks.
pub fn preflight(_con: &Connection, _duck: &Table, _m: &TableMapping, _o: &PreflightOptions) -> Result<Vec<Finding>> {
    anyhow::bail!("preflight not implemented yet (work stream B)")
}

/// Recreate every index of `t` on `on_table` from the captured metadata.
pub fn index_ddl(_t: &TargetTable, _on_table: &str) -> Vec<String> { vec![] }

/// Drop statements for nonclustered indexes and the PK/clustered index, in a safe order.
pub fn drop_index_ddl(_t: &TargetTable) -> Vec<String> { vec![] }

/// Staging table DDL: same columns, clustered key and compression as `t`, a CHECK on the partition
/// column when `partition_value` is given, ON the matching filegroup.
pub fn staging_ddl(_t: &TargetTable, _staging: &str, _partition_value: Option<&str>) -> Result<Vec<String>> {
    anyhow::bail!("staging_ddl not implemented yet (work stream B)")
}
