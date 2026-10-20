# DuckBridge 0.1.1 implementation plan: safe, large, production-grade loads

Implements every requirement in [requirements-large-load-safety.md](requirements-large-load-safety.md) (R1–R10).
Branch `v0.1.1`. This document is the coordination contract between the work streams: module ownership,
public interfaces, and the acceptance tests each stream must leave behind. Behaviour that the spec changes
on purpose (no `ROLLBACK IMMEDIATE`, no compatibility/recovery changes on existing databases, `nocase`
rewrite instead of a global collation, lock timeouts on imports) ships as the new default; everything else
is a new flag or subcommand and the 0.1.0 behaviour is unchanged.

## Work streams and file ownership

| Stream | Req | Files (owner writes only these) | Depends on |
|---|---|---|---|
| A. Guard, run log, plan mode, exit codes | R1, R8 | `src/mssql/guard.rs`, `src/core/runlog.rs`, call sites in `load.rs` / `backup.rs` / `api.rs` / `main.rs` / `emit.rs` | – |
| B. Target metadata + pre-flight | R2, R3 (checks) | `src/mssql/target.rs` (all functions; the types are already defined there) | – |
| C. Converters, exact DECIMAL, columnar fetch, pipelining, benches | R5, R3 (wire format) | `src/mssql/convert.rs`, `src/mssql/bulk.rs`, `benches/bulk.rs` | target.rs types |
| D. Job sources, strategies, state dir, SWITCH, heap-then-index | R4 | `src/mssql/jobs.rs`, `src/mssql/switch.rs` | target.rs types |
| E. Safe reads | R6 | `src/mssql/fingerprint.rs`, `src/mssql/import.rs` | runlog (A) |
| F. Translator semantics, strict mode, audit, exact math | R7 | `src/transpile/*`, `src/mathcompat.rs`, `src/audit.rs` | – |
| G. Verification + swap | R9, R10 | `src/verify.rs`, `src/mssql/swap.rs` | target.rs types, guard |
| H. Integration | all | `src/mssql/load.rs`, `src/api.rs`, `src/main.rs`, `extension/src/lib.rs`, `README.md`, `scripts/e2e_*.sh`, `tests/` | everything |
| I. String type in create mode | R3 | `src/core/typemap.rs`, `src/core/catalog.rs` | – |

Rules for every stream:

- Rust 2021, `anyhow::Result`, no new heavy dependencies without a note in this file (allowed: `regex`,
  `serde`, `serde_json`, `chrono`, `rust_decimal`, `tokio`, `tiberius`, `duckdb` already present).
- No `DROP DATABASE` / `ALTER DATABASE` / `ROLLBACK IMMEDIATE` text anywhere except `guard.rs`
  (a unit test scans `src/`).
- Unit tests next to the code; anything needing SQL Server goes into `scripts/e2e_large_load.sh`
  (stream H) and is described in the stream's hand-off note.
- Every statement sent to SQL Server goes through `mssql::exec_batch` or a `runlog::stmt` call, so
  `--plan` and `--log` see it.
- Checks that fail must return `core::runlog::Failure::Check(..)` (exit 1); guard refusals
  `Failure::Guard(..)` (exit 3); everything else exits 2.

## Shared types (already in the tree)

### `core::runlog`

```rust
pub enum Failure { Check(String), Run(String), Guard(String) }   // thiserror; exit codes 1 / 2 / 3
pub fn exit_code(e: &anyhow::Error) -> i32;
pub fn init(log_file: Option<&Path>, summary_file: Option<&Path>, plan_only: bool, progress_seconds: u64);
pub fn planning() -> bool;                       // --plan: exec_batch records and does not execute
pub fn stmt(role: &str, job: &str, sql: &str, rows: i64, started: Instant);  // one log line, never fails
pub fn note(msg: &str);                          // free-form log line
pub fn progress(job: &str, rows: i64);           // rate-limited progress line to stderr
pub fn summary_add(entry: SummaryEntry);         // per table/job
pub fn finish(ok: bool) -> Result<()>;           // writes the summary JSON
pub struct SummaryEntry { pub table: String, pub job: String, pub rows: i64, pub seconds: f64, pub strategy: String, pub checks: Vec<String>, pub ok: bool }
```

### `mssql::guard`

```rust
pub struct Guard { .. }
impl Guard {
    pub fn new(pattern: Option<&str>) -> Result<Guard>;        // flag > env DUCKBRIDGE_DISPOSABLE_DB_PATTERN > "^_"
    pub fn is_disposable(&self, db: &str) -> bool;
    pub fn assert_disposable(&self, db: &str, action: &str) -> Result<()>;   // Failure::Guard
    pub async fn exists(c: &mut MsClient, db: &str) -> Result<bool>;
    pub async fn ensure_database(&self, c, db, create: bool) -> Result<()>;  // R1.5
    pub async fn drop_database(&self, c, db) -> Result<()>;                  // R1.2: session check, no ROLLBACK IMMEDIATE
    pub async fn set_recovery(&self, c, db, model: &str) -> Result<()>;      // disposable only
    pub async fn set_compat_level(&self, c, db, level: u16) -> Result<()>;   // disposable only
    pub async fn database_state(c, db) -> Result<DbState>;                   // recovery model, compat level, RCSI, snapshot
    pub async fn sessions(c, db) -> Result<Vec<SessionInfo>>;
}
```

### `mssql::target` (types; stream B implements the functions)

```rust
pub struct TargetColumn { name, column_id, type_name /* sys.types name, lowercase */, max_length /* bytes, -1 = MAX */, precision, scale, collation: Option<String>, nullable, has_default, is_identity, is_computed }
pub struct TargetIndexColumn { name, descending, included, partition_ordinal }
pub struct TargetIndex { name, index_id, type_desc /* CLUSTERED | NONCLUSTERED | HEAP */, is_primary_key, is_unique, is_unique_constraint, columns, filter: Option<String>, data_compression /* NONE | ROW | PAGE */, fill_factor, data_space /* filegroup or partition scheme */, is_partitioned }
pub struct TargetPartitioning { scheme, function, column, range_right, boundaries: Vec<String> /* T-SQL literals */, filegroups: Vec<String> /* per partition number, 1-based */ }
pub struct TargetCheck { name, definition, is_not_trusted }
pub struct TargetTable { schema, name, object_id, columns, indexes, partitioning: Option<TargetPartitioning>, checks, row_count, modify_date }
impl TargetTable { fn clustered(&self) -> Option<&TargetIndex>; fn pk(&self) -> Option<&TargetIndex>; fn is_heap(&self) -> bool; fn column(&self, name: &str) -> Option<&TargetColumn> /* case-insensitive */; fn full_name(&self) -> String }

pub async fn read_target_tables(c: &mut MsClient, names: &[(String, String)]) -> Result<Vec<TargetTable>>;
pub async fn partition_row_counts(c: &mut MsClient, t: &TargetTable) -> Result<Vec<(i32, i64)>>;   // (partition_number, rows)

pub struct ColumnMapping { duck_index: usize, duck: core::model::Column, target: TargetColumn }
pub struct TableMapping { table: TargetTable, columns: Vec<ColumnMapping> /* target column_id order */, omitted_target_columns: Vec<String>, notes: Vec<String> }
pub fn map_columns(duck: &core::model::Table, target: &TargetTable, ignore_extra: bool) -> Result<TableMapping>;

pub enum PkCheck { None, Full, Hash }
pub struct PreflightOptions { check_pk: PkCheck, allow_rescale: bool, collation_check: bool, samples: usize }
pub struct Finding { table: String, column: Option<String>, code: &'static str, message: String, samples: Vec<String> }
// codes: LENGTH, CODEPAGE, NOT_NULL, RANGE, SCALE, PRECISION, BIT, DATE_RANGE, DATETIME_PRECISION, PK_DUP, CI_COLLISION, EXTRA_COLUMN, MISSING_COLUMN, TYPE
pub fn preflight(con: &duckdb::Connection, duck: &core::model::Table, m: &TableMapping, o: &PreflightOptions) -> Result<Vec<Finding>>;   // ONE aggregate query per table (+ PK / CI queries when asked)
pub fn index_ddl(t: &TargetTable, on_table: &str) -> Vec<String>;            // recreate every index from metadata (PK as ALTER TABLE ADD CONSTRAINT), with DATA_COMPRESSION, FILLFACTOR, INCLUDE, WHERE, ON data_space(col)
pub fn drop_index_ddl(t: &TargetTable) -> Vec<String>;                        // nonclustered indexes + PK/clustered, in the right order
pub fn staging_ddl(t: &TargetTable, staging: &str, partition_value: Option<&str>) -> Result<Vec<String>>; // same columns/types/collation/NULL, same clustered key + compression, CHECK on the partition column, ON the partition's filegroup
```

### `mssql::convert` and `mssql::bulk` (stream C)

```rust
pub enum Converter { Bit, TinyInt, SmallInt, Int, BigInt, Real, Float, Decimal { precision: u8, scale: u8 }, Char { max_chars: Option<usize>, unicode: bool, fixed: bool }, Date, Time { scale: u8 }, DateTime, SmallDateTime, DateTime2 { scale: u8 }, DateTimeOffset { scale: u8 }, Guid, Binary { max_len: Option<usize> }, Text /* fallback: NVARCHAR(MAX) text */ }
impl Converter {
    pub fn for_target(col: &TargetColumn, duck_type: &str) -> Result<Converter>;   // into-existing
    pub fn for_sql_type(sql_type: &str, duck_type: &str) -> Result<Converter>;     // create mode (col.sql_type)
    pub fn convert(&self, v: duckdb::types::ValueRef<'_>) -> Result<tiberius::ColumnData<'static>>;  // no string work for non-string types; DECIMAL from the mantissa; tinyint 0..255 or error
}
pub struct BulkOptions { pub batch_rows: usize, pub keep_identity: bool, pub table_lock: bool, pub prefetch_batches: usize /* default 4 */, pub order_by: Option<String> }
/// Reads `sql` from DuckDB on a reader thread (its own pooled connection via core::conn::acquire), converts
/// column by column, and streams TokenRows to SQL Server with a bounded channel. Returns rows sent.
pub async fn bulk_load(con: &duckdb::Connection, client: &mut MsClient, table_ref: &str, columns: &[String], convs: &[Converter], sql: &str, o: &BulkOptions, progress: &dyn Fn(i64)) -> Result<i64>;
```

### `mssql::jobs` and `mssql::switch` (stream D)

```rust
pub enum SplitSpec { Column(String), Files(String), Hash { cols: Vec<String>, n: usize }, Range { col: String, n: usize }, Auto }
pub fn parse_split(spec: &str) -> Result<SplitSpec>;     // column:Mkt | files:out/t/Mkt=*/ | hash:a,b:16 | range:id:8
pub enum Strategy { Switch, HeapThenIndex, Serial }
pub fn parse_strategy(s: &str) -> Result<Strategy>;
pub struct Job { pub id: String, pub table: usize, pub label: String, pub source_sql: String, pub partition_value: Option<String>, pub est_rows: i64 }
pub struct JobPlan { pub jobs: Vec<Job> /* largest first */, pub strategy: Vec<Strategy> /* per table index */ }
pub fn plan_jobs(con: &duckdb::Connection, cat: &core::model::Catalog, splits: &HashMap<String, SplitSpec>, strategies: &HashMap<String, Strategy>, default_strategy: Strategy, split_threshold: i64, parallel: usize) -> Result<JobPlan>;
pub struct StateDir { .. }  // <dir>/<table>/<job>.done with JSON {rows, seconds, checksum}
impl StateDir { pub fn open(p: &Path) -> Result<StateDir>; pub fn is_done(&self, job: &Job) -> bool; pub fn mark_done(&self, job: &Job, rows: i64, seconds: f64, checksum: Option<String>) -> Result<()>; }

pub struct Staging { pub name: String /* <table>__stg_<value> */, pub schema: String, pub partition_number: i32 }
pub async fn prepare_staging(c: &mut MsClient, t: &TargetTable, job: &Job) -> Result<Staging>;   // drops a leftover staging table of the same name first (its name must match __stg_)
pub async fn switch_in(c: &mut MsClient, t: &TargetTable, s: &Staging, expected_rows: i64, replace_partitions: bool) -> Result<()>;  // row-count check, SWITCH TO target PARTITION, old partition -> __old_<value> when replacing
pub async fn drop_staging(c: &mut MsClient, s: &Staging) -> Result<()>;
pub fn staging_name(table: &str, value: &str) -> String;
```

### `mssql::fingerprint` (stream E)

```rust
pub struct Fingerprint { pub rows: i64, pub last_user_update: Option<String>, pub modify_date: String }
pub async fn take(c: &mut MsClient, schema: &str, table: &str) -> Result<Fingerprint>;
pub async fn held_locks(c: &mut MsClient, schema: &str, table: &str) -> Result<Vec<LockInfo>>;   // X / IX / SCH-M by other sessions
pub async fn apply_session_settings(c: &mut MsClient, o: &ReadSafety) -> Result<()>;            // LOCK_TIMEOUT, DEADLOCK_PRIORITY LOW, isolation
pub struct ReadSafety { pub lock_timeout_ms: i64, pub isolation: Isolation, pub lock_check: bool, pub lock_wait_s: u64, pub on_lock: OnLock, pub max_rereads: u32, pub readonly_intent: bool }
pub enum Isolation { ReadCommitted, ReadUncommitted, Snapshot }  pub enum OnLock { Fail, Skip }
pub struct ManifestRow { table, rows_read, rows_source_stats, fingerprint_before, fingerprint_after, started, finished, seconds, isolation, rereads }
pub fn write_manifest(path: &Path, rows: &[ManifestRow]) -> Result<()>;   // CSV + .json
pub async fn wait_for(c: &mut MsClient, sql: &str, timeout: Duration) -> Result<()>;
```

### `verify` and `mssql::swap` (stream G)

```rust
pub enum VerifyLevel { Counts, Checksum, Except { sample: usize } }
pub struct VerifyResult { pub table: String, pub partition: Option<i32>, pub ok: bool, pub detail: String }
pub fn verify_table(con: &duckdb::Connection, c: &mut MsClient, duck: &core::model::Table, target: &TargetTable, m: &TableMapping, level: VerifyLevel) -> Result<Vec<VerifyResult>>;   // same canonical text in both engines
pub async fn verify_indexes(c: &mut MsClient, before: &TargetTable) -> Result<Vec<VerifyResult>>;
pub async fn swap(c: &mut MsClient, pairs: &[(String, String)], lock_timeout_ms: i64, keep_old: bool) -> Result<Vec<String>>;   // one transaction, sp_rename, SCHEMABINDING check first
pub async fn drop_old(c: &mut MsClient, before: &str) -> Result<Vec<String>>;
```

### Translator (stream F)

- `transpile::Options` gains `strict: bool`, `nocase_mode: NocaseMode { Global, Rewrite, Off }`, `mssql_math: bool`,
  `types: HashMap<String, String>` (column → SQL Server type for exact arithmetic).
- Warnings become `Warning { code: &'static str, line: usize, col: usize, message: String }` with `Display`
  `W-DIV-DECIMAL (line 12:8): ...`; `Translated.warnings: Vec<Warning>` (keep a `warning_strings()` helper
  so existing callers compile).
- Codes: `W-DIV-DECIMAL`, `W-ROUND-TRUNC`, `W-FORMAT-NUM`, `W-TRYCAST-EMPTY`, `W-CI-COMPARE`,
  `W-TRAILING-SPACE`, `W-PROCEDURAL`, `W-POWER-DOUBLE`.
- `duckdb_prelude(mode)`: no `default_collation` unless `Global`.
- `mathcompat::macros() -> Vec<String>`: `mssql_div`, `mssql_round`, `mssql_round_trunc`, `mssql_try_int`,
  `mssql_format_pct` as DuckDB macros with integer/HUGEINT arithmetic; `mathcompat::verify_cases(n, seed)`.
- `audit::audit_ci(con, tables, columns, script: Option<&str>) -> Vec<AuditRow>`.

## CLI surface (stream H)

```
import  … --isolation <read-committed|read-uncommitted|snapshot> --lock-timeout <ms> --lock-check/--no-lock-check
          --lock-wait <s> --on-lock <fail|skip> --max-rereads <n> --manifest <file> --readonly --wait-for <sql> --wait-timeout <s>
run     … --strict --nocase <global|rewrite|off> --mssql-math --types <csv>
export  … --into-existing --ignore-extra-columns --allow-rescale --check-only --check-pk [full|hash]
          --split <table>=<spec> (repeatable) --strategy <table|*>=<switch|heap-then-index|serial> (repeatable)
          --max-jobs-per-table <n> --state-dir <dir> --replace-partitions --drop-old
          --string-type <varchar|nvarchar|auto> --string-sizes <from-data|max|file>
          --verify <counts|checksum|except> --sample <n>
          --disposable-db-pattern <regex> --compat-level <n> --recovery <simple|full|none> --create-database
          --plan --log <file> --summary <file> --progress-seconds <n>
          --source-parquet <glob>
audit-ci <duckdb> --tables … --columns … [--script file]
verify-math --host … [--cases n]
swap    --host … --database … --pairs new=live,… [--keep-old] ; drop-old --before <date>
```

Exit codes: 0 ok, 1 check failed, 2 could not run, 3 guard.

## Order

1. A (guard + runlog) lands first so every other stream builds on it.
2. B, C, E, F, I in parallel (independent files).
3. D and G once B's functions exist.
4. H: wire everything, e2e `scripts/e2e_large_load.sh` (partitioned 10 M-row table on the 2019 container,
   kill/rerun, lock contention, fingerprint, guard, math), README, version bump, release.
