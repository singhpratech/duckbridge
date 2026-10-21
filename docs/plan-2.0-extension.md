# Plan: the DuckBridge extension on DuckDB 2.0 (function-based design)

Status 2026-10-20. Supersedes the "blocked" verdict in docs/handoff-neo.md §"The extension on neo".

## Why a redesign

The v2 C extension API (`duckdb_v2_extension_input` = `{get_api, extension, context, err}`) gives an
extension no instance and no connection: `duckdb_v2_connection_create` needs an instance, the
borrowed `context` reaches only a factory, the config, the file system and logging, and every
`duckdb_v2_extension_*` / `duckdb_v2_context_*` function is a registration or a getter. Verified
against DuckDB main and against commit d54cbbe3df (the build the community registry labels
v2.0.0): same 620-entry API table, same order, as the v2.0.0-alpha49003 our `duckdb-neo` pin
targets, so one build serves both.

So on 2.0 the extension cannot run SQL against the database that loaded it. Everything it does
must be a *function DuckDB calls*: table functions (rows out), a COPY function (rows in), and
pragma query functions (SQL text DuckDB executes). All three exist in duckdb-neo at our pin
(`TableFunctionBuilder`, `CopyFunctionBuilder::register` on `Extension`, `PragmaFunctionBuilder::query`).

The 1.5.6 build (`extension/src/duckdb_v1.rs`) is unchanged.

## Interface (DuckDB 2.0 build only)

```sql
LOAD duckbridge;

-- catalog of a SQL Server database
SELECT * FROM bridge_tables('mssql://sa:p%40ss@sql01:1433/Sales', tables := 'dbo.*', exclude := NULL);
--  schema VARCHAR, name VARCHAR, kind VARCHAR ('table' | 'view'), rows BIGINT, columns BIGINT,
--  pk VARCHAR (comma list), import_sql VARCHAR (the CREATE TABLE ... AS FROM bridge_scan(...) for it)

-- one table, or any query, streamed over TDS
SELECT * FROM bridge_scan('mssql://sa:p%40ss@sql01:1433/Sales', 'dbo.orders');
SELECT * FROM bridge_scan(dsn, 'SELECT id, total FROM dbo.orders WHERE total > 100');
CREATE TABLE orders AS FROM bridge_scan(dsn, 'dbo.orders', nolock := true);
--  named: nolock BOOLEAN (false), batch_rows BIGINT (50000), lock_timeout BIGINT ms (1000),
--         isolation VARCHAR ('read committed' | 'snapshot' | 'read uncommitted'), readonly BOOLEAN (false)

-- whole database: the extension generates the script, DuckDB runs it
PRAGMA bridge_import('mssql://sa:p%40ss@sql01:1433/Sales', tables := 'dbo.*', exclude := NULL,
                     schema_map := 'dbo=main', keys := true, overwrite := false, nolock := false);
--  CREATE SCHEMA IF NOT EXISTS ...; CREATE TABLE s.t AS FROM bridge_scan(...); ALTER TABLE ... ADD PRIMARY KEY ...

-- the team's T-SQL against this database (translated, prelude first)
PRAGMA bridge_run('UPDATE dbo.orders SET total = qty * price WHERE ...',
                  nocase_mode := 'rewrite', strict := false, mssql_math := false, types := false);

-- this database -> SQL Server, bulk copy
COPY orders TO 'mssql://sa:p%40ss@sql01:1433/Sales/dbo.orders' (FORMAT duckbridge);
COPY (SELECT ...) TO 'mssql://sa:p%40ss@sql01:1433/Sales/dbo.kpi'
     (FORMAT duckbridge, CREATE true, DROP_EXISTING false, KEEP_IDENTITY false, TABLE_LOCK true,
      BATCH_ROWS 50000, CHECK_CONSTRAINTS true, ORDER_BY 'id', STRING_TYPE 'auto', TARGET_VERSION 2019);
--  path = mssql://user:pw@host:port/Database/schema.table ; TABLE 'schema.table' option overrides the
--  last path segment. The table must exist unless CREATE true (DDL from the bound column types via
--  core::typemap / core::emit, Flavor::Mssql). The guard still owns CREATE/DROP DATABASE: COPY never
--  creates a database.

-- unchanged
SELECT bridge_transpile('tsql', 'duckdb', 'SELECT TOP 5 ISNULL(a, 0) FROM dbo.t');
SELECT bridge_prelude(true);
```

`bridge_import(...)` / `bridge_export(...)` / `bridge_run(...)` as *table functions* stay registered
on 2.0 and fail at bind with a message naming the replacement (`PRAGMA bridge_import`, `COPY ... TO
'mssql://...' (FORMAT duckbridge)`, `PRAGMA bridge_run`). DuckDB 2.0 does run several statements
returned by one pragma (`PRAGMA version; PRAGMA version` works in one string; verify with ours).

DSN percent-encoding and `mssql://` parsing: `api::parse_dsn` + `Dsn::mssql_target(Some(db))`,
as the CLI and the 1.x extension do. The database part is required; for COPY the path has one
more segment.

## Mechanics

### Core additions (`src/mssql/`, backend-neutral, compile under both manifests)

* `scan.rs` (new)
  * `pub async fn describe(client, sql) -> Result<Vec<ScanColumn>>` via
    `sys.dm_exec_describe_first_result_set(@sql, NULL, 0)` (name, system_type_name, is_nullable);
    `ScanColumn { name, sql_type, duck_type }` with `duck_type` from `core::typemap::parse_type` +
    `mssql_to_duck`. Works for `schema.table` (wrapped as `SELECT * FROM [s].[t]`) and for queries.
  * `pub fn source_sql(spec: &str, nolock: bool) -> String`: `schema.table` / `[s].[t]` → SELECT *
    (with `WITH (NOLOCK)` when asked); anything starting with SELECT/WITH/`(` is a query (NOLOCK
    not applied; document).
  * `pub struct RowStream`: owns a tokio runtime + task; `RowStream::start(target, database,
    safety: ReadSafety, sql, batch_rows) -> Result<(Vec<ScanColumn>, RowStream)>`;
    `fn next_batch(&mut self) -> Result<Option<Vec<Vec<duck::Value>>>>` (blocking recv on a bounded
    channel, 4 batches deep); conversion with `import::column_to_value` (make it `pub(crate)` →
    `pub`). Errors from the task surface on `next_batch`. Drop cancels.
  * Catalog listing for `bridge_tables` / `PRAGMA bridge_import`: reuse
    `import::read_catalog_with` (`ImportOptions` include/exclude/schema_map) through
    `SourceClient::connect`; `pub fn import_script(dsn_text, tables, views, opts, keys) -> String`
    renders the CTAS script (`quote_duck` for identifiers, single-quoted DSN with `'` doubled;
    `bridge_scan(dsn, 'schema.table', nolock := ...)`).
* `bulk.rs`
  * `pub fn rows_from_batch(batch: &RecordBatch, convs: &[Converter], first_row: i64) -> Result<Vec<TokenRow<'static>>>`:
    the fast path (`convert_column`) when every field `has_fast_path`, else row by row through
    `value_ref` + `Converter::convert` (same error type `ConvertError`).
  * `pub struct BulkSink`: `BulkSink::open(target: &MssqlTarget, database: &str, table_ref: &str,
    columns: &[String], convs: &[Converter], o: &BulkOptions) -> Result<BulkSink>` (own tokio
    runtime; connects; `column_metadata` + `wire_fixups`; spawns `send_loop` fed by an mpsc channel),
    `fn send(&mut self, rows: Vec<TokenRow<'static>>) -> Result<()>` (blocking_send),
    `fn finish(self) -> Result<i64>` (sends Done, joins, returns rows sent), Drop aborts.
    `send_loop` stays private; `BulkSink` lives next to it.
  * Table creation for COPY `CREATE true`: `pub fn create_table_ddl(schema, table, columns: &[(name,
    duck_type)], o) -> Vec<String>` built from `core::model::Column/Table` + `typemap::map_column_with`
    + `emit::Emitter::new(Flavor::Mssql, ..)` (`create_schema` if needed, `create_table`), run through
    `mssql::exec_batch_as("target", ..)` so `--plan`/run log semantics hold. Converters then come from
    `Converter::for_sql_type(sql_type, duck_type)` with the rendered SQL Server types, or from
    `target::read_target_tables` + `Converter::for_target` when the table exists.

### Extension glue (`extension/src/duckdb_neo.rs`, plus backend-neutral helpers in `lib.rs`)

* `bridge_scan`: `TableFunctionBuilder` with positional `dsn`, `source` (VARCHAR) and the named
  parameters above as `FunctionParameter::with_default(..).kind(Named)`; in `bind` the `Vec<BindArgument>`
  is positional-fixed first, then the named ones in declaration order (check `counts()` in a test).
  `bind`: parse DSN, `describe` → `add_result_column(name, type)` for each column (logical types
  from the `duck_type` text: `LogicalType` via `Factory` / `create_type_from_text`), `set_cardinality`
  when the source is a table (`sys.dm_db_partition_stats`), keep `(target, database, safety, sql,
  columns)` in BindData. `init_global_state`: start the `RowStream`, max threads 1. `exec`: one
  batch → output vectors. Writing: reuse `core::duck::neo::write_column` (make it `pub` behind the
  `duckdb-neo` feature, generic over `&mut DataChunkRef`), one `Kind` per declared column type;
  DECIMAL/UUID/HUGEINT columns: write the text into a scratch `DataChunk::create(ctx, [VARCHAR])`
  vector and `cast_rows` into the output vector (or `DecimalValue`/`UuidValueRaw` typed writes when
  cheaper). Set the chunk size to the batch length; 0 rows ends the scan.
* `bridge_tables`: table function, single chunk per call page (`Paged` from lib.rs).
* `PRAGMA bridge_import` / `PRAGMA bridge_run`: `PragmaFunctionBuilder::query`; arguments through
  `BindArguments` (`index(name)`); `bridge_run` = `bridge_prelude(nocase)` lines + translated
  statements joined with `;\n` (reuse `transpile_text` / `api::nocase_mode`, strict → error).
* COPY `duckbridge`: `CopyFunctionBuilder::new("duckbridge", Copy).register(ext)`.
  `bind`: parse path + options (`CopyToBindInfo::options()`, `Value::get::<String|bool|i64>`),
  record column names + DuckDB types (`LogicalType` → text via `to_string`/`alias`); fail on
  unsupported types with the column name. `init`: connect (`MssqlTarget::connect(database)` inside a
  runtime), DROP_EXISTING/CREATE, read target columns, build converters, `BulkSink::open`; InitData =
  `Mutex<Option<BulkSink>>` + `AtomicI64` rows. `batch`: scan the `ColumnDataCollection` chunks →
  `ArrowExporter::new(ctx, columns, None)` → `next_array()` → `transmute` the C-data structs into
  `arrow::ffi::FFI_ArrowArray` / `FFI_ArrowSchema` (arrow 58 `arrow::ffi::from_ffi`; the duckdb-neo
  test does exactly this) → `RecordBatch` → `rows_from_batch` → `Vec<TokenRow>` (BatchData).
  `flush`: `sink.send(rows)`. `finalize`: `sink.finish()`, compare with rows counted in `batch`.
  `batch_size`: `Some(BATCH_ROWS)`.
* Keep `bridge_import` / `bridge_export` / `bridge_run` table functions as `Unavailable` with the
  new message text (`NO_CONNECTION` → `USE_INSTEAD`).

### Tests

* `scripts/e2e_extension_neo.py` (DuckDB 2.0 wheel in the scratch venv; sqlcmd for setup/verify):
  LOAD; `bridge_tables` lists the fixture tables; `bridge_scan` row counts + a checksum for every
  column type the fixture has (tinyint..bigint, bit, decimal, money, float, real, char/varchar/
  nvarchar(max), uniqueidentifier, date, time(7), datetime, datetime2, datetimeoffset, binary,
  varbinary(max)); query mode; `PRAGMA bridge_import` into a fresh in-memory DB then counts;
  `PRAGMA bridge_run` creating a table via T-SQL (`SELECT ... INTO`), strict mode error;
  COPY TO with CREATE true into `d2m_neo_out` (disposable pattern `^d2m_`), then sqlcmd COUNT(*) and a
  CHECKSUM_AGG parity against the DuckDB side; COPY TO an existing table; failure cases (bad DSN,
  unknown table, type that does not fit). Old table-function names fail with the new message.
* Unit tests in core: `source_sql`, `import_script`, `rows_from_batch` (mixed fast/slow columns),
  `describe` against the container (`#[ignore]`-free but skipped without `MSSQL_*` like the others).
* Regression: `cargo test --release` (root), `cargo test --release --manifest-path neo/Cargo.toml
  --no-default-features --features duckdb-neo,bundled-neo`, `scripts/e2e_mssql.sh`,
  `scripts/e2e_extension.sh` (v1, unchanged), `scripts/e2e_large_load.sh`, `make release-neo` +
  `e2e_extension_neo.py`.

## After it works

1. `duckbridge-extension` (template repo, builds in the community registry for v2.0.0): make its
   Makefile build `extension/neo/Cargo.toml` when `DUCKDB_VERSION` starts with `v2`, and the v1
   manifest otherwise (one extension name, both lines). Then bump the community descriptor.
2. README: a "DuckDB 2.0" section with the interface above; the 1.x section stays.
3. `private/decisions-log.md`, memory.
