# Hand-off: the `duckdb-neo` backend (DuckDB 2.0), step 2 of 2

Branch `duckdb-neo` (from `main` with the `core::duck` layer of step 1, see
docs/handoff-duck-layer.md). Goal: a second DuckDB backend on `duckdb-neo` (DuckDB's Rust bindings
for the v2 C API of DuckDB 2.0) for the engine (CLI) and the loadable extension, verified against
the DuckDB 2.0.0 alpha build duckdb-neo pins, with the default `duckdb-v1` backend unchanged.

Pins: duckdb-rs `main` at `c4936cea71f313deb4375fbc5b47ea9e17214236` ("Neo extensions", #910,
2026-10-20), crates `duckdb-neo` / `libduckdb-sys` / `duckdb-loadable-macros` 1.20000.0, DuckDB
`v2.0.0-alpha49003` = `b32b1368b8db5f398b5e91fbacbf4d80f6fe7139` (`crates/libduckdb-sys/.duckdb-release`).

Commits (on top of step 1):

1. `core: duckdb-neo backend for core::duck (DuckDB 2.0), built from neo/Cargo.toml`
2. `extension: duckdb-neo glue for DuckDB 2.0 (extension/neo/Cargo.toml, make release-neo)`
3. `core(neo): keep DuckDB 2.0 accepting the x -> expr lambda syntax`
4. `scripts: DuckDB 2.0 test helpers (duckdb CLI stand-in, extension smoke test)`
5. `docs: hand-off for the duckdb-neo backend` (this file)

## Results

| | duckdb-v1 (default) | duckdb-neo (DuckDB 2.0.0) |
|---|---|---|
| `cargo test --release` (lib + translator) | 103 + 81 pass | 105 + 81 pass |
| `scripts/e2e_mssql.sh` | 45/45 | 45/45 |
| `scripts/e2e_large_load.sh` | 118/118 | 118/118 |
| extension | `make release` + `e2e_extension.sh` 33/33 (DuckDB 1.5.6 CLI and the 2.0.0 Python wheel, same binary) | `make release-neo` + `e2e_extension_neo.py` on the `duckdb==2.0.0` wheel: 139 pass, 0 fail |

The neo CLI writes DuckDB 2.0 storage (file version 69), which the 1.5.6 CLI cannot open, so the CLI
suites on the neo binary need a 2.0 reader: `BIN=neo/target/release/duckbridge DUCK=<wrapper around
scripts/duckdb_py_cli.py in a duckdb==2.0.0 venv> scripts/e2e_mssql.sh`. With the 1.5.6 CLI as
`DUCK`, 14 of the 45 and 3 of the 118 checks fail for that reason alone.

neo was also run with `download-neo` (prebuilt `libduckdb.so` of the pinned build, which includes
ICU): `cargo test` 94 + 81 pass. Streaming benchmark (`cargo test --release --lib -- --ignored
bench_convert --nocapture`, 2 M rows, 17 columns, this box): DuckDB read only v1 7.9 M rows/s, neo
9.0 M rows/s; read + convert v1 1.46 M rows/s, neo 1.26 M rows/s; pipelined v1 1.21 M rows/s, neo
1.15 M rows/s.

## Building

### Why two manifests

The task asked for `duckdb-neo = ["dep:duckdb-neo"]` next to `duckdb-v1` in the root Cargo.toml
(and a `neo` feature in extension/Cargo.toml). Cargo refuses that graph even with both optional:

```
package `libduckdb-sys` links to the native library `duckdb`, but it conflicts with a previous package
help: try to adjust your dependencies so that only one package uses the `links = "duckdb"` value
failed to select a version for `libduckdb-sys` which could resolve this conflict
```

`duckdb` 1.10506.1 requires `libduckdb-sys = "=1.10506.1"` (crates.io), duckdb-neo needs the git
`libduckdb-sys` 1.20000.0, both declare `links = "duckdb"`, and the lockfile resolves optional
dependencies too. So the neo builds use their own manifests over the **same sources**:

* `neo/Cargo.toml` - package `duckbridge` (lib `../src/lib.rs`, bin `../src/main.rs`, test
  `../tests/transpile_tests.rs`), its own workspace and `Cargo.lock` (seeded from the root lock,
  so shared crates keep their versions). Features: `duckdb-neo = ["dep:duckdb-neo", "dep:arrow"]`,
  `bundled-neo` (default), `download-neo`, and `duckdb-v1 = []` (only so naming it gives the
  "exactly one backend" compile error).
* `extension/neo/Cargo.toml` - package `duckbridge_ext` (lib `../src/lib.rs`, cdylib), feature
  `neo` (default) selecting `src/duckdb_neo.rs`; depends on `duckbridge` from `../../neo` with
  `features = ["duckdb-neo"]`, `duckdb-neo` with `build-extension`, `duckdb-loadable-macros` with `neo`.
* Root `Cargo.toml` and `extension/Cargo.toml` only gained a `[lints.rust] unexpected_cfgs`
  entry declaring the `duckdb-neo` / `neo` feature names for check-cfg (0 warnings as before);
  their dependency graphs and `Cargo.lock`s are untouched, so the v1 build is byte-for-byte the
  same crates. The root package stays publishable (no git dependency in it).

`src/core/duck.rs` is now the facade: `compile_error!` when both or neither backend feature is
on, `mod v1` (`duck/v1.rs`, step 1's code moved verbatim) or `mod neo` (`duck/neo.rs`), `pub use`.

### Commands

```sh
# v1 (unchanged)
cargo build --release && cargo test --release
cd extension && make release && make test

# neo CLI + tests; DuckDB 2.0 compiled in (CMake build, ~4 min once on 20 cores; needs cmake)
cargo build --release --manifest-path neo/Cargo.toml                       # neo/target/release/duckbridge
cargo test  --release --manifest-path neo/Cargo.toml --no-default-features --features duckdb-neo,bundled-neo
# neo against the prebuilt libduckdb of the pinned build (downloaded by libduckdb-sys at build time)
cargo build --release --manifest-path neo/Cargo.toml --no-default-features --features duckdb-neo,download-neo
# neo extension
cd extension && make release-neo                                          # build/neo/duckbridge.duckdb_extension
```

`bundled-neo` = `duckdb-neo/bundled-cmake-icu` + `duckdb-neo/bundled-json` (parquet is always in
the CMake build). Not the simpler `bundled` (cc) build: that one compiled in 3 min too but lacks
ICU, and DuckDB 2.0 alpha builds have no extension repository to autoload it from
(`http://extensions.duckdb.org/b32b1368b8/linux_amd64/icu.duckdb_extension.gz` is a 404), which
breaks `SET TimeZone`, `strftime` with zones and `transpile_tests::mssql_math_rewrites_and_runs`.
`bundled-cmake` needs the full DuckDB sources: cargo checks out duckdb-rs' `duckdb-sources`
submodule for git dependencies (~300 MB, at exactly `b32b1368b8`).

`download-neo` builds in under a minute (libduckdb-sys downloads
`<DUCKDB_RELEASE_URL>/duckdb-shared-libs-linux-amd64.tar.gz` and copies `libduckdb.so` into its
`OUT_DIR`); the binary then needs `LD_LIBRARY_PATH=<target>/release/build/libduckdb-sys-*/out`
outside `cargo run` / `cargo test`, so the e2e runs used `bundled-neo`.

### Running the e2e scripts on neo

There is no DuckDB 2.0 CLI for the alpha (`<DUCKDB_RELEASE_URL>/duckdb_cli-linux-amd64.zip` is a
404; only `duckdb-shared-libs-linux-amd64.tar.gz` exists). `scripts/duckdb_py_cli.py` is a stand-in
for the CLI subset the scripts use, over the `duckdb` Python package
(`pip install duckdb==2.0.0.dev2610192346`, `PRAGMA version` = v2.0.0-alpha49003 / b32b1368b8):

```sh
printf '#!/bin/sh\nexec /path/to/venv/bin/python -I %s/scripts/duckdb_py_cli.py "$@"\n' "$PWD" > /tmp/duck20 && chmod +x /tmp/duck20
BIN=neo/target/release/duckbridge DUCK=/tmp/duck20 scripts/e2e_mssql.sh
mkdir -p /tmp/large && BIN=neo/target/release/duckbridge DUCK=/tmp/duck20 WORK=/tmp/large scripts/e2e_large_load.sh
/path/to/venv/bin/python -I scripts/e2e_extension_neo.py         # EXT= to point at another build
```

(`WORK` must exist when given; the scripts only `mktemp -d` it themselves when it is unset.)
The DuckDB 2.0 engine reads the 1.x fixture `tests/fixtures/sample.duckdb` without rewriting it
(checksum unchanged after the runs).

## What differs in the neo backend (`src/core/duck/neo.rs`)

Same public names and signatures as v1 (`Connection`, `Statement`, `Rows`, `Row`, `Value`,
`ValueRef`, `TimeUnit`, `Decimal`, `ListType`, `ToSql`, `FromSql`, `arrow`, `Appender`,
`ArrowStream`, `open`, `open_readonly`, `open_in_memory`, `try_clone`, `exec`, `execute`,
`set_setting`, `query_scalar`, `query_one`, `query_rows`, `query_rows_with`, `for_each_row`,
`list_of_strings`, `appender`, `stream`, `arrow_batches`), plus the methods the direct call sites
use (`Connection::prepare`, `Statement::query([] | &[&dyn ToSql])`, `Rows::next`,
`Row::{get::<_, T>, get_ref, get_ref_unwrap, column_count}`, `r.as_ref().column_count()`, and
`Connection::interrupt_handle().interrupt()`, which `mssql/bulk.rs` uses and the step-1 list
missed). No call site outside `duck/` changed.

* **Instances**: one process-wide `Environment` (DuckDB 2.0's file-conflict detection is per
  environment). `open` = `environment.open(OnDisk(path))`, `open_readonly` = `Instance::new` +
  `attach(AttachBuilder::new(path).option("read_only","true").make_default())`, `open_in_memory`
  = `open(InMemory)`. `Connection` wraps the neo connection plus an `Arc<Instance>`, so
  `try_clone` is `instance.connect()`.
* **Results through Arrow**: every query runs as `PreparedStatement::execute_arrow`; the Arrow C
  data interface structs (`libduckdb_sys::v2::ArrowSchema/ArrowArray`, which no longer depend on
  an arrow crate since duckdb-rs #898) are transmuted into `arrow::ffi::FFI_ArrowSchema/
  FFI_ArrowArray` of our own `arrow = "58"` (the version duckdb-rs 1.x re-exports, so `bulk.rs`
  compiles unchanged) and imported with `arrow::ffi::from_ffi`. `stream` reads 2048-row arrays
  (one per DuckDB vector, like duckdb-rs 1.x's `stream_arrow`), the row helpers 16384-row arrays.
* **Value model**: `ValueRef`, `Value`, `TimeUnit`, `Decimal` (+ `DecimalError`), `ListType`,
  `EnumType` are DuckBridge-owned, with duckdb-rs 1.x's variants and semantics; `Row::get_ref`
  maps Arrow arrays exactly as duckdb-rs' `value_ref_internal` does (Decimal128(38,0) =
  `HugeInt`, dictionaries = `Enum`, ...), plus `Utf8View` / `BinaryView` / `Time64(ns)` and
  geometry by Arrow extension name. `FromSql` covers the types the code reads (`String`, all
  integers, `f32`/`f64`, `bool`, `Vec<u8>`, `Decimal`, `Option<T>`) with duckdb-rs' conversions
  (floats truncate toward zero, decimals round half away from zero, text is parsed, `bool` from
  integers). `ToSql` is any duckdb-neo `QueryParameter`.
* **Appender**: duckdb-neo has no table appender. `Appender` buffers `Value` rows; `flush` (also on
  drop, and every 131072 rows) writes them into 2048-row data chunks of a `ColumnDataCollection`
  whose column types follow the values (first non-NULL per column; VARCHAR when mixed or all NULL;
  DECIMAL as text) and runs `INSERT INTO "schema"."table" SELECT * FROM __duckbridge_append` with
  the collection bound to the statement (`Statement::add_collection`). The INSERT casts to the
  table's column types, as the v1 appender's typed appends do. `import` flushes every
  `batch_rows`, so one INSERT per batch.
* **Errors**: DuckDB's message only; duckdb-neo's `Display` appends ` (code N)`, which is dropped
  so texts match v1 (`Query returned no rows` for `query_one` without a row, as duckdb-rs).
* **DuckDB 2.0 behaviour**: 2.0 rejects the deprecated `x -> expr` lambda syntax
  ("Deprecated lambda arrow (->) detected"). `catalog.rs`' ENUM expansion, the one place DuckBridge
  used it, now writes `lambda x: expr` (accepted by DuckDB >= 1.3, so by v1 too); the interim
  `lambda_syntax = 'ENABLE_SINGLE_ARROW'` instance setting was removed on 2026-10-21. Users' scripts
  with arrows fail on 2.0 as they do on plain DuckDB 2.0. Nothing else in the 45 + 118 e2e checks
  needed a change.
* `mssql/guard.rs`' source-scan test now also finds `../src` when built from `neo/`.

## The extension on neo (`extension/src/duckdb_neo.rs`)

The v2 extension API gives an extension no connection to the database that loaded it
(`duckdb_v2_extension_input` = extension handle, borrowed context, error slot; duckdb-neo's
`Extension` / `Context` wrap exactly that, `crates/duckdb-neo/src/extension.rs:22`,
`connection.rs:318`). So on DuckDB 2.0 the extension does not read or write DuckDB tables itself:
everything is a function DuckDB calls (design: docs/plan-2.0-extension.md, which also lists what
deviated from it). `LOAD`, the footer (`v2.0.0-alpha49003` / `C_STRUCT_UNSTABLE`), the entry point
`duckbridge_init_c_api_v2`, `bridge_transpile` (both overloads) and `bridge_prelude` are unchanged.

```sql
SELECT * FROM bridge_tables('mssql://sa:p%40ss@sql01:1433/Sales', tables := 'dbo.*', exclude := 'dbo.v_*');
SELECT * FROM bridge_scan('mssql://.../Sales', 'dbo.orders', nolock := true);           -- or a SELECT / WITH query
PRAGMA bridge_import('mssql://.../Sales', tables = 'dbo.*', schema_map = 'dbo=main');   -- PRAGMA arguments: name = value
PRAGMA bridge_run('UPDATE dbo.orders SET total = qty * price', strict = true);
COPY orders TO 'mssql://.../Sales/dbo.orders' (FORMAT duckbridge, CREATE true, SORT_BY 'id');
```

* **`bridge_scan(dsn, source, nolock, batch_rows, lock_timeout, isolation, readonly)`** (table
  function). Bind connects (`SourceClient`, R6 session settings), describes the source with
  `sys.dm_exec_describe_first_result_set` (`core::mssql::scan::plan`), declares one result column per
  source column (the importer's type mapping; `money` read as DECIMAL(19,4), xml / hierarchyid /
  geography cast as the importer does) and the `sys.partitions` row count as cardinality. Init starts a
  `scan::RowStream` (own runtime, one worker thread, bounded channel 4 batches deep) and re-describes:
  a source that changed since bind is an error naming the column. Exec writes at most 2048 rows per
  call with `core::duck::write_column` (`Kind::of_type` per declared type; DECIMAL mantissas, UUID,
  TIMESTAMPTZ written directly). Max threads 1. Query mode runs the text unchanged; the source
  connection is read-only (`assert_read_only`: `SELECT ... INTO`, `EXEC`, DML are refused).
* **`bridge_tables(dsn, tables, exclude, schema_map, <scan options>)`**: the importer's catalog,
  columns schema, name, kind, rows, columns, pk, import_sql (`CREATE TABLE ... AS FROM
  bridge_scan(...)`). `tables` / `exclude` take names, `schema.name` and `*` / `?` wildcards (the
  importer's filter, now with globs; the CLI's `--tables` / `--exclude` get them too).
* **`PRAGMA bridge_import(dsn, tables, exclude, schema_map, keys, keys_after_load, overwrite, views,
  schema_only, <scan options>)`** (query pragma). Returns a script DuckDB runs in place of the
  PRAGMA: the importer's own DDL (`import::duck_schema_sql`: schemas, identity sequences, tables with
  NOT NULL / identity defaults / PK / UNIQUE / FK), one `INSERT INTO t (cols) SELECT cols FROM
  bridge_scan(...)` per table in FK order, keys after the load, translated views, and a closing
  `SELECT` of (object, kind, rows, note) rows - the 1.x result shape, with each table's DuckDB count.
  The DSN (with its password) is part of the script text DuckDB parses.
* **`PRAGMA bridge_run(sql, dialect, nocase_mode, strict, mssql_math, types, schema_map, reset)`**.
  Translated statements behind the prelude and the math macros. Two DuckDB 2.0 facts shape it:
  `integer_division` is applied when a statement is *parsed* and DuckDB parses a pragma's whole script
  before running it, so the callback sets the prelude on the session through `Context::config()`
  (before the script is parsed; the script repeats it as `SET SESSION`); and DuckDB refuses a pragma
  expansion in which a row-returning statement is followed by others ("cannot be served as one
  result"), so SELECTs before the last statement are left out (a comment marks them) and `reset =
  true` keeps the last statement's rows in a temp table while the settings are RESET. The prelude
  settings otherwise stay on the session (1.x reset them).
* **`COPY ... TO 'mssql://user:pw@host:port/Database/schema.table' (FORMAT duckbridge, CREATE,
  DROP_EXISTING, KEEP_IDENTITY, TABLE_LOCK, BATCH_ROWS, CHECK_CONSTRAINTS, SORT_BY, STRING_TYPE,
  TARGET_VERSION, TABLE)`**. Bind checks path, options (unknown ones are errors) and column types
  (nested types refused with the column name). Init (`v2::copy_open`) drops / creates the table
  (`bulk::create_table_ddl`: `typemap::map_column_with` + the T-SQL emitter, strings MAX), maps the
  input columns by name onto `target::read_target_tables`, builds `Converter::for_target`s and opens a
  `bulk::BulkSink` (writer thread, own connection, `send_loop`, `wire_fixups` checks). Batch exports
  the `ColumnDataCollection` with duckdb-neo's `ArrowExporter`, imports it as arrow `RecordBatch`es
  (`core::duck::import_arrow`) and converts with `bulk::rows_from_batch` (batches may run in parallel);
  flush sends in DuckDB's batch order; finalize commits and compares the rows sent with the rows read.
  DuckDB's own COPY options (`BATCH_SIZE`, `ORDER_BY`, `PARTITION_BY`, ...) never reach the format;
  `ORDER_BY` is DuckDB's, so the bulk ORDER hint is `SORT_BY` (the query must produce that order; the
  server refuses unsorted data). COPY never creates a database.
* `bridge_import(...)` / `bridge_export(...)` / `bridge_run(...)` table functions stay registered with
  the 1.x call shapes and fail at bind naming the 2.0 replacement.

Table-function bind gets `Vec<BindArgument>` without names: positional arguments, then the named ones
in declaration order with defaults filled in (an explicit NULL arrives as a NULL value). Checked by a
probe build and by `bridge_scan` calls with the named arguments permuted (`isolation`, `batch_rows`,
`lock_timeout` errors come from the right slot); the glue checks the argument count.

Core pieces added for it (backend-neutral, both manifests): `src/mssql/scan.rs`; in `bulk.rs`
`rows_from_batch` (columnar fast path per column, Arrow display text for dictionaries / nested),
`BulkSink`, `create_table_ddl`, `TdsRows`, `describe_read_error` public; in `import.rs`
`duck_schema_sql` / `keys_sql` / `duck_view_sql` (what `create_duck_schema` / `add_keys` /
`create_duck_views` now run), `select_expr`, `column_to_value` public, glob filters; in
`core/duck/neo.rs` `Kind` (+ DECIMAL, UUID, TIMESTAMPTZ), `Kind::of_type`, `ChunkMut`, generic
`write_column`, `import_arrow`.

Fixed on the way (affects the CLI import on both backends): `import::column_to_value` subtracted the
`datetimeoffset` offset from a value TDS already sends as UTC, so imported `datetimeoffset` columns
with a non-zero offset were off by that offset (tiberius' own `DateTime<Utc>` decoder has the same
bug; its encoder and `DateTime<FixedOffset>` decoder treat the wire value as UTC). The existing e2e
fixtures only had offset 0.

Verification (scratch script, DuckDB 2.0 wheel + the 2019 container): `bridge_scan` of a 5002-row
fixture with tinyint..bigint, bit, decimal(18,4)/(38,10), numeric, money, smallmoney, float, real,
char, varchar, nvarchar(max), varchar(max), nchar, uniqueidentifier, date, time(7), datetime,
smalldatetime, datetime2(7)/(3), datetimeoffset(7) (offsets -13h..+13h, +14h extreme), binary,
varbinary(max), xml, NULLs and extremes: every value equal to sqlcmd's canonical rendering, md5 of all
canonical rows equal; query mode likewise; `PRAGMA bridge_import` of d2m_extsrc: row counts per table
equal, identity sequence continues at 1001, PK / NOT NULL kept, views run; `PRAGMA bridge_run` with
`SELECT ... INTO`, UPDATE and a final SELECT; COPY of 100 000 rows x 17 types with `CREATE true`
(value-by-value + md5 equal) and into an existing table with narrower types (CHECKSUM_AGG equal to SQL
Server's own CAST of the same data, identity and DEFAULT columns filled); 3 M rows with `SORT_BY` into
a clustered PK at ~0.95 M rows/s; scan ~1.3-1.7 M rows/s; 88/88 checks (also: empty COPY input with
CREATE true, the TABLE option, STRING_TYPE, glob filters, and the error cases: bad password / DSN / database /
table, read-only guard, values that do not fit naming column and row, server constraint errors,
identity without KEEP_IDENTITY, nested types, unknown options, missing database never created).

## Makefile / CI

* `extension/Makefile`: `make release-neo` builds `extension/neo` and packs
  `build/neo/duckbridge.duckdb_extension` with `NEO_DUCKDB_VERSION ?= v2.0.0` (the 2.0.0 release; the
  alpha the crate pins has the identical v2 API table, so the binary loads on the release) and
  `NEO_ABI ?= C_STRUCT_UNSTABLE` (the footer's duckdb version must equal the engine's
  `PRAGMA version` string exactly, else the engine needs `allow_extensions_metadata_mismatch`).
  `make clean` also cleans `neo/`.
* DuckDB's extension-ci-tools: the 2.0 line is `v2.0-cyanoptera` (`PRAGMA version`'s codename).
  A CI job for the neo extension would set `TARGET_DUCKDB_VERSION` / `DUCKDB_EXTENSION_MIN_DUCKDB_VERSION`
  to the 2.0 version, build from `extension/neo/Cargo.toml` (the ci-tools' Rust template runs
  `cargo build` in the extension directory, so either point it at `neo/` or swap manifests in a
  pre-build step), and pass `C_STRUCT_UNSTABLE` + the exact version to
  `append_extension_metadata.py` (`-dv`), as `pack.sh` does. The extension does not link DuckDB,
  so no DuckDB build is needed for it.
* CI matrix entry for the engine: `cargo test --release --manifest-path neo/Cargo.toml
  --no-default-features --features duckdb-neo,bundled-neo` (needs cmake + a C++ toolchain; cache
  `neo/target`), or `download-neo` to skip compiling DuckDB.

## Open issues

* Extension on 2.0: the functions are a different interface from 1.x (table functions + COPY + pragmas
  instead of `bridge_import` / `bridge_export` / `bridge_run` table functions). Not available on 2.0:
  `bridge_export`'s .bak / parquet / postgres / script targets, parallel or split imports (`bridge_scan`
  is one connection per table, `PRAGMA bridge_import` loads tables one after the other), the import
  manifest, lock pre-check and fingerprint re-reads of R6 (`bridge_scan` applies the session settings
  only), `--plan` for COPY (the runlog is never initialised by the extension). They need either a
  connection from the v2 API (`duckdb_v2_context_get_instance` or a context query API) or the CLI.
* `PRAGMA bridge_import` / `bridge_tables` put the DSN, password included, into SQL text DuckDB parses
  (visible in query logs / profiling output).
* `PRAGMA bridge_run`: only the last statement's rows are returned (earlier SELECTs are skipped); the
  prelude settings stay on the session unless `reset = true`; translator warnings appear only as
  comments in the script (`strict = true` turns them into an error; `bridge_transpile` shows them).
* `bridge_scan` query mode: types tiberius cannot decode (hierarchyid, geography, geometry, sql_variant)
  must be cast in the query (table mode casts them); `money` in query mode arrives as a float64 from
  tiberius (exact up to about 9e11), table mode reads it as DECIMAL(19,4).
* COPY: rows are committed every `BATCH_ROWS` (100 000 by default): a failing COPY leaves the earlier
  batches in the table, as `export --into-existing` does; nested types (LIST / STRUCT / MAP / UNION)
  must be converted in the query (`to_json(col)`); with `CREATE true` strings become NVARCHAR(MAX) /
  VARCHAR(MAX) (nothing is measured before the stream starts).
* `lambda_syntax` compatibility setting removed (2026-10-21): catalog.rs uses `lambda x: ...` (DuckDB >= 1.3,
  so v1 too); users' scripts with `x -> expr` fail on 2.0 as they would on plain DuckDB 2.0.
* Appender: values of mixed variants in one column (never produced by `import.rs`) go in as text;
  LIST / STRUCT / MAP / UNION `Value`s are rendered as debug text (the v1 appender rejects most of
  them too).
* `download-neo` binaries load `libduckdb.so` at run time: set `LD_LIBRARY_PATH` to the directory
  libduckdb-sys copies it to (`neo/target/release/build/libduckdb-sys-*/out`), `cargo run` /
  `cargo test` do this themselves.
* The neo backend links ICU/parquet/json statically; other extensions (httpfs, ...) cannot be
  autoloaded on alpha builds.
* clippy on neo: one more `needless_question_mark` (in `jobs.rs`, because neo's `Row::get` already
  returns `anyhow::Result`); none in `duck/neo.rs`.
* No CI workflow was changed; the matrix entry above is what it needs.
* Streaming conversion on neo is ~10 % slower than v1 in `bench_convert` (the DuckDB read itself
  is faster); not investigated (candidates: the Arrow string layout DuckDB 2.0 exports, batch size).

## When DuckDB 2.0.0 and a duckdb-neo release are out

1. `neo/Cargo.toml` and `extension/neo/Cargo.toml`: replace the `git` + `rev` dependencies
   (`duckdb-neo`, `duckdb-loadable-macros`) with the released versions (`version = "=1.2000x.y"`).
   Re-run `cargo update` in both (lockfiles).
2. If duckdb-rs then publishes a `duckdb` 1.2000x for DuckDB 2.0 with the same `libduckdb-sys`
   as `duckdb-neo`, the `links` conflict disappears only for a v1 backend *on DuckDB 2.0*; the
   1.x backend (`duckdb` 1.10506) still cannot share a graph with it, so the two manifests stay
   until the v1 backend is retired.
3. `extension/Makefile`: `NEO_DUCKDB_VERSION` -> `v2.0.0` (or whatever `PRAGMA version` prints);
   if the v2 extension API becomes stable, `NEO_ABI` -> `C_STRUCT` (check what DuckDB's loader
   expects for v2 entry points).
4. `extension/src/duckdb_neo.rs`: the `api_version = "v2.0.0"` of the entry point; once the API
   gives extensions an instance/connection, the 1.x table functions can come back next to the
   function-based ones (mirror `duckdb_v1.rs`: `install_pool` with 64 connections, `Params` over named
   bind arguments, `RESULT_COLUMNS`, `Paged::next_page`) and the `allow(dead_code)` in `lib.rs` goes.
5. `src/core/duck/neo.rs`: revisit `COMPAT_SETTINGS` (lambda syntax), the Arrow batch sizes,
   and whether duckdb-neo grew a table appender (replace the collection + INSERT path).
6. `neo/Cargo.toml` `bundled-neo`: if the release has an extension repository, the cc `bundled`
   build (no cmake, no submodule) plus autoloaded ICU is an option again.
7. `scripts/duckdb_py_cli.py` / `e2e_extension_neo.py` become unnecessary once a 2.0 CLI binary
   exists: run `scripts/e2e_mssql.sh`, `e2e_large_load.sh`, `e2e_extension.sh` with `DUCK=` that CLI.
