# Hand-off: the DuckDB layer (`core::duck`), step 1 of 2

Branch `duckdb-2.0-prep` (from `main` at v0.1.1). Goal: put every use of the `duckdb` crate in
the core library behind one internal module, with **no behaviour change**, so that a second
backend (`duckdb-neo`, the Rust bindings for DuckDB 2.0's new C API) can be added behind a cargo
feature in step 2. duckdb-neo itself is **not** added yet.

Commits:

1. `core: route every DuckDB use through core::duck; duckdb-v1 cargo feature`
2. `extension: split backend-neutral logic from the duckdb-rs glue`
3. `docs: hand-off for the DuckDB layer` (this file)

Verified on this branch: `cargo build --release` (0 warnings; clippy warning count unchanged at
47, none in the touched code), `cargo test --release` (94 lib + 81 translator, all pass),
`cd extension && cargo check --release` (0 warnings) and `make release`,
`scripts/e2e_mssql.sh` 45/45, `scripts/e2e_extension.sh` 33/33, `scripts/e2e_large_load.sh`
118/118 (SQL Server 2019 container on localhost:14319).

## Cargo features

```toml
[features]
default   = ["duckdb-v1", "bundled"]
bundled   = ["duckdb-v1", "duckdb/bundled"]   # CLI: DuckDB compiled in
duckdb-v1 = ["dep:duckdb"]                    # backend: duckdb-rs, DuckDB 1.x C API
```

`duckdb` is now an optional dependency. Without a backend feature the crate stops with
`compile_error!("duckbridge needs a DuckDB backend: enable the duckdb-v1 cargo feature ...")`
(in `src/core/duck.rs`). The extension depends on the core with
`default-features = false, features = ["duckdb-v1"]` and keeps its own `duckdb` dependency
(`loadable-extension`, `vscalar`), which cargo unifies with the core's.

## Module API: `crate::core::duck` (`src/core/duck.rs`)

Types (aliases / re-exports today; step 2 decides per type whether to alias neo's or own them):

| name | today | used by |
|---|---|---|
| `Connection` | `type = duckdb::Connection` | everywhere, `core::conn::ConnProvider`, extension pool |
| `Statement<'c>`, `Rows<'s>`, `Row<'s>` | type aliases | row loops, helper closures (`&Row`) |
| `Value` | re-export | `mssql::import` (TDS -> appender values) |
| `ValueRef` | re-export | `core::script::literal`, `pg::copy_text`, `mssql::convert`, `mssql::bulk` row fallback |
| `TimeUnit`, `Decimal`, `ListType` | re-export | `script`, `convert`, `bulk`, `import`, `list_of_strings` |
| `ToSql`, `FromSql` | re-export | parameter binding, `query_scalar` |
| `arrow` | re-export of duckdb's arrow crate | `mssql::bulk` (columnar fast path), `convert` bench |
| `Appender<'c>` | **own wrapper** around `duckdb::Appender` | `mssql::import::copy_one` |
| `ArrowStream<'c>` | **own struct** (statement + schema) | `mssql::bulk::read_rows`, `convert` bench |

Functions (all return `anyhow::Result`; errors keep duckdb's text, so `e.to_string()` and
`{:#}` chains are unchanged):

| function | does | replaces |
|---|---|---|
| `open(path)` | open/create a database file | `Connection::open` (CLI `run`, `import`) |
| `open_readonly(path)` | `Config::access_mode(ReadOnly)` + `open_with_flags` | `catalog::open_readonly` internals |
| `open_in_memory()` | in-memory database | ~19 test/CLI call sites |
| `try_clone(con)` | second connection to the same db | `core::conn::acquire` |
| `exec(con, sql)` | `execute_batch` | ~40 call sites |
| `execute(con, sql) -> usize` | single statement, changed rows | `api::run_script` |
| `set_setting(con, name, value)` | `SET name=value`, value is SQL text verbatim | CLI `memory_limit`, `threads`, `preserve_insertion_order` |
| `query_scalar::<T>(con, sql)` | first column of first row | `query_row(sql, [], \|r\| r.get(0))` |
| `query_one(con, sql, f)` | `f` on the first row (no row: `QueryReturnedNoRows`) | `query_row` with tuple closures |
| `query_rows(con, sql, f) -> Vec<T>` | `f` on every row | `prepare` + `query_map([])` + collect |
| `query_rows_with(con, sql, &[&dyn ToSql], f)` | same with `?` parameters | catalog's `duckdb_*()` queries |
| `for_each_row(con, sql, f)` | visit rows without collecting | `bridge_run` / CLI `run` SELECTs |
| `list_of_strings(row, i)` | `VARCHAR[]` cell -> `Vec<String>` | was private in `catalog.rs` |
| `appender(con, schema, table)` | `appender_to_db(table, schema)`; `append_row(Vec<Value>)`, `flush()`, flush on drop | `import.rs` `append_values` (ToSqlOutput juggling moved here) |
| `stream(con, sql) -> ArrowStream` | prepare + `stream_arrow` (contexts `preparing <sql>` / `running <sql>`); `schema()`, `next_chunk() -> Option<StructArray>`, `rows()` (row fallback on the same running query) | `bulk::read_rows` |
| `arrow_batches(con, sql)` | `Iterator<Item = Result<RecordBatch>>` over `stream` | new, for later callers |

Closures given to the helpers take `&Row` and return `anyhow::Result<T>`, so they no longer
depend on `duckdb::Error`.

## Remaining direct `duckdb::` uses, and why

* `src/core/duck.rs` - by design, the only place in `src/` that names the crate.
* `extension/src/duckdb_v1.rs` - the duckdb-rs glue: `VTab`, `VScalar`, `BindInfo`,
  `DataChunkHandle`/`Inserter`/`WritableVector`, `LogicalTypeHandle`, `DuckString`, `ffi`,
  `Connection::open_from_raw`, `register_table_function` / `register_scalar_function`. Wrapping
  these in the core would be artificial: they are exactly what step 2 rewrites for neo.
* `extension/Cargo.toml` - its own `duckdb` dependency with `loadable-extension` + `vscalar`.

Nothing else: `grep -rn 'duckdb::\|use duckdb' src tests` hits only `src/core/duck.rs`.
`tests/transpile_tests.rs` and `src/main.rs` use `duckbridge::core::duck`. `mssql/convert.rs`
(which the brief allowed to keep) goes through `duck::{Decimal, TimeUnit, ValueRef}` too, since
plain re-exports did the job.

### Method-level surface still called on the re-exported types

The call sites below still use duckdb-rs methods directly on `Connection` / `Statement` / `Rows`
/ `Row` (through the aliases). They were left because the loop body awaits (async COPY /
INSERT batches), writes script output with `?` on io errors, or attaches its own error context
at prepare/query time; moving them into closures would have changed control flow or messages.
A second backend must offer these methods on its types, or step 2 moves these sites to helpers
(e.g. a `RowCursor` with `next()`):

| site | methods |
|---|---|
| `core/script.rs` `write_script` (INSERT batches) | `con.prepare`, `st.query([])`, `rows.next()`, `r.get_ref_unwrap(i)` |
| `pg/mod.rs` `copy_table` (COPY text) | same |
| `mssql/load.rs` `insert_table` | same |
| `verify.rs` `duck_rows` | `prepare` (with context), `query`, `next`, `r.as_ref().column_count()`, `r.get::<Option<String>>` |
| `mssql/target.rs` `query_strings`, `preflight` | `prepare`/`query` (with contexts), `next`, `r.get` |
| `main.rs` `run` | `r.as_ref().column_count()`, `r.get_ref_unwrap(i)` (Debug print) inside `for_each_row` |
| `mssql/bulk.rs` row fallback | `row.get_ref(c)` on `ArrowStream::rows()` |
| helper closures everywhere | `r.get::<T>(i)` for `String`, `Option<String>`, `i64`, `Option<i64>`, `bool`, `Option<bool>`; `list_of_strings` |

Value model the engine depends on:

* `ValueRef` variants matched in `core/script.rs::literal`, `pg::copy_text`, `mssql/convert.rs`
  and `bulk.rs`: Null, Boolean, TinyInt/SmallInt/Int/BigInt/HugeInt, UTinyInt/USmallInt/UInt/
  UBigInt/UHugeInt, Float, Double, Decimal, Text, Blob, Date32, Time64(TimeUnit, i64),
  Timestamp(TimeUnit, i64), Interval, List, Enum, Geometry; plus `Decimal::new(38, scale,
  mantissa)` in `convert.rs`.
* `Value` variants built in `mssql/import.rs::column_to_value` (Boolean, UTinyInt, SmallInt, Int,
  BigInt, Float, Double, Text, Blob, Date32, Time64, Timestamp, Null).
* Arrow arrays in `bulk.rs` (Int*/UInt*, Float*, Decimal32/64/128, Utf8/LargeUtf8/Utf8View,
  Binary/LargeBinary/BinaryView/FixedSizeBinary, Date32, Time64 us/ns, Timestamp s/ms/us/ns,
  IntervalMonthDayNano, Boolean, NullBuffer) from the `arrow` crate version duckdb-rs re-exports.

## Extension layout

* `extension/src/lib.rs` - backend-neutral: connection pool (`POOL`, `Pool: ConnProvider`,
  `install_pool(open)`, `Pooled`, `connect`), parameter tables (`IMPORT_PARAMS`,
  `EXPORT_PARAMS`, `RUN_PARAMS`, `RESULT_COLUMNS` with a neutral `Ty`), the `Params` trait
  (`positional`, `given`, `text`, `int`, `boolean`), request builders (`import_bind`,
  `export_bind`, `run_bind` -> `ImportRequest` / `ExportRequest` + `BakRequest` / `RunRequest`),
  operations (`import_run`, `export_run`, `run_run`), result paging (`Paged::next_page(run,
  write)`), scalar text (`transpile_text`, `prelude_text`).
* `extension/src/duckdb_v1.rs` - duckdb-rs glue: `Ty -> LogicalTypeHandle`, `Bind(&BindInfo):
  Params`, `write_rows` into the output chunk, `ImportVTab` / `ExportVTab` / `RunVTab`,
  `TranspileScalar` / `PreludeScalar`, `init_internal` + `#[no_mangle] duckbridge_init_c_api`,
  the `DUCKDB_VERSION` pin.

Function names, parameter names/types/order, defaults (incl. the `parallel` clamp to
`MAX_PARALLEL`, `compat_level`/`bak` "given even as NULL" checks) and error texts are unchanged.

## Deviations from the brief

* Besides the requested helpers there are `execute`, `try_clone`, `query_one`,
  `query_rows_with`, `for_each_row`, `list_of_strings` and `stream`/`ArrowStream`: the code
  needed them to drop every direct `duckdb::` import without changing behaviour.
* `bulk::read_rows` uses `duck::stream` rather than `arrow_batches`: it needs the schema before
  the first chunk and falls back to row-wise reads on the same running query when a column type
  has no fast path. Chunks stay `StructArray`s exactly as before. `arrow_batches` is there for
  new callers.
* `Appender` is a small wrapper, not a re-export, so `ToSqlOutput` stays inside `duck.rs`.
* `set_setting` takes the value as SQL text, put in verbatim (`'4GB'` with quotes, `8`), so the
  CLI sends the same `SET` statements as before.
* The extension split uses two files (`lib.rs` neutral, `duckdb_v1.rs` glue) rather than two
  sections of one file.

## Step 2: what the `duckdb-neo` backend must implement

1. **Cargo**: `duckdb-neo = ["dep:duckdb-neo"]` (plus a bundled variant if neo has one for the
   CLI). Exactly one backend: add `compile_error!` when both or neither are enabled. Make
   `duck.rs` a facade: `#[cfg(feature = "duckdb-v1")] mod v1;` and
   `#[cfg(feature = "duckdb-neo")] mod neo;`, then `pub use backend::*` (move today's body into
   `duck/v1.rs`). Whatever neo's API is, the facade must keep the names and signatures in the
   table above.
2. **Connection**: open file / read-only / in-memory, clone a connection to the same database
   (used by `core::conn::acquire` for worker threads), run a multi-statement batch (`exec`), run
   one statement and return changed rows (`execute`), prepare + bind `?` parameters (VARCHAR
   for the catalog queries).
3. **Statement / QueryResult / Row**: row cursor (`next`), typed `get::<T>` for the types listed
   above, `get_ref` / `get_ref_unwrap` returning the engine's `ValueRef`, column count. If neo's
   value types differ, the simplest route is to make `Value` / `ValueRef` / `TimeUnit` /
   `Decimal` / `ListType` DuckBridge-owned enums in `duck.rs` with the variants listed above, and
   convert neo's values into them, so `script.rs`, `pg`, `convert.rs`, `bulk.rs` and
   `import.rs` do not change. Alternatively give the remaining direct method sites (table above)
   helper equivalents first.
4. **Arrow streaming** (`stream` / `ArrowStream` / `arrow_batches`): schema before the first
   chunk, streaming chunks as Arrow arrays (`StructArray` today; switching to `RecordBatch`
   only needs `chunk.len()` -> `chunk.num_rows()` in `bulk.rs`), and row-wise reads of the same
   result. `duck::arrow` must be the arrow crate version neo produces (or chunks go through the
   Arrow C data interface). This is the hot path of every export: benchmark it
   (`cargo test --release --lib -- --ignored bench --nocapture`) against v1.
5. **Appender**: `appender(con, schema, table)`, `append_row(Vec<Value>)`, `flush()`, flush on
   drop. With neo either the row appender or an Arrow append (rows -> RecordBatch -> append).
   Hot path of `import`.
6. **Extension glue** (new `extension/src/duckdb_neo.rs`, selected by an extension feature that
   also picks the core's backend feature): table-function registration with one VARCHAR
   positional parameter plus the named parameters of `IMPORT_PARAMS` / `EXPORT_PARAMS` /
   `RUN_PARAMS`, bind (`Params` impl over neo's bind info: given / NULL / text / BIGINT /
   BOOLEAN), the four result columns, init, and `func` writing `Paged::next_page` pages (VARCHAR
   and BIGINT vectors, `set_len`); scalar-function registration for `bridge_transpile` (two
   overloads: 3 x VARCHAR, and 3 x VARCHAR + BIGINT -> VARCHAR, NULL in -> NULL out) and
   `bridge_prelude(BOOLEAN) -> VARCHAR` (NULL counts as true); the entry point with
   `#[duckdb_entrypoint_c_api_v2]`, which must open the connections for `install_pool` while
   loading (64 of them; DuckDB frees the database handle after load) and register the five
   functions. Then the Makefile / `pack.sh` metadata (DuckDB version pin and ABI type:
   `C_STRUCT_UNSTABLE` today; the stable C API should allow the plain `C_STRUCT` ABI).
7. **Tests**: run `cargo test --release`, `e2e_mssql.sh`, `e2e_extension.sh` and
   `e2e_large_load.sh` under both features; add a CI matrix entry for neo.
