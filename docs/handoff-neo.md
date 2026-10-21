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

| | duckdb-v1 (default) | duckdb-neo (DuckDB v2.0.0-alpha49003) |
|---|---|---|
| `cargo test --release` (lib + translator) | 94 + 81 pass | 94 + 81 pass |
| `scripts/e2e_mssql.sh` | 45/45 | 45/45 |
| `scripts/e2e_large_load.sh` | 118/118 | 118/118 |
| extension | `make release` + `e2e_extension.sh` 33/33 | `make release-neo` + `e2e_extension_neo.py`: 13 pass, 3 blocked (below) |

Blocked = `bridge_run` creating a table, `bridge_import` from SQL Server, `bridge_export(...,
create_database := true, database := '_neo_out')`: the v2 extension API gives no connection (see
"The extension on neo"). They fail with an explanatory error and touch nothing on SQL Server.

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
  ("Deprecated lambda arrow (->) detected"), which `catalog.rs`' ENUM expansion and users' scripts
  use. Every neo instance gets `lambda_syntax = 'ENABLE_SINGLE_ARROW'` (`COMPAT_SETTINGS` in
  `neo.rs`), so both backends send the same SQL. Nothing else in the 45 + 118 e2e checks needed a
  change.
* `mssql/guard.rs`' source-scan test now also finds `../src` when built from `neo/`.

## The extension on neo (`extension/src/duckdb_neo.rs`)

Works: `LOAD` (unsigned, footer `v2.0.0-alpha49003` / `C_STRUCT_UNSTABLE` / platform, entry point
`duckbridge_init_c_api_v2` from `#[duckdb_entrypoint_c_api_v2(extension_name = "duckbridge",
api_version = "v2.0.0")]`), `bridge_transpile` (both overloads, registered as two scalar functions
of the same name, NULL in -> NULL out), `bridge_prelude(BOOLEAN)`.

**Blocked: `bridge_import`, `bridge_export`, `bridge_run`.** They need connections to the database
that loaded the extension. The v1 glue opens a pool of 64 from `duckdb_extension_access::
get_database` while loading. The v2 extension API has no equivalent:

* `duckdb_v2_extension_input` (DuckDB `src/include/duckdb_v2.h:2619`, bindings
  `crates/libduckdb-sys/src/bindgen_bundled_version_loadable_v2.rs:471`) carries `get_api`, an
  extension handle, a borrowed context ("valid only until the extension entrypoint returns") and
  the error slot. No database / instance handle.
* duckdb-neo wraps exactly that: `crates/duckdb-neo/src/extension.rs:22` (`Extension { handle,
  context }`), `:41-61` (`run_extension_entrypoint` builds it from `input.extension` /
  `input.context`).
* Connections are created only from an instance: `crates/duckdb-neo/src/connection.rs:196`
  (`Connection::new(instance: &Instance)`, crate-private) via `duckdb_v2_connection_create(instance, ...)`,
  reached through `Instance::connect` (`crates/duckdb-neo/src/instance.rs:296`).
* `Context` (`crates/duckdb-neo/src/connection.rs:318-341`), which bind/init/exec callbacks get, has
  `config()` and the factory only; the whole v2 API has no function that takes a context or an
  extension handle and returns an instance or a connection, or runs SQL (the C functions taking a
  context are `context_get_config`, `context_get_factory`, `context_get_file_system`,
  `context_log`, `arrow_importer_create`, `arrow_exporter_create`), although the header calls the
  entrypoint's context one "to read and run queries through".

So the three table functions are registered with the v1 call shapes (one VARCHAR, any named
parameters via `NamedVariadic` ANY) and fail at bind with "bridge_import is not available on
DuckDB 2.0 yet: the v2 C extension API gives an extension no connection to the database that
loaded it ...". `lib.rs`' pool / parameter / paging code is kept (dead code under `neo`, allowed)
for when the API grows a way in. A second, smaller gap for that day: `TableFunctionCallbacks::bind`
(`crates/duckdb-neo/src/table_function/mod.rs:743`) receives `Vec<BindArgument>` (type + value,
`bind_arguments.rs:19`), not the names; `BindArguments::name` / `index` exist but the callback
only gets the collected vector (`table_function/mod.rs:94`), so named-parameter lookup (and the
"given even as NULL" checks of `compat_level` / `bak`) would need duckdb-neo to pass
`BindArguments` through.

What DuckDB / duckdb-neo would need to provide (either is enough):
`duckdb_v2_context_get_instance` (or `duckdb_v2_extension_get_instance`) returning a retainable
instance handle, exposed as e.g. `Extension::instance()` / `Context::connect()`; or a context
query API usable from table-function callbacks. A different design that works today without a
connection: `bridge_import` as a table function that *produces* one SQL Server table's rows
(`CREATE TABLE t AS FROM bridge_read('mssql://...', 'dbo.orders')`) and `bridge_export` as a
COPY function (`COPY t TO 'mssql://...' (FORMAT bridge)`, duckdb-neo `copy_function`), both
without catalog access, i.e. a different user-facing API.

`scripts/e2e_extension.sh` already takes `DUCK` and `EXT` from the environment, but needs a
DuckDB CLI and the table functions; `scripts/e2e_extension_neo.py` runs the checks DuckDB 2.0
allows (footer, LOAD, transpile, prelude, function list) and asserts that `bridge_run` creating a
table, `bridge_import` from `mssql://sa:...@localhost:14319/d2m_extsrc` and
`bridge_export(... create_database := true, database := '_neo_out')` fail with that explanation
without touching SQL Server (`_neo_out` is checked absent and dropped with sqlcmd if present).

## Makefile / CI

* `extension/Makefile`: `make release-neo` builds `extension/neo` and packs
  `build/neo/duckbridge.duckdb_extension` with `NEO_DUCKDB_VERSION ?= v2.0.0-alpha49003` and
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

* Extension table functions blocked on the v2 API (above).
* The `lambda_syntax` setting is marked "revert to the deprecated behavior ... before DuckDB's next
  release". When it goes away, switch `catalog.rs`' `x -> ...` to `lambda x: ...` (accepted by
  DuckDB >= 1.3, so by v1 too) and drop `COMPAT_SETTINGS`; users' scripts with arrows will then
  fail on 2.0 regardless.
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
   gives extensions an instance/connection, replace `Unavailable` with the real table functions
   (mirror `duckdb_v1.rs`: `install_pool` with 64 connections, `Params` over named bind arguments,
   `RESULT_COLUMNS`, `Paged::next_page`) and drop the `allow(dead_code)` in `lib.rs`.
5. `src/core/duck/neo.rs`: revisit `COMPAT_SETTINGS` (lambda syntax), the Arrow batch sizes,
   and whether duckdb-neo grew a table appender (replace the collection + INSERT path).
6. `neo/Cargo.toml` `bundled-neo`: if the release has an extension repository, the cc `bundled`
   build (no cmake, no submodule) plus autoloaded ICU is an option again.
7. `scripts/duckdb_py_cli.py` / `e2e_extension_neo.py` become unnecessary once a 2.0 CLI binary
   exists: run `scripts/e2e_mssql.sh`, `e2e_large_load.sh`, `e2e_extension.sh` with `DUCK=` that CLI.
