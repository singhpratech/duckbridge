//! The engine's whole DuckDB surface.
//!
//! Every other module of the library reaches DuckDB through this module only: types are
//! re-exported from here and the operations the code repeats (run a batch, read a scalar, read
//! rows, open a database, append rows, stream Arrow chunks) are helper functions here. Nothing
//! outside this module names a DuckDB crate. See docs/handoff-duck-layer.md and docs/handoff-neo.md.
//!
//! Two backends, exactly one per build:
//! * `duckdb-v1` (root Cargo.toml, default) = duckdb-rs (`duckdb` crate, DuckDB 1.x C API): `v1.rs`.
//! * `duckdb-neo` (neo/Cargo.toml) = duckdb-neo (DuckDB 2.0, v2 C API): `neo.rs`. It provides the
//!   same names and signatures; rows are read through the Arrow C data interface, values are
//!   DuckBridge-owned types with the same variants as duckdb-rs'.
//!
//! Besides the helpers, the code calls a small set of methods directly on the types
//! (`Connection::prepare`, `Statement::query`, `Rows::next`, `Row::get`/`get_ref`, ...);
//! docs/handoff-duck-layer.md lists them, both backends provide them.

#[cfg(all(feature = "duckdb-v1", feature = "duckdb-neo"))]
compile_error!("duckbridge: enable exactly one DuckDB backend, `duckdb-v1` (root Cargo.toml) or `duckdb-neo` (neo/Cargo.toml), not both");
#[cfg(not(any(feature = "duckdb-v1", feature = "duckdb-neo")))]
compile_error!("duckbridge needs a DuckDB backend: enable the `duckdb-v1` cargo feature (on by default)");

#[cfg(feature = "duckdb-v1")]
mod v1;
#[cfg(feature = "duckdb-v1")]
pub use v1::*;

#[cfg(all(feature = "duckdb-neo", not(feature = "duckdb-v1")))]
mod neo;
#[cfg(all(feature = "duckdb-neo", not(feature = "duckdb-v1")))]
pub use neo::*;
