//! duckdb-neo glue (feature `neo`, DuckDB 2.0 v2 C extension API).
//!
//! Mirrors `duckdb_v1.rs` with duckdb-neo's scalar / table function builders and the
//! `#[duckdb_entrypoint_c_api_v2]` entry point. The work is done by the backend-neutral
//! functions in `lib.rs`.
//!
//! What works: `bridge_transpile` (both overloads) and `bridge_prelude`, which need no database.
//!
//! What cannot work yet: `bridge_import`, `bridge_export` and `bridge_run` need connections to the
//! database that loaded the extension (the v1 glue opens a pool of 64 from the
//! `duckdb_extension_access::get_database` handle). The v2 extension API hands the entry point an
//! extension handle, a borrowed context and an error slot only (`duckdb_v2_extension_input`), and
//! neither the context nor the function callbacks expose the instance, a connection, or a way to
//! run SQL; duckdb-neo's `Extension` / `Context` wrap exactly that (see docs/handoff-neo.md for the
//! file/line references). The three table functions are registered anyway, accepting the same
//! call shapes, so that calling them fails at bind time with [`NO_CONNECTION`] instead of
//! "function does not exist".

use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::connection::Context;
use duckdb_neo::data_chunk::{DataChunkRef, VectorCollection};
use duckdb_neo::extension::Extension;
use duckdb_neo::scalar::{ScalarCallbacks, ScalarFunctionBuilder};
use duckdb_neo::signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder};
use duckdb_neo::table_function::{BindFunctionHandle, ExecColumnInfo, TableFunctionBuilder, TableFunctionCallbacks};
use duckdb_neo::types::Any;
use duckdb_neo::vector::{Unknown, Vector};
use duckdb_neo::bind_arguments::BindArgument;
use duckdb_neo::{DuckDBType, Error, Result};

use crate::{prelude_text, transpile_text, Ty};

/// Why the table functions fail on DuckDB 2.0 (see the module docs).
const NO_CONNECTION: &str = "is not available on DuckDB 2.0 yet: the v2 C extension API gives an extension no connection to \
the database that loaded it (duckdb_v2_extension_input carries an extension handle, a borrowed context and an error slot only), \
so DuckBridge cannot read or write tables from inside DuckDB 2.0; use the duckbridge CLI (built with the duckdb-neo backend) instead";

fn logical(ext: &Extension, t: Ty) -> Result<duckdb_neo::logical_type::LogicalType> {
    match t { Ty::Varchar => String::logical_type(ext), Ty::Bigint => i64::logical_type(ext), Ty::Boolean => bool::logical_type(ext) }
}

fn err(e: impl std::fmt::Display) -> Error { Error::invalid_input(e.to_string()) }

// ------------------------------------------------------------------ bridge_import / bridge_export / bridge_run

/// `bridge_import` / `bridge_export` / `bridge_run`: bind fails with [`NO_CONNECTION`].
struct Unavailable(&'static str);

impl TableFunctionCallbacks for Unavailable {
    type BindData = ();
    type LocalState = ();
    type GlobalState = ();

    fn bind(&self, _context: &Context, _arguments: Vec<BindArgument>, _bind: BindFunctionHandle<'_>) -> Result<()> {
        Err(Error::invalid_input(format!("{} {NO_CONNECTION}", self.0)))
    }

    fn exec(&self, _: &(), _: Option<&()>, _: Option<&mut ()>, _: &Context, _output: DataChunkRef<'_>, _: ExecColumnInfo<'_>) -> Result<()> {
        Ok(()) // never reached: bind fails
    }
}

fn register_unavailable(ext: &Extension, name: &'static str) -> Result<()> {
    // one VARCHAR plus any named parameters: the v1 call shapes all reach bind
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("target"))
        .param(FunctionParameter::new::<Any>("options").kind(FunctionParameterKind::NamedVariadic));
    TableFunctionBuilder::new(name, sig, Unavailable(name)).register(ext)
}

// ------------------------------------------------------------------ bridge_transpile

/// `bridge_transpile(from, to, sql [, target_version])`.
struct TranspileScalar;

fn strings(v: &Vector<'_, String>, n: usize) -> Result<Vec<Option<String>>> {
    (0..n).map(|i| v.get(i).map(|s| s.map(str::to_owned))).collect()
}

impl ScalarCallbacks for TranspileScalar {
    type BindData = ();
    type InitData = ();

    fn exec(&self, _: Option<&()>, _: Option<&mut ()>, _: &Context, input: &mut VectorCollection, output: Vector<'_, Unknown>) -> Result<()> {
        let n = input.row_count()?;
        let from = strings(input.get_vector_at::<String>(0)?, n)?;
        let to = strings(input.get_vector_at::<String>(1)?, n)?;
        let sql = strings(input.get_vector_at::<String>(2)?, n)?;
        let version: Vec<Option<i64>> = if input.col_count() > 3 {
            let v = input.get_vector_at::<i64>(3)?;
            (0..n).map(|i| v.get(i).map(|x| x.copied())).collect::<Result<_>>()?
        } else { vec![None; n] };
        let mut out = output.cast::<String>()?;
        out.set_size(n)?;
        for i in 0..n {
            let (Some(f), Some(t), Some(s)) = (&from[i], &to[i], &sql[i]) else { out.write(i, None)?; continue; };
            let text = transpile_text(f, t, s, version[i]).map_err(err)?;
            out.write(i, Some(text.as_str()))?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ bridge_prelude

/// `bridge_prelude(nocase)`: the SET statements that make a DuckDB session behave like SQL Server.
struct PreludeScalar;

impl ScalarCallbacks for PreludeScalar {
    type BindData = ();
    type InitData = ();

    fn exec(&self, _: Option<&()>, _: Option<&mut ()>, _: &Context, input: &mut VectorCollection, output: Vector<'_, Unknown>) -> Result<()> {
        let n = input.row_count()?;
        let v = input.get_vector_at::<bool>(0)?;
        let mut out = output.cast::<String>()?;
        out.set_size(n)?;
        for i in 0..n {
            let nocase = v.get(i)?.copied().unwrap_or(true);
            out.write(i, Some(prelude_text(nocase).as_str()))?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ entrypoint

#[duckdb_entrypoint_c_api_v2(extension_name = "duckbridge", api_version = "v2.0.0")]
pub fn entrypoint(ext: &Extension) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let varchar = || logical(ext, Ty::Varchar);
    ScalarFunctionBuilder::new(
        "bridge_transpile",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("from"))
            .param(FunctionParameter::new::<String>("to"))
            .param(FunctionParameter::new::<String>("sql"))
            .return_type(varchar()?),
        TranspileScalar,
    )
    .register(ext)?;
    ScalarFunctionBuilder::new(
        "bridge_transpile",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("from"))
            .param(FunctionParameter::new::<String>("to"))
            .param(FunctionParameter::new::<String>("sql"))
            .param(FunctionParameter::with_type("target_version", logical(ext, Ty::Bigint)?))
            .return_type(varchar()?),
        TranspileScalar,
    )
    .register(ext)?;
    ScalarFunctionBuilder::new(
        "bridge_prelude",
        SignatureBuilder::new().param(FunctionParameter::with_type("nocase", logical(ext, Ty::Boolean)?)).return_type(varchar()?),
        PreludeScalar,
    )
    .register(ext)?;
    for name in ["bridge_import", "bridge_export", "bridge_run"] { register_unavailable(ext, name)?; }
    Ok(())
}
