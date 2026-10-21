//! DuckBridge as a loadable DuckDB extension.
//!
//! ```sql
//! LOAD 'duckbridge.duckdb_extension';
//! -- SQL Server -> this DuckDB database
//! SELECT * FROM bridge_import('mssql://sa:pw@sql01:1433/Sales', parallel := 8);
//! -- run the team's T-SQL against DuckDB (SQL Server compatibility prelude applied)
//! SELECT * FROM bridge_run('UPDATE dbo.orders SET total = qty * price WHERE ...');
//! -- ship the result back: live load + restorable .bak
//! SELECT * FROM bridge_export('mssql://sa:pw@sql01:1433/Sales_new', bak := '/backups/Sales_new.bak', fetch := 'docker:mssql');
//! -- translate text
//! SELECT bridge_transpile('tsql', 'duckdb', 'SELECT TOP 5 GETDATE(), ISNULL(a, 0) FROM t');
//! ```
//!
//! The extension talks to the database that loaded it through a fresh connection per call,
//! so nothing here depends on the caller's transaction.
//!
//! Layout (prepared for a second DuckDB backend, see docs/handoff-duck-layer.md):
//! * this file: the backend-neutral part. It only calls the core library (`api::*`,
//!   `transpile::*`, `core::conn`, `core::duck`) and sees the host's function-call parameters
//!   through the [`Params`] trait: connection pool, function signatures, parameter parsing into
//!   `ImportRequest` / `ExportRequest` / `RunRequest` / `BakRequest`, the operations themselves,
//!   result paging and the text of the scalar functions.
//! * `duckdb_v1.rs`: the duckdb-rs glue (DuckDB 1.x C API): `VTab` / `VScalar` impls, vector
//!   reads/writes, the `Params` impl over `BindInfo`, and the `duckbridge_init_c_api` entry point.
//! * `duckdb_neo.rs` (feature `neo`, built from extension/neo/Cargo.toml): the duckdb-neo glue
//!   (DuckDB 2.0 v2 C API). Only the scalar functions work there so far; see that file and
//!   docs/handoff-neo.md for why the table functions cannot get a connection yet.

// With the `neo` glue only the scalar functions are wired up (no connection on DuckDB 2.0 yet),
// so the pool and the table-function plumbing below are unused there.
#![cfg_attr(feature = "neo", allow(dead_code))]

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use duckbridge::api::{self, Row};
use duckbridge::core::duck::{self, Connection};
use duckbridge::transpile::{self, Dialect};

#[cfg(not(feature = "neo"))]
mod duckdb_v1;
#[cfg(feature = "neo")]
mod duckdb_neo;

/// Rows per output chunk (DuckDB's standard vector size).
const VECTOR_SIZE: usize = 2048;

// ------------------------------------------------------------------ connection pool

/// Connections to the database that loaded us.
///
/// DuckDB frees the raw database handle it lends to the entry point as soon as loading completes,
/// so connections cannot be created later. The entry point opens a pool of them up front; every
/// call borrows one (and worker threads borrow more through the core's connection provider), so
/// nothing here touches the caller's own transaction.
const POOL_SIZE: usize = 64;
/// bulk_load uses a second pooled connection per worker, so parallel is capped at half the pool.
const MAX_PARALLEL: usize = POOL_SIZE / 2;
static POOL: Mutex<Vec<Connection>> = Mutex::new(Vec::new());

struct Pool;
impl duckbridge::core::conn::ConnProvider for Pool {
    fn acquire(&self) -> Result<Connection> {
        POOL.lock().map_err(|_| anyhow!("duckbridge: connection pool poisoned"))?.pop()
            .ok_or_else(|| anyhow!("duckbridge: all {POOL_SIZE} pooled DuckDB connections are in use; lower parallel :="))
    }
    fn release(&self, con: Connection) { if let Ok(mut p) = POOL.lock() { p.push(con); } }
}

/// Fill the pool with `POOL_SIZE` connections from `open` and install it as the core's
/// connection provider (called once by the entry point).
fn install_pool(mut open: impl FnMut() -> std::result::Result<Connection, Box<dyn Error>>) -> std::result::Result<(), Box<dyn Error>> {
    {
        let mut pool = POOL.lock().map_err(|_| "duckbridge: connection pool poisoned")?;
        for _ in 0..POOL_SIZE { pool.push(open()?); }
    }
    duckbridge::core::conn::set_provider(Box::new(Pool));
    Ok(())
}

/// A pooled connection that goes back to the pool on drop.
struct Pooled(Option<Connection>);
impl Drop for Pooled { fn drop(&mut self) { if let Some(c) = self.0.take() { duckbridge::core::conn::ConnProvider::release(&Pool, c); } } }
impl std::ops::Deref for Pooled { type Target = Connection; fn deref(&self) -> &Connection { self.0.as_ref().unwrap() } }

fn connect() -> Result<Pooled> { Ok(Pooled(Some(duckbridge::core::conn::ConnProvider::acquire(&Pool)?))) }

// ------------------------------------------------------------------ parameters

/// Logical type of a function parameter or result column.
#[derive(Debug, Clone, Copy)]
enum Ty { Varchar, Bigint, Boolean }

/// The parameters of one table-function call, as the backend's bind info exposes them.
trait Params {
    /// Positional parameter `i` as text.
    fn positional(&self, i: usize) -> String;
    /// Whether the named parameter was given at all (even as NULL).
    fn given(&self, name: &str) -> bool;
    /// Named parameter as text; `None` when absent or NULL.
    fn text(&self, name: &str) -> Option<String>;
    /// Named parameter as BIGINT; `None` when absent or NULL.
    fn int(&self, name: &str) -> Option<i64>;
    /// Named parameter as BOOLEAN; `None` when absent or NULL.
    fn boolean(&self, name: &str) -> Option<bool>;
}

fn s_param(p: &dyn Params, name: &str) -> Option<String> { p.text(name) }
fn i_param(p: &dyn Params, name: &str, default: i64) -> i64 { p.int(name).unwrap_or(default) }
fn b_param(p: &dyn Params, name: &str, default: bool) -> bool { p.boolean(name).unwrap_or(default) }

/// Result columns of the three table functions.
const RESULT_COLUMNS: &[(&str, Ty)] = &[("object", Ty::Varchar), ("kind", Ty::Varchar), ("rows", Ty::Bigint), ("note", Ty::Varchar)];

// ------------------------------------------------------------------ result paging

/// Shared init state: the operation runs once on the first call, its rows are then
/// paged out VECTOR_SIZE at a time.
pub struct Paged {
    rows: Mutex<Option<Vec<Row>>>,
    cursor: AtomicUsize,
}

impl Paged {
    fn new() -> Paged { Paged { rows: Mutex::new(None), cursor: AtomicUsize::new(0) } }

    /// Run the operation on the first call, then give `write` the next page (at most VECTOR_SIZE
    /// rows; empty once everything was handed out).
    fn next_page(&self, run: impl FnOnce() -> Result<Vec<Row>>, write: impl FnOnce(&[Row])) -> std::result::Result<(), Box<dyn Error>> {
        let mut guard = self.rows.lock().map_err(|_| "duckbridge: state poisoned")?;
        if guard.is_none() { *guard = Some(run()?); }
        let rows = guard.as_ref().unwrap();
        let start = self.cursor.load(Ordering::Relaxed).min(rows.len());
        let end = (start + VECTOR_SIZE).min(rows.len());
        write(&rows[start..end]);
        self.cursor.store(end, Ordering::Relaxed);
        Ok(())
    }
}

// ------------------------------------------------------------------ bridge_import

const IMPORT_PARAMS: &[(&str, Ty)] = &[
    ("tables", Ty::Varchar), ("exclude", Ty::Varchar), ("parallel", Ty::Bigint), ("split_threshold", Ty::Bigint),
    ("batch_rows", Ty::Bigint), ("schema_map", Ty::Varchar), ("nolock", Ty::Boolean), ("schema_only", Ty::Boolean),
    ("keys_after_load", Ty::Boolean), ("overwrite", Ty::Boolean),
    ("isolation", Ty::Varchar), ("lock_timeout", Ty::Bigint), ("lock_check", Ty::Boolean), ("lock_wait", Ty::Bigint), ("on_lock", Ty::Varchar),
    ("max_rereads", Ty::Bigint), ("manifest", Ty::Varchar), ("readonly", Ty::Boolean), ("wait_for", Ty::Varchar), ("wait_timeout", Ty::Bigint),
    ("log_file", Ty::Varchar), ("summary_file", Ty::Varchar),
];

pub struct ImportBind { dsn: String, req: api::ImportRequest }

fn import_bind(p: &dyn Params) -> ImportBind {
    let d = api::ImportRequest::default();
    let req = api::ImportRequest {
        tables: s_param(p, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
        exclude: s_param(p, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
        parallel: i_param(p, "parallel", 0).max(0) as usize,
        split_threshold: i_param(p, "split_threshold", d.split_threshold),
        batch_rows: i_param(p, "batch_rows", d.batch_rows as i64).max(1) as usize,
        schema_map: s_param(p, "schema_map").unwrap_or_default(),
        nolock: b_param(p, "nolock", false),
        schema_only: b_param(p, "schema_only", false),
        keys_after_load: b_param(p, "keys_after_load", false),
        overwrite: b_param(p, "overwrite", false),
        isolation: s_param(p, "isolation").unwrap_or(d.isolation),
        lock_timeout_ms: i_param(p, "lock_timeout", d.lock_timeout_ms),
        lock_check: b_param(p, "lock_check", true),
        lock_wait_s: i_param(p, "lock_wait", d.lock_wait_s as i64).max(0) as u64,
        on_lock: s_param(p, "on_lock").unwrap_or(d.on_lock),
        max_rereads: i_param(p, "max_rereads", d.max_rereads as i64).max(0) as u32,
        manifest: s_param(p, "manifest"),
        readonly: b_param(p, "readonly", false),
        wait_for: s_param(p, "wait_for"),
        wait_timeout_s: i_param(p, "wait_timeout", d.wait_timeout_s as i64).max(0) as u64,
        log_file: s_param(p, "log_file"),
        summary_file: s_param(p, "summary_file"),
    };
    ImportBind { dsn: p.positional(0), req }
}

fn import_run(b: &ImportBind) -> Result<Vec<Row>> {
    let dsn = api::parse_dsn(&b.dsn)?;
    let con = connect()?;
    api::import_mssql(&con, &dsn, &b.req)
}

// ------------------------------------------------------------------ bridge_export

const EXPORT_PARAMS: &[(&str, Ty)] = &[
    ("database", Ty::Varchar), ("tables", Ty::Varchar), ("exclude", Ty::Varchar), ("schema_map", Ty::Varchar),
    ("parallel", Ty::Bigint), ("drop_existing", Ty::Boolean), ("schema_only", Ty::Boolean), ("target_version", Ty::Bigint),
    ("string_mode", Ty::Varchar), ("method", Ty::Varchar), ("pk_inline", Ty::Boolean), ("order_by_key", Ty::Boolean),
    ("split_threshold", Ty::Bigint), ("batch_rows", Ty::Bigint), ("create_database", Ty::Boolean),
    ("disposable_db_pattern", Ty::Varchar), ("compat_level", Ty::Bigint), ("recovery", Ty::Varchar),
    ("plan", Ty::Boolean), ("log_file", Ty::Varchar), ("summary_file", Ty::Varchar), ("string_type", Ty::Varchar), ("string_sizes", Ty::Varchar),
    ("into_existing", Ty::Boolean), ("ignore_extra_columns", Ty::Boolean), ("allow_rescale", Ty::Boolean), ("check_only", Ty::Boolean), ("check_pk", Ty::Varchar),
    ("split", Ty::Varchar), ("strategy", Ty::Varchar), ("max_jobs_per_table", Ty::Bigint), ("state_dir", Ty::Varchar), ("replace_partitions", Ty::Boolean), ("drop_old", Ty::Boolean),
    ("verify", Ty::Varchar), ("sample", Ty::Bigint), ("source_parquet", Ty::Varchar),
    ("parquet_compression", Ty::Varchar),
    ("bak", Ty::Varchar), ("backup_dir", Ty::Varchar), ("fetch_via", Ty::Varchar), ("stripes", Ty::Bigint),
    ("compression", Ty::Boolean), ("verify_restore", Ty::Boolean), ("drop_staging", Ty::Boolean), ("backup_to_url", Ty::Varchar),
];

pub struct ExportBind { target: String, req: api::ExportRequest, bak: Option<api::BakRequest> }

fn export_bind(p: &dyn Params) -> ExportBind {
    let d = api::ExportRequest::default();
    let req = api::ExportRequest {
        database: s_param(p, "database"),
        tables: s_param(p, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
        exclude: s_param(p, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
        schema_map: s_param(p, "schema_map").unwrap_or_default(),
        parallel: (i_param(p, "parallel", 0).max(0) as usize).clamp(0, MAX_PARALLEL).max(if i_param(p, "parallel", 0) == 0 { 0 } else { 1 }),
        drop_existing: b_param(p, "drop_existing", false),
        schema_only: b_param(p, "schema_only", false),
        target_version: i_param(p, "target_version", 2019) as u16,
        string_mode: s_param(p, "string_mode").unwrap_or(d.string_mode),
        string_type: s_param(p, "string_type").unwrap_or(d.string_type),
        string_sizes: s_param(p, "string_sizes").unwrap_or(d.string_sizes),
        method: s_param(p, "method").unwrap_or(d.method),
        pk_inline: b_param(p, "pk_inline", false),
        order_by_key: b_param(p, "order_by_key", false),
        split_threshold: i_param(p, "split_threshold", d.split_threshold),
        batch_rows: i_param(p, "batch_rows", d.batch_rows as i64).max(1) as usize,
        create_database: b_param(p, "create_database", false),
        parquet_compression: s_param(p, "parquet_compression").unwrap_or(d.parquet_compression),
        source_label: "duckdb".into(),
        disposable_db_pattern: s_param(p, "disposable_db_pattern"),
        compat_level: p.given("compat_level").then(|| i_param(p, "compat_level", 0) as u16),
        recovery: s_param(p, "recovery"),
        plan: b_param(p, "plan", false),
        log_file: s_param(p, "log_file"),
        summary_file: s_param(p, "summary_file"),
        progress_seconds: d.progress_seconds,
        into_existing: b_param(p, "into_existing", false),
        ignore_extra_columns: b_param(p, "ignore_extra_columns", false),
        allow_rescale: b_param(p, "allow_rescale", false),
        check_only: b_param(p, "check_only", false),
        check_pk: s_param(p, "check_pk").unwrap_or_default(),
        splits: s_param(p, "split").unwrap_or_default(),
        strategies: s_param(p, "strategy").unwrap_or_default(),
        max_jobs_per_table: i_param(p, "max_jobs_per_table", 0).max(0) as usize,
        state_dir: s_param(p, "state_dir"),
        replace_partitions: b_param(p, "replace_partitions", false),
        drop_old: b_param(p, "drop_old", false),
        verify: s_param(p, "verify").unwrap_or(d.verify),
        sample: i_param(p, "sample", d.sample as i64).max(0) as usize,
        source_parquet: s_param(p, "source_parquet").unwrap_or_default(),
    };
    let bd = api::BakRequest::default();
    let wants_bak = p.given("bak") || b_param(p, "verify_restore", false) || s_param(p, "backup_to_url").is_some();
    let bak = wants_bak.then(|| api::BakRequest {
        out: s_param(p, "bak").unwrap_or_default(),
        backup_dir: s_param(p, "backup_dir").unwrap_or(bd.backup_dir),
        fetch: s_param(p, "fetch_via").unwrap_or(bd.fetch),
        stripes: i_param(p, "stripes", 1).max(1) as usize,
        compression: b_param(p, "compression", true),
        verify_restore: b_param(p, "verify_restore", false),
        drop_staging: b_param(p, "drop_staging", false),
        to_url: s_param(p, "backup_to_url"),
    });
    ExportBind { target: p.positional(0), req, bak }
}

fn export_run(b: &ExportBind) -> Result<Vec<Row>> {
    let con = connect()?;
    let t = b.target.trim();
    if let Some(p) = t.strip_prefix("script:") { return api::export_script(&con, p, &b.req); }
    if let Some(p) = t.strip_prefix("parquet:") { return api::export_parquet(&con, p, &b.req); }
    if t.starts_with("postgres://") || t.starts_with("postgresql://") || t.contains("host=") {
        return api::export_postgres(&con, t, &b.req);
    }
    let dsn = api::parse_dsn(t)?;
    match dsn.scheme.as_str() {
        "mssql" | "sqlserver" => api::export_mssql(&con, &dsn, &b.req, b.bak.as_ref()),
        other => Err(anyhow!("unsupported export target {other}://  (use mssql://, postgres://, parquet:<dir>, script:<file>)")),
    }
}

// ------------------------------------------------------------------ bridge_run

const RUN_PARAMS: &[(&str, Ty)] = &[
    ("from", Ty::Varchar), ("nocase", Ty::Boolean), ("nocase_mode", Ty::Varchar), ("strict", Ty::Boolean), ("mssql_math", Ty::Boolean),
    ("types", Ty::Varchar), ("schema_map", Ty::Varchar), ("continue_on_error", Ty::Boolean),
];

pub struct RunBind { sql: String, req: api::RunRequest }

fn run_bind(p: &dyn Params) -> std::result::Result<RunBind, Box<dyn Error>> {
    let from_s = s_param(p, "from").unwrap_or_else(|| "tsql".into());
    let from = Dialect::parse(&from_s).ok_or_else(|| format!("unknown dialect {from_s:?} (tsql, duckdb, postgres)"))?;
    // nocase := true/false keeps working (rewrite / off); nocase_mode := 'global' | 'rewrite' | 'off' is explicit
    let nocase = s_param(p, "nocase_mode").unwrap_or_else(|| if b_param(p, "nocase", true) { "rewrite".into() } else { "off".into() });
    Ok(RunBind {
        sql: p.positional(0),
        req: api::RunRequest {
            from,
            nocase,
            strict: b_param(p, "strict", false),
            mssql_math: b_param(p, "mssql_math", false),
            types: s_param(p, "types").unwrap_or_default(),
            schema_map: s_param(p, "schema_map").unwrap_or_default(),
            continue_on_error: b_param(p, "continue_on_error", false),
        },
    })
}

fn run_run(b: &RunBind) -> Result<Vec<Row>> {
    let con = connect()?;
    let r = api::run_script(&con, &b.sql, &b.req);
    let _ = duck::exec(&con, "RESET integer_division; RESET default_null_order; RESET TimeZone; RESET default_collation;");
    r
}

// ------------------------------------------------------------------ bridge_transpile / bridge_prelude

/// `bridge_transpile(from, to, sql [, target_version])` for one row.
fn transpile_text(from: &str, to: &str, sql: &str, version: Option<i64>) -> std::result::Result<String, Box<dyn Error>> {
    let src = Dialect::parse(from).ok_or_else(|| format!("unknown source dialect {from:?}"))?;
    let dst = Dialect::parse(to).ok_or_else(|| format!("unknown target dialect {to:?}"))?;
    let default: &[(&str, &str)] = match (src, dst) { (Dialect::Tsql, _) => &[("dbo", "main")], (_, Dialect::Tsql) => &[("main", "dbo")], _ => &[("main", "public")] };
    let sm = api::parse_schema_map("", default);
    let opts = transpile::Options { target_version: version.unwrap_or(2019) as u16, prelude: false, float_division: true, ..transpile::Options::default() };
    let tr = transpile::translate_with(sql, src, dst, &sm, &opts)?;
    Ok(if tr.warnings.is_empty() { tr.sql } else {
        let mut w: String = tr.warnings.iter().map(|w| format!("-- WARNING: {w}\n")).collect();
        w.push_str(&tr.sql);
        w
    })
}

/// `bridge_prelude(nocase)`: the SET statements that make a DuckDB session behave like SQL Server.
fn prelude_text(nocase: bool) -> String {
    transpile::duckdb_prelude_for(if nocase { transpile::NocaseMode::Rewrite } else { transpile::NocaseMode::Off }).iter().map(|p| p.trim().trim_end_matches(';').to_string() + ";").collect::<Vec<_>>().join("\n")
}
