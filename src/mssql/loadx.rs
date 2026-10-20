//! `--into-existing` loads: fit DuckDB data into tables that already exist with production DDL.
//!
//! Sequence (spec R2, R4, R5, R9):
//! 1. read the target metadata, map columns by name, run the pre-flight checks; stop before any data
//!    moves when a blocking finding exists (or when `--check-only`);
//! 2. plan jobs (column / files / hash / range splits) and pick a strategy per table;
//! 3. heap-then-index tables: capture and drop their indexes (DDL kept in the state dir);
//! 4. workers: switch jobs go through a staging table that is SWITCHed into the partition, other jobs
//!    bulk-load straight into the table; every finished job writes a done marker;
//! 5. recreate indexes, verify counts (always) and checksums / row samples when asked, drop old
//!    partitions when asked.
use super::bulk::{bulk_load, BulkOptions};
use super::convert::Converter;
use super::guard::Guard;
use super::jobs::{self, Job, JobQueue, SplitSpec, StateDir, Strategy};
use super::switch;
use super::target::{self, is_blocking, Finding, PreflightOptions, TableMapping, TargetTable};
use super::{MsClient, MssqlTarget};
use crate::core::emit::Emitter;
use crate::core::model::{Catalog, Column, Conv, Table};
use crate::core::runlog::{self, Failure, SummaryEntry};
use crate::core::typemap::quote_duck;
use crate::verify::{self, VerifyLevel, VerifyResult};
use anyhow::{anyhow, bail, Context, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

pub struct ExistingLoadOptions {
    pub database: String,
    pub guard: Guard,
    pub parallel: usize,
    pub max_jobs_per_table: Option<usize>,
    pub batch_rows: usize,
    pub splits: HashMap<String, SplitSpec>,
    pub strategies: HashMap<String, Strategy>,
    pub default_strategy: Strategy,
    pub split_threshold: i64,
    pub state_dir: Option<PathBuf>,
    pub replace_partitions: bool,
    pub drop_old: bool,
    pub ignore_extra_columns: bool,
    pub preflight: PreflightOptions,
    pub check_only: bool,
    pub verify: VerifyLevel,
    pub order_by_key: bool,
    /// `table=glob` Parquet sources that stand in for DuckDB tables.
    pub parquet_sources: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub struct ExistingLoadReport {
    pub rows: i64,
    pub seconds: f64,
    pub warnings: Vec<String>,
    pub findings: Vec<Finding>,
    pub verify: Vec<VerifyResult>,
    pub jobs_done: usize,
    pub jobs_skipped: usize,
    pub tables: Vec<String>,
}

/// Describe a Parquet source as a catalog table (schema from DuckDB, row count from the files).
pub fn parquet_table(con: &Connection, schema: &str, name: &str, glob: &str) -> Result<Table> {
    let src = format!("read_parquet('{}', hive_partitioning = true)", glob.replace('\'', "''"));
    let mut st = con.prepare(&format!("DESCRIBE SELECT * FROM {src}"))?;
    let cols: Vec<(String, String)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;
    if cols.is_empty() { bail!("--source-parquet {glob}: no columns found"); }
    let row_count: i64 = con.query_row(&format!("SELECT count(*) FROM {src}"), [], |r| r.get(0))?;
    let columns = cols.into_iter().map(|(n, t)| Column { select_expr: quote_duck(&n), name: n, duck_type: t, nullable: true, default: None, comment: None, sql_type: String::new(), conv: Conv::Identity, max_len: None, identity: None, enum_values: None, warnings: vec![] }).collect();
    Ok(Table { schema: schema.into(), name: name.into(), columns, constraints: vec![], indexes: vec![], row_count, comment: None })
}

/// Replace the SELECT list of a job's source SQL with `exprs` (target column order).
fn rewrite_select(job_sql: &str, exprs: &[String]) -> Option<String> {
    let up = job_sql.to_uppercase();
    let i = up.find(" FROM ")?;
    Some(format!("SELECT {} {}", exprs.join(", "), job_sql[i + 1..].trim()))
}

fn mapped_exprs(duck: &Table, m: &TableMapping, allow_rescale: bool) -> Vec<String> {
    m.columns.iter().map(|c| {
        let col = &duck.columns[c.duck_index];
        let e = if col.select_expr.is_empty() { quote_duck(&col.name) } else { col.select_expr.clone() };
        // the converter never rounds: with --allow-rescale the DuckDB side casts to the target scale
        if allow_rescale && matches!(c.target.type_name.as_str(), "decimal" | "numeric" | "money" | "smallmoney") {
            let (base, arg) = crate::core::typemap::parse_type(&col.duck_type);
            let src_scale: u8 = if base == "DECIMAL" { arg.split(',').nth(1).and_then(|s| s.trim().parse().ok()).unwrap_or(0) } else { 0 };
            if src_scale > c.target.scale { return format!("CAST({e} AS DECIMAL({},{}))", c.target.precision.max(1), c.target.scale); }
        }
        e
    }).collect()
}

struct TableCtx {
    duck: Table,
    target: TargetTable,
    mapping: TableMapping,
    convs: Vec<Converter>,
    columns: Vec<String>,
    exprs: Vec<String>,
    strategy: Strategy,
    keep_identity: bool,
}

pub fn load_into_existing(con: &Connection, cat: &Catalog, target: &MssqlTarget, em: &Emitter, o: &ExistingLoadOptions,
                          progress: Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<ExistingLoadReport> {
    let t0 = std::time::Instant::now();
    let rt = super::runtime()?;
    let db = &o.database;
    let mut rep = ExistingLoadReport::default();

    // ---- 1. metadata + pre-flight (no DDL, no data) -------------------------------------------
    let mut c = rt.block_on(async {
        let mut m = target.connect("master").await?;
        o.guard.ensure_database(&mut m, db, false).await?;
        let st = Guard::database_state(&mut m, db).await?;
        rep.warnings.extend(Guard::describe(&st, db));
        target.connect(db).await
    })?;
    let mut ducks: Vec<Table> = cat.tables.clone();
    for (name, glob) in &o.parquet_sources {
        let (schema, tname) = name.split_once('.').map(|(s, t)| (s.to_string(), t.to_string())).unwrap_or(("main".into(), name.clone()));
        if let Some(t) = ducks.iter_mut().find(|t| t.name.eq_ignore_ascii_case(&tname) && t.schema.eq_ignore_ascii_case(&schema)) {
            let p = parquet_table(con, &t.schema, &t.name, glob)?;
            t.columns = p.columns; t.row_count = p.row_count;
        } else {
            ducks.push(parquet_table(con, &schema, &tname, glob)?);
        }
    }
    let names: Vec<(String, String)> = ducks.iter().map(|t| (em.schema(&t.schema), t.name.clone())).collect();
    let targets = rt.block_on(target::read_target_tables(&mut c, &names))?;
    let mut ctxs: Vec<TableCtx> = vec![];
    let mut findings: Vec<Finding> = vec![];
    for (duck, tt) in ducks.iter().zip(targets.into_iter()) {
        let mapping = target::map_columns(duck, &tt, o.ignore_extra_columns)?;
        rep.warnings.extend(mapping.notes.iter().map(|n| format!("{}: {n}", tt.full_name())));
        let fs = target::preflight(con, duck, &mapping, &o.preflight)?;
        for f in &fs { if !is_blocking(f) { rep.warnings.push(f.to_string()); } }
        findings.extend(fs);
        let convs: Vec<Converter> = mapping.columns.iter().map(|cm| Converter::for_target(&cm.target, &duck.columns[cm.duck_index].duck_type).with_context(|| format!("{}.{}", tt.full_name(), cm.target.name))).collect::<Result<_>>()?;
        let columns: Vec<String> = mapping.columns.iter().map(|cm| cm.target.name.clone()).collect();
        let exprs = mapped_exprs(duck, &mapping, o.preflight.allow_rescale);
        let strategy = jobs::lookup(&o.strategies, &duck.schema, &duck.name).copied()
            .or_else(|| jobs::lookup(&o.strategies, &duck.schema, "*").copied()).or_else(|| o.strategies.get("*").copied())
            .unwrap_or(o.default_strategy);
        if strategy == Strategy::Switch && tt.partitioning.is_none() { bail!("{}: strategy switch needs a partitioned target table", tt.full_name()); }
        let keep_identity = mapping.columns.iter().any(|cm| cm.target.is_identity);
        ctxs.push(TableCtx { duck: duck.clone(), target: tt, mapping, convs, columns, exprs, strategy, keep_identity });
    }
    let blocking: Vec<&Finding> = findings.iter().filter(|f| is_blocking(f)).collect();
    rep.findings = findings.clone();
    rep.tables = ctxs.iter().map(|x| x.target.full_name()).collect();
    if !blocking.is_empty() {
        let list: Vec<String> = blocking.iter().map(|f| f.to_string()).collect();
        return Err(Failure::Check(format!("pre-flight found {} blocking problem(s); no data was moved:\n  {}", blocking.len(), list.join("\n  "))).into());
    }
    if o.check_only {
        rep.seconds = t0.elapsed().as_secs_f64();
        rep.warnings.push(format!("check only: {} table(s) checked, no data moved", ctxs.len()));
        return Ok(rep);
    }

    // ---- 2. jobs ---------------------------------------------------------------------------
    let plan_cat = Catalog { tables: ducks.clone(), ..cat.clone() };
    let plan = jobs::plan_jobs(con, &plan_cat, &o.splits, &o.strategies, o.default_strategy, o.split_threshold, o.parallel)?;
    let state = o.state_dir.as_ref().map(|p| StateDir::open(p)).transpose()?;
    let mut pending: Vec<Job> = match &state { Some(s) => plan.pending(s), None => plan.jobs.clone() };
    let skipped = plan.jobs.len() - pending.len();
    if let Some(s) = &state { for j in &plan.jobs { if s.is_stale(j) { rep.warnings.push(format!("{}: done marker is from a different input; delete it to reload", j.id)); } } }
    // empty-table placeholder jobs have no FROM clause: nothing to send
    pending.retain(|j| j.source_sql.to_uppercase().contains(" FROM "));
    rep.jobs_skipped = skipped;
    if runlog::planning() {
        for j in &plan.jobs {
            let x = &ctxs[j.table];
            let sql = rewrite_select(&j.source_sql, &x.exprs).unwrap_or_else(|| j.source_sql.clone());
            runlog::stmt("target", &j.id, &format!("BULK INSERT {} via {} ({} rows): {sql}", x.target.full_name(), x.strategy.as_str(), j.est_rows), j.est_rows, std::time::Instant::now());
        }
        rep.warnings.push(format!("plan mode: {} job(s) listed, nothing loaded", plan.jobs.len()));
        rep.seconds = t0.elapsed().as_secs_f64();
        return Ok(rep);
    }

    // ---- 3. heap-then-index: capture + drop indexes before the load ---------------------------
    let mut index_ddl: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, x) in ctxs.iter().enumerate() {
        if x.strategy != Strategy::HeapThenIndex || !plan.jobs_of(i).any(|j| pending.iter().any(|p| p.id == j.id)) { continue; }
        let saved = state.as_ref().map(|s| s.path().join(format!("{}.{}", x.target.schema, x.target.name)).join("indexes.sql"));
        let ddl = match saved.as_ref().filter(|p| p.exists()) {
            Some(p) => { rep.warnings.push(format!("{}: reusing captured index DDL from {}", x.target.full_name(), p.display())); std::fs::read_to_string(p)?.split("\n;;\n").filter(|s| !s.trim().is_empty()).map(String::from).collect() }
            None => {
                let ddl = rt.block_on(switch::capture_and_drop_indexes(&mut c, &x.target))?;
                if let Some(p) = &saved { if let Some(d) = p.parent() { std::fs::create_dir_all(d)?; } std::fs::write(p, ddl.join("\n;;\n"))?; }
                ddl
            }
        };
        index_ddl.insert(i, ddl);
    }
    drop(c);

    // ---- 4. workers ------------------------------------------------------------------------
    let queue = Arc::new(JobQueue::new(pending.clone(), o.max_jobs_per_table));
    for (i, x) in ctxs.iter().enumerate() { if x.strategy == Strategy::Serial { queue.set_table_limit(i, 1); } }
    let ctxs = Arc::new(ctxs);
    let state = Arc::new(state);
    let total = Arc::new(AtomicI64::new(0));
    let done = Arc::new(AtomicI64::new(0));
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let n_workers = o.parallel.max(1).min(pending.len().max(1));
    let mut handles = vec![];
    for _ in 0..n_workers {
        let con = crate::core::conn::acquire(con)?;
        let (queue, ctxs, state, total, done, errors, progress, target) = (queue.clone(), ctxs.clone(), state.clone(), total.clone(), done.clone(), errors.clone(), progress.clone(), target.clone());
        let (database, batch_rows, replace_partitions, order_by_key) = (db.clone(), o.batch_rows, o.replace_partitions, o.order_by_key);
        handles.push(std::thread::spawn(move || -> Result<()> {
            let rt = super::runtime()?;
            let con_ref = &con;
            let r = rt.block_on(async move {
                let mut client = target.connect(&database).await?;
                while let Some(job) = queue.next_blocking() {
                    let x = &ctxs[job.table];
                    let t0 = std::time::Instant::now();
                    let r = run_job(con_ref, &mut client, x, &job, batch_rows, replace_partitions, order_by_key, &progress).await;
                    queue.finish(&job);
                    match r {
                        Ok(rows) => {
                            total.fetch_add(rows, Ordering::Relaxed);
                            done.fetch_add(1, Ordering::Relaxed);
                            if let Some(s) = state.as_ref() { s.mark_done(&job, rows, t0.elapsed().as_secs_f64(), None)?; }
                            runlog::summary_add(SummaryEntry { table: x.target.full_name(), job: job.id.clone(), rows, seconds: t0.elapsed().as_secs_f64(), strategy: x.strategy.as_str().into(), checks: vec![], ok: true });
                        }
                        Err(e) => {
                            errors.lock().unwrap().push(format!("{}: {e:#}", job.id));
                            runlog::summary_add(SummaryEntry { table: x.target.full_name(), job: job.id.clone(), rows: 0, seconds: t0.elapsed().as_secs_f64(), strategy: x.strategy.as_str().into(), checks: vec![format!("{e:#}")], ok: false });
                            queue.cancel();
                            return Err(e);
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
            crate::core::conn::release(con);
            r
        }));
    }
    let mut first_err: Option<anyhow::Error> = None;
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => { if first_err.is_none() { first_err = Some(e); } }
            Err(_) => { if first_err.is_none() { first_err = Some(anyhow!("worker thread panicked")); } }
        }
    }
    if let Some(e) = first_err { return Err(e).context(format!("load stopped; {} job(s) finished (done markers kept{})", done.load(Ordering::Relaxed), if o.state_dir.is_some() { ", rerun with the same --state-dir to resume" } else { "" })); }
    rep.rows = total.load(Ordering::Relaxed);
    rep.jobs_done = done.load(Ordering::Relaxed) as usize;

    // ---- 5. indexes, verification, old partitions -----------------------------------------
    let mut c = rt.block_on(target.connect(db))?;
    for (i, ddl) in &index_ddl {
        let x = &ctxs[*i];
        let t1 = std::time::Instant::now();
        rt.block_on(switch::recreate_indexes(&mut c, ddl, &format!("{}#indexes", x.target.full_name())))?;
        runlog::summary_add(SummaryEntry { table: x.target.full_name(), job: "indexes".into(), rows: 0, seconds: t1.elapsed().as_secs_f64(), strategy: "heap-then-index".into(), checks: vec![format!("{} index statement(s)", ddl.len())], ok: true });
        if let Some(s) = state.as_ref() { let _ = std::fs::remove_file(s.path().join(format!("{}.{}", x.target.schema, x.target.name)).join("indexes.sql")); }
    }
    let mut results: Vec<VerifyResult> = vec![];
    for x in ctxs.iter() {
        let level = if o.verify == VerifyLevel::Counts { VerifyLevel::Counts } else { o.verify.clone() };
        let mut r = rt.block_on(verify::verify_table(con, &mut c, &x.duck, &x.target, &x.mapping, level))?;
        if matches!(x.strategy, Strategy::HeapThenIndex | Strategy::Switch) { r.extend(rt.block_on(verify::verify_indexes(&mut c, &x.target))?); }
        for v in &r { runlog::note(&format!("verify {}", v)); }
        runlog::summary_add(SummaryEntry { table: x.target.full_name(), job: "verify".into(), rows: x.duck.row_count, seconds: 0.0, strategy: format!("{:?}", o.verify).to_lowercase(), checks: r.iter().map(|v| v.to_string()).collect(), ok: r.iter().all(|v| v.ok) });
        results.extend(r);
        if o.drop_old && x.strategy == Strategy::Switch {
            let dropped = rt.block_on(switch::drop_old_tables(&mut c, &x.target.schema, &x.target.name))?;
            for d in dropped { rep.warnings.push(format!("dropped old partition table {d}")); }
        }
    }
    rep.verify = results.clone();
    rep.seconds = t0.elapsed().as_secs_f64();
    verify::check_results(&results)?;
    Ok(rep)
}

#[allow(clippy::too_many_arguments)]
async fn run_job(con: &Connection, client: &mut MsClient, x: &TableCtx, job: &Job, batch_rows: usize, replace_partitions: bool, order_by_key: bool, progress: &Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<i64> {
    let mut sql = rewrite_select(&job.source_sql, &x.exprs).ok_or_else(|| anyhow!("{}: cannot rewrite job SQL {:?}", job.id, job.source_sql))?;
    if order_by_key && !sql.to_uppercase().contains(" ORDER BY ") {
        if let Some(ci) = x.target.clustered() {
            let keys: Vec<String> = ci.columns.iter().filter(|k| !k.included).map(|k| x.mapping.columns.iter().find(|cm| cm.target.name.eq_ignore_ascii_case(&k.name)).map(|cm| format!("{}{}", x.exprs[x.mapping.columns.iter().position(|c| c.duck_index == cm.duck_index).unwrap()], if k.descending { " DESC" } else { "" }))).flatten().collect();
            if !keys.is_empty() { sql.push_str(&format!(" ORDER BY {}", keys.join(", "))); }
        }
    }
    let label = job.id.clone();
    let prog = move |n: i64| progress(&label, n);
    let opts = BulkOptions { batch_rows, keep_identity: x.keep_identity, table_lock: true, prefetch_batches: 4, order_by: None, check_constraints: true };
    match x.strategy {
        Strategy::Switch => {
            let mut s = switch::prepare_staging(client, &x.target, job).await?;
            let rows = bulk_load(con, client, &s.table_ref(), &x.columns, &x.convs, &sql, &opts, &prog).await.with_context(|| format!("loading staging {}", s.table_ref()))?;
            switch::finish_staging(client, &x.target, &mut s).await?;
            switch::switch_in(client, &x.target, &s, rows, replace_partitions).await?;
            switch::drop_staging(client, &s).await?;
            Ok(rows)
        }
        _ => {
            let tref = format!("{}.{}", super::bracket(&x.target.schema), super::bracket(&x.target.name));
            bulk_load(con, client, &tref, &x.columns, &x.convs, &sql, &opts, &prog).await.with_context(|| format!("loading {tref}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_keeps_from_and_where() {
        let s = rewrite_select("SELECT \"a\", \"b\" FROM \"main\".\"t\" WHERE \"mkt\" = 'AAA'", &["\"b\"".into(), "CAST(\"a\" AS VARCHAR)".into()]).unwrap();
        assert_eq!(s, "SELECT \"b\", CAST(\"a\" AS VARCHAR) FROM \"main\".\"t\" WHERE \"mkt\" = 'AAA'");
        assert!(rewrite_select("SELECT 0", &[]).is_none());
    }

    #[test]
    fn parquet_source_describes_columns() {
        let con = Connection::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.parquet");
        con.execute_batch(&format!("COPY (SELECT 1 AS id, 'x' AS s, DATE '2024-01-01' AS d) TO '{}' (FORMAT PARQUET)", p.display())).unwrap();
        let t = parquet_table(&con, "main", "t", &p.display().to_string()).unwrap();
        assert_eq!(t.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "s", "d"]);
        assert_eq!(t.row_count, 1);
        assert_eq!(t.columns[1].duck_type, "VARCHAR");
    }
}
