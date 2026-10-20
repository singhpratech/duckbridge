//! DuckDB -> SQL Server direct load over the TDS bulk-copy protocol, N tables in parallel.
use super::guard::Guard;
use super::{exec_batch, MsClient, MssqlTarget};
use crate::core::emit::{Emitter, Flavor};
use crate::core::model::*;
use crate::core::script::select_sql;
use crate::core::typemap::quote_duck;
use super::split::ranges;
use anyhow::{Context, Result};
use crate::core::duck::{self, Connection};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

pub struct LoadOptions {
    pub database: String,
    pub drop_existing: bool,
    pub with_data: bool,
    pub parallel: usize,
    pub batch_rows: usize,
    pub method: LoadMethod,
    /// Safety guard: database-level statements only on disposable databases (spec R1).
    pub guard: Guard,
    /// Create the database when it does not exist (its name must be disposable). Never implicit.
    pub create_database: bool,
    /// `SIMPLE` / `FULL` / `BULK_LOGGED`, applied only to a disposable database and only when asked.
    pub recovery: Option<String>,
    /// Compatibility level, applied only to a disposable database and only when asked.
    pub compat_level: Option<u16>,
    /// Load into a heap and add the clustered primary key afterwards (fastest for big tables).
    pub pk_after_load: bool,
    /// Tables with at least this many rows are split into key ranges across workers.
    pub split_threshold: i64,
    /// Add ORDER BY key to each range (cheap when DuckDB data is already sorted).
    pub order_by_key: bool,
}

struct Job {
    table: usize,
    key: Option<String>,
    range: Option<super::split::Range>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMethod {
    Bulk,
    Insert,
}

pub struct LoadReport {
    pub rows: i64,
    pub seconds: f64,
    pub warnings: Vec<String>,
}

/// Create the database (if needed) and all tables; returns the emitter used.
pub async fn prepare_schema(client_master: &mut MsClient, target: &MssqlTarget, cat: &mut Catalog, em: &Emitter, o: &LoadOptions) -> Result<MsClient> {
    let db = &o.database;
    if o.drop_existing { o.guard.drop_database(client_master, db).await?; }
    o.guard.ensure_database(client_master, db, o.create_database).await?;
    if let Some(l) = o.compat_level { o.guard.set_compat_level(client_master, db, l).await?; }
    if let Some(r) = &o.recovery { if !r.eq_ignore_ascii_case("none") { o.guard.set_recovery(client_master, db, r).await?; } }
    let st = Guard::database_state(client_master, db).await?;
    cat.warnings.extend(Guard::describe(&st, db));
    // in --plan mode the database may not exist yet; DDL is only recorded, so any connection will do
    let mut c = if crate::core::runlog::planning() && !st.exists { target.connect("master").await? } else { target.connect(db).await? };
    for s in &cat.schemas {
        if em.schema(s) != "dbo" { exec_batch(&mut c, &em.create_schema(s)).await?; }
    }
    // drop in reverse dependency order first (FKs) if the tables exist
    if o.drop_existing {
        for v in &cat.views { exec_batch(&mut c, &em.drop_view_if_exists(v)).await?; }
        for t in cat.tables.iter().rev() { exec_batch(&mut c, &em.drop_table_if_exists(t)).await?; }
    }
    for t in cat.tables.iter_mut() {
        let ddl = em.create_table_opts(t, !(o.pk_after_load && o.with_data));
        exec_batch(&mut c, &ddl).await.with_context(|| format!("creating table {}.{}", t.schema, t.name))?;
        for s in em.enum_checks(t) { exec_batch(&mut c, &s).await?; }
        if let Some(cm) = &t.comment { let _ = exec_batch(&mut c, &em.comment(t, None, cm)).await; }
        for col in &t.columns { if let Some(cm) = &col.comment { let _ = exec_batch(&mut c, &em.comment(t, Some(col), cm)).await; } }
    }
    Ok(c)
}

pub async fn finish_schema(c: &mut MsClient, cat: &Catalog, em: &Emitter, o: &LoadOptions, warnings: &mut Vec<String>) -> Result<()> {
    for t in &cat.tables {
        if o.pk_after_load && o.with_data { if let Some(s) = em.add_primary_key(t) { exec_batch(c, &s).await.with_context(|| format!("adding primary key on {}.{}", t.schema, t.name))?; } }
        if let Some(s) = em.reseed_identity(t) { exec_batch(c, &s).await?; }
        for (stmt, must) in em.post_constraints(t) {
            if let Err(e) = exec_batch(c, &stmt).await {
                if must { return Err(e); }
                warnings.push(format!("check constraint on {}.{} failed: {}", t.schema, t.name, first_line(&e)));
            }
        }
        for stmt in em.create_indexes(t) {
            exec_batch(c, &stmt).await.with_context(|| format!("creating index on {}.{}", t.schema, t.name))?;
        }
    }
    let mut pending: Vec<&View> = cat.views.iter().collect();
    for _ in 0..3 {
        let mut failed = vec![];
        for v in pending {
            match em.create_view(v) {
                Err(e) => warnings.push(format!("view {}.{}: not translatable ({e})", v.schema, v.name)),
                Ok(stmt) => if let Err(e) = exec_batch(c, &stmt).await { failed.push((v, e)); },
            }
        }
        if failed.is_empty() { break; }
        pending = failed.iter().map(|(v, _)| *v).collect();
        if pending.len() == failed.len() && failed.len() == cat.views.len() {
            for (v, e) in &failed { warnings.push(format!("view {}.{} failed on SQL Server: {}", v.schema, v.name, first_line(e))); }
            break;
        }
    }
    Ok(())
}

fn first_line(e: &anyhow::Error) -> String {
    format!("{e:#}").lines().next().unwrap_or("").chars().take(300).collect()
}

/// Stream all tables' data into SQL Server using `parallel` worker threads, each with its own
/// DuckDB cursor and TDS connection.
pub fn load_data(con: &Connection, cat: &Catalog, target: &MssqlTarget, em: &Emitter, o: &LoadOptions,
                 progress: Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<LoadReport> {
    let t0 = std::time::Instant::now();
    let total = Arc::new(AtomicI64::new(0));
    let warnings = Arc::new(Mutex::new(Vec::<String>::new()));
    // build jobs: big tables with a single integer key are split into ranges; largest first
    let mut jobs: Vec<Job> = vec![];
    for (i, t) in cat.tables.iter().enumerate() {
        if t.row_count == 0 { continue; }
        let key = split_key(t);
        if let (Some(k), true) = (&key, t.row_count >= o.split_threshold && o.parallel > 1) {
            let fq = format!("{}.{}", quote_duck(&t.schema), quote_duck(&t.name));
            let (mn, mx): (Option<i64>, Option<i64>) = duck::query_one(con, &format!("SELECT CAST(min({0}) AS BIGINT), CAST(max({0}) AS BIGINT) FROM {fq}", quote_duck(k)), |r| Ok((r.get(0)?, r.get(1)?)))?;
            let n = ((t.row_count / o.split_threshold.max(1)) as usize + 1).clamp(2, o.parallel * 2);
            for r in ranges(mn.unwrap_or(0) as i128, mx.unwrap_or(0) as i128, n) { jobs.push(Job { table: i, key: Some(k.clone()), range: Some(r) }); }
        } else {
            jobs.push(Job { table: i, key: key.clone(), range: None });
        }
    }
    jobs.sort_by_key(|j| std::cmp::Reverse(cat.tables[j.table].row_count));
    if crate::core::runlog::planning() {
        // --plan: describe every bulk job, send nothing
        for j in &jobs {
            let t = &cat.tables[j.table];
            let where_ = match (&j.key, &j.range) { (Some(k), Some(r)) => format!(" WHERE {}", r.predicate(&quote_duck(k))), _ => String::new() };
            crate::core::runlog::stmt("target", &format!("{}.{}", t.schema, t.name), &format!("BULK INSERT {} ({} rows{}) FROM DuckDB: {}{where_}", em.table_ref(&t.schema, &t.name), t.row_count, if j.range.is_some() { ", key range" } else { "" }, select_sql(t)), t.row_count, std::time::Instant::now());
        }
        return Ok(LoadReport { rows: 0, seconds: t0.elapsed().as_secs_f64(), warnings: vec![format!("plan mode: {} bulk job(s) listed, nothing loaded", jobs.len())] });
    }
    let queue = Arc::new(Mutex::new(jobs));
    let n_workers = o.parallel.max(1).min(queue.lock().unwrap().len().max(1));
    let mut handles = vec![];
    for _ in 0..n_workers {
        let con = crate::core::conn::acquire(con)?;
        let queue = queue.clone();
        let total = total.clone();
        let warnings = warnings.clone();
        let progress = progress.clone();
        let target = target.clone();
        let tables: Vec<Table> = cat.tables.clone();
        let schema_map = em.schema_map.clone();
        let batch_rows = o.batch_rows;
        let method = o.method;
        let database = o.database.clone();
        let order_by_key = o.order_by_key;
        handles.push(std::thread::spawn(move || -> Result<()> {
            let rt = super::runtime()?;
            let em = Emitter::new(Flavor::Mssql, schema_map);
            let con_ref = &con;
            let r = rt.block_on(async move {
                let mut client = target.connect(&database).await?;
                loop {
                    let job = { let mut q = queue.lock().unwrap(); q.pop() };
                    let Some(job) = job else { break };
                    let t = &tables[job.table];
                    let mut sql = select_sql(t);
                    if let (Some(k), Some(r)) = (&job.key, &job.range) { sql.push_str(&format!(" WHERE {}", r.predicate(&quote_duck(k)))); }
                    if order_by_key { if let Some(k) = &job.key { sql.push_str(&format!(" ORDER BY {}", quote_duck(k))); } }
                    let n = match method {
                        LoadMethod::Bulk => bulk_table(con_ref, &mut client, t, &sql, &em, batch_rows, &progress).await,
                        LoadMethod::Insert => insert_table(con_ref, &mut client, t, &sql, &em, &progress).await,
                    }.with_context(|| format!("loading {}.{}", t.schema, t.name))?;
                    total.fetch_add(n, Ordering::Relaxed);
                }
                Ok::<(), anyhow::Error>(())
            });
            crate::core::conn::release(con);
            let _ = warnings;
            r
        }));
    }
    for h in handles { h.join().map_err(|_| anyhow::anyhow!("worker thread panicked"))??; }
    let ws = warnings.lock().unwrap().clone();
    Ok(LoadReport { rows: total.load(Ordering::Relaxed), seconds: t0.elapsed().as_secs_f64(), warnings: ws })
}

fn split_key(t: &Table) -> Option<String> {
    let int_types = ["INT", "BIGINT", "SMALLINT", "TINYINT"];
    let is_int = |c: &Column| int_types.contains(&c.sql_type.to_uppercase().as_str());
    if let Some(pk) = t.pk() { if pk.columns.len() == 1 { if let Some(c) = t.columns.iter().find(|c| c.name == pk.columns[0]) { if is_int(c) { return Some(c.name.clone()); } } } }
    t.columns.iter().find(|c| c.identity.is_some() && is_int(c)).map(|c| c.name.clone())
}

async fn bulk_table(con: &Connection, client: &mut MsClient, t: &Table, sql: &str, em: &Emitter, batch_rows: usize, progress: &Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<i64> {
    use super::bulk::{bulk_load, BulkOptions};
    use super::convert::Converter;
    let tref = em.table_ref(&t.schema, &t.name);
    let convs: Vec<Converter> = t.columns.iter().map(|c| Converter::for_sql_type(&c.sql_type, &c.duck_type).with_context(|| format!("{}.{}.{}", t.schema, t.name, c.name))).collect::<Result<_>>()?;
    let cols: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
    let o = BulkOptions { batch_rows, keep_identity: t.has_identity(), table_lock: true, prefetch_batches: 4, order_by: None, check_constraints: true };
    let tag = format!("{}.{}", t.schema, t.name);
    bulk_load(con, client, &tref, &cols, &convs, sql, &o, &|n| progress(&tag, n)).await
}

async fn insert_table(con: &Connection, client: &mut MsClient, t: &Table, sql: &str, em: &Emitter, progress: &Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<i64> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    let mut batch: Vec<Vec<String>> = vec![];
    let mut n = 0i64;
    if let Some(s) = em.identity_insert(t, true) { exec_batch(client, &s).await?; }
    while let Some(r) = rows.next()? {
        batch.push(t.columns.iter().enumerate().map(|(i, c)| crate::core::script::literal(Flavor::Mssql, r.get_ref_unwrap(i), c)).collect());
        n += 1;
        if batch.len() >= 1000 {
            exec_batch(client, &em.insert_batch(t, &batch)).await?;
            batch.clear();
            progress(&format!("{}.{}", t.schema, t.name), n);
        }
    }
    if !batch.is_empty() { exec_batch(client, &em.insert_batch(t, &batch)).await?; }
    if let Some(s) = em.identity_insert(t, false) { exec_batch(client, &s).await?; }
    progress(&format!("{}.{}", t.schema, t.name), n);
    Ok(n)
}
