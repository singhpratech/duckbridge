//! SQL Server -> DuckDB import: catalog from sys.* views, data streamed over TDS into DuckDB appenders,
//! N tables (or N key ranges of one big table) in parallel.
use super::split::{ranges, Range};
use super::{bracket, MsClient, MssqlTarget};
use crate::core::typemap::{mssql_to_duck, quote_duck};
use crate::transpile::{self, Dialect};
use anyhow::{Context, Result};
use duckdb::types::Value;
use duckdb::Connection;
use futures_util::TryStreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tiberius::{ColumnData, QueryItem};

#[derive(Debug, Clone)]
pub struct MsColumn {
    pub name: String,
    pub type_name: String,
    pub max_length: i16,
    pub precision: u8,
    pub scale: u8,
    pub nullable: bool,
    pub is_identity: bool,
    pub seed: i64,
    pub increment: i64,
    pub duck_type: String,
    pub select_expr: String,
}

#[derive(Debug, Clone)]
pub struct MsTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<MsColumn>,
    pub pk: Vec<String>,
    pub uniques: Vec<Vec<String>>,
    pub fks: Vec<(Vec<String>, String, String, Vec<String>)>, // cols, ref schema, ref table, ref cols
    pub row_count: i64,
}

#[derive(Debug, Clone)]
pub struct MsView {
    pub schema: String,
    pub name: String,
    pub definition: String,
}

pub struct ImportOptions {
    /// Create tables without constraints, add PRIMARY KEY / UNIQUE after the copy (FKs are skipped with a warning).
    pub keys_after_load: bool,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub parallel: usize,
    pub split_threshold: i64,
    pub batch_rows: usize,
    pub schema_map: HashMap<String, String>, // dbo -> main
    pub nolock: bool,
}

pub struct ImportReport {
    pub tables: usize,
    pub rows: i64,
    pub seconds: f64,
    pub warnings: Vec<String>,
}

fn wanted(o: &ImportOptions, schema: &str, name: &str) -> bool {
    let full = format!("{schema}.{name}").to_lowercase();
    let n = name.to_lowercase();
    if !o.include.is_empty() && !o.include.iter().any(|i| { let i = i.to_lowercase(); i == n || i == full }) { return false; }
    !o.exclude.iter().any(|e| { let e = e.to_lowercase(); e == n || e == full })
}

pub async fn read_catalog(c: &mut MsClient, o: &ImportOptions, warnings: &mut Vec<String>) -> Result<(Vec<MsTable>, Vec<MsView>)> {
    let cols = c.simple_query(
        "SELECT s.name, t.name, c.name, ty.name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, \
                CAST(ISNULL(ic.seed_value, 0) AS BIGINT), CAST(ISNULL(ic.increment_value, 0) AS BIGINT), c.column_id \
         FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id \
         JOIN sys.columns c ON c.object_id = t.object_id \
         JOIN sys.types ty ON ty.user_type_id = c.user_type_id \
         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         WHERE t.is_ms_shipped = 0 AND t.temporal_type <> 1 ORDER BY s.name, t.name, c.column_id").await?.into_first_result().await?;
    let mut tables: Vec<MsTable> = vec![];
    for r in cols {
        let schema: String = r.get::<&str, _>(0).unwrap_or("").into();
        let name: String = r.get::<&str, _>(1).unwrap_or("").into();
        if !wanted(o, &schema, &name) { continue; }
        if tables.last().map(|t| t.schema != schema || t.name != name).unwrap_or(true) {
            tables.push(MsTable { schema: schema.clone(), name: name.clone(), columns: vec![], pk: vec![], uniques: vec![], fks: vec![], row_count: 0 });
        }
        let cname: String = r.get::<&str, _>(2).unwrap_or("").into();
        let type_name: String = r.get::<&str, _>(3).unwrap_or("").into();
        let max_length: i16 = r.get(4).unwrap_or(0);
        let precision: u8 = r.get(5).unwrap_or(0);
        let scale: u8 = r.get(6).unwrap_or(0);
        let nullable: bool = r.get(7).unwrap_or(true);
        let is_identity: bool = r.get(8).unwrap_or(false);
        let seed: i64 = r.get(9).unwrap_or(0);
        let increment: i64 = r.get(10).unwrap_or(0);
        let (duck_type, w) = mssql_to_duck(&type_name, max_length, precision, scale);
        if let Some(w) = w { warnings.push(format!("{schema}.{name}.{cname}: {w}")); }
        let q = bracket(&cname);
        let select_expr = match type_name.to_lowercase().as_str() {
            "xml" => format!("CAST({q} AS NVARCHAR(MAX)) AS {q}"),
            "hierarchyid" | "geography" | "geometry" => format!("{q}.ToString() AS {q}"),
            "sql_variant" => format!("CAST({q} AS NVARCHAR(MAX)) AS {q}"),
            "text" => format!("CAST({q} AS VARCHAR(MAX)) AS {q}"),
            "ntext" => format!("CAST({q} AS NVARCHAR(MAX)) AS {q}"),
            "image" => format!("CAST({q} AS VARBINARY(MAX)) AS {q}"),
            "timestamp" | "rowversion" => format!("CAST({q} AS BINARY(8)) AS {q}"),
            "json" => format!("CAST({q} AS NVARCHAR(MAX)) AS {q}"),
            "vector" => format!("CAST({q} AS NVARCHAR(MAX)) AS {q}"),
            _ => q.clone(),
        };
        tables.last_mut().unwrap().columns.push(MsColumn { name: cname, type_name, max_length, precision, scale, nullable, is_identity, seed, increment, duck_type, select_expr });
    }
    // keys
    let keys = c.simple_query(
        "SELECT s.name, t.name, kc.type, kc.name, c.name, ic.key_ordinal FROM sys.key_constraints kc \
         JOIN sys.tables t ON t.object_id = kc.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
         JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         ORDER BY s.name, t.name, kc.name, ic.key_ordinal").await?.into_first_result().await?;
    let mut uq: HashMap<(String, String, String), Vec<String>> = HashMap::new();
    for r in keys {
        let (s, t, kind, kname, col): (String, String, String, String, String) = (r.get::<&str, _>(0).unwrap_or("").into(), r.get::<&str, _>(1).unwrap_or("").into(), r.get::<&str, _>(2).unwrap_or("").into(), r.get::<&str, _>(3).unwrap_or("").into(), r.get::<&str, _>(4).unwrap_or("").into());
        if let Some(tb) = tables.iter_mut().find(|x| x.schema == s && x.name == t) {
            if kind.trim() == "PK" { tb.pk.push(col); } else { uq.entry((s, t, kname)).or_default().push(col); }
        }
    }
    for ((s, t, _), cols) in uq { if let Some(tb) = tables.iter_mut().find(|x| x.schema == s && x.name == t) { tb.uniques.push(cols); } }
    let fks = c.simple_query(
        "SELECT s.name, t.name, fk.name, c.name, rs.name, rt.name, rc.name, fkc.constraint_column_id FROM sys.foreign_keys fk \
         JOIN sys.tables t ON t.object_id = fk.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
         JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id JOIN sys.schemas rs ON rs.schema_id = rt.schema_id \
         JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id \
         JOIN sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id \
         JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id \
         ORDER BY s.name, t.name, fk.name, fkc.constraint_column_id").await?.into_first_result().await?;
    let mut fkmap: HashMap<(String, String, String), (Vec<String>, String, String, Vec<String>)> = HashMap::new();
    for r in fks {
        let g = |i: usize| -> String { r.get::<&str, _>(i).unwrap_or("").into() };
        let e = fkmap.entry((g(0), g(1), g(2))).or_insert_with(|| (vec![], g(4), g(5), vec![]));
        e.0.push(g(3));
        e.3.push(g(6));
    }
    for ((s, t, _), fk) in fkmap { if let Some(tb) = tables.iter_mut().find(|x| x.schema == s && x.name == t) { if wanted(o, &fk.1, &fk.2) { tb.fks.push(fk); } } }
    // identity: next value = IDENT_CURRENT + increment, so new DuckDB inserts never collide
    for t in tables.iter_mut() {
        let fq = format!("{}.{}", bracket(&t.schema), bracket(&t.name)).replace('\'', "''");
        if t.columns.iter().any(|c| c.is_identity) {
            let rows = c.simple_query(&format!("SELECT CAST(IDENT_CURRENT('{fq}') AS BIGINT), CAST(IDENT_INCR('{fq}') AS BIGINT), CAST(IDENT_SEED('{fq}') AS BIGINT)")).await?.into_first_result().await?;
            if let Some(r) = rows.first() {
                let cur: Option<i64> = r.get(0); let inc: i64 = r.get::<i64, _>(1).unwrap_or(1).max(1); let seed: i64 = r.get::<i64, _>(2).unwrap_or(1);
                for col in t.columns.iter_mut().filter(|c| c.is_identity) { col.increment = inc; col.seed = match cur { Some(v) => v + inc, None => seed }; }
            }
        }
    }
    // row counts (fast, from partition stats)
    let counts = c.simple_query("SELECT s.name, t.name, SUM(p.rows) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1) GROUP BY s.name, t.name").await?.into_first_result().await?;
    for r in counts {
        let (s, t): (String, String) = (r.get::<&str, _>(0).unwrap_or("").into(), r.get::<&str, _>(1).unwrap_or("").into());
        if let Some(tb) = tables.iter_mut().find(|x| x.schema == s && x.name == t) { tb.row_count = r.get::<i64, _>(2).unwrap_or(0); }
    }
    let views = c.simple_query("SELECT s.name, v.name, m.definition FROM sys.views v JOIN sys.schemas s ON s.schema_id = v.schema_id JOIN sys.sql_modules m ON m.object_id = v.object_id WHERE v.is_ms_shipped = 0 ORDER BY s.name, v.name").await?.into_first_result().await?;
    let views: Vec<MsView> = views.iter().filter_map(|r| {
        let s: String = r.get::<&str, _>(0)?.into(); let n: String = r.get::<&str, _>(1)?.into(); let d: String = r.get::<&str, _>(2)?.into();
        if wanted(o, &s, &n) { Some(MsView { schema: s, name: n, definition: d }) } else { None }
    }).collect();
    Ok((tables, views))
}

/// Order tables so FK targets come first; drops FKs that would form a cycle.
fn topo_order(tables: &[MsTable], warnings: &mut Vec<String>) -> (Vec<usize>, HashSet<(usize, usize)>) {
    let idx: HashMap<(String, String), usize> = tables.iter().enumerate().map(|(i, t)| ((t.schema.clone(), t.name.clone()), i)).collect();
    let mut done: Vec<bool> = vec![false; tables.len()];
    let mut order = vec![];
    let mut dropped_fks: HashSet<(usize, usize)> = HashSet::new();
    let mut remaining: Vec<usize> = (0..tables.len()).collect();
    while !remaining.is_empty() {
        let mut progressed = false;
        remaining.retain(|&i| {
            let deps_ok = tables[i].fks.iter().enumerate().all(|(k, fk)| dropped_fks.contains(&(i, k)) || idx.get(&(fk.1.clone(), fk.2.clone())).map(|&j| done[j] || j == i).unwrap_or(true));
            if deps_ok { done[i] = true; order.push(i); progressed = true; false } else { true }
        });
        if !progressed {
            // break the cycle: drop FKs of the first remaining table
            let i = remaining[0];
            for k in 0..tables[i].fks.len() { dropped_fks.insert((i, k)); }
            warnings.push(format!("{}.{}: foreign keys dropped to break a dependency cycle", tables[i].schema, tables[i].name));
        }
    }
    (order, dropped_fks)
}

pub fn create_duck_schema(con: &Connection, tables: &[MsTable], views: &[MsView], o: &ImportOptions, warnings: &mut Vec<String>) -> Result<Vec<usize>> {
    let sm = |s: &str| o.schema_map.get(s).cloned().unwrap_or_else(|| s.to_string());
    let mut schemas: Vec<String> = tables.iter().map(|t| sm(&t.schema)).chain(views.iter().map(|v| sm(&v.schema))).collect();
    schemas.sort(); schemas.dedup();
    for s in &schemas { if s != "main" { con.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote_duck(s)))?; } }
    let (order, dropped) = topo_order(tables, warnings);
    for &i in &order {
        let t = &tables[i];
        let mut lines = vec![];
        for c in &t.columns {
            let mut l = format!("{} {}", quote_duck(&c.name), c.duck_type);
            if !c.nullable { l.push_str(" NOT NULL"); }
            if c.is_identity {
                let seq = format!("seq_{}_{}", t.name, c.name).chars().map(|ch| if ch.is_alphanumeric() || ch == '_' { ch } else { '_' }).collect::<String>();
                con.execute_batch(&format!("CREATE SEQUENCE IF NOT EXISTS {}.{} START {} INCREMENT {}", quote_duck(&sm(&t.schema)), quote_duck(&seq), c.seed, c.increment.max(1)))?;
                l.push_str(&format!(" DEFAULT nextval('{}.{}')", sm(&t.schema), seq));
            }
            lines.push(l);
        }
        if !o.keys_after_load && !t.pk.is_empty() { lines.push(format!("PRIMARY KEY ({})", t.pk.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", "))); }
        if !o.keys_after_load { for u in &t.uniques { lines.push(format!("UNIQUE ({})", u.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", "))); } }
        if o.keys_after_load && !t.fks.is_empty() { warnings.push(format!("{}.{}: foreign keys skipped (--keys-after-load; DuckDB cannot add FKs after the fact)", t.schema, t.name)); }
        for (k, fk) in t.fks.iter().enumerate() {
            if o.keys_after_load || dropped.contains(&(i, k)) { continue; }
            if fk.1 == t.schema && fk.2 == t.name { warnings.push(format!("{}.{}: self-referencing FK skipped", t.schema, t.name)); continue; }
            lines.push(format!("FOREIGN KEY ({}) REFERENCES {}.{} ({})", fk.0.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", "), quote_duck(&sm(&fk.1)), quote_duck(&fk.2), fk.3.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", ")));
        }
        let ddl = format!("CREATE TABLE {}.{} (\n  {}\n)", quote_duck(&sm(&t.schema)), quote_duck(&t.name), lines.join(",\n  "));
        con.execute_batch(&ddl).with_context(|| format!("creating DuckDB table {}.{}:\n{ddl}", t.schema, t.name))?;
    }
    Ok(order)
}

/// With --keys-after-load: add PRIMARY KEY / UNIQUE constraints now that the data is in.
pub fn add_keys(con: &Connection, tables: &[MsTable], o: &ImportOptions, warnings: &mut Vec<String>) -> Result<()> {
    if !o.keys_after_load { return Ok(()); }
    let sm = |s: &str| o.schema_map.get(s).cloned().unwrap_or_else(|| s.to_string());
    for t in tables {
        let tref = format!("{}.{}", quote_duck(&sm(&t.schema)), quote_duck(&t.name));
        if !t.pk.is_empty() {
            let sql = format!("ALTER TABLE {tref} ADD PRIMARY KEY ({})", t.pk.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", "));
            if let Err(e) = con.execute_batch(&sql) { warnings.push(format!("{}.{}: primary key not added: {}", t.schema, t.name, e.to_string().lines().next().unwrap_or(""))); }
        }
        for u in &t.uniques {
            let sql = format!("CREATE UNIQUE INDEX {} ON {tref} ({})", quote_duck(&format!("uq_{}_{}", t.name, u.join("_"))), u.iter().map(|c| quote_duck(c)).collect::<Vec<_>>().join(", "));
            if let Err(e) = con.execute_batch(&sql) { warnings.push(format!("{}.{}: unique index not added: {}", t.schema, t.name, e.to_string().lines().next().unwrap_or(""))); }
        }
    }
    Ok(())
}

pub fn create_duck_views(con: &Connection, views: &[MsView], o: &ImportOptions, warnings: &mut Vec<String>) {
    let sm = |s: &str| o.schema_map.get(s).cloned().unwrap_or_else(|| s.to_string());
    let mut pending: Vec<&MsView> = views.iter().collect();
    for _ in 0..3 {
        let mut failed = vec![];
        for v in pending {
            match transpile::translate(&v.definition, Dialect::Tsql, Dialect::DuckDb, &o.schema_map) {
                Ok(tr) => {
                    // definition is "CREATE VIEW [s].[v] AS SELECT ..."; re-target name into DuckDB
                    let select = transpile::view_body(&tr.sql, false);
                    let ddl = format!("CREATE OR REPLACE VIEW {}.{} AS {}", quote_duck(&sm(&v.schema)), quote_duck(&v.name), select);
                    match con.execute_batch(&ddl) {
                        Ok(_) => { for w in tr.warnings { warnings.push(format!("view {}.{}: {w}", v.schema, v.name)); } }
                        Err(e) => failed.push((v, e.to_string())),
                    }
                }
                Err(e) => warnings.push(format!("view {}.{}: {e}", v.schema, v.name)),
            }
        }
        if failed.is_empty() { return; }
        if failed.len() == views.len() || failed.len() == pending_len(&failed) {
            // no progress possible
        }
        pending = failed.iter().map(|(v, _)| *v).collect();
        if pending.len() == failed.len() { let _ = 0; }
        if failed.iter().all(|_| true) && pending.is_empty() { return; }
        // last pass records failures
        if pending.len() == views.len() { for (v, e) in &failed { warnings.push(format!("view {}.{} could not be created in DuckDB: {}", v.schema, v.name, e.lines().next().unwrap_or(""))); } return; }
    }
}

fn pending_len<T>(v: &[T]) -> usize { v.len() }

struct Job {
    table: usize,
    range: Option<(String, Range)>,
}

pub fn copy_data(con: &Connection, target: &MssqlTarget, tables: &[MsTable], o: &ImportOptions, progress: Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<ImportReport> {
    let t0 = std::time::Instant::now();
    let warnings = Arc::new(Mutex::new(Vec::<String>::new()));
    // build jobs: big tables with a single integer key get split into ranges
    let mut jobs: Vec<Job> = vec![];
    let rt = super::runtime()?;
    {
        let mut c = rt.block_on(target.connect(&target.database))?;
        for (i, t) in tables.iter().enumerate() {
            if t.row_count == 0 { continue; }
            let key = split_key(t);
            if t.row_count >= o.split_threshold && o.parallel > 1 && key.is_some() {
                let key = key.unwrap();
                let (mn, mx) = rt.block_on(async {
                    let rows = c.simple_query(&format!("SELECT CAST(MIN({0}) AS BIGINT), CAST(MAX({0}) AS BIGINT) FROM {1}.{2}", bracket(&key), bracket(&t.schema), bracket(&t.name))).await?.into_first_result().await?;
                    let r = rows.first();
                    Ok::<(i64, i64), anyhow::Error>((r.and_then(|r| r.get::<i64, _>(0)).unwrap_or(0), r.and_then(|r| r.get::<i64, _>(1)).unwrap_or(0)))
                })?;
                let n = ((t.row_count / o.split_threshold.max(1)) as usize + 1).min(o.parallel * 2).max(2);
                for r in ranges(mn as i128, mx as i128, n) { jobs.push(Job { table: i, range: Some((key.clone(), r)) }); }
            } else {
                jobs.push(Job { table: i, range: None });
            }
        }
    }
    jobs.sort_by_key(|j| std::cmp::Reverse(tables[j.table].row_count));
    let queue = Arc::new(Mutex::new(jobs));
    let total = Arc::new(AtomicI64::new(0));
    let n_workers = o.parallel.max(1).min(queue.lock().unwrap().len().max(1));
    let mut handles = vec![];
    for _ in 0..n_workers {
        let con = crate::core::conn::acquire(con)?;
        let queue = queue.clone();
        let total = total.clone();
        let progress = progress.clone();
        let target = target.clone();
        let tables: Vec<MsTable> = tables.to_vec();
        let sm = o.schema_map.clone();
        let nolock = o.nolock;
        let batch_rows = o.batch_rows;
        handles.push(std::thread::spawn(move || -> Result<()> {
            let rt = super::runtime()?;
            let con_ref = &con;
            let r = rt.block_on(async move {
                let mut client = target.connect(&target.database).await?;
                loop {
                    let job = { queue.lock().unwrap().pop() };
                    let Some(job) = job else { break };
                    let t = &tables[job.table];
                    let n = copy_one(con_ref, &mut client, t, job.range.as_ref(), &sm, nolock, batch_rows, &progress).await.with_context(|| format!("copying {}.{}", t.schema, t.name))?;
                    total.fetch_add(n, Ordering::Relaxed);
                }
                Ok::<(), anyhow::Error>(())
            });
            crate::core::conn::release(con);
            r
        }));
    }
    for h in handles { h.join().map_err(|_| anyhow::anyhow!("worker thread panicked"))??; }
    let ws = warnings.lock().unwrap().clone();
    Ok(ImportReport { tables: tables.len(), rows: total.load(Ordering::Relaxed), seconds: t0.elapsed().as_secs_f64(), warnings: ws })
}

fn split_key(t: &MsTable) -> Option<String> {
    let int_types = ["int", "bigint", "smallint", "tinyint"];
    if t.pk.len() == 1 {
        let c = t.columns.iter().find(|c| c.name == t.pk[0])?;
        if int_types.contains(&c.type_name.to_lowercase().as_str()) { return Some(c.name.clone()); }
    }
    t.columns.iter().find(|c| c.is_identity && int_types.contains(&c.type_name.to_lowercase().as_str())).map(|c| c.name.clone())
}

async fn copy_one(con: &Connection, client: &mut MsClient, t: &MsTable, range: Option<&(String, Range)>, sm: &HashMap<String, String>, nolock: bool, batch_rows: usize, progress: &Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<i64> {
    let schema = sm.get(&t.schema).cloned().unwrap_or_else(|| t.schema.clone());
    let cols: Vec<String> = t.columns.iter().map(|c| c.select_expr.clone()).collect();
    let hint = if nolock { " WITH (NOLOCK)" } else { "" };
    let mut sql = format!("SELECT {} FROM {}.{}{hint}", cols.join(", "), bracket(&t.schema), bracket(&t.name));
    if let Some((k, r)) = range { sql.push_str(&format!(" WHERE {}", r.predicate(&bracket(k)))); }
    let mut app = con.appender_to_db(&t.name, &schema)?;
    let mut stream = client.query(sql, &[]).await?;
    let mut n = 0i64;
    let mut buf: Vec<Vec<Value>> = Vec::with_capacity(batch_rows);
    while let Some(item) = stream.try_next().await? {
        if let QueryItem::Row(row) = item {
            let vals: Vec<Value> = row.into_iter().map(column_to_value).collect();
            buf.push(vals);
            if buf.len() >= batch_rows {
                for r in buf.drain(..) { append_values(&mut app, r)?; }
                app.flush()?;
                n += batch_rows as i64;
                progress(&format!("{}.{}", t.schema, t.name), n);
            }
        }
    }
    n += buf.len() as i64;
    for r in buf.drain(..) { append_values(&mut app, r)?; }
    app.flush()?;
    progress(&format!("{}.{}", t.schema, t.name), n);
    Ok(n)
}

fn column_to_value(c: ColumnData<'static>) -> Value {
    use ColumnData as C;
    match c {
        C::Bit(b) => b.map(Value::Boolean).unwrap_or(Value::Null),
        C::U8(x) => x.map(Value::UTinyInt).unwrap_or(Value::Null),
        C::I16(x) => x.map(Value::SmallInt).unwrap_or(Value::Null),
        C::I32(x) => x.map(Value::Int).unwrap_or(Value::Null),
        C::I64(x) => x.map(Value::BigInt).unwrap_or(Value::Null),
        C::F32(x) => x.map(Value::Float).unwrap_or(Value::Null),
        C::F64(x) => x.map(Value::Double).unwrap_or(Value::Null),
        C::Numeric(x) => x.map(|n| Value::Text(n.to_string())).unwrap_or(Value::Null),
        C::String(s) => s.map(|s| Value::Text(s.into_owned())).unwrap_or(Value::Null),
        C::Guid(g) => g.map(|g| Value::Text(g.to_string())).unwrap_or(Value::Null),
        C::Binary(b) => b.map(|b| Value::Blob(b.into_owned())).unwrap_or(Value::Null),
        C::Xml(x) => x.map(|x| Value::Text(x.to_string())).unwrap_or(Value::Null),
        C::Date(d) => d.map(|d| { let nd: chrono::NaiveDate = chrono::NaiveDate::from_num_days_from_ce_opt(d.days() as i32 + 1).unwrap_or_default(); Value::Date32((nd - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32) }).unwrap_or(Value::Null),
        C::Time(t) => t.map(|t| Value::Time64(duckdb::types::TimeUnit::Microsecond, time_micros(&t))).unwrap_or(Value::Null),
        C::DateTime(dt) => dt.map(|dt| Value::Timestamp(duckdb::types::TimeUnit::Microsecond, (dt.days() as i64 - 25_567) * 86_400_000_000 + (dt.seconds_fragments() as i64) * 1_000_000 / 300)).unwrap_or(Value::Null),
        C::SmallDateTime(dt) => dt.map(|dt| Value::Timestamp(duckdb::types::TimeUnit::Microsecond, (dt.days() as i64 - 25_567) * 86_400_000_000 + dt.seconds_fragments() as i64 * 60_000_000)).unwrap_or(Value::Null),
        C::DateTime2(dt) => dt.map(|dt| Value::Timestamp(duckdb::types::TimeUnit::Microsecond, dt2_micros(&dt))).unwrap_or(Value::Null),
        C::DateTimeOffset(dto) => dto.map(|dto| {
            let micros = dt2_micros(&dto.datetime2()) - dto.offset() as i64 * 60_000_000; // stored local; convert to UTC
            Value::Timestamp(duckdb::types::TimeUnit::Microsecond, micros)
        }).unwrap_or(Value::Null),
    }
}

fn time_micros(t: &tiberius::time::Time) -> i64 {
    let inc = t.increments() as i64;
    let scale = t.scale() as u32;
    // increments are in 10^-scale seconds
    if scale >= 6 { inc / 10i64.pow(scale - 6) } else { inc * 10i64.pow(6 - scale) }
}

fn dt2_micros(dt: &tiberius::time::DateTime2) -> i64 {
    let days = dt.date().days() as i64; // days since 0001-01-01
    let epoch_days = days - 719_162; // 1970-01-01 is day 719162 from 0001-01-01
    epoch_days * 86_400_000_000 + time_micros(&dt.time())
}

fn append_values(app: &mut duckdb::Appender<'_>, r: Vec<Value>) -> Result<()> {
    let outs: Vec<duckdb::types::ToSqlOutput<'static>> = r.into_iter().map(duckdb::types::ToSqlOutput::Owned).collect();
    let refs: Vec<&dyn duckdb::ToSql> = outs.iter().map(|o| o as &dyn duckdb::ToSql).collect();
    app.append_row(refs.as_slice())?;
    Ok(())
}
