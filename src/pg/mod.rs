//! PostgreSQL direct load via COPY ... FROM STDIN (text format), streamed from DuckDB.
use crate::core::emit::{Emitter, Flavor};
use crate::core::model::*;
use crate::core::script::{date_from_days, naive_ts, select_sql, time_from_units, trim_ns};
use anyhow::{Context, Result};
use crate::core::duck::{Connection, ValueRef};
use futures_util::SinkExt;
use std::pin::pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tokio_postgres::NoTls;

pub struct PgOptions {
    pub dsn: String, // e.g. host=localhost user=postgres password=... dbname=target
    pub drop_existing: bool,
    pub with_data: bool,
    pub parallel: usize,
}

pub struct PgReport {
    pub rows: i64,
    pub seconds: f64,
    pub warnings: Vec<String>,
}

async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls).await.context("connecting to PostgreSQL")?;
    tokio::spawn(async move { let _ = conn.await; });
    Ok(client)
}

pub fn load(con: &Connection, cat: &mut Catalog, em: &Emitter, o: &PgOptions, progress: Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<PgReport> {
    let t0 = std::time::Instant::now();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut warnings = cat.warnings.clone();
    rt.block_on(async {
        let c = connect(&o.dsn).await?;
        for s in &cat.schemas { if em.schema(s) != "public" { c.batch_execute(&em.create_schema(s)).await?; } }
        for t in cat.tables.iter().rev() { if o.drop_existing { c.batch_execute(&em.drop_table_if_exists(t)).await?; } }
        for t in cat.tables.iter_mut() {
            let ddl = em.create_table(t);
            c.batch_execute(&ddl).await.with_context(|| format!("creating table {}.{}", t.schema, t.name))?;
            for s in em.enum_checks(t) { c.batch_execute(&s).await?; }
            if let Some(cm) = &t.comment { c.batch_execute(&em.comment(t, None, cm)).await?; }
            for col in &t.columns { if let Some(cm) = &col.comment { c.batch_execute(&em.comment(t, Some(col), cm)).await?; } }
        }
        Ok::<(), anyhow::Error>(())
    })?;
    let total = Arc::new(AtomicI64::new(0));
    if o.with_data {
        let mut order: Vec<usize> = (0..cat.tables.len()).filter(|i| cat.tables[*i].row_count > 0).collect();
        order.sort_by_key(|i| std::cmp::Reverse(cat.tables[*i].row_count));
        let queue = Arc::new(Mutex::new(order));
        let n_workers = o.parallel.max(1).min(queue.lock().unwrap().len().max(1));
        let mut handles = vec![];
        for _ in 0..n_workers {
            let con = crate::core::conn::acquire(con)?;
            let queue = queue.clone();
            let total = total.clone();
            let progress = progress.clone();
            let tables = cat.tables.clone();
            let dsn = o.dsn.clone();
            let sm = em.schema_map.clone();
            handles.push(std::thread::spawn(move || -> Result<()> {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                let em = Emitter::new(Flavor::Postgres, sm);
                let con_ref = &con;
                let r = rt.block_on(async move {
                    let c = connect(&dsn).await?;
                    loop {
                        let idx = { queue.lock().unwrap().pop() };
                        let Some(idx) = idx else { break };
                        let t = &tables[idx];
                        let n = copy_table(con_ref, &c, t, &em, &progress).await.with_context(|| format!("loading {}.{}", t.schema, t.name))?;
                        total.fetch_add(n, Ordering::Relaxed);
                    }
                    Ok::<(), anyhow::Error>(())
                });
                crate::core::conn::release(con);
                r
            }));
        }
        for h in handles { h.join().map_err(|_| anyhow::anyhow!("worker thread panicked"))??; }
    }
    rt.block_on(async {
        let c = connect(&o.dsn).await?;
        for t in &cat.tables {
            for (stmt, must) in em.post_constraints(t) {
                if let Err(e) = c.batch_execute(&stmt).await { if must { return Err(e.into()); } warnings.push(format!("check constraint on {}.{} failed: {e}", t.schema, t.name)); }
            }
            for stmt in em.create_indexes(t) { c.batch_execute(&stmt).await?; }
        }
        for v in &cat.views {
            match em.create_view(v) {
                Ok(stmt) => if let Err(e) = c.batch_execute(&stmt).await { warnings.push(format!("view {}.{} failed on PostgreSQL: {e}", v.schema, v.name)); },
                Err(e) => warnings.push(format!("view {}.{}: not translatable ({e})", v.schema, v.name)),
            }
        }
        Ok::<(), anyhow::Error>(())
    })?;
    warnings.extend(cat.column_warnings());
    Ok(PgReport { rows: total.load(Ordering::Relaxed), seconds: t0.elapsed().as_secs_f64(), warnings })
}

async fn copy_table(con: &Connection, c: &tokio_postgres::Client, t: &Table, em: &Emitter, progress: &Arc<dyn Fn(&str, i64) + Send + Sync>) -> Result<i64> {
    let cols: Vec<String> = t.columns.iter().map(|c| em.ident(&c.name)).collect();
    let copy_sql = format!("COPY {} ({}) FROM STDIN (FORMAT text)", em.table_ref(&t.schema, &t.name), cols.join(", "));
    let sink = c.copy_in(&copy_sql).await?;
    let mut sink = pin!(sink);
    let mut st = con.prepare(&select_sql(t))?;
    let mut rows = st.query([])?;
    let mut buf = String::with_capacity(1 << 20);
    let mut n = 0i64;
    while let Some(r) = rows.next()? {
        for (i, c) in t.columns.iter().enumerate() {
            if i > 0 { buf.push('\t'); }
            copy_text(&mut buf, r.get_ref_unwrap(i), c);
        }
        buf.push('\n');
        n += 1;
        if buf.len() >= (1 << 20) {
            sink.send(bytes::Bytes::from(std::mem::take(&mut buf))).await?;
            if n % 100_000 == 0 { progress(&format!("{}.{}", t.schema, t.name), n); }
        }
    }
    if !buf.is_empty() { sink.send(bytes::Bytes::from(buf)).await?; }
    sink.close().await?;
    progress(&format!("{}.{}", t.schema, t.name), n);
    Ok(n)
}

fn esc(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch { '\\' => out.push_str("\\\\"), '\t' => out.push_str("\\t"), '\n' => out.push_str("\\n"), '\r' => out.push_str("\\r"), c => out.push(c) }
    }
}

fn copy_text(out: &mut String, v: ValueRef<'_>, col: &Column) {
    match v {
        ValueRef::Null => out.push_str("\\N"),
        ValueRef::Boolean(b) => out.push_str(if b { "t" } else { "f" }),
        ValueRef::TinyInt(x) => out.push_str(&x.to_string()),
        ValueRef::SmallInt(x) => out.push_str(&x.to_string()),
        ValueRef::Int(x) => out.push_str(&x.to_string()),
        ValueRef::BigInt(x) => out.push_str(&x.to_string()),
        ValueRef::HugeInt(x) => out.push_str(&x.to_string()),
        ValueRef::UTinyInt(x) => out.push_str(&x.to_string()),
        ValueRef::USmallInt(x) => out.push_str(&x.to_string()),
        ValueRef::UInt(x) => out.push_str(&x.to_string()),
        ValueRef::UBigInt(x) => out.push_str(&x.to_string()),
        ValueRef::Float(f) => out.push_str(&pg_float(f as f64)),
        ValueRef::Double(f) => out.push_str(&pg_float(f)),
        ValueRef::Decimal(d) => out.push_str(&d.to_string()),
        ValueRef::Text(b) => { let s = String::from_utf8_lossy(b); if col.conv == Conv::TsNsText { esc(out, &trim_ns(&s)) } else { esc(out, &s) } }
        ValueRef::Blob(b) => { out.push_str("\\\\x"); out.push_str(&hex::encode(b)); }
        ValueRef::Date32(d) => out.push_str(&date_from_days(d).to_string()),
        ValueRef::Time64(u, t) => out.push_str(&time_from_units(u, t)),
        ValueRef::Timestamp(u, ts) => { out.push_str(&naive_ts(u, ts).format("%Y-%m-%d %H:%M:%S%.6f").to_string()); if col.conv == Conv::DateTimeTz { out.push_str("+00"); } }
        ValueRef::Interval { months, days, nanos } => out.push_str(&format!("{months} months {days} days {} microseconds", nanos / 1000)),
        other => esc(out, &format!("{other:?}")),
    }
}

fn pg_float(f: f64) -> String {
    if f.is_nan() { "NaN".into() } else if f.is_infinite() { if f > 0.0 { "Infinity".into() } else { "-Infinity".into() } } else { format!("{f:?}") }
}
