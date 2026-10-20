//! DuckDB -> SQL Server direct load over the TDS bulk-copy protocol, N tables in parallel.
use super::guard::Guard;
use super::{exec_batch, MsClient, MssqlTarget};
use crate::core::emit::{Emitter, Flavor};
use crate::core::model::*;
use crate::core::script::{date_from_days, naive_ts, select_sql, to_micros, trim_ns};
use crate::core::typemap::quote_duck;
use super::split::ranges;
use anyhow::{bail, Context, Result};
use duckdb::types::ValueRef;
use duckdb::Connection;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tiberius::{ColumnData, TokenRow};

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
            let (mn, mx): (Option<i64>, Option<i64>) = con.query_row(&format!("SELECT CAST(min({0}) AS BIGINT), CAST(max({0}) AS BIGINT) FROM {fq}", quote_duck(k)), [], |r| Ok((r.get(0)?, r.get(1)?)))?;
            let n = ((t.row_count / o.split_threshold.max(1)) as usize + 1).clamp(2, o.parallel * 2);
            for r in ranges(mn.unwrap_or(0) as i128, mx.unwrap_or(0) as i128, n) { jobs.push(Job { table: i, key: Some(k.clone()), range: Some(r) }); }
        } else {
            jobs.push(Job { table: i, key: key.clone(), range: None });
        }
    }
    jobs.sort_by_key(|j| std::cmp::Reverse(cat.tables[j.table].row_count));
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
    let tref = em.table_ref(&t.schema, &t.name);
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    let mut n = 0i64;
    let mut in_batch = 0usize;
    let col_names: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
    let opts = if t.has_identity() { tiberius::SqlBulkCopyOption::KeepIdentity | tiberius::SqlBulkCopyOption::TableLock } else { tiberius::SqlBulkCopyOptions::from(tiberius::SqlBulkCopyOption::TableLock) };
    let mut req = client.bulk_insert_with_options(&tref, &col_refs, opts, &[]).await.with_context(|| format!("starting bulk insert into {tref}"))?;
    while let Some(r) = rows.next()? {
        let mut row = TokenRow::new();
        for (i, c) in t.columns.iter().enumerate() {
            row.push(to_column_data(r.get_ref_unwrap(i), c)?);
        }
        req.send(row).await?;
        n += 1;
        in_batch += 1;
        if in_batch >= batch_rows {
            // finalize + restart to bound server-side transaction size
            req.finalize().await?;
            req = client.bulk_insert_with_options(&tref, &col_refs, opts, &[]).await?;
            in_batch = 0;
            progress(&format!("{}.{}", t.schema, t.name), n);
        }
    }
    req.finalize().await?;
    progress(&format!("{}.{}", t.schema, t.name), n);
    Ok(n)
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

/// Convert a DuckDB value to the TDS column type that the target SQL type expects.
pub fn to_column_data(v: ValueRef<'_>, col: &Column) -> Result<ColumnData<'static>> {
    use ColumnData as C;
    let ty = col.sql_type.to_uppercase();
    let base = ty.split('(').next().unwrap_or("").to_string();
    if matches!(v, ValueRef::Null) {
        return Ok(match base.as_str() {
            "BIT" => C::Bit(None), "TINYINT" => C::U8(None), "SMALLINT" => C::I16(None), "INT" => C::I32(None), "BIGINT" => C::I64(None),
            "REAL" => C::F32(None), "FLOAT" => C::F64(None), "DECIMAL" | "NUMERIC" => C::Numeric(None),
            "DATE" => C::Date(None), "TIME" => C::Time(None), "DATETIME2" => C::DateTime2(None), "DATETIMEOFFSET" => C::DateTimeOffset(None),
            "UNIQUEIDENTIFIER" => C::Guid(None), "VARBINARY" | "BINARY" => C::Binary(None), _ => C::String(None),
        });
    }
    let as_i128 = |v: &ValueRef| -> Option<i128> {
        Some(match *v { ValueRef::TinyInt(x) => x as i128, ValueRef::SmallInt(x) => x as i128, ValueRef::Int(x) => x as i128, ValueRef::BigInt(x) => x as i128,
            ValueRef::HugeInt(x) => x, ValueRef::UTinyInt(x) => x as i128, ValueRef::USmallInt(x) => x as i128, ValueRef::UInt(x) => x as i128, ValueRef::UBigInt(x) => x as i128,
            ValueRef::Boolean(b) => b as i128, _ => return None })
    };
    let as_f64 = |v: &ValueRef| -> Option<f64> {
        Some(match *v { ValueRef::Float(f) => f as f64, ValueRef::Double(f) => f, ValueRef::Decimal(d) => d.to_string().parse().ok()?, _ => as_i128(v)? as f64 })
    };
    let text = |v: &ValueRef| -> String {
        match *v {
            ValueRef::Text(b) => String::from_utf8_lossy(b).to_string(),
            ValueRef::Timestamp(u, ts) => naive_ts(u, ts).format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            ValueRef::Date32(d) => date_from_days(d).to_string(),
            ValueRef::Time64(u, t) => crate::core::script::time_from_units(u, t),
            ValueRef::Interval { months, days, nanos } => format!("{months} months {days} days {} microseconds", nanos / 1000),
            ValueRef::Blob(b) => hex::encode(b),
            other => format!("{other:?}"),
        }
    };
    Ok(match base.as_str() {
        "BIT" => C::Bit(Some(match v { ValueRef::Boolean(b) => b, other => as_i128(&other).map(|x| x != 0).unwrap_or(false) })),
        "TINYINT" => C::U8(Some(as_i128(&v).context("int")? as u8)),
        "SMALLINT" => C::I16(Some(as_i128(&v).context("int")? as i16)),
        "INT" => C::I32(Some(as_i128(&v).context("int")? as i32)),
        "BIGINT" => C::I64(Some(as_i128(&v).context("int")? as i64)),
        "REAL" => { let f = as_f64(&v).context("float")?; if f.is_finite() { C::F32(Some(f as f32)) } else { C::F32(None) } }
        "FLOAT" => { let f = as_f64(&v).context("float")?; if f.is_finite() { C::F64(Some(f)) } else { C::F64(None) } }
        "DECIMAL" | "NUMERIC" => {
            let scale: u32 = ty.split(',').nth(1).and_then(|s| s.trim_end_matches(')').trim().parse().ok()).unwrap_or(0);
            let d: rust_decimal::Decimal = match v {
                ValueRef::Decimal(d) => d.to_string().parse().context("decimal")?,
                ValueRef::Text(b) => String::from_utf8_lossy(b).parse().context("decimal text")?,
                other => match as_i128(&other) { Some(i) => rust_decimal::Decimal::from_i128_with_scale(i, 0), None => rust_decimal::Decimal::from_f64_retain(as_f64(&other).context("decimal")?).context("decimal")? },
            };
            let mut d = d;
            d.rescale(scale);
            C::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(d.mantissa(), d.scale() as u8)))
        }
        "DATE" => match v {
            ValueRef::Date32(d) => C::Date(Some(tiberius::time::Date::new(days_from_ce(date_from_days(d))))),
            ValueRef::Timestamp(u, ts) => { let dt = naive_ts(u, ts); C::Date(Some(tiberius::time::Date::new(days_from_ce(dt.date())))) }
            ValueRef::Text(b) => { let s = String::from_utf8_lossy(b); let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").with_context(|| format!("date {s}"))?; C::Date(Some(tiberius::time::Date::new(days_from_ce(d)))) }
            _ => bail!("cannot convert {v:?} to DATE"),
        },
        "TIME" => match v {
            ValueRef::Time64(u, t) => { let micros = to_micros(u, t); C::Time(Some(tiberius::time::Time::new((micros * 10) as u64, 7))) }
            ValueRef::Text(b) => { let s = String::from_utf8_lossy(b); let t = parse_naive_time(&s)?; use chrono::Timelike; let ticks = t.num_seconds_from_midnight() as u64 * 10_000_000 + t.nanosecond() as u64 / 100; C::Time(Some(tiberius::time::Time::new(ticks, 7))) }
            _ => bail!("cannot convert {v:?} to TIME"),
        },
        "DATETIME2" => match v {
            ValueRef::Timestamp(u, ts) => { let dt = naive_ts(u, ts); C::DateTime2(Some(dt2(dt))) }
            ValueRef::Date32(d) => C::DateTime2(Some(dt2(date_from_days(d).and_hms_opt(0, 0, 0).unwrap()))),
            ValueRef::Text(b) => { let s = trim_ns(&String::from_utf8_lossy(b)); C::DateTime2(Some(dt2(parse_naive_ts(&s)?))) }
            _ => bail!("cannot convert {v:?} to DATETIME2"),
        },
        "DATETIMEOFFSET" => match v {
            ValueRef::Timestamp(u, ts) => { let dt = naive_ts(u, ts); C::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(dt2(dt), 0))) }
            ValueRef::Text(b) => { let s = String::from_utf8_lossy(b); let (dt, off_min) = parse_ts_offset(&s)?; C::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(dt2(dt), off_min))) }
            _ => bail!("cannot convert {v:?} to DATETIMEOFFSET"),
        },
        "UNIQUEIDENTIFIER" => match v { ValueRef::Text(b) => C::Guid(Some(uuid::Uuid::parse_str(&String::from_utf8_lossy(b)).context("uuid")?)), other => { let s = format!("{other:?}"); C::Guid(uuid::Uuid::parse_str(s.trim_matches(|c| c == '"' || c == '\'' )).ok()) } },
        "VARBINARY" | "BINARY" => match v { ValueRef::Blob(b) => C::Binary(Some(b.to_vec().into())), ValueRef::Text(b) => C::Binary(Some(b.to_vec().into())), _ => bail!("cannot convert {v:?} to binary") },
        _ => C::String(Some(text(&v).into())),
    })
}

fn parse_naive_ts(s: &str) -> Result<chrono::NaiveDateTime> {
    let s = s.trim();
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(s, f) { return Ok(d); }
        if f == "%Y-%m-%d" { if let Ok(d) = chrono::NaiveDate::parse_from_str(s, f) { return Ok(d.and_hms_opt(0, 0, 0).unwrap()); } }
    }
    bail!("cannot parse timestamp {s:?}")
}

fn parse_naive_time(s: &str) -> Result<chrono::NaiveTime> {
    let s = s.trim();
    for f in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(t) = chrono::NaiveTime::parse_from_str(s, f) { return Ok(t); }
    }
    bail!("cannot parse time {s:?}")
}

/// "2024-01-01 10:00:00.123456+00:00" / "+00" / "Z" -> (local naive datetime, offset minutes)
fn parse_ts_offset(s: &str) -> Result<(chrono::NaiveDateTime, i16)> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) { return Ok((dt.naive_local(), (dt.offset().local_minus_utc() / 60) as i16)); }
    for f in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f %:z", "%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%:z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, f) { return Ok((dt.naive_local(), (dt.offset().local_minus_utc() / 60) as i16)); }
    }
    Ok((parse_naive_ts(s.trim_end_matches('Z'))?, 0))
}

fn days_from_ce(d: chrono::NaiveDate) -> u32 {
    use chrono::Datelike;
    (d.num_days_from_ce() - 1) as u32
}

fn dt2(dt: chrono::NaiveDateTime) -> tiberius::time::DateTime2 {
    use chrono::Timelike;
    let date = tiberius::time::Date::new(days_from_ce(dt.date()));
    let secs = dt.num_seconds_from_midnight() as u64;
    let nanos = dt.nanosecond() as u64;
    let increments = secs * 10_000_000 + nanos / 100; // 100ns ticks, scale 7
    tiberius::time::DateTime2::new(date, tiberius::time::Time::new(increments, 7))
}
