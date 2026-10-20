//! Stream a self-contained, restorable SQL script (T-SQL or PostgreSQL) from a DuckDB database.
use super::emit::{Emitter, Flavor, Literal};
use super::model::*;
use super::typemap::quote_duck;
use anyhow::Result;
use super::duck::{Connection, TimeUnit, ValueRef};
use std::io::Write;

pub struct ScriptOptions {
    pub database: Option<String>,
    pub create_db: bool,
    pub with_data: bool,
    pub drop_existing: bool,
    pub rows_per_insert: Option<usize>,
    pub statements_per_batch: usize,
}

pub fn select_sql(t: &Table) -> String {
    let cols: Vec<&str> = t.columns.iter().map(|c| c.select_expr.as_str()).collect();
    format!("SELECT {} FROM {}.{}", cols.join(", "), quote_duck(&t.schema), quote_duck(&t.name))
}

pub fn write_script<W: Write>(con: &Connection, cat: &mut Catalog, em: &Emitter, out: &mut W, o: &ScriptOptions,
                              progress: &mut dyn FnMut(&str, i64)) -> Result<Vec<String>> {
    let mut warnings = vec![];
    let term = em.batch_terminator();
    let rpi = o.rows_per_insert.unwrap_or(em.max_rows_per_insert()).min(em.max_rows_per_insert());
    macro_rules! go { () => { if let Some(t) = term { writeln!(out, "{t}")?; } } }

    writeln!(out, "{}", em.script_header(cat, o.database.as_deref(), o.create_db))?;
    go!();

    writeln!(out, "\n-- ===== schemas =====")?;
    for s in &cat.schemas {
        if em.schema(s) != em.default_schema() {
            writeln!(out, "{}", em.create_schema(s))?;
        }
    }
    go!();

    writeln!(out, "\n-- ===== tables =====")?;
    if o.drop_existing {
        for v in &cat.views {
            writeln!(out, "{}", em.drop_view_if_exists(v))?;
        }
        for t in cat.tables.iter().rev() {
            writeln!(out, "{}", em.drop_table_if_exists(t))?;
        }
        go!();
    }
    for t in &mut cat.tables {
        writeln!(out, "{}", em.create_table(t))?;
        for s in em.enum_checks(t) {
            writeln!(out, "{s}")?;
        }
        if let Some(c) = &t.comment {
            writeln!(out, "{}", em.comment(t, None, c))?;
        }
        for c in &t.columns {
            if let Some(cm) = &c.comment {
                writeln!(out, "{}", em.comment(t, Some(c), cm))?;
            }
        }
        go!();
    }

    if o.with_data {
        writeln!(out, "\n-- ===== data =====")?;
        for t in &cat.tables {
            if t.row_count == 0 {
                continue;
            }
            writeln!(out, "-- {}.{}: {} rows", t.schema, t.name, t.row_count)?;
            if let Some(s) = em.identity_insert(t, true) {
                writeln!(out, "{s}")?;
            }
            let mut st = con.prepare(&select_sql(t))?;
            let mut rows = st.query([])?;
            let mut batch: Vec<Vec<Literal>> = Vec::with_capacity(rpi);
            let (mut n_stmts, mut n_rows) = (0usize, 0i64);
            while let Some(r) = rows.next()? {
                let lits: Vec<Literal> = t.columns.iter().enumerate().map(|(i, c)| literal(em.flavor, r.get_ref_unwrap(i), c)).collect();
                batch.push(lits);
                n_rows += 1;
                if batch.len() >= rpi {
                    writeln!(out, "{}", em.insert_batch(t, &batch))?;
                    batch.clear();
                    n_stmts += 1;
                    if n_stmts % o.statements_per_batch == 0 { go!(); progress(&t.name, n_rows); }
                }
            }
            if !batch.is_empty() {
                writeln!(out, "{}", em.insert_batch(t, &batch))?;
            }
            if let Some(s) = em.identity_insert(t, false) {
                writeln!(out, "{s}")?;
            }
            if let Some(s) = em.reseed_identity(t) {
                writeln!(out, "{s}")?;
            }
            progress(&t.name, n_rows);
            go!();
        }
    }

    writeln!(out, "\n-- ===== constraints & indexes =====")?;
    let mut deferred = vec![];
    for t in &cat.tables {
        for (stmt, must) in em.post_constraints(t) {
            if must { writeln!(out, "{stmt}")?; } else { deferred.push(stmt); }
        }
        for s in em.create_indexes(t) {
            writeln!(out, "{s}")?;
        }
        go!();
    }
    if !deferred.is_empty() {
        writeln!(out, "\n-- ===== check constraints (best effort, translated from DuckDB) =====")?;
        for s in deferred {
            writeln!(out, "{}", em.guarded(&s))?;
        }
        go!();
    }
    if !cat.views.is_empty() {
        writeln!(out, "\n-- ===== views (translated from DuckDB SQL) =====")?;
        for v in &cat.views {
            match em.create_view(v) {
                Ok(s) => { writeln!(out, "{}", em.guarded(&s))?; }
                Err(e) => {
                    warnings.push(format!("view {}.{}: not translatable ({e}); emitted as comment", v.schema, v.name));
                    writeln!(out, "/* VIEW {}.{} could not be translated automatically. Original DuckDB SQL:\n{}\n*/", v.schema, v.name, v.sql.replace("*/", "* /"))?;
                }
            }
            go!();
        }
    }
    writeln!(out, "\n-- ===== done =====")?;
    warnings.extend(cat.column_warnings());
    Ok(warnings)
}

/// Render a DuckDB value as a SQL literal in the target flavor.
pub fn literal(flavor: Flavor, v: ValueRef<'_>, col: &Column) -> Literal {
    let mssql = flavor == Flavor::Mssql;
    let str_lit = |s: &str| -> String {
        let body = s.replace('\'', "''");
        if mssql { format!("N'{body}'") } else { format!("'{body}'") }
    };
    match v {
        ValueRef::Null => "NULL".into(),
        ValueRef::Boolean(b) => if mssql { if b { "1" } else { "0" }.into() } else { if b { "TRUE" } else { "FALSE" }.into() },
        ValueRef::TinyInt(x) => x.to_string(),
        ValueRef::SmallInt(x) => x.to_string(),
        ValueRef::Int(x) => x.to_string(),
        ValueRef::BigInt(x) => x.to_string(),
        ValueRef::HugeInt(x) => x.to_string(),
        ValueRef::UTinyInt(x) => x.to_string(),
        ValueRef::USmallInt(x) => x.to_string(),
        ValueRef::UInt(x) => x.to_string(),
        ValueRef::UBigInt(x) => x.to_string(),
        ValueRef::Float(f) => float_lit(f as f64, mssql),
        ValueRef::Double(f) => float_lit(f, mssql),
        ValueRef::Decimal(d) => d.to_string(),
        ValueRef::Text(b) => {
            let s = String::from_utf8_lossy(b);
            if col.conv == Conv::TsNsText { format!("'{}'", trim_ns(&s)) } else { str_lit(&s) }
        }
        ValueRef::Blob(b) => if mssql { if b.is_empty() { "0x".into() } else { format!("0x{}", hex::encode(b)) } } else { format!("'\\x{}'::bytea", hex::encode(b)) },
        ValueRef::Date32(d) => format!("'{}'", date_from_days(d)),
        ValueRef::Time64(unit, t) => format!("'{}'", time_from_units(unit, t)),
        ValueRef::Timestamp(unit, ts) => {
            if col.conv == Conv::DateTimeTz { format!("'{} +00:00'", naive_ts(unit, ts).format("%Y-%m-%d %H:%M:%S%.6f")) } else { format!("'{}'", ts_from_units(unit, ts)) }
        }
        ValueRef::Interval { months, days, nanos } => str_lit(&format!("{months} months {days} days {} microseconds", nanos / 1000)),
        other => {
            // enums, lists, structs are read via select_expr casts; anything else falls back to Debug text
            let s = format!("{other:?}");
            str_lit(&s)
        }
    }
}

fn float_lit(f: f64, mssql: bool) -> String {
    if f.is_nan() { return if mssql { "NULL".into() } else { "'NaN'".into() }; }
    if f.is_infinite() { return if mssql { "NULL".into() } else if f > 0.0 { "'Infinity'".into() } else { "'-Infinity'".into() }; }
    let s = format!("{f:?}");
    if s.ends_with(".0") { s } else { s }
}

pub fn trim_ns(s: &str) -> String {
    match s.rfind('.') {
        Some(i) if s.len() - i - 1 > 7 => s[..i + 8].to_string(),
        _ => s.to_string(),
    }
}

pub fn date_from_days(d: i32) -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(d as i64)
}

pub fn time_from_units(unit: TimeUnit, t: i64) -> String {
    let micros = to_micros(unit, t);
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    format!("{:02}:{:02}:{:02}.{:06}", secs / 3600, (secs % 3600) / 60, secs % 60, frac)
}

pub fn to_micros(unit: TimeUnit, t: i64) -> i64 {
    match unit {
        TimeUnit::Second => t * 1_000_000,
        TimeUnit::Millisecond => t * 1_000,
        TimeUnit::Microsecond => t,
        TimeUnit::Nanosecond => t.div_euclid(1_000),
    }
}

pub fn ts_from_units(unit: TimeUnit, ts: i64) -> String {
    let micros = to_micros(unit, ts);
    let dt = chrono::DateTime::from_timestamp_micros(micros).map(|d| d.naive_utc()).unwrap_or_default();
    dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

pub fn naive_ts(unit: TimeUnit, ts: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_micros(to_micros(unit, ts)).map(|d| d.naive_utc()).unwrap_or_default()
}
