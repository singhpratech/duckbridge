//! High-level operations shared by the CLI and the DuckDB extension.
//!
//! Everything here works on an open DuckDB [`Connection`] and reports results as
//! plain [`Row`]s so that the extension can return them as a table and the CLI can print them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use duckdb::Connection;

use crate::core::catalog::{read_catalog, CatalogOptions};
use crate::core::emit::{Emitter, Flavor};
use crate::core::script::{write_script, ScriptOptions};
use crate::core::typemap::{StringMode, Target};
use crate::mssql::{self, MssqlTarget};
use crate::storage::{self, Location};
use crate::transpile::{self, Dialect};

/// One result line of an operation (a table copied, a warning, a summary...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub object: String,
    pub kind: String,
    pub rows: i64,
    pub note: String,
}

impl Row {
    fn new(object: impl Into<String>, kind: &str, rows: i64, note: impl Into<String>) -> Row {
        Row { object: object.into(), kind: kind.into(), rows, note: note.into() }
    }
    fn warn(w: &str) -> Row { Row::new("", "warning", 0, w) }
}

/// A parsed connection URL such as
/// `mssql://sa:secret@sql01:1433/Sales?trust_cert=true&encrypt=true`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dsn {
    pub scheme: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub params: HashMap<String, String>,
}

fn pct(s: &str) -> String { percent_encoding::percent_decode_str(s).decode_utf8_lossy().into_owned() }

pub fn parse_dsn(s: &str) -> Result<Dsn> {
    let (scheme, rest) = s.split_once("://").ok_or_else(|| anyhow!("connection string must look like scheme://user:password@host:port/database, got {s:?}"))?;
    let (main, query) = rest.split_once('?').map(|(m, q)| (m, Some(q))).unwrap_or((rest, None));
    let (userinfo, hostpart) = match main.rfind('@') { Some(i) => (Some(&main[..i]), &main[i + 1..]), None => (None, main) };
    let (hostport, database) = hostpart.split_once('/').map(|(h, d)| (h, Some(d))).unwrap_or((hostpart, None));
    // accept both host:port and SQL Server's host,port
    let hp = hostport.replace(',', ":");
    let (host, port) = match hp.rfind(':') {
        Some(i) if !hp[i + 1..].is_empty() && hp[i + 1..].chars().all(|c| c.is_ascii_digit()) && !hp[..i].contains(']') || (hp.starts_with('[') && hp[..hp.rfind(':').unwrap()].ends_with(']')) => {
            let i = hp.rfind(':').unwrap();
            (hp[..i].trim_matches(['[', ']']).to_string(), Some(hp[i + 1..].parse::<u16>().with_context(|| format!("bad port in {hostport:?}"))?))
        }
        _ => (hp.trim_matches(['[', ']']).to_string(), None),
    };
    let (user, password) = match userinfo {
        Some(u) => match u.split_once(':') { Some((a, b)) => (Some(pct(a)), Some(pct(b))), None => (Some(pct(u)), None) },
        None => (None, None),
    };
    let mut params = HashMap::new();
    if let Some(q) = query {
        for kv in q.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = kv.split_once('=').unwrap_or((kv, "true"));
            params.insert(pct(k).to_lowercase(), pct(v));
        }
    }
    Ok(Dsn { scheme: scheme.to_lowercase(), user, password, host: if host.is_empty() { "localhost".into() } else { host }, port, database: database.filter(|d| !d.is_empty()).map(pct), params })
}

impl Dsn {
    fn flag(&self, k: &str, default: bool) -> bool {
        self.params.get(k).map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on")).unwrap_or(default)
    }
    /// Build a SQL Server target; `database` overrides the one in the URL.
    pub fn mssql_target(&self, database: Option<&str>) -> MssqlTarget {
        MssqlTarget {
            host: self.host.clone(),
            port: self.port.unwrap_or(1433),
            user: self.user.clone().unwrap_or_default(),
            password: self.password.clone().unwrap_or_default(),
            database: database.map(|s| s.to_string()).or_else(|| self.database.clone()).unwrap_or_else(|| "master".into()),
            trust_cert: self.flag("trust_cert", true) || self.flag("trustservercertificate", true),
            encrypt: self.flag("encrypt", true),
            windows_auth: self.flag("windows_auth", false) || self.flag("integrated", false) || self.flag("trusted_connection", false) || self.flag("integrated_security", false),
        }
    }
}

pub fn parse_schema_map(spec: &str, default: &[(&str, &str)]) -> HashMap<String, String> {
    let mut m: HashMap<String, String> = default.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
    for kv in spec.split([',', ';']).map(str::trim).filter(|s| !s.is_empty()) {
        if let Some((a, b)) = kv.split_once('=') { m.insert(a.trim().to_string(), b.trim().to_string()); }
    }
    m
}

pub fn csv_list(s: &str) -> Vec<String> { s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect() }

fn cores(n: usize) -> usize { if n == 0 { num_cpus::get() } else { n } }

// ---------------------------------------------------------------- import

#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    /// 0 = number of cores
    pub parallel: usize,
    pub split_threshold: i64,
    pub batch_rows: usize,
    pub schema_map: String,
    pub nolock: bool,
    pub schema_only: bool,
    pub keys_after_load: bool,
    /// Drop target tables/views that already exist in DuckDB before creating them.
    pub overwrite: bool,
}

impl Default for ImportRequest {
    fn default() -> Self {
        ImportRequest { tables: vec![], exclude: vec![], parallel: 0, split_threshold: 20_000_000, batch_rows: 50_000, schema_map: String::new(), nolock: false, schema_only: false, keys_after_load: false, overwrite: false }
    }
}

/// Copy a SQL Server database into the DuckDB database behind `con`.
pub fn import_mssql(con: &Connection, dsn: &Dsn, r: &ImportRequest) -> Result<Vec<Row>> {
    use crate::mssql::import::*;
    if dsn.scheme != "mssql" && dsn.scheme != "sqlserver" { bail!("bridge_import expects an mssql:// URL, got {}://", dsn.scheme); }
    let database = dsn.database.clone().ok_or_else(|| anyhow!("the URL must name the source database: mssql://user:pass@host:port/Database"))?;
    let target = dsn.mssql_target(Some(&database));
    let opts = ImportOptions { keys_after_load: r.keys_after_load, include: r.tables.clone(), exclude: r.exclude.clone(), parallel: cores(r.parallel), split_threshold: r.split_threshold, batch_rows: r.batch_rows, schema_map: parse_schema_map(&r.schema_map, &[("dbo", "main")]), nolock: r.nolock };
    let rt = mssql::runtime()?;
    let mut warnings = vec![];
    let (tables, views) = rt.block_on(async {
        let mut c = target.connect(&database).await?;
        read_catalog(&mut c, &opts, &mut warnings).await
    })?;
    let t0 = std::time::Instant::now();
    if r.overwrite {
        use crate::core::typemap::quote_duck;
        let mapped = |s: &str| opts.schema_map.get(s).cloned().unwrap_or_else(|| s.to_string());
        for v in &views { con.execute_batch(&format!("DROP VIEW IF EXISTS {}.{}", quote_duck(&mapped(&v.schema)), quote_duck(&v.name)))?; }
        for t in &tables { con.execute_batch(&format!("DROP TABLE IF EXISTS {}.{} CASCADE", quote_duck(&mapped(&t.schema)), quote_duck(&t.name)))?; }
    }
    create_duck_schema(con, &tables, &views, &opts, &mut warnings)?;
    let mut out = vec![];
    let mut total = 0i64;
    if !r.schema_only {
        let progress: Arc<dyn Fn(&str, i64) + Send + Sync> = Arc::new(|_, _| {});
        let rep = copy_data(con, &target, &tables, &opts, progress)?;
        warnings.extend(rep.warnings);
        total = rep.rows;
        add_keys(con, &tables, &opts, &mut warnings)?;
    }
    create_duck_views(con, &views, &opts, &mut warnings);
    // Flush the WAL now: inside the extension the host may never close the instance itself.
    let _ = con.execute_batch("CHECKPOINT");
    for t in &tables {
        let schema = opts.schema_map.get(&t.schema).cloned().unwrap_or_else(|| t.schema.clone());
        out.push(Row::new(format!("{schema}.{}", t.name), "table", if r.schema_only { 0 } else { t.row_count }, if r.schema_only { "schema only" } else { "" }));
    }
    for v in &views {
        let schema = opts.schema_map.get(&v.schema).cloned().unwrap_or_else(|| v.schema.clone());
        out.push(Row::new(format!("{schema}.{}", v.name), "view", 0, ""));
    }
    let secs = t0.elapsed().as_secs_f64();
    out.push(Row::new("*", "summary", total, format!("{} tables, {} views, {:.1}s, {:.0} rows/s", tables.len(), views.len(), secs, total as f64 / secs.max(0.001))));
    out.extend(warnings.iter().map(|w| Row::warn(w)));
    Ok(out)
}

// ---------------------------------------------------------------- export

#[derive(Debug, Clone)]
pub struct ExportRequest {
    pub database: Option<String>,
    pub tables: Vec<String>,
    pub exclude: Vec<String>,
    pub schema_map: String,
    /// 0 = number of cores
    pub parallel: usize,
    pub drop_existing: bool,
    pub schema_only: bool,
    pub target_version: u16,
    /// "fit" (size NVARCHAR from the data) or "max"
    pub string_mode: String,
    /// "bulk" or "insert"
    pub method: String,
    pub pk_inline: bool,
    pub order_by_key: bool,
    pub split_threshold: i64,
    pub batch_rows: usize,
    /// Script export: include CREATE DATABASE / USE
    pub create_database: bool,
    pub parquet_compression: String,
    /// Label used in script headers
    pub source_label: String,
}

impl Default for ExportRequest {
    fn default() -> Self {
        ExportRequest { database: None, tables: vec![], exclude: vec![], schema_map: String::new(), parallel: 0, drop_existing: false, schema_only: false, target_version: 2019, string_mode: "fit".into(), method: "bulk".into(), pk_inline: false, order_by_key: false, split_threshold: 5_000_000, batch_rows: 100_000, create_database: false, parquet_compression: "zstd".into(), source_label: "duckdb".into() }
    }
}

/// Options for producing a `.bak` after a live load.
#[derive(Debug, Clone)]
pub struct BakRequest {
    /// Where the .bak should end up (local path or s3:// az:// gs://); "" keeps it on the server.
    pub out: String,
    pub backup_dir: String,
    /// "local" | "docker:<container>" | "none"
    pub fetch: String,
    pub stripes: usize,
    pub compression: bool,
    pub verify_restore: bool,
    pub drop_staging: bool,
    pub to_url: Option<String>,
}

impl Default for BakRequest {
    fn default() -> Self {
        BakRequest { out: String::new(), backup_dir: "/var/opt/mssql/backup".into(), fetch: "none".into(), stripes: 1, compression: true, verify_restore: false, drop_staging: false, to_url: None }
    }
}

fn catalog_for(con: &Connection, r: &ExportRequest, target: Target) -> Result<crate::core::model::Catalog> {
    crate::core::typemap::set_mssql_version(r.target_version);
    let opts = CatalogOptions { include: r.tables.clone(), exclude: r.exclude.clone(), string_mode: if r.string_mode.eq_ignore_ascii_case("max") { StringMode::Max } else { StringMode::Fit }, target, measure: true };
    read_catalog(con, &r.source_label, &opts)
}

fn table_rows(cat: &crate::core::model::Catalog, em: &Emitter, with_data: bool) -> Vec<Row> {
    let mut out: Vec<Row> = cat.tables.iter().map(|t| Row::new(format!("{}.{}", em.schema(&t.schema), t.name), "table", if with_data { t.row_count } else { 0 }, "")).collect();
    out.extend(cat.views.iter().map(|v| Row::new(format!("{}.{}", em.schema(&v.schema), v.name), "view", 0, "")));
    out
}

/// Live load into SQL Server (and optionally a `.bak` of the result).
pub fn export_mssql(con: &Connection, dsn: &Dsn, r: &ExportRequest, bak: Option<&BakRequest>) -> Result<Vec<Row>> {
    use crate::mssql::backup::*;
    use crate::mssql::load::*;
    let db_name = r.database.clone().or_else(|| dsn.database.clone()).ok_or_else(|| anyhow!("name the target database: database := 'Name' or mssql://.../Name"))?;
    let mut cat = catalog_for(con, r, Target::Mssql)?;
    let em = Emitter::new(Flavor::Mssql, parse_schema_map(&r.schema_map, &[("main", "dbo")]));
    let target = dsn.mssql_target(Some("master"));
    let lo = LoadOptions { database: db_name.clone(), drop_existing: r.drop_existing, with_data: !r.schema_only, parallel: cores(r.parallel), batch_rows: r.batch_rows, method: if r.method.eq_ignore_ascii_case("insert") { LoadMethod::Insert } else { LoadMethod::Bulk }, recovery_simple: true, pk_after_load: !r.pk_inline, split_threshold: r.split_threshold, order_by_key: r.order_by_key };
    let rt = mssql::runtime()?;
    let mut warnings = vec![];
    let t0 = std::time::Instant::now();
    let mut c = rt.block_on(async {
        let mut m = target.connect("master").await?;
        prepare_schema(&mut m, &target, &mut cat, &em, &lo).await
    })?;
    let mut total = 0;
    if lo.with_data {
        let progress: Arc<dyn Fn(&str, i64) + Send + Sync> = Arc::new(|_, _| {});
        let rep = load_data(con, &cat, &target, &em, &lo, progress)?;
        warnings.extend(rep.warnings);
        total = rep.rows;
    }
    rt.block_on(finish_schema(&mut c, &cat, &em, &lo, &mut warnings))?;
    let mut out = table_rows(&cat, &em, lo.with_data);
    let secs = t0.elapsed().as_secs_f64();
    out.push(Row::new(format!("[{db_name}]"), "summary", total, format!("loaded on {}:{} in {:.1}s ({:.0} rows/s)", target.host, target.port, secs, total as f64 / secs.max(0.001))));

    if let Some(b) = bak {
        let loc = if b.out.is_empty() { None } else { Some(Location::parse(&b.out)?) };
        let file_name = loc.as_ref().map(|l| l.file_name()).unwrap_or_else(|| format!("{db_name}.bak"));
        let bo = BackupOptions { server_dir: b.backup_dir.clone(), compression: b.compression, stripes: b.stripes.max(1), file_name, to_url: b.to_url.clone() };
        let mut m = rt.block_on(target.connect("master"))?;
        let br = rt.block_on(backup(&mut m, &db_name, &bo))?;
        out.push(Row::new(br.server_paths.join(", "), "bak", br.bytes as i64, format!("written on the server in {:.1}s", br.seconds)));
        if b.to_url.is_none() {
            if b.verify_restore {
                let vdb = format!("{db_name}_verify");
                let counts = rt.block_on(verify_restore(&target, &br.server_paths, &vdb, false))?;
                let mut mism = 0;
                for t in &cat.tables {
                    let key = format!("{}.{}", em.schema(&t.schema), t.name);
                    let got = counts.get(&key).copied().unwrap_or(-1);
                    if got != t.row_count { mism += 1; out.push(Row::new(key, "verify-mismatch", got, format!("expected {} rows", t.row_count))); }
                }
                if mism > 0 { bail!("verify failed: {mism} table(s) differ after restoring the .bak"); }
                out.push(Row::new(format!("[{vdb}]"), "verify", cat.tables.len() as i64, "restored the .bak; all table row counts match"));
            }
            if let Some(loc) = &loc {
                let how = if b.fetch == "local" { Fetch::Local } else if let Some(c) = b.fetch.strip_prefix("docker:") { Fetch::Docker(c.into()) } else { Fetch::None };
                if !matches!(how, Fetch::None) {
                    let local_dir = if loc.is_local() { PathBuf::from(&b.out).parent().map(|p| p.to_path_buf()).filter(|p| !p.as_os_str().is_empty()).unwrap_or(PathBuf::from(".")) } else { std::env::temp_dir().join("duckbridge") };
                    std::fs::create_dir_all(&local_dir)?;
                    let files = fetch(&br.server_paths, &how, &local_dir)?;
                    for f in &files {
                        let dest = if files.len() == 1 { loc.clone() } else { Location::parse(&b.out.replace(&loc.file_name(), &f.file_name().unwrap().to_string_lossy()))? };
                        if !loc.is_local() { storage::upload(f, &dest, &mut |_, _| {})?; }
                        else if files.len() == 1 && f != &PathBuf::from(&b.out) { std::fs::rename(f, &b.out).or_else(|_| std::fs::copy(f, &b.out).map(|_| ()))?; }
                        out.push(Row::new(dest.to_string(), "bak-file", std::fs::metadata(f).map(|m| m.len() as i64).unwrap_or(0), format!("RESTORE DATABASE [{db_name}] FROM DISK = N'<path>' WITH MOVE ..., REPLACE")));
                    }
                }
            }
        }
        if b.drop_staging {
            rt.block_on(mssql::exec_batch(&mut m, &format!("ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {0}", mssql::bracket(&db_name))))?;
            out.push(Row::new(format!("[{db_name}]"), "dropped", 0, "staging database dropped"));
        }
    }
    warnings.extend(cat.column_warnings());
    out.extend(warnings.iter().map(|w| Row::warn(w)));
    Ok(out)
}

/// Write a T-SQL script (schema + data) to a local path or object-store URL.
pub fn export_script(con: &Connection, out_path: &str, r: &ExportRequest) -> Result<Vec<Row>> {
    let mut cat = catalog_for(con, r, Target::Mssql)?;
    let em = Emitter::new(Flavor::Mssql, parse_schema_map(&r.schema_map, &[("main", "dbo")]));
    let loc = Location::parse(out_path)?;
    let local = if loc.is_local() { PathBuf::from(out_path) } else { std::env::temp_dir().join("duckbridge").join(loc.file_name()) };
    if let Some(p) = local.parent() { if !p.as_os_str().is_empty() { std::fs::create_dir_all(p)?; } }
    let f = std::fs::File::create(&local)?;
    let mut w = std::io::BufWriter::with_capacity(4 << 20, f);
    let so = ScriptOptions { database: r.database.clone(), create_db: r.create_database, with_data: !r.schema_only, drop_existing: r.drop_existing, rows_per_insert: None, statements_per_batch: 50 };
    let ws = write_script(con, &mut cat, &em, &mut w, &so, &mut |_, _| {})?;
    std::io::Write::flush(&mut w)?;
    drop(w);
    if !loc.is_local() { storage::upload(&local, &loc, &mut |_, _| {})?; }
    let mut out = table_rows(&cat, &em, !r.schema_only);
    out.push(Row::new(loc.to_string(), "script", std::fs::metadata(&local).map(|m| m.len() as i64).unwrap_or(0), "bytes written; run with sqlcmd -i"));
    out.extend(ws.iter().map(|w| Row::warn(w)));
    Ok(out)
}

/// Load into PostgreSQL. `pg_dsn` is anything tokio-postgres accepts (URL or key=value).
pub fn export_postgres(con: &Connection, pg_dsn: &str, r: &ExportRequest) -> Result<Vec<Row>> {
    let mut cat = catalog_for(con, r, Target::Postgres)?;
    let em = Emitter::new(Flavor::Postgres, parse_schema_map(&r.schema_map, &[("main", "public")]));
    let po = crate::pg::PgOptions { dsn: pg_dsn.to_string(), drop_existing: r.drop_existing, with_data: !r.schema_only, parallel: cores(r.parallel) };
    let progress: Arc<dyn Fn(&str, i64) + Send + Sync> = Arc::new(|_, _| {});
    let rep = crate::pg::load(con, &mut cat, &em, &po, progress)?;
    let mut out = table_rows(&cat, &em, !r.schema_only);
    out.push(Row::new("postgresql", "summary", rep.rows, format!("loaded in {:.1}s", rep.seconds)));
    out.extend(rep.warnings.iter().map(|w| Row::warn(w)));
    Ok(out)
}

/// Write one Parquet file per table plus a portable `schema.sql`.
pub fn export_parquet(con: &Connection, out_dir: &str, r: &ExportRequest) -> Result<Vec<Row>> {
    let mut cat = catalog_for(con, r, Target::Postgres)?;
    let em = Emitter::new(Flavor::Postgres, parse_schema_map(&r.schema_map, &[("main", "public")]));
    let loc = Location::parse(out_dir)?;
    let local_dir = if loc.is_local() { PathBuf::from(out_dir) } else { std::env::temp_dir().join("duckbridge").join(loc.file_name()) };
    let po = crate::parquet::ParquetOptions { compression: r.parquet_compression.clone(), row_group_size: 122_880, partition_rows: None };
    let files = crate::parquet::export_parquet(con, &cat, &local_dir, &po, &mut |_, _| {})?;
    let mut ddl = String::new();
    for t in cat.tables.iter_mut() { ddl.push_str(&em.create_table(t)); ddl.push('\n'); }
    std::fs::write(local_dir.join("schema.sql"), ddl)?;
    let mut out = vec![];
    for (name, f) in &files {
        let dest = if loc.is_local() { f.display().to_string() } else {
            let d = Location::parse(&format!("{}/{}", out_dir.trim_end_matches('/'), f.file_name().unwrap().to_string_lossy()))?;
            storage::upload(f, &d, &mut |_, _| {})?;
            d.to_string()
        };
        let rows = cat.tables.iter().find(|t| &t.name == name || &format!("{}.{}", t.schema, t.name) == name).map(|t| t.row_count).unwrap_or(0);
        out.push(Row::new(dest, "parquet", rows, std::fs::metadata(f).map(|m| format!("{} bytes", m.len())).unwrap_or_default()));
    }
    if !loc.is_local() { storage::upload(&local_dir.join("schema.sql"), &Location::parse(&format!("{}/schema.sql", out_dir.trim_end_matches('/')))?, &mut |_, _| {})?; }
    out.push(Row::new(format!("{}/schema.sql", out_dir.trim_end_matches('/')), "schema", cat.tables.len() as i64, "portable DDL"));
    out.extend(cat.column_warnings().iter().map(|w| Row::warn(w)));
    Ok(out)
}

// ---------------------------------------------------------------- run

/// Translate a script from `from` to DuckDB SQL and execute it statement by statement on `con`.
/// The SQL Server compatibility prelude is applied to this session only.
pub fn run_script(con: &Connection, sql: &str, from: Dialect, nocase: bool, schema_map: &str, continue_on_error: bool) -> Result<Vec<Row>> {
    let sm = parse_schema_map(schema_map, &[("dbo", "main")]);
    let opts = transpile::Options { prelude: false, nocase, ..transpile::Options::default() };
    let tr = transpile::translate_with(sql, from, Dialect::DuckDb, &sm, &opts)?;
    let mut out: Vec<Row> = tr.warnings.iter().map(|w| Row::warn(w)).collect();
    if from == Dialect::Tsql {
        for p in transpile::duckdb_prelude(nocase) {
            // keep the compatibility settings local to this session
            let s = p.trim_start().strip_prefix("SET ").map(|rest| format!("SET SESSION {rest}")).unwrap_or_else(|| p.to_string());
            con.execute_batch(&s)?;
        }
    }
    let mut failed = 0;
    for stmt in transpile::tokens::statements(&transpile::tokens::tokenize(&tr.sql, false)) {
        let s = transpile::tokens::join(&stmt).trim().to_string();
        if s.is_empty() || s.starts_with("--") { continue; }
        let label: String = s.lines().next().unwrap_or("").chars().take(120).collect();
        let up = s.to_uppercase();
        let t0 = std::time::Instant::now();
        let res: Result<i64> = if up.starts_with("SELECT") || up.starts_with("WITH") || up.starts_with("FROM") {
            (|| { let mut st = con.prepare(&s)?; let mut rows = st.query([])?; let mut n = 0i64; while rows.next()?.is_some() { n += 1; } Ok(n) })()
        } else {
            con.execute(&s, []).map(|n| n as i64).map_err(|e| e.into())
        };
        match res {
            Ok(n) => out.push(Row::new(label, "ok", n, format!("{:.2}s", t0.elapsed().as_secs_f64()))),
            Err(e) => {
                failed += 1;
                out.push(Row::new(label, "failed", 0, e.to_string()));
                if !continue_on_error { bail!("{e}\n  in: {}", s.chars().take(300).collect::<String>()); }
            }
        }
    }
    let _ = con.execute_batch("CHECKPOINT");
    let ok = out.iter().filter(|r| r.kind == "ok").count();
    out.push(Row::new("*", "summary", ok as i64, format!("{ok} statement(s) succeeded, {failed} failed")));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_full() {
        let d = parse_dsn("mssql://sa:D2m%21Strong%232019@localhost:14319/Sales?trust_cert=true&encrypt=false").unwrap();
        assert_eq!(d.scheme, "mssql");
        assert_eq!(d.user.as_deref(), Some("sa"));
        assert_eq!(d.password.as_deref(), Some("D2m!Strong#2019"));
        assert_eq!(d.host, "localhost");
        assert_eq!(d.port, Some(14319));
        assert_eq!(d.database.as_deref(), Some("Sales"));
        let t = d.mssql_target(None);
        assert!(t.trust_cert && !t.encrypt && !t.windows_auth);
        assert_eq!(t.database, "Sales");
    }

    #[test]
    fn dsn_minimal_and_sqlserver_comma() {
        let d = parse_dsn("mssql://sql01,1433").unwrap();
        assert_eq!((d.host.as_str(), d.port, d.database.clone(), d.user.clone()), ("sql01", Some(1433), None, None));
        let t = d.mssql_target(Some("X"));
        assert_eq!((t.user.as_str(), t.port, t.database.as_str()), ("", 1433, "X"));
        assert_eq!(t.auth_kind(), crate::mssql::AuthKind::Integrated);
        let t = parse_dsn("mssql://CORP%5Csvc_etl:pw@sql01/Sales").unwrap().mssql_target(None);
        assert_eq!((t.user.as_str(), t.auth_kind()), ("CORP\\svc_etl", crate::mssql::AuthKind::Windows));
        let t = parse_dsn("mssql://etl@corp.local:pw@sql01/Sales").unwrap().mssql_target(None);
        assert_eq!((t.user.as_str(), t.auth_kind()), ("etl@corp.local", crate::mssql::AuthKind::Windows));
        let t = parse_dsn("mssql://sa:pw@sql01/Sales").unwrap().mssql_target(None);
        assert_eq!(t.auth_kind(), crate::mssql::AuthKind::SqlLogin);
        let t = parse_dsn("mssql://svc:pw@sql01/Sales?trusted_connection=yes").unwrap().mssql_target(None);
        assert_eq!(t.auth_kind(), crate::mssql::AuthKind::Windows);
        let d = parse_dsn("postgres://u@pg/db").unwrap();
        assert_eq!((d.host.as_str(), d.port, d.database.as_deref()), ("pg", None, Some("db")));
        assert!(parse_dsn("nonsense").is_err());
    }

    #[test]
    fn schema_map_and_lists() {
        let m = parse_schema_map("dbo=main, sales = sales", &[("dbo", "x")]);
        assert_eq!(m["dbo"], "main");
        assert_eq!(m["sales"], "sales");
        assert_eq!(csv_list("a, b,,c"), vec!["a", "b", "c"]);
    }
}
