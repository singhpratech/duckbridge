//! duckbridge CLI: DuckDB <-> SQL Server / PostgreSQL / Parquet bridge.
use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use duckbridge::core::catalog::{open_readonly, read_catalog, CatalogOptions};
use duckbridge::core::emit::{Emitter, Flavor};
use duckbridge::core::script::{write_script, ScriptOptions};
use duckbridge::core::typemap::{StringMode, Target};
use duckbridge::mssql::guard::Guard;
use duckbridge::mssql::{self, MssqlTarget};
use duckbridge::storage::{self, Location};
use duckbridge::transpile::{self, Dialect};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "duckbridge", version, about = "DuckBridge: move data between DuckDB and other databases. import a SQL Server database into DuckDB, run translated T-SQL on DuckDB, export back as a T-SQL script, a live load, a restorable .bak, PostgreSQL or Parquet.")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
    /// Quiet: only warnings and the final summary
    #[arg(short, long, global = true)]
    quiet: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show the catalog of a DuckDB file and how each column maps to SQL Server (or PostgreSQL)
    Inspect(InspectArgs),
    /// Translate SQL between dialects: tsql, duckdb, postgres
    Transpile(TranspileArgs),
    /// Translate a T-SQL (or PostgreSQL) script and run it on a DuckDB file
    Run(RunArgs),
    /// Copy a SQL Server database into a DuckDB file
    Import(ImportArgs),
    /// Export a DuckDB file to SQL Server (script / live / .bak), PostgreSQL or Parquet
    Export(ExportArgs),
    /// Copy a file between local disk and s3:// az:// gs:// (uses the built-in storage clients)
    Cp { src: String, dst: String },
}

#[derive(Args)]
struct InspectArgs {
    /// DuckDB file (local path or s3:// az:// gs:// URL)
    duckdb: String,
    #[arg(long, value_enum, default_value = "mssql")]
    target: TargetArg,
    #[arg(long)]
    json: bool,
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,
}

#[derive(Args)]
struct TranspileArgs {
    /// Input file ('-' for stdin)
    input: String,
    #[arg(long, value_enum, default_value = "tsql")]
    from: DialectArg,
    #[arg(long, value_enum, default_value = "duckdb")]
    to: DialectArg,
    /// Output file (default: stdout)
    #[arg(short, long)]
    out: Option<PathBuf>,
    /// Schema rename, e.g. dbo=main (repeatable)
    #[arg(long = "schema-map", value_parser = parse_kv)]
    schema_map: Vec<(String, String)>,
    /// SQL Server version the T-SQL output targets (2019, 2022, 2025)
    #[arg(long, default_value_t = 2019)]
    target_version: u16,
    /// Do not emit the DuckDB session prelude (integer division, NULL order, nocase collation, UTC)
    #[arg(long)]
    no_prelude: bool,
    /// Assume SQL Server's default case-insensitive collation (LIKE -> ILIKE, nocase)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    nocase: bool,
}

#[derive(Args)]
struct RunArgs {
    /// DuckDB file to run against
    duckdb: String,
    /// SQL script ('-' for stdin)
    script: String,
    #[arg(long, value_enum, default_value = "tsql")]
    from: DialectArg,
    #[arg(long = "schema-map", value_parser = parse_kv)]
    schema_map: Vec<(String, String)>,
    /// Keep going after a failing statement
    #[arg(long)]
    continue_on_error: bool,
    /// DuckDB memory limit, e.g. 40GB
    #[arg(long)]
    memory_limit: Option<String>,
    #[arg(long)]
    threads: Option<usize>,
    /// Assume SQL Server's default case-insensitive collation (LIKE -> ILIKE, nocase)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    nocase: bool,
}

#[derive(Args, Clone)]
struct ConnArgs {
    #[arg(long, env = "MSSQL_HOST", default_value = "localhost")]
    host: String,
    #[arg(long, env = "MSSQL_PORT", default_value_t = 1433)]
    port: u16,
    /// SQL login, or Windows credentials as DOMAIN\user / user@domain. Omit it to use
    /// integrated Windows authentication (the identity running duckbridge; Windows only).
    #[arg(long, env = "MSSQL_USER")]
    user: Option<String>,
    #[arg(long, env = "MSSQL_PASSWORD", default_value = "", hide_env_values = true)]
    password: String,
    /// Accept the server's TLS certificate without validation (self-signed / containers)
    #[arg(long, default_value_t = true)]
    trust_cert: bool,
    #[arg(long, default_value_t = true)]
    encrypt: bool,
    /// Treat --user/--password as Windows credentials even without a DOMAIN\ prefix
    #[arg(long)]
    windows_auth: bool,
}

impl ConnArgs {
    fn target(&self, database: &str) -> MssqlTarget {
        MssqlTarget { host: self.host.clone(), port: self.port, user: self.user.clone().unwrap_or_default(), password: self.password.clone(), database: database.into(), trust_cert: self.trust_cert, encrypt: self.encrypt, windows_auth: self.windows_auth }
    }
}

#[derive(Args)]
struct ImportArgs {
    /// Output DuckDB file
    out: PathBuf,
    #[command(flatten)]
    conn: ConnArgs,
    /// Source database name
    #[arg(long, short = 'd')]
    database: String,
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,
    /// Parallel streams (default: number of cores)
    #[arg(long, short = 'j')]
    parallel: Option<usize>,
    /// Tables with at least this many rows are split into key ranges across streams
    #[arg(long, default_value_t = 20_000_000)]
    split_threshold: i64,
    #[arg(long, default_value_t = 50_000)]
    batch_rows: usize,
    /// Rename schemas, e.g. dbo=main (default: dbo=main)
    #[arg(long = "schema-map", value_parser = parse_kv)]
    schema_map: Vec<(String, String)>,
    /// Read with NOLOCK (dirty reads; avoids blocking the source)
    #[arg(long)]
    nolock: bool,
    /// Skip data, copy schema only
    #[arg(long)]
    schema_only: bool,
    /// Overwrite the output file if it exists
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    memory_limit: Option<String>,
    /// Create tables without constraints and add PRIMARY KEY/UNIQUE after the copy (fastest for billions of rows; FKs skipped)
    #[arg(long)]
    keys_after_load: bool,
}

#[derive(Args)]
struct ExportArgs {
    /// Source DuckDB file (local path or s3:// az:// gs:// URL)
    duckdb: String,
    #[arg(long, value_enum)]
    to: ExportKind,
    /// Output: file path or s3:// az:// gs:// URL (script/bak/parquet)
    #[arg(long, short = 'o')]
    out: Option<String>,
    /// Target database name (SQL Server) / database in the PostgreSQL DSN
    #[arg(long, short = 'd')]
    database: Option<String>,
    #[command(flatten)]
    conn: ConnArgs,
    /// PostgreSQL DSN, e.g. "host=localhost user=postgres password=x dbname=target"
    #[arg(long, env = "PG_DSN")]
    pg_dsn: Option<String>,
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,
    /// Size NVARCHAR columns from the data (fit) or always NVARCHAR(MAX) (max)
    #[arg(long, value_enum, default_value = "fit")]
    string_mode: StringModeArg,
    #[arg(long = "schema-map", value_parser = parse_kv)]
    schema_map: Vec<(String, String)>,
    #[arg(long)]
    drop_existing: bool,
    #[arg(long)]
    schema_only: bool,
    #[arg(long, short = 'j')]
    parallel: Option<usize>,
    #[arg(long, default_value_t = 100_000)]
    batch_rows: usize,
    /// Data path for live loads: TDS bulk copy (fast) or multi-row INSERT (compatible)
    #[arg(long, value_enum, default_value = "bulk")]
    method: MethodArg,
    /// Live load: create the target database if it does not exist (its name must match the disposable
    /// pattern). Script: include CREATE DATABASE / USE.
    #[arg(long)]
    create_database: bool,
    /// Regex of database names duckbridge may create, drop or alter (env DUCKBRIDGE_DISPOSABLE_DB_PATTERN)
    #[arg(long, default_value = "^_", env = "DUCKBRIDGE_DISPOSABLE_DB_PATTERN")]
    disposable_db_pattern: String,
    /// Set the compatibility level of the (disposable) database duckbridge creates, e.g. 150
    #[arg(long)]
    compat_level: Option<u16>,
    /// Set the recovery model of the (disposable) database: simple | full | bulk_logged (default: leave as is)
    #[arg(long)]
    recovery: Option<String>,
    /// Directory ON THE SQL SERVER HOST where the .bak is written
    #[arg(long, default_value = "/var/opt/mssql/backup")]
    backup_dir: String,
    /// How to fetch the .bak from the server: local | docker:<container> | none
    #[arg(long, default_value = "none")]
    fetch: String,
    #[arg(long, default_value_t = 1)]
    stripes: usize,
    #[arg(long, default_value_t = true)]
    compression: bool,
    /// After backup, restore it under another name and compare row counts
    #[arg(long)]
    verify_restore: bool,
    /// Drop the staging database after the .bak is produced
    #[arg(long)]
    drop_staging: bool,
    /// Keep the clustered primary key inline during the load (default: load into a heap, add the key afterwards)
    #[arg(long)]
    pk_inline: bool,
    /// Tables with at least this many rows are split into key ranges across parallel streams
    #[arg(long, default_value_t = 5_000_000)]
    split_threshold: i64,
    /// ORDER BY the key inside each range (cheap when the DuckDB data is already sorted)
    #[arg(long)]
    order_by_key: bool,
    /// Parquet compression: zstd | snappy | gzip | uncompressed
    #[arg(long, default_value = "zstd")]
    parquet_compression: String,
    /// SQL Server version of the target (2019, 2022, 2025): enables json/vector types and newer T-SQL in views
    #[arg(long, default_value_t = 2019)]
    target_version: u16,
    /// SQL Server 2022+: BACKUP TO URL (s3://... or https://<account>.blob.core.windows.net/...), needs a server CREDENTIAL
    #[arg(long)]
    backup_to_url: Option<String>,
    #[arg(long)]
    memory_limit: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum TargetArg { Mssql, Postgres }
#[derive(Clone, Copy, ValueEnum)]
enum DialectArg { Tsql, Duckdb, Postgres }
#[derive(Clone, Copy, ValueEnum)]
enum StringModeArg { Fit, Max }
#[derive(Clone, Copy, ValueEnum)]
enum MethodArg { Bulk, Insert }
#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ExportKind { Script, Server, Bak, Postgres, Parquet }

impl From<DialectArg> for Dialect {
    fn from(d: DialectArg) -> Self { match d { DialectArg::Tsql => Dialect::Tsql, DialectArg::Duckdb => Dialect::DuckDb, DialectArg::Postgres => Dialect::Postgres } }
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=').map(|(a, b)| (a.to_string(), b.to_string())).ok_or_else(|| format!("expected key=value, got {s}"))
}

fn schema_map(pairs: &[(String, String)], default: &[(&str, &str)]) -> HashMap<String, String> {
    let mut m: HashMap<String, String> = default.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
    for (a, b) in pairs { m.insert(a.clone(), b.clone()); }
    m
}

fn log(quiet: bool, msg: impl AsRef<str>) {
    if !quiet { eprintln!("{}", msg.as_ref()); }
}

fn print_warnings(ws: &[String]) {
    if ws.is_empty() { return; }
    eprintln!("\n{} warning(s):", ws.len());
    for w in ws { eprintln!("  - {w}"); }
}

fn scratch_dir() -> PathBuf {
    std::env::temp_dir().join("duckbridge")
}

fn main() {
    if let Err(e) = real_main() {
        eprintln!("error: {e:#}");
        // 1 = a check failed, 2 = could not run, 3 = stopped by the safety guard
        std::process::exit(duckbridge::core::runlog::exit_code(&e));
    }
}

fn real_main() -> Result<()> {
    let cli = Cli::parse();
    let q = cli.quiet;
    match cli.cmd {
        Cmd::Inspect(a) => inspect(a, q),
        Cmd::Transpile(a) => transpile_cmd(a),
        Cmd::Run(a) => run(a, q),
        Cmd::Import(a) => import(a, q),
        Cmd::Export(a) => export(a, q),
        Cmd::Cp { src, dst } => {
            let local = localize(&src, q)?;
            let dest = Location::parse(&dst)?;
            let mut last = 0u64;
            storage::upload(&local, &dest, &mut |done, total| { if done - last > 64 * 1024 * 1024 || done == total { last = done; log(q, format!("  {:.0} / {:.0} MB", done as f64 / 1e6, total as f64 / 1e6)); } })?;
            println!("copied {src} -> {dest}");
            Ok(())
        }
    }
}

fn localize(path: &str, q: bool) -> Result<PathBuf> {
    let loc = Location::parse(path)?;
    if !loc.is_local() { log(q, format!("downloading {loc} ...")); }
    let mut last = 0u64;
    storage::localize(&loc, &scratch_dir(), &mut |done, total| { if done - last > 256 * 1024 * 1024 || done == total { last = done; log(q, format!("  {:.0} / {:.0} MB", done as f64 / 1e6, total as f64 / 1e6)); } })
}

fn inspect(a: InspectArgs, q: bool) -> Result<()> {
    let path = localize(&a.duckdb, q)?;
    let con = open_readonly(&path.to_string_lossy())?;
    let opts = CatalogOptions { include: a.tables, exclude: a.exclude, string_mode: StringMode::Fit, target: match a.target { TargetArg::Mssql => Target::Mssql, TargetArg::Postgres => Target::Postgres }, measure: true };
    let cat = read_catalog(&con, &a.duckdb, &opts)?;
    if a.json {
        println!("{}", serde_json::to_string_pretty(&cat)?);
        return Ok(());
    }
    println!("DuckDB {}  source {}  schemas {}  tables {}  views {}  rows {}", cat.duckdb_version, cat.source, cat.schemas.len(), cat.tables.len(), cat.views.len(), cat.total_rows());
    for t in &cat.tables {
        println!("\n{}.{}  ({} rows){}", t.schema, t.name, t.row_count, t.pk().map(|p| format!("  PK({})", p.columns.join(", "))).unwrap_or_default());
        for c in &t.columns {
            println!("  {:<32} {:<28} -> {:<22}{}{}", c.name, c.duck_type, c.sql_type, if c.nullable { "" } else { " NOT NULL" }, if c.identity.is_some() { " IDENTITY" } else { "" });
            for w in &c.warnings { println!("      ! {w}"); }
        }
    }
    for v in &cat.views { println!("\nview {}.{}", v.schema, v.name); }
    Ok(())
}

fn read_input(p: &str) -> Result<String> {
    if p == "-" { let mut s = String::new(); std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)?; Ok(s) } else { std::fs::read_to_string(p).with_context(|| format!("reading {p}")) }
}

fn transpile_cmd(a: TranspileArgs) -> Result<()> {
    let sql = read_input(&a.input)?;
    let (from, to): (Dialect, Dialect) = (a.from.into(), a.to.into());
    let default = match (from, to) { (Dialect::Tsql, _) => [("dbo", "main")], (_, Dialect::Tsql) => [("main", "dbo")], _ => [("main", "public")] };
    let sm = schema_map(&a.schema_map, &default);
    let opts = transpile::Options { target_version: a.target_version, prelude: !a.no_prelude, nocase: a.nocase, float_division: true };
    let out = transpile::translate_with(&sql, from, to, &sm, &opts)?;
    match a.out {
        Some(p) => { std::fs::write(&p, &out.sql)?; eprintln!("wrote {}", p.display()); }
        None => println!("{}", out.sql),
    }
    print_warnings(&out.warnings);
    Ok(())
}

fn run(a: RunArgs, q: bool) -> Result<()> {
    let sql = read_input(&a.script)?;
    let from: Dialect = a.from.into();
    let sm = schema_map(&a.schema_map, &[("dbo", "main")]);
    let opts = transpile::Options { prelude: false, nocase: a.nocase, ..transpile::Options::default() };
    let tr = transpile::translate_with(&sql, from, Dialect::DuckDb, &sm, &opts)?;
    print_warnings(&tr.warnings);
    let con = duckdb::Connection::open(&a.duckdb)?;
    if from == Dialect::Tsql { for p in transpile::duckdb_prelude(a.nocase) { con.execute_batch(p)?; } }
    if let Some(m) = &a.memory_limit { con.execute_batch(&format!("SET memory_limit='{m}'"))?; }
    if let Some(t) = a.threads { con.execute_batch(&format!("SET threads={t}"))?; }
    let mut ok = 0;
    let mut failed = 0;
    for stmt in duckbridge::transpile::tokens::statements(&duckbridge::transpile::tokens::tokenize(&tr.sql, false)) {
        let s = duckbridge::transpile::tokens::join(&stmt).trim().to_string();
        if s.is_empty() || s.starts_with("--") { continue; }
        let t0 = std::time::Instant::now();
        let is_query = s.to_uppercase().starts_with("SELECT") || s.to_uppercase().starts_with("WITH") || s.to_uppercase().starts_with("FROM");
        let res: Result<()> = if is_query {
            (|| { let mut st = con.prepare(&s)?; let mut rows = st.query([])?; let mut n = 0; let mut shown = 0; while let Some(r) = rows.next()? { n += 1; if shown < 20 { let w = r.as_ref().column_count(); let vals: Vec<String> = (0..w).map(|i| format!("{:?}", r.get_ref_unwrap(i))).collect(); println!("{}", vals.join(" | ")); shown += 1; } } if n > shown { println!("... {n} rows"); } Ok(()) })()
        } else { con.execute_batch(&s).map_err(|e| e.into()) };
        match res {
            Ok(_) => { ok += 1; log(q, format!("ok   ({:.2}s) {}", t0.elapsed().as_secs_f64(), s.lines().next().unwrap_or("").chars().take(100).collect::<String>())); }
            Err(e) => { failed += 1; eprintln!("FAIL {}\n     {e}", s.chars().take(300).collect::<String>()); if !a.continue_on_error { bail!("stopped after {ok} statement(s); use --continue-on-error to keep going"); } }
        }
    }
    eprintln!("{ok} statement(s) succeeded, {failed} failed");
    Ok(())
}

fn import(a: ImportArgs, q: bool) -> Result<()> {
    use duckbridge::mssql::import::*;
    if a.out.exists() { if a.overwrite { std::fs::remove_file(&a.out)?; let _ = std::fs::remove_file(format!("{}.wal", a.out.display())); } else { bail!("{} exists; use --overwrite", a.out.display()); } }
    let target = a.conn.target(&a.database);
    let opts = ImportOptions { keys_after_load: a.keys_after_load, include: a.tables, exclude: a.exclude, parallel: a.parallel.unwrap_or_else(num_cpus::get), split_threshold: a.split_threshold, batch_rows: a.batch_rows, schema_map: schema_map(&a.schema_map, &[("dbo", "main")]), nolock: a.nolock };
    let rt = mssql::runtime()?;
    let mut warnings = vec![];
    let (tables, views) = rt.block_on(async {
        let mut c = target.connect(&a.database).await?;
        let v = c.simple_query("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(50)), CAST(SERVERPROPERTY('Edition') AS NVARCHAR(100))").await?.into_first_result().await?;
        if let Some(r) = v.first() { log(q, format!("SQL Server {} ({})", r.get::<&str, _>(0).unwrap_or("?"), r.get::<&str, _>(1).unwrap_or("?"))); }
        read_catalog(&mut c, &opts, &mut warnings).await
    })?;
    let total_rows: i64 = tables.iter().map(|t| t.row_count).sum();
    log(q, format!("{} tables, {} views, ~{} rows", tables.len(), views.len(), total_rows));
    let con = duckdb::Connection::open(&a.out)?;
    if let Some(m) = &a.memory_limit { con.execute_batch(&format!("SET memory_limit='{m}'"))?; }
    con.execute_batch("SET preserve_insertion_order=false")?;
    create_duck_schema(&con, &tables, &views, &opts, &mut warnings)?;
    log(q, "schema created in DuckDB");
    if !a.schema_only {
        let qq = q;
        let progress: Arc<dyn Fn(&str, i64) + Send + Sync> = Arc::new(move |t, n| log(qq, format!("  {t}: {n} rows")));
        let rep = copy_data(&con, &target, &tables, &opts, progress)?;
        warnings.extend(rep.warnings);
        log(q, format!("copied {} rows in {:.1}s ({:.0} rows/s)", rep.rows, rep.seconds, rep.rows as f64 / rep.seconds.max(0.001)));
        add_keys(&con, &tables, &opts, &mut warnings)?;
    }
    create_duck_views(&con, &views, &opts, &mut warnings);
    con.execute_batch("CHECKPOINT")?;
    drop(con);
    let size = std::fs::metadata(&a.out).map(|m| m.len()).unwrap_or(0);
    println!("wrote {} ({:.1} MB)", a.out.display(), size as f64 / 1e6);
    print_warnings(&warnings);
    Ok(())
}

fn export(a: ExportArgs, q: bool) -> Result<()> {
    let path = localize(&a.duckdb, q)?;
    duckbridge::core::typemap::set_mssql_version(a.target_version);
    let con = open_readonly(&path.to_string_lossy())?;
    if let Some(m) = &a.memory_limit { con.execute_batch(&format!("SET memory_limit='{m}'"))?; }
    let target_kind = if matches!(a.to, ExportKind::Postgres | ExportKind::Parquet) { Target::Postgres } else { Target::Mssql };
    let opts = CatalogOptions { include: a.tables.clone(), exclude: a.exclude.clone(), string_mode: match a.string_mode { StringModeArg::Fit => StringMode::Fit, StringModeArg::Max => StringMode::Max }, target: target_kind, measure: true };
    let mut cat = read_catalog(&con, &a.duckdb, &opts)?;
    log(q, format!("source: DuckDB {}  {} tables, {} views, {} rows", cat.duckdb_version, cat.tables.len(), cat.views.len(), cat.total_rows()));
    let default_map = if matches!(a.to, ExportKind::Postgres | ExportKind::Parquet) { [("main", "public")] } else { [("main", "dbo")] };
    let sm = schema_map(&a.schema_map, &default_map);
    let parallel = a.parallel.unwrap_or_else(num_cpus::get);
    let qq = q;
    let progress: Arc<dyn Fn(&str, i64) + Send + Sync> = Arc::new(move |t, n| log(qq, format!("  {t}: {n} rows")));
    let db_name = a.database.clone().unwrap_or_else(|| path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or("duckdb_export".into()));

    match a.to {
        ExportKind::Script => {
            let em = Emitter::new(Flavor::Mssql, sm);
            let out = a.out.clone().unwrap_or_else(|| format!("{db_name}.sql"));
            let loc = Location::parse(&out)?;
            let local = if loc.is_local() { PathBuf::from(&out) } else { scratch_dir().join(loc.file_name()) };
            if let Some(p) = local.parent() { if !p.as_os_str().is_empty() { std::fs::create_dir_all(p)?; } }
            let f = std::fs::File::create(&local)?;
            let mut w = std::io::BufWriter::with_capacity(4 << 20, f);
            let so = ScriptOptions { database: Some(db_name.clone()), create_db: a.create_database, with_data: !a.schema_only, drop_existing: a.drop_existing, rows_per_insert: None, statements_per_batch: 50 };
            let ws = write_script(&con, &mut cat, &em, &mut w, &so, &mut |t, n| log(qq, format!("  {t}: {n} rows")))?;
            w.flush()?;
            drop(w);
            if !loc.is_local() { log(q, format!("uploading to {loc} ...")); storage::upload(&local, &loc, &mut |_, _| {})?; }
            println!("wrote {} ({:.1} MB). Run with: sqlcmd -S host -U sa -i {}", loc, std::fs::metadata(&local).map(|m| m.len()).unwrap_or(0) as f64 / 1e6, loc.file_name());
            print_warnings(&ws);
        }
        ExportKind::Server | ExportKind::Bak => {
            use duckbridge::mssql::load::*;
            let em = Emitter::new(Flavor::Mssql, sm);
            let target = a.conn.target("master");
            let guard = Guard::new(Some(&a.disposable_db_pattern))?;
            let lo = LoadOptions { database: db_name.clone(), drop_existing: a.drop_existing, with_data: !a.schema_only, parallel, batch_rows: a.batch_rows, method: match a.method { MethodArg::Bulk => LoadMethod::Bulk, MethodArg::Insert => LoadMethod::Insert }, guard: guard.clone(), create_database: a.create_database || a.to == ExportKind::Bak, recovery: a.recovery.clone(), compat_level: a.compat_level, pk_after_load: !a.pk_inline, split_threshold: a.split_threshold, order_by_key: a.order_by_key };
            let rt = mssql::runtime()?;
            let mut warnings = vec![];
            let mut c = rt.block_on(async {
                let mut m = target.connect("master").await?;
                let v = m.simple_query("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(50)), CAST(SERVERPROPERTY('Edition') AS NVARCHAR(100))").await?.into_first_result().await?;
                if let Some(r) = v.first() { log(q, format!("SQL Server {} ({})", r.get::<&str, _>(0).unwrap_or("?"), r.get::<&str, _>(1).unwrap_or("?"))); }
                prepare_schema(&mut m, &target, &mut cat, &em, &lo).await
            })?;
            log(q, format!("prepared database [{db_name}] with {} tables", cat.tables.len()));
            if lo.with_data {
                let rep = load_data(&con, &cat, &target, &em, &lo, progress.clone())?;
                warnings.extend(rep.warnings);
                log(q, format!("loaded {} rows in {:.1}s ({:.0} rows/s)", rep.rows, rep.seconds, rep.rows as f64 / rep.seconds.max(0.001)));
            }
            rt.block_on(finish_schema(&mut c, &cat, &em, &lo, &mut warnings))?;
            log(q, "constraints, indexes and views created");
            if a.to == ExportKind::Bak {
                use duckbridge::mssql::backup::*;
                let out = a.out.clone().unwrap_or_else(|| format!("{db_name}.bak"));
                let loc = Location::parse(&out)?;
                let bo = BackupOptions { server_dir: a.backup_dir.clone(), compression: a.compression, stripes: a.stripes.max(1), file_name: loc.file_name(), to_url: a.backup_to_url.clone() };
                let mut m = rt.block_on(target.connect("master"))?;
                let br = rt.block_on(backup(&mut m, &db_name, &bo))?;
                log(q, format!("backup written on server ({:.1} MB in {:.1}s): {}", br.bytes as f64 / 1e6, br.seconds, br.server_paths.join(", ")));
                if a.backup_to_url.is_some() {
                    println!("backup written to {}", br.server_paths[0]);
                    if a.drop_staging { rt.block_on(guard.drop_database(&mut m, &db_name))?; }
                    warnings.extend(cat.column_warnings());
                    print_warnings(&warnings);
                    return Ok(());
                }
                if a.verify_restore {
                    let vdb = format!("{db_name}_verify");
                    let counts = rt.block_on(verify_restore(&target, &br.server_paths, &vdb, false, &guard))?;
                    let mut mismatches = 0;
                    for t in &cat.tables {
                        let key = format!("{}.{}", em.schema(&t.schema), t.name);
                        let got = counts.get(&key).copied().unwrap_or(-1);
                        if got != t.row_count { mismatches += 1; eprintln!("  MISMATCH {key}: expected {} rows, restored {got}", t.row_count); }
                    }
                    if mismatches == 0 { log(q, format!("verify: restored .bak as [{vdb}]; all {} table row counts match", cat.tables.len())); } else { bail!("verify failed: {mismatches} table(s) differ"); }
                }
                let how = if a.fetch == "local" { Fetch::Local } else if let Some(c) = a.fetch.strip_prefix("docker:") { Fetch::Docker(c.into()) } else { Fetch::None };
                let local_dir = if loc.is_local() { PathBuf::from(&out).parent().map(|p| p.to_path_buf()).filter(|p| !p.as_os_str().is_empty()).unwrap_or(PathBuf::from(".")) } else { scratch_dir() };
                let files = fetch(&br.server_paths, &how, &local_dir)?;
                if matches!(how, Fetch::None) {
                    println!("backup left on the server at: {}", br.server_paths.join(", "));
                    println!("(use --fetch local or --fetch docker:<container> to bring it here, or restore it in place)");
                } else {
                    for (i, f) in files.iter().enumerate() {
                        let dest = if files.len() == 1 { loc.clone() } else { Location::parse(&format!("{}", out.replace(&loc.file_name(), &f.file_name().unwrap().to_string_lossy())))? };
                        if !loc.is_local() { log(q, format!("uploading {} -> {dest} ...", f.display())); storage::upload(f, &dest, &mut |_, _| {})?; }
                        else if f != &PathBuf::from(&out) && i == 0 && files.len() == 1 { std::fs::rename(f, &out).or_else(|_| std::fs::copy(f, &out).map(|_| ()))?; }
                    }
                    println!("wrote {} ({:.1} MB). Restore with: RESTORE DATABASE [{db_name}] FROM DISK = N'<path>' WITH MOVE ..., REPLACE", loc, br.bytes as f64 / 1e6);
                }
                if a.drop_staging {
                    rt.block_on(guard.drop_database(&mut m, &db_name))?;
                    log(q, format!("dropped staging database [{db_name}]"));
                }
            } else {
                println!("database [{db_name}] loaded on {}:{}", a.conn.host, a.conn.port);
            }
            warnings.extend(cat.column_warnings());
            print_warnings(&warnings);
        }
        ExportKind::Postgres => {
            let em = Emitter::new(Flavor::Postgres, sm);
            let dsn = a.pg_dsn.clone().context("--pg-dsn (or PG_DSN) is required for --to postgres")?;
            let po = duckbridge::pg::PgOptions { dsn, drop_existing: a.drop_existing, with_data: !a.schema_only, parallel };
            let rep = duckbridge::pg::load(&con, &mut cat, &em, &po, progress)?;
            println!("loaded {} rows into PostgreSQL in {:.1}s", rep.rows, rep.seconds);
            print_warnings(&rep.warnings);
        }
        ExportKind::Parquet => {
            let out = a.out.clone().unwrap_or_else(|| format!("{db_name}_parquet"));
            let loc = Location::parse(&out)?;
            let local_dir = if loc.is_local() { PathBuf::from(&out) } else { scratch_dir().join(loc.file_name()) };
            let po = duckbridge::parquet::ParquetOptions { compression: a.parquet_compression.clone(), row_group_size: 122_880, partition_rows: None };
            let files = duckbridge::parquet::export_parquet(&con, &cat, &local_dir, &po, &mut |t, n| log(qq, format!("  {t}: {n} rows")))?;
            // schema script alongside (PostgreSQL flavour is the most portable DDL)
            let em = Emitter::new(Flavor::Postgres, sm);
            let mut ddl = String::new();
            for t in cat.tables.iter_mut() { ddl.push_str(&em.create_table(t)); ddl.push('\n'); }
            std::fs::write(local_dir.join("schema.sql"), ddl)?;
            if !loc.is_local() {
                for (_, f) in &files {
                    let dest = Location::parse(&format!("{}/{}", out.trim_end_matches('/'), f.file_name().unwrap().to_string_lossy()))?;
                    log(q, format!("uploading {dest} ...")); storage::upload(f, &dest, &mut |_, _| {})?;
                }
                storage::upload(&local_dir.join("schema.sql"), &Location::parse(&format!("{}/schema.sql", out.trim_end_matches('/')))?, &mut |_, _| {})?;
            }
            println!("wrote {} parquet file(s) + schema.sql to {}", files.len(), loc);
            print_warnings(&cat.column_warnings());
        }
    }
    Ok(())
}
