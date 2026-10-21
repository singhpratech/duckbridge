//! Streaming reads of one SQL Server table or query, for callers that hand the rows to DuckDB
//! themselves: the DuckDB 2.0 extension's `bridge_scan` table function, `bridge_tables` and the script
//! `PRAGMA bridge_import` returns (the v2 extension API gives an extension no DuckDB connection, so
//! nothing here writes to DuckDB).
//!
//! * [`parse_source`] / [`source_sql`]: a `schema.table` (`[s].[t]`, `"s"."t"`, bare `t` = `dbo.t`)
//!   or a query (anything starting with SELECT, WITH or `(`). Table mode reads `SELECT *` (with
//!   `WITH (NOLOCK)` when asked) and then casts what tiberius cannot decode, as the importer does
//!   (`import::select_expr`, plus `money` as DECIMAL(19,4) so it stays exact); query mode sends the
//!   query unchanged (NOLOCK is not applied, the query's own hints are).
//! * [`describe`]: the first result set's columns from `sys.dm_exec_describe_first_result_set`, typed
//!   with the importer's mapping (`typemap::mssql_to_duck`); [`plan`] resolves a source into the
//!   SELECT to stream, its columns and (tables) the row-count estimate from `sys.partitions`.
//! * [`RowStream`]: the SELECT streamed on its own runtime (one worker thread) into batches of
//!   `duck::Value` rows (`import::column_to_value`), over a bounded channel [`PREFETCH`] batches deep.
//!   Errors from the read surface on [`RowStream::next_batch`]; dropping the stream cancels the read
//!   (the runtime is shut down, which drops the TDS connection).
//! * [`catalog`] / [`import_script`]: the importer's catalog (`import::read_catalog_with`) and the
//!   DuckDB script that recreates it (the importer's DDL, `INSERT ... SELECT * FROM bridge_scan(..)`
//!   per table in FK order, keys, views, a summary query).
//!
//! Every read goes through a [`SourceClient`] (read-only by construction) with the R6 session settings
//! of [`ReadSafety`] (lock timeout, deadlock priority, isolation, read-only intent).
use super::fingerprint::{self, ReadSafety, SourceClient};
use super::import::{self, column_to_value, ImportOptions, MsTable, MsView};
use super::{bracket, MssqlTarget};
use crate::core::duck::Value;
use crate::core::typemap::{mssql_to_duck, quote_duck};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::TryStreamExt;
use tiberius::QueryItem;

/// Batches buffered between the TDS reader and the consumer.
pub const PREFETCH: usize = 4;

/// One result column: its name, the SQL Server type (`nvarchar(50)`, `decimal(18,4)`, ...), the
/// DuckDB type it is read as, and nullability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanColumn {
    pub name: String,
    pub sql_type: String,
    pub duck_type: String,
    pub nullable: bool,
}

/// What `bridge_scan` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Table { schema: String, name: String },
    Query(String),
}

/// One identifier part: `[a]]b]`, `"a""b"` or a bare name; returns it and the rest.
fn ident_part(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    let (open, close) = match s.chars().next()? { '[' => ('[', ']'), '"' => ('"', '"'), _ => (' ', ' ') };
    if open != ' ' {
        let mut out = String::new();
        let mut it = s[1..].char_indices().peekable();
        while let Some((i, c)) = it.next() {
            if c == close {
                if it.peek().map(|x| x.1) == Some(close) { out.push(c); it.next(); continue; }
                return Some((out, &s[1 + i + 1..]));
            }
            out.push(c);
        }
        return None;
    }
    let end = s.find(|c: char| !(c.is_alphanumeric() || matches!(c, '_' | '@' | '#' | '$'))).unwrap_or(s.len());
    let first = s.chars().next()?;
    if end == 0 || first.is_ascii_digit() || first == '$' { return None; }
    Some((s[..end].to_string(), &s[end..]))
}

/// `dbo.orders`, `[dbo].[my table]`, `"sales"."t"`, `orders` (schema `dbo`) -> (schema, table);
/// `None` when `spec` is not a one- or two-part name.
pub fn parse_table_name(spec: &str) -> Option<(String, String)> {
    let mut parts = vec![];
    let mut rest = spec.trim();
    loop {
        let (p, r) = ident_part(rest)?;
        parts.push(p);
        let r = r.trim_start();
        if r.is_empty() { break; }
        rest = r.strip_prefix('.')?;
    }
    match parts.len() {
        1 => Some(("dbo".into(), parts.remove(0))),
        2 => { let t = parts.remove(1); Some((parts.remove(0), t)) }
        _ => None,
    }
}

/// A table name (see [`parse_table_name`]) or a query (starts with SELECT, WITH or `(`).
pub fn parse_source(spec: &str) -> Result<Source> {
    let t = spec.trim();
    let head: String = t.chars().take_while(|c| c.is_alphabetic()).collect::<String>().to_ascii_uppercase();
    if t.starts_with('(') || head == "SELECT" || head == "WITH" { return Ok(Source::Query(t.trim_end_matches(';').trim().to_string())); }
    match parse_table_name(t) {
        Some((schema, name)) => Ok(Source::Table { schema, name }),
        None => bail!("source {spec:?} is neither a schema.table name nor a query starting with SELECT or WITH"),
    }
}

/// The SELECT that reads `spec`: `SELECT * FROM [s].[t]` (`WITH (NOLOCK)` with `nolock`) for a table,
/// the query itself otherwise (NOLOCK is not applied to queries).
pub fn source_sql(spec: &str, nolock: bool) -> String {
    match parse_source(spec) {
        Ok(Source::Table { schema, name }) => format!("SELECT * FROM {}.{}{}", bracket(&schema), bracket(&name), if nolock { " WITH (NOLOCK)" } else { "" }),
        _ => spec.trim().trim_end_matches(';').trim().to_string(),
    }
}

/// The table-mode SELECT list entry of a column (see the module docs).
fn table_select_expr(c: &ScanColumn) -> String {
    let base = c.sql_type.split('(').next().unwrap_or("").trim().to_ascii_lowercase();
    let q = bracket(&c.name);
    match base.as_str() {
        "money" => format!("CAST({q} AS DECIMAL(19,4)) AS {q}"),
        "smallmoney" => format!("CAST({q} AS DECIMAL(10,4)) AS {q}"),
        _ => import::select_expr(&base, &c.name),
    }
}

/// The DuckDB type a column of SQL Server type `system_type_name` is read as (the importer's mapping;
/// SQL Server 2025 `vector` arrives as its JSON text, so VARCHAR here).
fn duck_type_of(system_type_name: &str, max_length: i16, precision: u8, scale: u8) -> String {
    let base = system_type_name.split('(').next().unwrap_or("").trim().to_ascii_lowercase();
    let (t, _) = mssql_to_duck(&base, max_length, precision, scale);
    if t.ends_with("[]") { "VARCHAR".into() } else { t }
}

/// The columns of the first result set of `sql` (`sys.dm_exec_describe_first_result_set`; hidden
/// columns left out, unnamed ones called `col<n>`, duplicates suffixed `_<n>`). A query SQL Server
/// cannot describe fails with the server's message.
pub async fn describe(client: &mut SourceClient, sql: &str) -> Result<Vec<ScanColumn>> {
    let q = "SELECT name, system_type_name, CAST(is_nullable AS BIT), CAST(max_length AS SMALLINT), CAST(precision AS TINYINT), CAST(scale AS TINYINT), \
             CAST(is_hidden AS BIT), CAST(error_number AS INT), error_message, column_ordinal \
             FROM sys.dm_exec_describe_first_result_set(@P1, NULL, 0) ORDER BY column_ordinal";
    let rows = client.query(q, &[&sql]).await?.into_first_result().await.with_context(|| format!("describing {}", short(sql)))?;
    let mut out: Vec<ScanColumn> = vec![];
    for (i, r) in rows.iter().enumerate() {
        if let Some(n) = r.get::<i32, _>(7) {
            let msg = r.get::<&str, _>(8).unwrap_or("").to_string();
            bail!("SQL Server cannot describe {}: error {n}: {msg}", short(sql));
        }
        if r.get::<bool, _>(6).unwrap_or(false) { continue; }
        let sql_type = r.get::<&str, _>(1).unwrap_or("sql_variant").to_string();
        let mut name = r.get::<&str, _>(0).filter(|n| !n.is_empty()).map(str::to_string).unwrap_or_else(|| format!("col{}", i + 1));
        if out.iter().any(|c| c.name.eq_ignore_ascii_case(&name)) {
            let mut k = 2;
            while out.iter().any(|c| c.name.eq_ignore_ascii_case(&format!("{name}_{k}"))) { k += 1; }
            name = format!("{name}_{k}");
        }
        let duck_type = duck_type_of(&sql_type, r.get(3).unwrap_or(0), r.get(4).unwrap_or(0), r.get(5).unwrap_or(0));
        out.push(ScanColumn { name, sql_type, duck_type, nullable: r.get(2).unwrap_or(true) });
    }
    if out.is_empty() { bail!("{} returns no result set (bridge_scan reads SELECT queries and tables)", short(sql)); }
    Ok(out)
}

fn short(sql: &str) -> String {
    let one: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if one.chars().count() > 200 { format!("{}...", one.chars().take(200).collect::<String>()) } else { one }
}

/// A resolved scan: the columns, the SELECT that is streamed, the table (table mode) and its row
/// count from `sys.partitions` (an estimate under concurrent writes).
#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub columns: Vec<ScanColumn>,
    pub sql: String,
    pub table: Option<(String, String)>,
    pub rows: Option<i64>,
}

async fn plan_on(c: &mut SourceClient, spec: &str, nolock: bool) -> Result<ScanPlan> {
    match parse_source(spec)? {
        Source::Query(q) => {
            fingerprint::assert_read_only(&q)?;
            let columns = describe(c, &q).await?;
            Ok(ScanPlan { columns, sql: q, table: None, rows: None })
        }
        Source::Table { schema, name } => {
            let fq = format!("{}.{}", bracket(&schema), bracket(&name));
            let columns = describe(c, &format!("SELECT * FROM {fq}")).await.with_context(|| format!("reading the columns of {schema}.{name}"))?;
            let list: Vec<String> = columns.iter().map(table_select_expr).collect();
            let sql = format!("SELECT {} FROM {fq}{}", list.join(", "), if nolock { " WITH (NOLOCK)" } else { "" });
            let rows = c.query("SELECT CAST(SUM(p.rows) AS BIGINT) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(@P1) AND p.index_id IN (0, 1)", &[&fq.as_str()])
                .await?.into_first_result().await?.first().and_then(|r| r.get::<i64, _>(0));
            Ok(ScanPlan { columns, sql, table: Some((schema, name)), rows })
        }
    }
}

/// Resolve `spec` (a table or a query, see [`parse_source`]) on `database`: columns, SELECT, row count.
/// Blocking (own runtime).
pub fn plan(target: &MssqlTarget, database: &str, safety: &ReadSafety, spec: &str, nolock: bool) -> Result<ScanPlan> {
    let rt = super::runtime()?;
    rt.block_on(async {
        let mut c = SourceClient::connect(target, database, safety).await.with_context(|| format!("connecting to {database} on {}:{}", target.host, target.port))?;
        plan_on(&mut c, spec, nolock).await
    })
}

/// One batch of rows, one `Value` per column.
pub type Batch = Vec<Vec<Value>>;

/// A running read of one SELECT (see the module docs).
pub struct RowStream {
    rt: Option<tokio::runtime::Runtime>,
    rx: tokio::sync::mpsc::Receiver<Result<Batch>>,
}

impl RowStream {
    /// Connect to `database` with `safety`, describe `sql` and start streaming it in batches of
    /// `batch_rows` rows. Connection and describe errors are returned here, read errors by
    /// [`Self::next_batch`].
    pub fn start(target: &MssqlTarget, database: &str, safety: &ReadSafety, sql: &str, batch_rows: usize) -> Result<(Vec<ScanColumn>, RowStream)> {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).thread_name("duckbridge-scan").enable_all().build()?;
        let (mut client, columns) = rt.block_on(async {
            fingerprint::assert_read_only(sql)?;
            let mut c = SourceClient::connect(target, database, safety).await.with_context(|| format!("connecting to {database} on {}:{}", target.host, target.port))?;
            let cols = describe(&mut c, sql).await?;
            Ok::<_, anyhow::Error>((c, cols))
        })?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Batch>>(PREFETCH);
        let (sql, ncols, lock_ms) = (sql.to_string(), columns.len(), safety.lock_timeout_ms);
        rt.spawn(async move {
            if let Err(e) = read_batches(&mut client, &sql, ncols, batch_rows.max(1), &tx).await {
                let e = if fingerprint::is_lock_timeout(&e) { e.context(format!("lock timeout after {lock_ms} ms reading {} (raise lock_timeout, or use isolation 'snapshot')", short(&sql))) } else { e };
                let _ = tx.send(Err(e)).await;
            }
        });
        Ok((columns, RowStream { rt: Some(rt), rx }))
    }

    /// The next batch (never empty), `None` at the end of the result.
    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self.rx.blocking_recv() {
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() { rt.shutdown_background(); }
    }
}

async fn read_batches(client: &mut SourceClient, sql: &str, ncols: usize, batch_rows: usize, tx: &tokio::sync::mpsc::Sender<Result<Batch>>) -> Result<()> {
    let t0 = std::time::Instant::now();
    let mut stream = client.query(sql, &[]).await?;
    let mut buf: Batch = Vec::with_capacity(batch_rows.min(16_384));
    let mut n = 0i64;
    while let Some(item) = stream.try_next().await? {
        let QueryItem::Row(row) = item else { continue };
        if row.result_index() > 0 { break; } // the first result set only
        if row.len() != ncols { bail!("{} returned {} columns, {ncols} were described", short(sql), row.len()); }
        buf.push(row.into_iter().map(column_to_value).collect());
        if buf.len() >= batch_rows {
            n += buf.len() as i64;
            if tx.send(Ok(std::mem::replace(&mut buf, Vec::with_capacity(batch_rows.min(16_384))))).await.is_err() { return Ok(()); }
        }
    }
    n += buf.len() as i64;
    if !buf.is_empty() { let _ = tx.send(Ok(buf)).await; }
    crate::core::runlog::stmt("source", "scan", sql, n, t0);
    Ok(())
}

// ---------------------------------------------------------------- catalog / import script

/// The importer's catalog of `database` (tables with columns, keys, identities and row counts; views),
/// filtered by `o.include` / `o.exclude`, plus its warnings. Blocking (own runtime).
pub fn catalog(target: &MssqlTarget, database: &str, safety: &ReadSafety, o: &ImportOptions) -> Result<(Vec<MsTable>, Vec<MsView>, Vec<String>)> {
    let rt = super::runtime()?;
    let mut warnings = vec![];
    let (t, v) = rt.block_on(async {
        let mut c = fingerprint::connect(target, database, safety).await.with_context(|| format!("connecting to {database} on {}:{}", target.host, target.port))?;
        import::read_catalog_with(&mut c, o, safety, &mut warnings).await
    })?;
    Ok((t, v, warnings))
}

/// What [`import_script`] generates besides the tables.
#[derive(Debug, Clone)]
pub struct ScriptOptions {
    /// PRIMARY KEY / UNIQUE / FOREIGN KEY constraints (inline, or after the load with
    /// `ImportOptions::keys_after_load`); `false` creates the tables without any.
    pub keys: bool,
    /// Drop existing DuckDB tables / views / identity sequences of the same names first.
    pub overwrite: bool,
    /// Create the views (translated from T-SQL); ones that do not translate are reported as warnings.
    pub views: bool,
    /// Tables only, no data.
    pub schema_only: bool,
    /// Extra named arguments of every `bridge_scan` call, e.g. `, nolock := true` (already SQL).
    pub scan_args: String,
}

impl Default for ScriptOptions {
    fn default() -> Self { ScriptOptions { keys: true, overwrite: false, views: true, schema_only: false, scan_args: String::new() } }
}

/// `s` as a DuckDB string literal.
pub fn duck_str(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// The DuckDB script that imports `tables` / `views` (from [`catalog`]) through
/// `bridge_scan(dsn_text, ...)`: the importer's DDL (schemas, identity sequences, tables with NOT NULL,
/// identity defaults and keys), one `INSERT INTO ... SELECT ... FROM bridge_scan(..)` per table in FK
/// order, keys added after the load, views, and a closing `SELECT` of (object, kind, rows, note) rows:
/// one per table with its DuckDB row count, one per view, one per warning. `dsn_text` is the
/// `mssql://...` URL of the source database as given (it ends up in the script).
pub fn import_script(dsn_text: &str, tables: &[MsTable], views: &[MsView], o: &ImportOptions, s: &ScriptOptions, warnings: &mut Vec<String>) -> String {
    let sm = |x: &str| o.schema_map.get(x).cloned().unwrap_or_else(|| x.to_string());
    let stripped: Vec<MsTable>;
    let tables: &[MsTable] = if s.keys { tables } else {
        stripped = tables.iter().map(|t| MsTable { pk: vec![], uniques: vec![], fks: vec![], ..t.clone() }).collect();
        &stripped
    };
    let views: &[MsView] = if s.views { views } else { &[] };
    let mut out: Vec<String> = vec![format!("-- DuckBridge import: {} table(s), {} view(s) through bridge_scan", tables.len(), views.len())];
    let (ddl, order) = import::duck_schema_sql(tables, views, o, warnings);
    if s.overwrite {
        for v in views { out.push(format!("DROP VIEW IF EXISTS {}.{}", quote_duck(&sm(&v.schema)), quote_duck(&v.name))); }
        for &i in order.iter().rev() {
            let t = &tables[i];
            out.push(format!("DROP TABLE IF EXISTS {}.{} CASCADE", quote_duck(&sm(&t.schema)), quote_duck(&t.name)));
        }
        for d in &ddl {
            if let Some(rest) = d.sql.strip_prefix("CREATE SEQUENCE IF NOT EXISTS ") {
                let name = rest.split(" START ").next().unwrap_or(rest);
                out.push(format!("DROP SEQUENCE IF EXISTS {name}"));
            }
        }
    }
    out.extend(ddl.into_iter().map(|d| d.sql));
    if !s.schema_only {
        for &i in &order {
            let t = &tables[i];
            let cols: Vec<String> = t.columns.iter().map(|c| quote_duck(&c.name)).collect();
            out.push(format!("INSERT INTO {}.{} ({}) SELECT {} FROM bridge_scan({}, {}{})", quote_duck(&sm(&t.schema)), quote_duck(&t.name), cols.join(", "), cols.join(", "),
                duck_str(dsn_text), duck_str(&format!("{}.{}", bracket(&t.schema), bracket(&t.name))), s.scan_args));
        }
    }
    out.extend(import::keys_sql(tables, o).into_iter().map(|k| k.sql));
    let mut made_views = vec![];
    for v in views {
        match import::duck_view_sql(v, o) {
            Ok((sql, w)) => {
                warnings.extend(w.into_iter().map(|w| format!("view {}.{}: {w}", v.schema, v.name)));
                out.push(sql);
                made_views.push(v);
            }
            Err(e) => warnings.push(format!("view {}.{} not created: {e}", v.schema, v.name)),
        }
    }
    let mut rows: Vec<String> = vec![];
    for &i in &order {
        let t = &tables[i];
        let fq = format!("{}.{}", quote_duck(&sm(&t.schema)), quote_duck(&t.name));
        rows.push(format!("SELECT {} AS object, 'table' AS kind, (SELECT count(*) FROM {fq}) AS rows, {} AS note",
            duck_str(&format!("{}.{}", sm(&t.schema), t.name)), duck_str(&format!("source {}.{} ({} rows in sys.partitions)", t.schema, t.name, t.row_count))));
    }
    for v in made_views { rows.push(format!("SELECT {}, 'view', 0, ''", duck_str(&format!("{}.{}", sm(&v.schema), v.name)))); }
    for w in warnings.iter() { rows.push(format!("SELECT '', 'warning', 0, {}", duck_str(w))); }
    if rows.is_empty() { rows.push("SELECT '*' AS object, 'summary' AS kind, 0 AS rows, 'nothing to import' AS note".into()); }
    out.push(rows.join("\nUNION ALL "));
    out.join(";\n") + ";"
}

/// Whether the catalog lookup found `schema.name` among `tables`, case-insensitively (as SQL Server
/// resolves names under the default collations).
pub fn find_table<'a>(tables: &'a [MsTable], schema: &str, name: &str) -> Option<&'a MsTable> {
    tables.iter().find(|t| t.schema.eq_ignore_ascii_case(schema) && t.name.eq_ignore_ascii_case(name))
}

/// The database a URL names, or an error saying `what` needs one.
pub fn require_database(database: Option<&str>, what: &str) -> Result<String> {
    database.filter(|d| !d.trim().is_empty()).map(str::to_string).ok_or_else(|| anyhow!("{what}: the URL must name the database: mssql://user:password@host:port/Database"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mssql::import::MsColumn;

    #[test]
    fn scan_table_names_and_queries() {
        assert_eq!(parse_table_name("dbo.orders"), Some(("dbo".into(), "orders".into())));
        assert_eq!(parse_table_name("orders"), Some(("dbo".into(), "orders".into())));
        assert_eq!(parse_table_name(" [sales].[my]]t] "), Some(("sales".into(), "my]t".into())));
        assert_eq!(parse_table_name("\"a b\".\"c\"\"d\""), Some(("a b".into(), "c\"d".into())));
        assert_eq!(parse_table_name("db.dbo.t"), None);
        assert_eq!(parse_table_name("dbo.t WHERE 1=1"), None);
        assert_eq!(parse_table_name("1abc"), None);
        assert_eq!(parse_source("SELECT 1;").unwrap(), Source::Query("SELECT 1".into()));
        assert_eq!(parse_source(" with x as (select 1 a) select * from x").unwrap(), Source::Query("with x as (select 1 a) select * from x".into()));
        assert_eq!(parse_source("(SELECT 1)").unwrap(), Source::Query("(SELECT 1)".into()));
        assert!(parse_source("dbo.t WHERE 1=1").unwrap_err().to_string().contains("neither"));
        assert_eq!(source_sql("dbo.orders", false), "SELECT * FROM [dbo].[orders]");
        assert_eq!(source_sql("[s].[a]]b]", true), "SELECT * FROM [s].[a]]b] WITH (NOLOCK)");
        assert_eq!(source_sql("SELECT id FROM dbo.t WHERE x > 1;", true), "SELECT id FROM dbo.t WHERE x > 1");
        assert_eq!(source_sql("selectivity", false), "SELECT * FROM [dbo].[selectivity]");
    }

    #[test]
    fn scan_types_and_select_list() {
        assert_eq!(duck_type_of("decimal(18,4)", 9, 18, 4), "DECIMAL(18,4)");
        assert_eq!(duck_type_of("nvarchar(max)", -1, 0, 0), "VARCHAR");
        assert_eq!(duck_type_of("datetimeoffset(7)", 10, 34, 7), "TIMESTAMP WITH TIME ZONE");
        assert_eq!(duck_type_of("uniqueidentifier", 16, 0, 0), "UUID");
        assert_eq!(duck_type_of("vector(3)", 0, 0, 0), "VARCHAR");
        let col = |n: &str, t: &str| ScanColumn { name: n.into(), sql_type: t.into(), duck_type: String::new(), nullable: true };
        assert_eq!(table_select_expr(&col("m", "money")), "CAST([m] AS DECIMAL(19,4)) AS [m]");
        assert_eq!(table_select_expr(&col("x", "xml")), "CAST([x] AS NVARCHAR(MAX)) AS [x]");
        assert_eq!(table_select_expr(&col("g", "geography")), "[g].ToString() AS [g]");
        assert_eq!(table_select_expr(&col("a]", "int")), "[a]]]");
    }

    fn table(schema: &str, name: &str, cols: &[(&str, &str, bool)], pk: &[&str]) -> MsTable {
        MsTable {
            schema: schema.into(), name: name.into(), pk: pk.iter().map(|s| s.to_string()).collect(), uniques: vec![], fks: vec![], row_count: 3,
            columns: cols.iter().map(|(n, t, ident)| MsColumn { name: n.to_string(), type_name: String::new(), max_length: 0, precision: 0, scale: 0, nullable: !*ident,
                is_identity: *ident, seed: 11, increment: 1, duck_type: t.to_string(), select_expr: String::new() }).collect(),
        }
    }

    #[test]
    fn scan_import_script() {
        let mut child = table("dbo", "orders", &[("id", "INTEGER", true), ("cust", "INTEGER", false)], &["id"]);
        child.fks.push((vec!["cust".into()], "sales".into(), "cust's".into(), vec!["id".into()]));
        let parent = table("sales", "cust's", &[("id", "INTEGER", false), ("name", "VARCHAR", false)], &["id"]);
        let o = ImportOptions { keys_after_load: false, include: vec![], exclude: vec![], parallel: 1, split_threshold: 0, batch_rows: 1, schema_map: crate::api::parse_schema_map("", &[("dbo", "main")]), nolock: false };
        let views = vec![MsView { schema: "dbo".into(), name: "v".into(), definition: "CREATE VIEW dbo.v AS SELECT TOP 5 id FROM dbo.orders".into() }];
        let mut w = vec![];
        let dsn = "mssql://sa:p'w@h:1/Db";
        let s = import_script(dsn, &[child.clone(), parent.clone()], &views, &o, &ScriptOptions { scan_args: ", nolock := true".into(), ..ScriptOptions::default() }, &mut w);
        let stmts: Vec<&str> = s.split(";\n").collect();
        assert!(stmts[0].starts_with("-- DuckBridge import: 2 table(s), 1 view(s)"), "{s}");
        assert_eq!(stmts[1], "CREATE SCHEMA IF NOT EXISTS \"sales\"");
        // FK target first, identity sequence before its table
        let pos = |needle: &str| s.find(needle).unwrap_or_else(|| panic!("{needle} not in {s}"));
        assert!(pos("CREATE TABLE \"sales\".\"cust's\"") < pos("CREATE SEQUENCE IF NOT EXISTS \"main\".\"seq_orders_id\" START 11"));
        assert!(pos("seq_orders_id\" START 11") < pos("CREATE TABLE \"main\".\"orders\""));
        assert!(s.contains("FOREIGN KEY (\"cust\") REFERENCES \"sales\".\"cust's\" (\"id\")"));
        assert!(pos("INSERT INTO \"sales\".\"cust's\"") < pos("INSERT INTO \"main\".\"orders\""));
        assert!(s.contains("INSERT INTO \"main\".\"orders\" (\"id\", \"cust\") SELECT \"id\", \"cust\" FROM bridge_scan('mssql://sa:p''w@h:1/Db', '[dbo].[orders]', nolock := true)"), "{s}");
        assert!(s.contains("bridge_scan('mssql://sa:p''w@h:1/Db', '[sales].[cust''s]', nolock := true)"), "{s}");
        assert!(s.contains("CREATE OR REPLACE VIEW \"main\".\"v\" AS SELECT id FROM main.orders LIMIT 5"), "{s}");
        assert!(s.contains("SELECT 'main.orders' AS object, 'table' AS kind, (SELECT count(*) FROM \"main\".\"orders\") AS rows"), "{s}");
        assert!(s.ends_with(";") && !s.contains("DROP"));
        // overwrite, no keys, keys after load, schema only, no views
        let s = import_script(dsn, &[child.clone(), parent.clone()], &views, &o, &ScriptOptions { overwrite: true, keys: false, views: false, schema_only: true, scan_args: String::new() }, &mut vec![]);
        assert!(s.contains("DROP TABLE IF EXISTS \"main\".\"orders\" CASCADE") && s.contains("DROP TABLE IF EXISTS \"sales\".\"cust's\" CASCADE"), "{s}");
        assert!(s.find("DROP TABLE").unwrap() < s.find("CREATE SCHEMA").unwrap());
        assert!(s.contains("DROP SEQUENCE IF EXISTS \"main\".\"seq_orders_id\""));
        assert!(!s.contains("PRIMARY KEY") && !s.contains("FOREIGN KEY") && !s.contains("INSERT INTO") && !s.contains("VIEW \"main\""), "{s}");
        let o2 = ImportOptions { keys_after_load: true, ..o };
        let s = import_script(dsn, &[parent], &[], &o2, &ScriptOptions::default(), &mut vec![]);
        assert!(s.find("INSERT INTO").unwrap() < s.find("ALTER TABLE \"sales\".\"cust's\" ADD PRIMARY KEY (\"id\")").unwrap(), "{s}");
        // nothing to import
        assert!(import_script(dsn, &[], &[], &o2, &ScriptOptions::default(), &mut vec![]).contains("'nothing to import'"));
    }

    /// Against the local SQL Server (skipped unless MSSQL_PASSWORD is set; MSSQL_HOST / MSSQL_PORT /
    /// MSSQL_USER / MSSQL_DATABASE default to localhost / 14319 / sa / d2m_extsrc, the e2e fixture).
    #[test]
    fn scan_describe_live() {
        let Ok(password) = std::env::var("MSSQL_PASSWORD") else { eprintln!("MSSQL_PASSWORD not set; skipped"); return };
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let db = env("MSSQL_DATABASE", "d2m_extsrc");
        let target = MssqlTarget { host: env("MSSQL_HOST", "localhost"), port: env("MSSQL_PORT", "14319").parse().unwrap(), user: env("MSSQL_USER", "sa"), password,
            database: db.clone(), trust_cert: true, encrypt: false, windows_auth: false };
        let safety = ReadSafety::default();
        let p = plan(&target, &db, &safety, "dbo.customers", true).unwrap();
        let names: Vec<&str> = p.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "born", "vip", "score", "balance", "uid", "created", "notes"]);
        let ty = |n: &str| p.columns.iter().find(|c| c.name == n).unwrap().duck_type.clone();
        assert_eq!((ty("balance"), ty("uid"), ty("created"), ty("vip")), ("DECIMAL(18,4)".into(), "UUID".into(), "TIMESTAMP".into(), "BOOLEAN".into()));
        assert!(p.sql.ends_with("FROM [dbo].[customers] WITH (NOLOCK)") && p.rows == Some(3), "{p:?}");
        let q = plan(&target, &db, &safety, "SELECT id, CAST(amount AS DECIMAL(10,1)) AS a, 1, 2 AS id FROM dbo.orders", false).unwrap();
        let qn: Vec<(&str, &str)> = q.columns.iter().map(|c| (c.name.as_str(), c.duck_type.as_str())).collect();
        assert_eq!(qn, [("id", "INTEGER"), ("a", "DECIMAL(10,1)"), ("col3", "INTEGER"), ("id_2", "INTEGER")]);
        let e = plan(&target, &db, &safety, "dbo.no_such_table", false).unwrap_err();
        assert!(format!("{e:#}").contains("no_such_table"), "{e:#}");
        let e = plan(&target, &db, &safety, "SELECT * INTO dbo.x FROM dbo.orders", false).unwrap_err();
        assert!(format!("{e:#}").contains("read-only"), "{e:#}");
        // stream: all rows, batches capped at batch_rows, cancel by drop
        let (cols, mut s) = RowStream::start(&target, &db, &safety, "SELECT event_id, kind FROM dbo.events", 7_000).unwrap();
        assert_eq!(cols.len(), 2);
        let (mut n, mut batches) = (0, 0);
        while let Some(b) = s.next_batch().unwrap() { assert!(b.len() <= 7_000 && !b.is_empty()); n += b.len(); batches += 1; }
        assert_eq!((n, batches), (200_000, 29));
        let (_, mut s) = RowStream::start(&target, &db, &safety, "SELECT * FROM dbo.events", 100).unwrap();
        assert_eq!(s.next_batch().unwrap().unwrap().len(), 100);
        drop(s);
        let (_, mut s) = RowStream::start(&target, &db, &safety, "SELECT 1/0 AS x", 100).unwrap();
        assert!(format!("{:#}", s.next_batch().unwrap_err()).contains("Divide by zero"));
        let (t, v, _) = catalog(&target, &db, &safety, &ImportOptions { keys_after_load: false, include: vec![], exclude: vec!["events".into()], parallel: 1, split_threshold: 0, batch_rows: 1, schema_map: Default::default(), nolock: false }).unwrap();
        assert_eq!((t.len(), v.len()), (3, 2));
        assert!(find_table(&t, "DBO", "Orders").is_some());
    }
}
