//! Safe reads from a busy SQL Server source (spec R6).
//!
//! * [`apply_session_settings`]: `LOCK_TIMEOUT`, `DEADLOCK_PRIORITY LOW` and the isolation level on every
//!   import connection.
//! * [`held_locks`] / [`wait_for_unlocked`]: pre-flight lock check per table (X / IX / SIX / Sch-M held by
//!   other sessions), wait up to `lock_wait_s`, then fail or skip.
//! * [`Fingerprint`] / [`take`]: change fingerprint before and after every table read (partition stats
//!   rows, `last_user_update`, `modify_date`); a difference triggers a re-read.
//! * [`ManifestRow`] / [`write_manifest`]: what was read, as CSV plus a JSON copy.
//! * [`wait_for`]: poll a user gate query until it returns true.
//! * [`SourceClient`]: the source connection type used for data reads. It exposes only read methods and
//!   refuses statements that write ([`assert_read_only`]).
use super::{exec_batch_as, MsClient, MssqlTarget};
use crate::core::runlog::{self, Failure};
use anyhow::{bail, Context, Result};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tiberius::{ColumnData, Config, QueryStream, ToSql};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

// ---------------------------------------------------------------- options

/// Isolation level of the import connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Isolation {
    #[default]
    ReadCommitted,
    ReadUncommitted,
    Snapshot,
}

impl Isolation {
    /// `read-committed` / `read_committed` / `rc`, `read-uncommitted` / `nolock`, `snapshot`.
    pub fn parse(s: &str) -> Result<Isolation> {
        match s.trim().to_ascii_lowercase().replace([' ', '_'], "-").as_str() {
            "read-committed" | "rc" => Ok(Isolation::ReadCommitted),
            "read-uncommitted" | "ru" | "nolock" => Ok(Isolation::ReadUncommitted),
            "snapshot" => Ok(Isolation::Snapshot),
            other => bail!("unknown isolation '{other}': expected read-committed, read-uncommitted or snapshot"),
        }
    }
    /// The CLI spelling, used in the manifest.
    pub fn name(&self) -> &'static str {
        match self { Isolation::ReadCommitted => "read-committed", Isolation::ReadUncommitted => "read-uncommitted", Isolation::Snapshot => "snapshot" }
    }
    /// The T-SQL spelling for `SET TRANSACTION ISOLATION LEVEL`.
    pub fn sql(&self) -> &'static str {
        match self { Isolation::ReadCommitted => "READ COMMITTED", Isolation::ReadUncommitted => "READ UNCOMMITTED", Isolation::Snapshot => "SNAPSHOT" }
    }
}

impl std::fmt::Display for Isolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.name()) }
}

/// What to do with a table that is still locked after `lock_wait_s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnLock {
    #[default]
    Fail,
    Skip,
}

impl OnLock {
    pub fn parse(s: &str) -> Result<OnLock> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" => Ok(OnLock::Fail),
            "skip" => Ok(OnLock::Skip),
            other => bail!("unknown --on-lock value '{other}': expected fail or skip"),
        }
    }
}

/// Read-safety settings for `import` (CLI flags in parentheses).
#[derive(Debug, Clone, PartialEq)]
pub struct ReadSafety {
    /// `SET LOCK_TIMEOUT` in ms (`--lock-timeout`, default 1000; -1 waits forever).
    pub lock_timeout_ms: i64,
    /// `--isolation`, default read committed.
    pub isolation: Isolation,
    /// Pre-flight lock check per table (`--lock-check` / `--no-lock-check`, default on).
    pub lock_check: bool,
    /// How long to wait for other sessions' locks to go away (`--lock-wait`, seconds, default 30).
    pub lock_wait_s: u64,
    /// `--on-lock fail|skip`, default fail.
    pub on_lock: OnLock,
    /// Re-reads of a table whose fingerprint changed during the read (`--max-rereads`, default 2).
    pub max_rereads: u32,
    /// `ApplicationIntent=ReadOnly` (`--readonly`).
    pub readonly_intent: bool,
    /// Manifest CSV path; the JSON copy goes next to it (`--manifest`).
    pub manifest: Option<PathBuf>,
    /// Gate query polled before the import starts (`--wait-for`).
    pub wait_for: Option<String>,
    /// Timeout of the gate query in seconds (`--wait-timeout`, default 3600).
    pub wait_timeout_s: u64,
}

impl Default for ReadSafety {
    fn default() -> Self {
        ReadSafety { lock_timeout_ms: 1000, isolation: Isolation::ReadCommitted, lock_check: true, lock_wait_s: 30, on_lock: OnLock::Fail, max_rereads: 2, readonly_intent: false, manifest: None, wait_for: None, wait_timeout_s: 3600 }
    }
}

// ---------------------------------------------------------------- connections

/// The normal connection config with `ApplicationIntent=ReadOnly` (routes to a readable secondary
/// through an availability-group listener; ignored by a standalone server).
pub fn readonly_config(target: &MssqlTarget, database: &str) -> Result<Config> {
    let mut c = target.config(database)?;
    c.readonly(true);
    Ok(c)
}

/// tiberius 0.13 has `Config::readonly`, so `--readonly` is honoured.
pub fn readonly_supported() -> bool { true }

/// Connect to the source (read-only intent when asked, following an AG read-only routing redirect) and
/// apply the session settings.
pub async fn connect(target: &MssqlTarget, database: &str, o: &ReadSafety) -> Result<MsClient> {
    let mut cfg = if o.readonly_intent { readonly_config(target, database)? } else { target.config(database)? };
    let mut hops = 0;
    let mut c = loop {
        let tcp = TcpStream::connect(cfg.get_addr()).await.with_context(|| format!("connecting to SQL Server at {}", cfg.get_addr()))?;
        tcp.set_nodelay(true)?;
        match tiberius::Client::connect(cfg.clone(), tcp.compat_write()).await {
            Ok(c) => break c,
            Err(tiberius::error::Error::Routing { host, port }) if hops < 2 => {
                runlog::note(&format!("source: read-only routing to {host}:{port}"));
                cfg.host(&host);
                cfg.port(port);
                hops += 1;
            }
            Err(e) => return Err(anyhow::Error::new(e).context("SQL Server login failed")),
        }
    };
    apply_session_settings(&mut c, o).await?;
    Ok(c)
}

/// Row-versioning state of the current database: (ALLOW_SNAPSHOT_ISOLATION on, READ_COMMITTED_SNAPSHOT on).
pub async fn snapshot_state(c: &mut MsClient) -> Result<(bool, bool)> {
    let sql = "SELECT CAST(CASE WHEN snapshot_isolation_state = 1 THEN 1 ELSE 0 END AS BIT), is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()";
    let t0 = Instant::now();
    let rows = c.simple_query(sql).await?.into_first_result().await?;
    runlog::stmt("source", "", sql, rows.len() as i64, t0);
    Ok(rows.first().map(|r| (r.get::<bool, _>(0).unwrap_or(false), r.get::<bool, _>(1).unwrap_or(false))).unwrap_or((false, false)))
}

/// `SET LOCK_TIMEOUT n; SET DEADLOCK_PRIORITY LOW; SET TRANSACTION ISOLATION LEVEL ...` on this session.
/// `Snapshot` uses SNAPSHOT when ALLOW_SNAPSHOT_ISOLATION is on, else READ COMMITTED when the database has
/// READ_COMMITTED_SNAPSHOT (statement-level row versioning), else it is an error.
pub async fn apply_session_settings(c: &mut MsClient, o: &ReadSafety) -> Result<()> {
    if o.lock_timeout_ms < -1 { bail!("--lock-timeout must be -1 (wait forever), 0 or a number of milliseconds; got {}", o.lock_timeout_ms); }
    let level = match o.isolation {
        Isolation::Snapshot => {
            let (si, rcsi) = snapshot_state(c).await?;
            if si {
                "SNAPSHOT"
            } else if rcsi {
                runlog::note("source: ALLOW_SNAPSHOT_ISOLATION is off but READ_COMMITTED_SNAPSHOT is on; --isolation snapshot reads with READ COMMITTED (statement-level row versioning)");
                "READ COMMITTED"
            } else {
                return Err(Failure::Check("--isolation snapshot: the source database has neither ALLOW_SNAPSHOT_ISOLATION nor READ_COMMITTED_SNAPSHOT on; ask the DBA to enable ALLOW_SNAPSHOT_ISOLATION, or use --isolation read-committed".into()).into());
            }
        }
        other => other.sql(),
    };
    exec_batch_as(c, "source", "", &session_settings_sql(o.lock_timeout_ms, level)).await
}

fn session_settings_sql(lock_timeout_ms: i64, level: &str) -> String {
    format!("SET LOCK_TIMEOUT {lock_timeout_ms}; SET DEADLOCK_PRIORITY LOW; SET TRANSACTION ISOLATION LEVEL {level};")
}

/// Advice when the database supports row versioning and the caller is not using `--isolation snapshot`.
pub async fn recommend_isolation(c: &mut MsClient, current: Isolation) -> Result<Option<String>> {
    if current == Isolation::Snapshot { return Ok(None); }
    let (si, rcsi) = snapshot_state(c).await?;
    if !si && !rcsi { return Ok(None); }
    let on: Vec<&str> = [(si, "ALLOW_SNAPSHOT_ISOLATION"), (rcsi, "READ_COMMITTED_SNAPSHOT")].iter().filter(|x| x.0).map(|x| x.1).collect();
    Ok(Some(format!("the source database has {} on: --isolation snapshot gives consistent reads without locks", on.join(" and "))))
}

// ---------------------------------------------------------------- read-only source client

/// The source connection used for data reads. It cannot run DDL or DML: it exposes only `query` /
/// `simple_query` (checked with [`assert_read_only`]) and the read-only helpers of this module. Session
/// settings are applied before wrapping.
pub struct SourceClient(MsClient);

impl SourceClient {
    /// Connect with [`connect`] (settings applied) and wrap.
    pub async fn connect(target: &MssqlTarget, database: &str, o: &ReadSafety) -> Result<SourceClient> {
        Ok(SourceClient(connect(target, database, o).await?))
    }
    /// Apply the session settings to an existing connection and wrap it.
    pub async fn wrap(mut c: MsClient, o: &ReadSafety) -> Result<SourceClient> {
        apply_session_settings(&mut c, o).await?;
        Ok(SourceClient(c))
    }
    pub async fn query<'a, 'b>(&'a mut self, sql: impl Into<Cow<'b, str>>, params: &'b [&'b dyn ToSql]) -> Result<QueryStream<'a>>
    where
        'a: 'b,
    {
        let sql: Cow<'b, str> = sql.into();
        assert_read_only(&sql)?;
        Ok(self.0.query(sql, params).await?)
    }
    pub async fn simple_query<'a, 'b>(&'a mut self, sql: impl Into<Cow<'b, str>>) -> Result<QueryStream<'a>>
    where
        'a: 'b,
    {
        let sql: Cow<'b, str> = sql.into();
        assert_read_only(&sql)?;
        Ok(self.0.simple_query(sql).await?)
    }
    pub async fn take(&mut self, schema: &str, table: &str) -> Result<Fingerprint> { take(&mut self.0, schema, table).await }
    pub async fn held_locks(&mut self, schema: &str, table: &str) -> Result<Vec<LockInfo>> { held_locks(&mut self.0, schema, table).await }
    pub async fn wait_for_unlocked(&mut self, schema: &str, table: &str, o: &ReadSafety) -> Result<LockOutcome> { wait_for_unlocked(&mut self.0, schema, table, o).await }
    pub async fn wait_for(&mut self, sql: &str, timeout: Duration) -> Result<()> { wait_for(&mut self.0, sql, timeout).await }
    pub async fn recommend_isolation(&mut self, current: Isolation) -> Result<Option<String>> { recommend_isolation(&mut self.0, current).await }
}

const WRITE_KEYWORDS: &[&str] = &[
    "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "EXEC", "EXECUTE", "INTO", "GRANT", "REVOKE",
    "DENY", "BACKUP", "RESTORE", "DBCC", "KILL", "SHUTDOWN", "BULK", "RECONFIGURE", "WRITETEXT", "UPDATETEXT",
];

/// Refuse a statement on the source connection when it contains a write keyword outside string literals,
/// quoted/bracketed identifiers and comments (`Failure::Guard`).
pub fn assert_read_only(sql: &str) -> Result<()> {
    let b: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < b.len() {
        let ch = b[i];
        if ch == '\'' || ch == '"' || ch == '[' {
            let close = if ch == '[' { ']' } else { ch };
            i += 1;
            while i < b.len() {
                if b[i] == close {
                    if i + 1 < b.len() && b[i + 1] == close { i += 2; continue; }
                    break;
                }
                i += 1;
            }
            i += 1;
        } else if ch == '-' && b.get(i + 1) == Some(&'-') {
            while i < b.len() && b[i] != '\n' { i += 1; }
        } else if ch == '/' && b.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == '*' && b[i + 1] == '/') { i += 1; }
            i += 2;
        } else if ch.is_alphanumeric() || ch == '_' || ch == '@' || ch == '#' {
            let start = i;
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '@' || b[i] == '#' || b[i] == '$') { i += 1; }
            let word: String = b[start..i].iter().collect::<String>().to_ascii_uppercase();
            if WRITE_KEYWORDS.contains(&word.as_str()) {
                return Err(Failure::Guard(format!("the import source connection is read-only: refusing a statement containing {word}: {}", sql.chars().take(200).collect::<String>())).into());
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- locks

/// A lock held by another session on the table (object or one of its heaps/B-trees).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LockInfo {
    pub session_id: i32,
    pub request_mode: String,
    pub resource_type: String,
    pub login_name: String,
    pub host_name: String,
    pub program_name: String,
}

impl std::fmt::Display for LockInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "session {} ({}@{}, {}) holds {} on {}", self.session_id, self.login_name, self.host_name, self.program_name, self.request_mode, self.resource_type)
    }
}

/// Granted X / IX / SIX / Sch-M locks of other sessions on the object or its hobt_ids, in the current
/// database. `@P1` is the two-part bracketed table name. (U locks are compatible with readers and ignored.)
pub const HELD_LOCKS_SQL: &str = "SELECT DISTINCT l.request_session_id, CAST(l.request_mode AS NVARCHAR(60)), CAST(l.resource_type AS NVARCHAR(60)), \
        ISNULL(s.login_name, N''), ISNULL(s.host_name, N''), ISNULL(s.program_name, N'') \
 FROM sys.dm_tran_locks l LEFT JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id \
 WHERE l.resource_database_id = DB_ID() AND l.request_session_id <> @@SPID AND l.request_status = 'GRANT' \
   AND l.request_mode IN ('X', 'IX', 'SIX', 'Sch-M') \
   AND l.resource_associated_entity_id IN (SELECT CAST(OBJECT_ID(@P1) AS BIGINT) UNION ALL SELECT p.hobt_id FROM sys.partitions p WHERE p.object_id = OBJECT_ID(@P1)) \
 ORDER BY 1, 2, 3";

fn two_part(schema: &str, table: &str) -> String { format!("{}.{}", super::bracket(schema), super::bracket(table)) }

/// Locks of other sessions that may conflict with our reads (needs VIEW SERVER STATE).
pub async fn held_locks(c: &mut MsClient, schema: &str, table: &str) -> Result<Vec<LockInfo>> {
    let name = two_part(schema, table);
    let t0 = Instant::now();
    let rows = c.query(HELD_LOCKS_SQL, &[&name.as_str()]).await?.into_first_result().await.with_context(|| format!("lock check on {schema}.{table} (needs VIEW SERVER STATE)"))?;
    runlog::stmt("source", &format!("{schema}.{table}"), HELD_LOCKS_SQL, rows.len() as i64, t0);
    Ok(rows.iter().map(|r| {
        let s = |i: usize| -> String { r.get::<&str, _>(i).unwrap_or("").to_string() };
        LockInfo { session_id: r.get::<i32, _>(0).unwrap_or(0), request_mode: s(1), resource_type: s(2), login_name: s(3), host_name: s(4), program_name: s(5) }
    }).collect())
}

/// Lock modes that can block a reader at this isolation level: under SNAPSHOT / READ UNCOMMITTED only
/// a schema-modification lock blocks (the reader takes Sch-S), otherwise X / IX / SIX too.
pub fn blocks_reader(mode: &str, isolation: Isolation) -> bool {
    match isolation {
        Isolation::ReadCommitted => matches!(mode, "X" | "IX" | "SIX" | "Sch-M"),
        Isolation::ReadUncommitted | Isolation::Snapshot => mode == "Sch-M",
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LockOutcome {
    Clear,
    Skipped(Vec<LockInfo>),
}

const LOCK_POLL: Duration = Duration::from_secs(2);

/// Poll [`held_locks`] every 2 s for up to `lock_wait_s`. Still locked: `OnLock::Fail` returns
/// `Failure::Check` naming the sessions, `OnLock::Skip` returns `Skipped`.
pub async fn wait_for_unlocked(c: &mut MsClient, schema: &str, table: &str, o: &ReadSafety) -> Result<LockOutcome> {
    let deadline = Instant::now() + Duration::from_secs(o.lock_wait_s);
    let mut first = true;
    loop {
        let locks: Vec<LockInfo> = held_locks(c, schema, table).await?.into_iter().filter(|l| blocks_reader(&l.request_mode, o.isolation)).collect();
        if locks.is_empty() {
            if !first { runlog::note(&format!("{schema}.{table}: locks released")); }
            return Ok(LockOutcome::Clear);
        }
        let who = describe_locks(&locks);
        let now = Instant::now();
        if now >= deadline {
            return match o.on_lock {
                OnLock::Fail => Err(Failure::Check(format!("{schema}.{table} is locked by other sessions after waiting {}s: {who} (wait longer with --lock-wait, skip it with --on-lock skip, or read row versions with --isolation snapshot)", o.lock_wait_s)).into()),
                OnLock::Skip => {
                    runlog::note(&format!("{schema}.{table}: skipped, still locked after {}s: {who}", o.lock_wait_s));
                    Ok(LockOutcome::Skipped(locks))
                }
            };
        }
        if first { eprintln!("{schema}.{table}: waiting up to {}s for locks: {who}", o.lock_wait_s); }
        runlog::note(&format!("{schema}.{table}: waiting for locks ({}s left): {who}", (deadline - now).as_secs()));
        first = false;
        tokio::time::sleep(LOCK_POLL.min(deadline - now)).await;
    }
}

/// One entry per session: "session 57 (sa@host, prog) holds IX on OBJECT, X on KEY; session 60 ...".
pub fn describe_locks(locks: &[LockInfo]) -> String {
    let mut sessions: Vec<i32> = locks.iter().map(|l| l.session_id).collect();
    sessions.sort();
    sessions.dedup();
    let mut out: Vec<String> = vec![];
    for s in sessions {
        let ls: Vec<&LockInfo> = locks.iter().filter(|l| l.session_id == s).collect();
        let modes: Vec<String> = ls.iter().map(|l| format!("{} on {}", l.request_mode, l.resource_type)).collect();
        let l = ls[0];
        out.push(format!("session {s} ({}@{}, {}) holds {}", l.login_name, l.host_name, l.program_name, modes.join(", ")));
    }
    out.join("; ")
}

// ---------------------------------------------------------------- fingerprint

/// Change fingerprint of a table: partition-stats rows, last user update, schema modify date.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Fingerprint {
    pub rows: i64,
    pub last_user_update: Option<String>,
    pub modify_date: String,
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rows={} last_update={} modified={}", self.rows, self.last_user_update.as_deref().unwrap_or("none"), self.modify_date)
    }
}

/// `@P1` is the two-part bracketed table name.
pub const FINGERPRINT_SQL: &str = "SELECT CAST(ISNULL((SELECT SUM(ps.row_count) FROM sys.dm_db_partition_stats ps WHERE ps.object_id = OBJECT_ID(@P1) AND ps.index_id IN (0, 1)), 0) AS BIGINT), \
        (SELECT CONVERT(VARCHAR(27), MAX(u.last_user_update), 121) FROM sys.dm_db_index_usage_stats u WHERE u.database_id = DB_ID() AND u.object_id = OBJECT_ID(@P1)), \
        (SELECT CONVERT(VARCHAR(27), o.modify_date, 121) FROM sys.objects o WHERE o.object_id = OBJECT_ID(@P1))";

/// Without VIEW SERVER STATE / VIEW DATABASE STATE: rows from sys.partitions, no last update.
const FINGERPRINT_FALLBACK_SQL: &str = "SELECT CAST(ISNULL((SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(@P1) AND p.index_id IN (0, 1)), 0) AS BIGINT), \
        CAST(NULL AS VARCHAR(27)), (SELECT CONVERT(VARCHAR(27), o.modify_date, 121) FROM sys.objects o WHERE o.object_id = OBJECT_ID(@P1))";

async fn fingerprint_query(c: &mut MsClient, sql: &str, name: &str) -> Result<Option<Fingerprint>> {
    let t0 = Instant::now();
    let rows = c.query(sql, &[&name]).await?.into_first_result().await?;
    runlog::stmt("source", name, sql, rows.len() as i64, t0);
    let Some(r) = rows.first() else { return Ok(None) };
    let Some(modify_date) = r.get::<&str, _>(2) else { return Ok(None) };
    Ok(Some(Fingerprint { rows: r.get::<i64, _>(0).unwrap_or(0), last_user_update: r.get::<&str, _>(1).map(|s| s.to_string()), modify_date: modify_date.to_string() }))
}

/// Take the fingerprint of `schema.table`. Falls back to sys.partitions (no last-update time) when the
/// DMVs are not readable with the current permissions.
pub async fn take(c: &mut MsClient, schema: &str, table: &str) -> Result<Fingerprint> {
    let name = two_part(schema, table);
    let fp = match fingerprint_query(c, FINGERPRINT_SQL, &name).await {
        Ok(fp) => fp,
        Err(e) => {
            runlog::note(&format!("{schema}.{table}: fingerprint DMVs not readable ({}); using sys.partitions without last_user_update", e.to_string().lines().next().unwrap_or("")));
            fingerprint_query(c, FINGERPRINT_FALLBACK_SQL, &name).await?
        }
    };
    fp.ok_or_else(|| anyhow::anyhow!("fingerprint: table {schema}.{table} not found in the source database"))
}

// ---------------------------------------------------------------- manifest

/// One manifest line per table read (`rows_read = -1`: skipped because it stayed locked).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ManifestRow {
    pub table: String,
    pub rows_read: i64,
    pub rows_source_stats: i64,
    pub fingerprint_before: String,
    pub fingerprint_after: String,
    pub started: String,
    pub finished: String,
    pub seconds: f64,
    pub isolation: String,
    pub rereads: u32,
}

const MANIFEST_HEADER: &str = "table,rows_read,rows_source_stats,fingerprint_before,fingerprint_after,started,finished,seconds,isolation,rereads";

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) { format!("\"{}\"", s.replace('"', "\"\"")) } else { s.to_string() }
}

/// Path of the JSON copy: `x.csv` -> `x.json`, anything else -> `<path>.json`.
pub fn manifest_json_path(path: &Path) -> PathBuf {
    match path.extension().and_then(|e| e.to_str()) {
        Some(e) if e.eq_ignore_ascii_case("csv") => path.with_extension("json"),
        _ => PathBuf::from(format!("{}.json", path.display())),
    }
}

/// Write the manifest as CSV (header + rows) at `path` and as a JSON array next to it.
pub fn write_manifest(path: &Path, rows: &[ManifestRow]) -> Result<()> {
    let mut csv = String::from(MANIFEST_HEADER);
    csv.push('\n');
    for r in rows {
        let fields = [
            csv_field(&r.table), r.rows_read.to_string(), r.rows_source_stats.to_string(), csv_field(&r.fingerprint_before), csv_field(&r.fingerprint_after),
            csv_field(&r.started), csv_field(&r.finished), format!("{:.3}", r.seconds), csv_field(&r.isolation), r.rereads.to_string(),
        ];
        csv.push_str(&fields.join(","));
        csv.push('\n');
    }
    std::fs::write(path, csv).map_err(|e| Failure::Run(format!("writing manifest {}: {e}", path.display())))?;
    let jp = manifest_json_path(path);
    std::fs::write(&jp, serde_json::to_string_pretty(rows)?).map_err(|e| Failure::Run(format!("writing manifest {}: {e}", jp.display())))?;
    Ok(())
}

// ---------------------------------------------------------------- gate query

const GATE_POLL: Duration = Duration::from_secs(10);

/// True for bit 1, a non-zero number, or the strings 'true' / '1' / 'yes'.
fn truthy(v: &ColumnData<'_>) -> bool {
    match v {
        ColumnData::Bit(Some(b)) => *b,
        ColumnData::U8(Some(x)) => *x != 0,
        ColumnData::I16(Some(x)) => *x != 0,
        ColumnData::I32(Some(x)) => *x != 0,
        ColumnData::I64(Some(x)) => *x != 0,
        ColumnData::F32(Some(x)) => *x != 0.0,
        ColumnData::F64(Some(x)) => *x != 0.0,
        ColumnData::Numeric(Some(n)) => n.value() != 0,
        ColumnData::String(Some(s)) => matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
        _ => false,
    }
}

/// SQL Server error 1222: lock request time out period exceeded.
pub fn is_lock_timeout(e: &anyhow::Error) -> bool {
    e.chain().any(|c| matches!(c.downcast_ref::<tiberius::error::Error>(), Some(tiberius::error::Error::Server(t)) if t.code() == 1222))
}

/// Run the user's gate query every 10 s until its first column is true; `Failure::Check` after `timeout`.
/// A lock timeout (1222) counts as "not yet"; any other error stops the run.
pub async fn wait_for(c: &mut MsClient, sql: &str, timeout: Duration) -> Result<()> {
    assert_read_only(sql)?;
    let deadline = Instant::now() + timeout;
    let mut polls = 0u64;
    loop {
        polls += 1;
        let t0 = Instant::now();
        let res: Result<bool> = async {
            let rows = c.simple_query(sql).await?.into_first_result().await?;
            Ok(rows.into_iter().next().and_then(|r| r.into_iter().next()).map(|v| truthy(&v)).unwrap_or(false))
        }
        .await;
        runlog::stmt("source", "wait-for", sql, 1, t0);
        let ready = match res {
            Ok(r) => r,
            Err(e) if is_lock_timeout(&e) => false,
            Err(e) => return Err(e.context("--wait-for query failed")),
        };
        runlog::note(&format!("wait-for poll {polls}: {}", if ready { "ready" } else { "not ready" }));
        if ready { return Ok(()); }
        let now = Instant::now();
        if now >= deadline {
            return Err(Failure::Check(format!("--wait-for: the gate query did not return 1 within {}s ({polls} polls): {}", timeout.as_secs(), sql.chars().take(200).collect::<String>())).into());
        }
        tokio::time::sleep(GATE_POLL.min(deadline - now)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_parse_isolation_and_on_lock() {
        for s in ["read-committed", "read_committed", "rc", "READ COMMITTED"] { assert_eq!(Isolation::parse(s).unwrap(), Isolation::ReadCommitted); }
        for s in ["read-uncommitted", "nolock", "read_uncommitted"] { assert_eq!(Isolation::parse(s).unwrap(), Isolation::ReadUncommitted); }
        assert_eq!(Isolation::parse("Snapshot").unwrap(), Isolation::Snapshot);
        assert!(Isolation::parse("serializable").is_err());
        assert_eq!(OnLock::parse("fail").unwrap(), OnLock::Fail);
        assert_eq!(OnLock::parse("SKIP").unwrap(), OnLock::Skip);
        assert!(OnLock::parse("wait").is_err());
        assert_eq!(Isolation::Snapshot.sql(), "SNAPSHOT");
        assert_eq!(Isolation::ReadUncommitted.to_string(), "read-uncommitted");
    }

    #[test]
    fn fingerprint_defaults() {
        let d = ReadSafety::default();
        assert_eq!((d.lock_timeout_ms, d.isolation, d.lock_check, d.lock_wait_s, d.on_lock, d.max_rereads, d.readonly_intent, d.wait_timeout_s), (1000, Isolation::ReadCommitted, true, 30, OnLock::Fail, 2, false, 3600));
        assert!(d.manifest.is_none() && d.wait_for.is_none());
        assert_eq!(session_settings_sql(1000, "READ COMMITTED"), "SET LOCK_TIMEOUT 1000; SET DEADLOCK_PRIORITY LOW; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
        assert!(readonly_supported());
    }

    #[test]
    fn fingerprint_eq_display() {
        let a = Fingerprint { rows: 10, last_user_update: None, modify_date: "2026-01-01 10:00:00.000".into() };
        let mut b = a.clone();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), "rows=10 last_update=none modified=2026-01-01 10:00:00.000");
        b.last_user_update = Some("2026-01-02 11:00:00.123".into());
        assert_ne!(a, b);
        assert_eq!(b.to_string(), "rows=10 last_update=2026-01-02 11:00:00.123 modified=2026-01-01 10:00:00.000");
        let mut c = a.clone();
        c.rows = 11;
        assert_ne!(a, c);
    }

    #[test]
    fn fingerprint_lock_sql_text() {
        for needle in ["sys.dm_tran_locks", "sys.dm_exec_sessions", "resource_database_id = DB_ID()", "request_session_id <> @@SPID", "resource_associated_entity_id IN", "sys.partitions", "hobt_id", "OBJECT_ID(@P1)", "'X'", "'IX'", "'Sch-M'", "'GRANT'"] {
            assert!(HELD_LOCKS_SQL.contains(needle), "{needle}");
        }
        for needle in ["sys.dm_db_partition_stats", "index_id IN (0, 1)", "sys.dm_db_index_usage_stats", "last_user_update", "modify_date, 121", "VARCHAR(27)"] {
            assert!(FINGERPRINT_SQL.contains(needle), "{needle}");
        }
        assert!(blocks_reader("X", Isolation::ReadCommitted) && blocks_reader("IX", Isolation::ReadCommitted));
        assert!(!blocks_reader("X", Isolation::Snapshot) && blocks_reader("Sch-M", Isolation::Snapshot));
        assert!(!blocks_reader("U", Isolation::ReadCommitted));
        assert!(assert_read_only(HELD_LOCKS_SQL).is_ok() && assert_read_only(FINGERPRINT_SQL).is_ok() && assert_read_only(FINGERPRINT_FALLBACK_SQL).is_ok());
    }

    #[test]
    fn fingerprint_describe_locks() {
        let l = |s: i32, m: &str, r: &str| LockInfo { session_id: s, request_mode: m.into(), resource_type: r.into(), login_name: "sa".into(), host_name: "h".into(), program_name: "p".into() };
        let d = describe_locks(&[l(60, "Sch-M", "OBJECT"), l(57, "IX", "OBJECT"), l(57, "X", "KEY")]);
        assert_eq!(d, "session 57 (sa@h, p) holds IX on OBJECT, X on KEY; session 60 (sa@h, p) holds Sch-M on OBJECT");
    }

    #[test]
    fn fingerprint_read_only_guard() {
        assert!(assert_read_only("SELECT [a], [Delete], 'insert into x' FROM [dbo].[Update] WITH (NOLOCK) WHERE [id] >= 5").is_ok());
        assert!(assert_read_only("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM ctl.flags WHERE step='inputs' AND done=1) THEN 1 ELSE 0 END AS bit)").is_ok());
        assert!(assert_read_only("SELECT last_user_update -- delete me\n FROM t /* drop */").is_ok());
        for bad in ["DELETE FROM t", "select 1; update t set a = 1", "SELECT * INTO #x FROM t", "exec sp_who", "TRUNCATE TABLE t"] {
            let e = assert_read_only(bad).unwrap_err();
            assert_eq!(runlog::exit_code(&e), 3, "{bad}");
        }
    }

    #[test]
    fn fingerprint_truthy_values() {
        assert!(truthy(&ColumnData::Bit(Some(true))) && !truthy(&ColumnData::Bit(Some(false))));
        assert!(truthy(&ColumnData::I32(Some(1))) && !truthy(&ColumnData::I32(Some(0))) && !truthy(&ColumnData::I32(None)));
        assert!(truthy(&ColumnData::String(Some("TRUE".into()))) && truthy(&ColumnData::String(Some("1".into()))));
        assert!(!truthy(&ColumnData::String(Some("false".into()))) && !truthy(&ColumnData::U8(Some(0))));
    }

    #[test]
    fn fingerprint_manifest_csv_and_json() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            ManifestRow { table: "dbo.t".into(), rows_read: 5, rows_source_stats: 5, fingerprint_before: "rows=5 last_update=none modified=x".into(), fingerprint_after: "rows=5 last_update=none modified=x".into(), started: "2026-01-01T00:00:00.000Z".into(), finished: "2026-01-01T00:00:01.000Z".into(), seconds: 1.0, isolation: "read-committed".into(), rereads: 0 },
            ManifestRow { table: "dbo.a,b".into(), rows_read: -1, rows_source_stats: 3, fingerprint_before: "".into(), fingerprint_after: "".into(), started: "s".into(), finished: "f".into(), seconds: 0.0, isolation: "snapshot".into(), rereads: 1 },
        ];
        let p = dir.path().join("in.manifest.csv");
        write_manifest(&p, &rows).unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], MANIFEST_HEADER);
        assert_eq!(lines[1], "dbo.t,5,5,rows=5 last_update=none modified=x,rows=5 last_update=none modified=x,2026-01-01T00:00:00.000Z,2026-01-01T00:00:01.000Z,1.000,read-committed,0");
        assert!(lines[2].starts_with("\"dbo.a,b\",-1,3,"));
        let jp = dir.path().join("in.manifest.json");
        let j: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&jp).unwrap()).unwrap();
        assert_eq!(j[0]["table"], "dbo.t");
        assert_eq!(j[1]["rows_read"], -1);
        assert_eq!(manifest_json_path(Path::new("out/m")), PathBuf::from("out/m.json"));
        assert_eq!(manifest_json_path(Path::new("out/m.CSV")), PathBuf::from("out/m.json"));
    }
}
