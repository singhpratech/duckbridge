//! Safety guard for database-level operations (spec R1).
//!
//! Every `CREATE DATABASE`, `DROP DATABASE`, `ALTER DATABASE … SET RECOVERY` and
//! `ALTER DATABASE … SET COMPATIBILITY_LEVEL` in the code base lives here, and each one is allowed only
//! for a database whose name matches the disposable pattern. Nothing here ever disconnects another
//! session: a drop checks `sys.dm_exec_sessions` first and refuses when anyone else is connected.
use super::{bracket, exec_batch, MsClient};
use crate::core::runlog::Failure;
use anyhow::{Context, Result};

pub const ENV_PATTERN: &str = "DUCKBRIDGE_DISPOSABLE_DB_PATTERN";
pub const DEFAULT_PATTERN: &str = "^_";

#[derive(Debug, Clone)]
pub struct Guard {
    pattern: regex::Regex,
    source: String,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DbState {
    pub exists: bool,
    pub recovery_model: String,
    pub compatibility_level: i32,
    pub read_committed_snapshot: bool,
    pub snapshot_isolation: bool,
    pub collation: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionInfo {
    pub session_id: i32,
    pub login_name: String,
    pub host_name: String,
    pub program_name: String,
    pub status: String,
}

fn n(s: &str) -> String { s.replace('\'', "''") }

impl Guard {
    /// `pattern` (the `--disposable-db-pattern` flag) wins over `DUCKBRIDGE_DISPOSABLE_DB_PATTERN`,
    /// which wins over the default `^_`.
    pub fn new(pattern: Option<&str>) -> Result<Guard> {
        let (src, from) = match pattern {
            Some(p) if !p.trim().is_empty() => (p.trim().to_string(), "flag"),
            _ => match std::env::var(ENV_PATTERN) {
                Ok(p) if !p.trim().is_empty() => (p.trim().to_string(), "env"),
                _ => (DEFAULT_PATTERN.to_string(), "default"),
            },
        };
        let pattern = regex::Regex::new(&src).with_context(|| format!("invalid disposable database pattern {src:?} ({from})"))?;
        Ok(Guard { pattern, source: src })
    }

    pub fn pattern(&self) -> &str { &self.source }

    pub fn is_disposable(&self, db: &str) -> bool { self.pattern.is_match(db) }

    /// Error (exit code 3) unless `db` matches the disposable pattern.
    pub fn assert_disposable(&self, db: &str, action: &str) -> Result<()> {
        if self.is_disposable(db) { return Ok(()); }
        Err(Failure::Guard(format!("refusing {action} on '{db}': not a disposable database (pattern {})", self.source)).into())
    }

    pub async fn exists(c: &mut MsClient, db: &str) -> Result<bool> {
        let rows = c.simple_query(&format!("SELECT DB_ID(N'{}')", n(db))).await?.into_first_result().await?;
        Ok(rows.first().and_then(|r| r.get::<i16, _>(0)).is_some() || rows.first().and_then(|r| r.get::<i32, _>(0)).is_some())
    }

    /// Recovery model, compatibility level and snapshot settings of a database (empty state if missing).
    pub async fn database_state(c: &mut MsClient, db: &str) -> Result<DbState> {
        let rows = c.simple_query(&format!(
            "SELECT recovery_model_desc, CAST(compatibility_level AS INT), is_read_committed_snapshot_on, snapshot_isolation_state, ISNULL(collation_name, '') FROM sys.databases WHERE name = N'{}'", n(db))).await?.into_first_result().await?;
        let Some(r) = rows.first() else { return Ok(DbState::default()) };
        Ok(DbState {
            exists: true,
            recovery_model: r.get::<&str, _>(0).unwrap_or("").to_string(),
            compatibility_level: r.get::<i32, _>(1).unwrap_or(0),
            read_committed_snapshot: r.get::<bool, _>(2).unwrap_or(false),
            snapshot_isolation: r.get::<u8, _>(3).map(|v| v == 1).unwrap_or(false),
            collation: r.get::<&str, _>(4).unwrap_or("").to_string(),
        })
    }

    /// Other sessions currently using `db`.
    pub async fn sessions(c: &mut MsClient, db: &str) -> Result<Vec<SessionInfo>> {
        let rows = c.simple_query(&format!(
            "SELECT CAST(session_id AS INT), ISNULL(login_name, ''), ISNULL(host_name, ''), ISNULL(program_name, ''), ISNULL(status, '') \
             FROM sys.dm_exec_sessions WHERE database_id = DB_ID(N'{}') AND session_id <> @@SPID ORDER BY session_id", n(db))).await?.into_first_result().await?;
        Ok(rows.iter().map(|r| SessionInfo {
            session_id: r.get::<i32, _>(0).unwrap_or(0),
            login_name: r.get::<&str, _>(1).unwrap_or("").into(),
            host_name: r.get::<&str, _>(2).unwrap_or("").into(),
            program_name: r.get::<&str, _>(3).unwrap_or("").into(),
            status: r.get::<&str, _>(4).unwrap_or("").into(),
        }).collect())
    }

    /// The client host name of this connection as the server sees it.
    async fn my_host(c: &mut MsClient) -> Result<String> {
        let rows = c.simple_query("SELECT ISNULL(host_name, '') FROM sys.dm_exec_sessions WHERE session_id = @@SPID").await?.into_first_result().await?;
        Ok(rows.first().and_then(|r| r.get::<&str, _>(0)).unwrap_or("").to_string())
    }

    /// R1.5: the target must exist, unless `create` is set *and* the name is disposable.
    pub async fn ensure_database(&self, c: &mut MsClient, db: &str, create: bool) -> Result<()> {
        if Self::exists(c, db).await? { return Ok(()); }
        if !create {
            return Err(Failure::Guard(format!("database '{db}' does not exist; pass --create-database (the name must match the disposable pattern {}) or create it first", self.source)).into());
        }
        self.assert_disposable(db, "CREATE DATABASE")?;
        exec_batch(c, &format!("CREATE DATABASE {}", bracket(db))).await.with_context(|| format!("creating database {db}"))
    }

    /// R1.2: drop only a disposable database that nobody else is using. Never `ROLLBACK IMMEDIATE`.
    pub async fn drop_database(&self, c: &mut MsClient, db: &str) -> Result<()> {
        self.assert_disposable(db, "DROP DATABASE")?;
        if !Self::exists(c, db).await? { return Ok(()); }
        // Connections this process just closed can linger for a moment on the server. Sessions that
        // look like ours (same program name and client host) get a short grace period; anything else
        // is refused immediately.
        let me = Self::my_host(c).await?;
        let mut others = Self::sessions(c, db).await?;
        for _ in 0..20 {
            if others.is_empty() || !others.iter().all(|s| s.program_name == "duckbridge" && s.host_name == me) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            others = Self::sessions(c, db).await?;
        }
        if !others.is_empty() {
            let list: Vec<String> = others.iter().take(20).map(|s| format!("session {} ({} from {} via {}, {})", s.session_id, s.login_name, s.host_name, s.program_name, s.status)).collect();
            return Err(Failure::Guard(format!("refusing DROP DATABASE on '{db}': {} other session(s) connected: {}", others.len(), list.join("; "))).into());
        }
        exec_batch(c, &format!("DROP DATABASE {}", bracket(db))).await.with_context(|| format!("dropping database {db}"))
    }

    /// R1.4: change the recovery model of a disposable database (`SIMPLE`, `FULL`, `BULK_LOGGED`).
    pub async fn set_recovery(&self, c: &mut MsClient, db: &str, model: &str) -> Result<()> {
        self.assert_disposable(db, "ALTER DATABASE SET RECOVERY")?;
        let m = model.trim().to_uppercase();
        if !matches!(m.as_str(), "SIMPLE" | "FULL" | "BULK_LOGGED") { anyhow::bail!("unknown recovery model {model:?}"); }
        exec_batch(c, &format!("ALTER DATABASE {} SET RECOVERY {m}", bracket(db))).await
    }

    /// R1.3: set the compatibility level of a disposable database, only when asked explicitly.
    pub async fn set_compat_level(&self, c: &mut MsClient, db: &str, level: u16) -> Result<()> {
        self.assert_disposable(db, "ALTER DATABASE SET COMPATIBILITY_LEVEL")?;
        if !(100..=200).contains(&level) || level % 10 != 0 { anyhow::bail!("compatibility level must be 100, 110, …, 170; got {level}"); }
        exec_batch(c, &format!("ALTER DATABASE {} SET COMPATIBILITY_LEVEL = {level}", bracket(db))).await
    }

    /// Advice for a non-disposable target: report, never change (R1.4).
    pub fn describe(st: &DbState, db: &str) -> Vec<String> {
        let mut out = vec![];
        if !st.exists { return out; }
        out.push(format!("[{db}] recovery model {}, compatibility level {}, collation {}", st.recovery_model, st.compatibility_level, st.collation));
        if st.recovery_model.eq_ignore_ascii_case("FULL") { out.push(format!("[{db}] recovery model is FULL: bulk loads will be fully logged (SIMPLE or BULK_LOGGED is minimally logged)")); }
        if st.read_committed_snapshot || st.snapshot_isolation { out.push(format!("[{db}] snapshot isolation is available: --isolation snapshot gives consistent reads without locks")); }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pattern_and_flag() {
        let g = Guard::new(Some("^_")).unwrap();
        assert!(g.is_disposable("_scratch") && !g.is_disposable("Sales"));
        let e = g.assert_disposable("Sales", "DROP DATABASE").unwrap_err();
        assert_eq!(e.to_string(), "refusing DROP DATABASE on 'Sales': not a disposable database (pattern ^_)");
        assert_eq!(crate::core::runlog::exit_code(&e), 3);
        let g = Guard::new(Some("^d2m_")).unwrap();
        assert!(g.is_disposable("d2m_bak"));
        assert!(Guard::new(Some("(")).is_err());
    }

    /// R1 acceptance: no database-level statement text outside this module.
    #[test]
    fn no_database_statements_outside_guard() {
        fn walk(dir: &std::path::Path, hits: &mut Vec<String>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() { walk(&p, hits); continue; }
                if p.extension().map(|x| x != "rs").unwrap_or(true) || p.ends_with("guard.rs") { continue; }
                let text = std::fs::read_to_string(&p).unwrap().to_uppercase();
                for needle in ["DROP DATABASE", "ALTER DATABASE", "ROLLBACK IMMEDIATE"] {
                    for (i, line) in text.lines().enumerate() {
                        if line.contains(needle) && !line.trim_start().starts_with("//") {
                            hits.push(format!("{}:{}: {needle}", p.display(), i + 1));
                        }
                    }
                }
            }
        }
        let mut hits = vec![];
        walk(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut hits);
        assert!(hits.is_empty(), "database-level statements outside guard.rs:\n{}", hits.join("\n"));
    }
}
