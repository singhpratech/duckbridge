//! SQL Server connectivity (pure Rust TDS via tiberius). No ODBC driver needed.
pub mod backup;
pub mod bulk;
pub mod convert;
pub mod fingerprint;
pub mod guard;
pub mod import;
pub mod jobs;
pub mod load;
pub mod loadx;
pub mod split;
pub mod swap;
pub mod switch;
pub mod target;

use anyhow::{Context, Result};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub type MsClient = Client<Compat<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    SqlLogin,
    /// NTLM with explicit DOMAIN\user credentials
    Windows,
    /// The identity of the running process (Windows SSPI)
    Integrated,
}

#[derive(Debug, Clone)]
pub struct MssqlTarget {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub trust_cert: bool,
    pub encrypt: bool,
    pub windows_auth: bool,
}

impl MssqlTarget {
    /// Which login to use. Windows authentication is the default whenever no SQL login is given:
    ///
    /// * no user                      -> integrated (the process identity; Windows only)
    /// * `DOMAIN\\user` / `user@domain` -> Windows credentials (NTLM), works on every platform
    /// * `--windows-auth` with a plain user -> treated as Windows credentials too
    /// * otherwise                    -> SQL Server login
    pub fn auth_kind(&self) -> AuthKind {
        let user = self.user.trim();
        if user.is_empty() { return AuthKind::Integrated; }
        if self.windows_auth || user.contains('\\') || user.contains('@') { return AuthKind::Windows; }
        AuthKind::SqlLogin
    }

    fn auth_method(&self) -> Result<AuthMethod> {
        match self.auth_kind() {
            AuthKind::SqlLogin => Ok(AuthMethod::sql_server(&self.user, &self.password)),
            AuthKind::Windows => {
                #[cfg(any(windows, unix))]
                { Ok(AuthMethod::windows(&self.user, &self.password)) }
                #[cfg(not(any(windows, unix)))]
                { anyhow::bail!("Windows authentication is not available on this platform") }
            }
            AuthKind::Integrated => {
                #[cfg(windows)]
                { Ok(AuthMethod::Integrated) }
                #[cfg(not(windows))]
                { anyhow::bail!("no SQL Server login given. Integrated Windows authentication (the process identity) only works on Windows; on Linux/macOS pass Windows credentials as --user 'DOMAIN\\user' --password ... (or mssql://DOMAIN%5Cuser:pw@host/db), or a SQL login with --user/--password") }
            }
        }
    }

    pub fn config(&self, database: &str) -> Result<Config> {
        let mut c = Config::new();
        c.host(&self.host);
        c.port(self.port);
        c.database(database);
        c.authentication(self.auth_method()?);
        if self.trust_cert { c.trust_cert(); }
        c.encryption(if self.encrypt { EncryptionLevel::Required } else { EncryptionLevel::NotSupported });
        c.application_name("duckbridge");
        // tiberius 0.13 aborts a response that stalls for 30 s by default. Index builds, SWITCH waits,
        // BACKUP/RESTORE and checksum queries on big tables legitimately stay silent far longer; the
        // bounded waits DuckBridge wants are server-side (SET LOCK_TIMEOUT, spec R6), so wait indefinitely.
        c.command_timeout(None);
        Ok(c)
    }

    pub async fn connect(&self, database: &str) -> Result<MsClient> {
        let cfg = self.config(database)?;
        let tcp = TcpStream::connect(cfg.get_addr()).await.with_context(|| format!("connecting to SQL Server at {}:{}", self.host, self.port))?;
        tcp.set_nodelay(true)?;
        let client = Client::connect(cfg, tcp.compat_write()).await.context("SQL Server login failed")?;
        Ok(client)
    }
}

pub fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().enable_all().build()?)
}

/// Run all statements of a batch (semicolon/GO separated) one by one, draining results.
pub async fn exec_batch(client: &mut MsClient, sql: &str) -> Result<()> {
    exec_batch_as(client, "target", "", sql).await
}

/// `exec_batch` with a role/job label for the run log. In `--plan` mode statements are recorded
/// and not sent (metadata reads do not go through here, so they still run).
pub async fn exec_batch_as(client: &mut MsClient, role: &str, job: &str, sql: &str) -> Result<()> {
    for stmt in split_go(sql) {
        if stmt.trim().is_empty() { continue; }
        let t0 = std::time::Instant::now();
        if crate::core::runlog::planning() { crate::core::runlog::stmt(role, job, &stmt, 0, t0); continue; }
        client.simple_query(&stmt).await.with_context(|| format!("executing: {}", stmt.chars().take(200).collect::<String>()))?.into_results().await?;
        crate::core::runlog::stmt(role, job, &stmt, 0, t0);
    }
    Ok(())
}

pub fn split_go(sql: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    for line in sql.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            if !cur.trim().is_empty() { out.push(std::mem::take(&mut cur)); } else { cur.clear(); }
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() { out.push(cur); }
    out
}

pub fn bracket(s: &str) -> String {
    format!("[{}]", s.replace(']', "]]"))
}

pub fn nstr(s: &str) -> String {
    format!("N'{}'", s.replace('\'', "''"))
}
