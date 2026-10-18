//! SQL Server connectivity (pure Rust TDS via tiberius). No ODBC driver needed.
pub mod backup;
pub mod import;
pub mod load;
pub mod split;

use anyhow::{Context, Result};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub type MsClient = Client<Compat<TcpStream>>;

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
    pub fn config(&self, database: &str) -> Config {
        let mut c = Config::new();
        c.host(&self.host);
        c.port(self.port);
        c.database(database);
        if self.windows_auth {
            #[cfg(windows)]
            { c.authentication(AuthMethod::Integrated); }
            #[cfg(not(windows))]
            { c.authentication(AuthMethod::sql_server(&self.user, &self.password)); }
        } else {
            c.authentication(AuthMethod::sql_server(&self.user, &self.password));
        }
        if self.trust_cert { c.trust_cert(); }
        c.encryption(if self.encrypt { EncryptionLevel::Required } else { EncryptionLevel::NotSupported });
        c.application_name("duckbridge");
        c
    }

    pub async fn connect(&self, database: &str) -> Result<MsClient> {
        let cfg = self.config(database);
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
    for stmt in split_go(sql) {
        if stmt.trim().is_empty() { continue; }
        client.simple_query(&stmt).await.with_context(|| format!("executing: {}", stmt.chars().take(200).collect::<String>()))?.into_results().await?;
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
