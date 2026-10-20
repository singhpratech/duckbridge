//! BACKUP DATABASE to a .bak on the server, fetch it, and optionally verify by restoring it.
use super::guard::Guard;
use super::{bracket, exec_batch, MsClient, MssqlTarget};
use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub struct BackupOptions {
    pub server_dir: String,      // directory on the SQL Server host, e.g. /var/opt/mssql/backup or C:\Backups
    pub compression: bool,
    pub stripes: usize,
    pub file_name: String,       // base name, e.g. mydb.bak
    /// SQL Server 2022+: BACKUP TO URL (s3://bucket/key.bak or https://account.blob.core.windows.net/container/key.bak).
    /// Requires a server-side CREDENTIAL for that URL; INIT is not allowed for S3 so FORMAT is used alone.
    pub to_url: Option<String>,
}

pub struct BackupResult {
    pub server_paths: Vec<String>,
    pub bytes: u64,
    pub seconds: f64,
}

fn join_server_path(dir: &str, name: &str) -> String {
    let sep = if dir.contains('\\') && !dir.contains('/') { "\\" } else { "/" };
    format!("{}{sep}{name}", dir.trim_end_matches(['/', '\\']))
}

pub async fn backup(client: &mut MsClient, database: &str, o: &BackupOptions) -> Result<BackupResult> {
    let t0 = std::time::Instant::now();
    if let Some(url) = &o.to_url {
        let opts = format!("FORMAT, CHECKSUM, STATS = 10, MAXTRANSFERSIZE = 20971520{}", if o.compression { ", COMPRESSION" } else { "" });
        let sql = format!("BACKUP DATABASE {} TO URL = N'{}' WITH {opts}, NAME = N'{} full (duckbridge)'", bracket(database), url.replace('\'', "''"), database.replace('\'', "''"));
        exec_batch(client, &sql).await.context("BACKUP DATABASE TO URL failed (SQL Server 2022+ with a matching CREDENTIAL is required)")?;
        return Ok(BackupResult { server_paths: vec![url.clone()], bytes: 0, seconds: t0.elapsed().as_secs_f64() });
    }
    let paths: Vec<String> = if o.stripes <= 1 {
        vec![join_server_path(&o.server_dir, &o.file_name)]
    } else {
        let stem = o.file_name.trim_end_matches(".bak");
        (1..=o.stripes).map(|i| join_server_path(&o.server_dir, &format!("{stem}_{i}of{}.bak", o.stripes))).collect()
    };
    let disks: Vec<String> = paths.iter().map(|p| format!("DISK = N'{}'", p.replace('\'', "''"))).collect();
    let opts = format!("FORMAT, INIT, CHECKSUM, STATS = 10{}", if o.compression { ", COMPRESSION" } else { "" });
    let sql = format!("BACKUP DATABASE {} TO {} WITH {opts}, NAME = N'{} full (duckbridge)'", bracket(database), disks.join(", "), database.replace('\'', "''"));
    exec_batch(client, &sql).await.context("BACKUP DATABASE failed")?;
    let rows = client.simple_query(&format!(
        "SELECT TOP 1 CAST(backup_size AS BIGINT), CAST(ISNULL(compressed_backup_size, backup_size) AS BIGINT) FROM msdb.dbo.backupset WHERE database_name = N'{}' ORDER BY backup_finish_date DESC",
        database.replace('\'', "''"))).await?.into_first_result().await?;
    let bytes: u64 = rows.first().and_then(|r| r.get::<i64, _>(1)).unwrap_or(0) as u64;
    Ok(BackupResult { server_paths: paths, bytes, seconds: t0.elapsed().as_secs_f64() })
}

#[derive(Debug, Clone)]
pub enum Fetch {
    /// Server runs on this machine: the server directory is readable locally.
    Local,
    /// Server runs in a Docker container with this name; use `docker cp`.
    Docker(String),
    /// Leave the file on the server (print its path).
    None,
}

/// Bring the backup file(s) to a local directory. Returns local paths.
pub fn fetch(server_paths: &[String], how: &Fetch, local_dir: &Path) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(local_dir)?;
    let mut out = vec![];
    for sp in server_paths {
        let name = sp.rsplit(['/', '\\']).next().unwrap_or(sp).to_string();
        let local = local_dir.join(&name);
        match how {
            Fetch::None => out.push(PathBuf::from(sp)),
            Fetch::Local => {
                if Path::new(sp) != local { std::fs::copy(sp, &local).with_context(|| format!("copying {sp} (is the server really local? use --fetch docker:<name> otherwise)"))?; }
                out.push(local);
            }
            Fetch::Docker(name) => {
                let st = std::process::Command::new("docker").args(["cp", &format!("{name}:{sp}"), local.to_string_lossy().as_ref()]).status().context("running docker cp")?;
                if !st.success() { bail!("docker cp failed for {sp}"); }
                out.push(local);
            }
        }
    }
    Ok(out)
}

/// Restore the backup as `new_db` and return per-table row counts. Proves the .bak is a pure, restorable database.
pub async fn verify_restore(target: &MssqlTarget, server_paths: &[String], new_db: &str, keep: bool, guard: &Guard) -> Result<BTreeMap<String, i64>> {
    guard.assert_disposable(new_db, "RESTORE DATABASE (verification copy)")?;
    let mut c = target.connect("master").await?;
    let disks: Vec<String> = server_paths.iter().map(|p| format!("DISK = N'{}'", p.replace('\'', "''"))).collect();
    let files = c.simple_query(&format!("RESTORE FILELISTONLY FROM {}", disks.join(", "))).await?.into_first_result().await?;
    let data_dir: String = c.simple_query("SELECT CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(4000))").await?.into_first_result().await?
        .first().and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string())).unwrap_or_else(|| "/var/opt/mssql/data/".into());
    let sep = if data_dir.contains('\\') { "\\" } else { "/" };
    let data_dir = data_dir.trim_end_matches(['/', '\\']).to_string();
    let mut moves = vec![];
    for (i, f) in files.iter().enumerate() {
        let logical: &str = f.get(0).unwrap_or("");
        let ftype: &str = f.get(2).unwrap_or("D");
        let ext = if ftype == "L" { "ldf" } else { "mdf" };
        moves.push(format!("MOVE N'{}' TO N'{data_dir}{sep}{new_db}_{i}.{ext}'", logical.replace('\'', "''")));
    }
    guard.drop_database(&mut c, new_db).await?;
    exec_batch(&mut c, &format!("RESTORE DATABASE {} FROM {} WITH {}, REPLACE, CHECKSUM", bracket(new_db), disks.join(", "), moves.join(", "))).await.context("RESTORE DATABASE failed")?;
    let mut d = target.connect(new_db).await?;
    let rows = d.simple_query(
        "SELECT s.name, t.name, SUM(p.rows) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1) GROUP BY s.name, t.name"
    ).await?.into_first_result().await?;
    let mut counts = BTreeMap::new();
    for r in rows {
        let s: &str = r.get(0).unwrap_or("");
        let t: &str = r.get(1).unwrap_or("");
        let n: i64 = r.get::<i64, _>(2).unwrap_or(0);
        counts.insert(format!("{s}.{t}"), n);
    }
    drop(d);
    if !keep { guard.drop_database(&mut c, new_db).await?; }
    Ok(counts)
}
