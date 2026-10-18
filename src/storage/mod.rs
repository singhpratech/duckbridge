//! Storage abstraction: local filesystem (default), S3-compatible (`s3://`), Azure Blob (`az://`),
//! Google Cloud Storage via its S3-interoperable XML API (`gs://`). All clients are our own,
//! built on `ureq` and the standard library; no cloud SDKs.
pub mod azure;
pub mod s3;

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    Local(PathBuf),
    S3 { bucket: String, key: String },
    Azure { container: String, blob: String },
    Gcs { bucket: String, key: String },
}

impl Location {
    pub fn parse(s: &str) -> Result<Location> {
        if let Some(rest) = s.strip_prefix("s3://") {
            let (b, k) = rest.split_once('/').context("s3 url needs bucket/key")?;
            return Ok(Location::S3 { bucket: b.into(), key: k.into() });
        }
        if let Some(rest) = s.strip_prefix("gs://").or_else(|| s.strip_prefix("gcs://")) {
            let (b, k) = rest.split_once('/').context("gs url needs bucket/key")?;
            return Ok(Location::Gcs { bucket: b.into(), key: k.into() });
        }
        if let Some(rest) = s.strip_prefix("az://").or_else(|| s.strip_prefix("azure://")).or_else(|| s.strip_prefix("abfss://")) {
            let (c, b) = rest.split_once('/').context("az url needs container/blob")?;
            return Ok(Location::Azure { container: c.into(), blob: b.into() });
        }
        if let Some(rest) = s.strip_prefix("https://") {
            // https://account.blob.core.windows.net/container/blob
            if let Some((host, path)) = rest.split_once('/') {
                if host.contains(".blob.core.windows.net") {
                    let (c, b) = path.split_once('/').context("azure https url needs container/blob")?;
                    return Ok(Location::Azure { container: c.into(), blob: b.into() });
                }
            }
            bail!("unsupported https location {s}; use s3://, az:// or gs://");
        }
        Ok(Location::Local(PathBuf::from(s)))
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Location::Local(_))
    }

    pub fn file_name(&self) -> String {
        match self {
            Location::Local(p) => p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
            Location::S3 { key, .. } | Location::Gcs { key, .. } => key.rsplit('/').next().unwrap_or(key).to_string(),
            Location::Azure { blob, .. } => blob.rsplit('/').next().unwrap_or(blob).to_string(),
        }
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::Local(p) => write!(f, "{}", p.display()),
            Location::S3 { bucket, key } => write!(f, "s3://{bucket}/{key}"),
            Location::Gcs { bucket, key } => write!(f, "gs://{bucket}/{key}"),
            Location::Azure { container, blob } => write!(f, "az://{container}/{blob}"),
        }
    }
}

/// Upload a local file to `dest`. Local destinations are a plain copy/rename.
pub fn upload(local: &Path, dest: &Location, progress: &mut dyn FnMut(u64, u64)) -> Result<()> {
    match dest {
        Location::Local(p) => {
            if let Some(parent) = p.parent() { if !parent.as_os_str().is_empty() { std::fs::create_dir_all(parent)?; } }
            if std::fs::rename(local, p).is_err() { std::fs::copy(local, p)?; }
            Ok(())
        }
        Location::S3 { bucket, key } => s3::Client::from_env(false)?.upload_file(bucket, key, local, progress),
        Location::Gcs { bucket, key } => s3::Client::from_env(true)?.upload_file(bucket, key, local, progress),
        Location::Azure { container, blob } => azure::Client::from_env()?.upload_file(container, blob, local, progress),
    }
}

/// Make `src` available as a local file. Remote objects are downloaded into `scratch_dir`.
pub fn localize(src: &Location, scratch_dir: &Path, progress: &mut dyn FnMut(u64, u64)) -> Result<PathBuf> {
    match src {
        Location::Local(p) => Ok(p.clone()),
        _ => {
            std::fs::create_dir_all(scratch_dir)?;
            let target = scratch_dir.join(src.file_name());
            match src {
                Location::S3 { bucket, key } => s3::Client::from_env(false)?.download_file(bucket, key, &target, progress)?,
                Location::Gcs { bucket, key } => s3::Client::from_env(true)?.download_file(bucket, key, &target, progress)?,
                Location::Azure { container, blob } => azure::Client::from_env()?.download_file(container, blob, &target, progress)?,
                Location::Local(_) => unreachable!(),
            }
            Ok(target)
        }
    }
}

pub(crate) fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

pub(crate) fn http_agent() -> ureq::Agent {
    ureq::Agent::config_builder().timeout_global(Some(std::time::Duration::from_secs(600))).build().into()
}
