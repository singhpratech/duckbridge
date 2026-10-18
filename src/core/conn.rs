//! Where worker threads get their DuckDB connections from.
//!
//! The CLI owns its database, so a worker simply clones the main connection. Inside the DuckDB
//! extension the database belongs to the host process and `try_clone` is not available
//! (the raw database handle DuckDB lends to an extension dies right after loading), so the
//! extension installs a provider that hands out pre-opened connections from a pool instead.

use std::sync::RwLock;

use anyhow::Result;
use duckdb::Connection;

pub trait ConnProvider: Send + Sync {
    fn acquire(&self) -> Result<Connection>;
    fn release(&self, con: Connection);
}

static PROVIDER: RwLock<Option<Box<dyn ConnProvider>>> = RwLock::new(None);

/// Install a global provider (used by the DuckDB extension at load time).
pub fn set_provider(p: Box<dyn ConnProvider>) {
    *PROVIDER.write().unwrap_or_else(|e| e.into_inner()) = Some(p);
}

/// A connection for a worker thread: from the provider if one is installed, else a clone of `base`.
pub fn acquire(base: &Connection) -> Result<Connection> {
    if let Some(p) = PROVIDER.read().unwrap_or_else(|e| e.into_inner()).as_ref() { return p.acquire(); }
    Ok(base.try_clone()?)
}

/// Give a worker connection back (returned to the pool, or simply closed).
pub fn release(con: Connection) {
    if let Some(p) = PROVIDER.read().unwrap_or_else(|e| e.into_inner()).as_ref() { p.release(con); }
}
