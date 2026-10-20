//! duckbridge core library: shared by the CLI and the DuckDB extension (extension/).
pub mod api;
pub mod core;
pub mod transpile;
pub mod storage;
pub mod mssql;
pub mod pg;
pub mod parquet;
pub mod audit;
pub mod mathcompat;
pub mod verify;
