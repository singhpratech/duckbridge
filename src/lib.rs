//! duckdb2mssql core library: shared by the CLI and (later) the DuckDB extension.
pub mod core;
pub mod transpile;
pub mod storage;
pub mod mssql;
pub mod pg;
pub mod parquet;
