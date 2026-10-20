//! Parquet export: one file per table (plus a schema script), written by DuckDB itself.
//! Remote destinations (s3://, az://, gs://) are written through DuckDB's httpfs/azure extensions
//! when available, otherwise written locally and uploaded with our storage clients.
use crate::core::model::Catalog;
use crate::core::typemap::quote_duck;
use anyhow::Result;
use crate::core::duck::{self, Connection};

pub struct ParquetOptions {
    pub compression: String, // zstd | snappy | gzip | uncompressed
    pub row_group_size: usize,
    pub partition_rows: Option<usize>,
}

/// Returns (table, local file path) pairs.
pub fn export_parquet(con: &Connection, cat: &Catalog, out_dir: &std::path::Path, o: &ParquetOptions,
                      progress: &mut dyn FnMut(&str, i64)) -> Result<Vec<(String, std::path::PathBuf)>> {
    std::fs::create_dir_all(out_dir)?;
    let mut files = vec![];
    for t in &cat.tables {
        let fname = format!("{}.{}.parquet", t.schema, t.name).replace(['/', '\\'], "_");
        let path = out_dir.join(&fname);
        let p = path.to_string_lossy().replace('\'', "''");
        let sql = format!("COPY (SELECT * FROM {}.{}) TO '{}' (FORMAT PARQUET, COMPRESSION '{}', ROW_GROUP_SIZE {})",
            quote_duck(&t.schema), quote_duck(&t.name), p, o.compression, o.row_group_size);
        duck::exec(con, &sql)?;
        progress(&format!("{}.{}", t.schema, t.name), t.row_count);
        files.push((format!("{}.{}", t.schema, t.name), path));
    }
    Ok(files)
}
