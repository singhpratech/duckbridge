//! Backend `duckdb-v1`: duckdb-rs (`duckdb` crate, DuckDB 1.x C API). See [`super`] for the surface.
//!
//! Besides the helpers, the code still calls a small set of methods directly on the re-exported
//! types (`Connection::prepare`, `Statement::query`, `Rows::next`, `Row::get`/`get_ref`, ...);
//! docs/handoff-duck-layer.md lists them, the `duckdb-neo` backend (`neo.rs`) provides them too.

use anyhow::{Context, Result};

// ------------------------------------------------------------------ types

/// A DuckDB connection.
pub type Connection = duckdb::Connection;
/// A prepared statement on a [`Connection`].
pub type Statement<'c> = duckdb::Statement<'c>;
/// A row cursor of an executed [`Statement`].
pub type Rows<'s> = duckdb::Rows<'s>;
/// One result row (`get`, `get_ref`, `get_ref_unwrap`).
pub type Row<'s> = duckdb::Row<'s>;

pub use duckdb::types::{Decimal, FromSql, ListType, TimeUnit, ToSql, Value, ValueRef};
/// The Arrow crate the backend speaks (same version as the backend's own arrow dependency).
pub use duckdb::arrow;

use arrow::array::StructArray;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

// ------------------------------------------------------------------ opening

/// Open (or create) the database file at `path`.
pub fn open(path: impl AsRef<std::path::Path>) -> Result<Connection> {
    Ok(Connection::open(path)?)
}

/// Open the database file at `path` read-only.
pub fn open_readonly(path: impl AsRef<std::path::Path>) -> Result<Connection> {
    let cfg = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
    Ok(Connection::open_with_flags(path, cfg)?)
}

/// A fresh in-memory database.
pub fn open_in_memory() -> Result<Connection> {
    Ok(Connection::open_in_memory()?)
}

/// Another connection to the same database as `con` (not available inside the extension, see
/// [`super::conn`]).
pub fn try_clone(con: &Connection) -> Result<Connection> {
    Ok(con.try_clone()?)
}

// ------------------------------------------------------------------ statements

/// Run one or more `;`-separated statements, discarding any result.
pub fn exec(con: &Connection, sql: &str) -> Result<()> {
    con.execute_batch(sql)?;
    Ok(())
}

/// Run a single statement and return the number of rows it changed.
pub fn execute(con: &Connection, sql: &str) -> Result<usize> {
    Ok(con.execute(sql, [])?)
}

/// `SET <name>=<value>`. `value` is SQL text and goes in verbatim (quote string settings yourself).
pub fn set_setting(con: &Connection, name: &str, value: &str) -> Result<()> {
    exec(con, &format!("SET {name}={value}"))
}

/// First column of the first row of `sql` (an error when there is no row).
pub fn query_scalar<T: FromSql>(con: &Connection, sql: &str) -> Result<T> {
    Ok(con.query_row(sql, [], |r| r.get::<_, T>(0))?)
}

/// `f` applied to the first row of `sql` (an error when there is no row).
pub fn query_one<T>(con: &Connection, sql: &str, f: impl FnOnce(&Row<'_>) -> Result<T>) -> Result<T> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    match rows.next()? {
        Some(r) => f(r),
        None => Err(duckdb::Error::QueryReturnedNoRows.into()),
    }
}

/// `f` applied to every row of `sql`, collected.
pub fn query_rows<T>(con: &Connection, sql: &str, mut f: impl FnMut(&Row<'_>) -> Result<T>) -> Result<Vec<T>> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? { out.push(f(r)?); }
    Ok(out)
}

/// Like [`query_rows`] with positional `?` parameters bound to `params`.
pub fn query_rows_with<T>(con: &Connection, sql: &str, params: &[&dyn ToSql], mut f: impl FnMut(&Row<'_>) -> Result<T>) -> Result<Vec<T>> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query(params)?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? { out.push(f(r)?); }
    Ok(out)
}

/// Visit every row of `sql` without collecting (for large results).
pub fn for_each_row(con: &Connection, sql: &str, mut f: impl FnMut(&Row<'_>) -> Result<()>) -> Result<()> {
    let mut st = con.prepare(sql)?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? { f(r)?; }
    Ok(())
}

/// The strings of a `VARCHAR[]` column of `r` (empty for NULL, non-list or non-string values).
pub fn list_of_strings(r: &Row<'_>, i: usize) -> Vec<String> {
    match r.get_ref(i) {
        Ok(ValueRef::List(list, idx)) => {
            use arrow::array::Array;
            let arr: std::sync::Arc<dyn Array> = match list { ListType::Regular(a) => a.value(idx), ListType::Large(a) => a.value(idx) };
            let sa = arr.as_any().downcast_ref::<arrow::array::StringArray>();
            match sa { Some(sa) => (0..sa.len()).filter(|&j| !sa.is_null(j)).map(|j| sa.value(j).to_string()).collect(), None => vec![] }
        }
        _ => vec![],
    }
}

// ------------------------------------------------------------------ appender

/// Row-wise bulk insert into one table (DuckDB's appender). Pending rows are flushed on
/// [`Appender::flush`] and on drop.
pub struct Appender<'c>(duckdb::Appender<'c>);

/// An appender on `schema.table`.
pub fn appender<'c>(con: &'c Connection, schema: &str, table: &str) -> Result<Appender<'c>> {
    Ok(Appender(con.appender_to_db(table, schema)?))
}

impl Appender<'_> {
    /// Append one row (one value per column, in table order).
    pub fn append_row(&mut self, row: Vec<Value>) -> Result<()> {
        let outs: Vec<duckdb::types::ToSqlOutput<'static>> = row.into_iter().map(duckdb::types::ToSqlOutput::Owned).collect();
        let refs: Vec<&dyn ToSql> = outs.iter().map(|o| o as &dyn ToSql).collect();
        self.0.append_row(refs.as_slice())?;
        Ok(())
    }

    /// Write the pending rows to the table.
    pub fn flush(&mut self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }
}

// ------------------------------------------------------------------ Arrow streaming

/// A streaming query: its result schema is known up front, then the result is read either as
/// Arrow chunks ([`ArrowStream::next_chunk`]) or row by row ([`ArrowStream::rows`]) - one or the
/// other, not both.
pub struct ArrowStream<'c> {
    stmt: Statement<'c>,
    schema: SchemaRef,
}

/// Prepare and start `sql` as a streaming query (DuckDB produces chunks as they are read).
/// Errors carry `preparing <sql>` / `running <sql>` contexts (first 300 characters of the SQL).
pub fn stream<'c>(con: &'c Connection, sql: &str) -> Result<ArrowStream<'c>> {
    let mut stmt = con.prepare(sql).with_context(|| format!("preparing {}", sql.chars().take(300).collect::<String>()))?;
    let schema = stmt.stream_arrow([]).with_context(|| format!("running {}", sql.chars().take(300).collect::<String>()))?.get_schema();
    Ok(ArrowStream { stmt, schema })
}

impl<'c> ArrowStream<'c> {
    /// The result schema.
    pub fn schema(&self) -> &SchemaRef { &self.schema }

    /// The next result chunk (columns in result order), `None` at the end.
    pub fn next_chunk(&mut self) -> Result<Option<StructArray>> {
        Ok(self.stmt.step()?)
    }

    /// Read the (already running) result row by row instead.
    pub fn rows(&mut self) -> Rows<'_> {
        self.stmt.raw_query()
    }
}

/// `sql` as a stream of Arrow record batches (see [`stream`]).
pub fn arrow_batches<'c>(con: &'c Connection, sql: &str) -> Result<impl Iterator<Item = Result<RecordBatch>> + 'c> {
    let mut s = stream(con, sql)?;
    Ok(std::iter::from_fn(move || match s.next_chunk() {
        Ok(Some(chunk)) => {
            let (fields, columns, _) = chunk.into_parts();
            Some(RecordBatch::try_new(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)), columns).map_err(anyhow::Error::from))
        }
        Ok(None) => None,
        Err(e) => Some(Err(e)),
    }))
}
