//! Job sources, per-table load strategies, the shared job queue and the restart state directory
//! (spec R4, work stream D; see docs/plan-0.1.1.md).
//!
//! A table is cut into jobs by a [`SplitSpec`]:
//!
//! * `column:<col>`: one job per distinct value (largest first), e.g. the market column;
//! * `files:<glob>`: one job per Parquet file, or per Hive directory when the glob ends with `/`;
//! * `hash:<cols>:<n>`: `hash(cols) % n = i`, for tables without a natural split column;
//! * `range:<int col>[:<n>]`: integer key ranges (the 0.1.0 behaviour);
//! * `auto`: the 0.1.0 rule (single integer PK / identity column, row count >= threshold, `-j` > 1).
//!
//! Every job carries the DuckDB SQL that produces its rows, an estimated row count and, for
//! `column:` and Hive `files:` jobs, the T-SQL literal of its partition value (used by the
//! `switch` strategy in [`super::switch`]). Job ids are deterministic so a rerun with the same
//! `--state-dir` skips finished jobs.
use crate::core::model::{Catalog, Column, Table};
use crate::core::script::select_sql;
use crate::core::typemap::quote_duck;
use super::split::ranges;
use anyhow::{bail, Context, Result};
use crate::core::duck::{self, Connection};
use sha2::Digest;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// SQL Server allows at most this many partitions per table; more distinct values than this is a mistake.
const MAX_COLUMN_VALUES: usize = 15_000;

// ---------------------------------------------------------------------------------------------
// specs

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitSpec {
    /// One job per distinct value of the column.
    Column(String),
    /// One job per file matched by the glob, or per directory when the glob ends with `/`.
    Files(String),
    /// `n` jobs `hash(cols) % n = i`.
    Hash { cols: Vec<String>, n: usize },
    /// Integer key ranges; `n == 0` means "derive from the row count like `auto`".
    Range { col: String, n: usize },
    /// The 0.1.0 rule.
    Auto,
}

/// `column:Mkt` | `files:out/big/Mkt=*/` | `hash:a,b:16` | `range:id:8` | `range:id` | `auto`.
pub fn parse_split(spec: &str) -> Result<SplitSpec> {
    let s = spec.trim();
    let (kind, rest) = match s.split_once(':') { Some((k, r)) => (k.trim().to_ascii_lowercase(), r), None => (s.to_ascii_lowercase(), "") };
    let need = |what: &str| -> Result<()> { if rest.trim().is_empty() { bail!("--split {spec:?}: {kind}: needs {what}") } Ok(()) };
    let count = |n: &str| -> Result<usize> {
        let v: usize = n.trim().parse().with_context(|| format!("--split {spec:?}: {n:?} is not a job count"))?;
        if v == 0 { bail!("--split {spec:?}: the job count must be at least 1"); }
        Ok(v)
    };
    Ok(match kind.as_str() {
        "auto" if rest.trim().is_empty() => SplitSpec::Auto,
        "column" | "col" => { need("a column name")?; if rest.contains(',') { bail!("--split {spec:?}: column: takes one column; use hash:a,b:n for several") } SplitSpec::Column(rest.trim().to_string()) }
        // everything after the first ':' is the glob (so `files:C:/data/*.parquet` works)
        "files" | "file" => { need("a glob")?; SplitSpec::Files(rest.trim().to_string()) }
        "hash" => {
            need("columns and a job count")?;
            let (cols, n) = rest.rsplit_once(':').with_context(|| format!("--split {spec:?}: expected hash:<col,col,...>:<n>"))?;
            let cols: Vec<String> = cols.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
            if cols.is_empty() { bail!("--split {spec:?}: hash: needs at least one column"); }
            SplitSpec::Hash { cols, n: count(n)? }
        }
        "range" => {
            need("an integer column")?;
            match rest.rsplit_once(':') {
                Some((col, n)) => SplitSpec::Range { col: col.trim().to_string(), n: count(n)? },
                None => SplitSpec::Range { col: rest.trim().to_string(), n: 0 },
            }
        }
        _ => bail!("--split {spec:?}: unknown split; expected column:<col>, files:<glob>, hash:<cols>:<n>, range:<col>[:<n>] or auto"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum Strategy {
    /// Bulk load each job into its own staging table and `ALTER TABLE .. SWITCH` it into the partition.
    Switch,
    /// Drop the indexes, bulk load the heap from several sessions, recreate the indexes.
    HeapThenIndex,
    /// Plain bulk load into the table (0.1.0 behaviour).
    Serial,
}

impl Strategy {
    pub fn as_str(&self) -> &'static str {
        match self { Strategy::Switch => "switch", Strategy::HeapThenIndex => "heap-then-index", Strategy::Serial => "serial" }
    }
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.as_str()) }
}

pub fn parse_strategy(s: &str) -> Result<Strategy> {
    Ok(match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "switch" => Strategy::Switch,
        "heap-then-index" | "heap" => Strategy::HeapThenIndex,
        "serial" => Strategy::Serial,
        other => bail!("unknown strategy {other:?}; expected switch, heap-then-index or serial"),
    })
}

/// Parse repeated `table=spec` flags (`--split kpi=column:Mkt`, `--strategy *=serial`). Keys are
/// table names (`kpi`, `dbo.kpi`) or `*`, stored lowercase; the value is everything after the first `=`.
pub fn parse_table_spec_map(specs: &[String]) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for s in specs {
        let (k, v) = s.split_once('=').with_context(|| format!("{s:?}: expected <table>=<spec> (table may be `*`)"))?;
        let k = k.trim().to_lowercase();
        let v = v.trim();
        if k.is_empty() || v.is_empty() { bail!("{s:?}: expected <table>=<spec> (table may be `*`)"); }
        if out.insert(k.clone(), v.to_string()).is_some() { bail!("{s:?}: table {k:?} is given more than once"); }
    }
    Ok(out)
}

/// `parse_table_spec_map` + `parse_split` for every value.
pub fn parse_splits(specs: &[String]) -> Result<HashMap<String, SplitSpec>> {
    parse_table_spec_map(specs)?.into_iter().map(|(k, v)| Ok((k, parse_split(&v)?))).collect()
}

/// `parse_table_spec_map` + `parse_strategy` for every value.
pub fn parse_strategies(specs: &[String]) -> Result<HashMap<String, Strategy>> {
    parse_table_spec_map(specs)?.into_iter().map(|(k, v)| Ok((k, parse_strategy(&v)?))).collect()
}

/// Look a table up in a `table=spec` map: `schema.name`, then `name`, then `*` (case-insensitive).
pub fn lookup<'a, V>(map: &'a HashMap<String, V>, schema: &str, name: &str) -> Option<&'a V> {
    let full = format!("{schema}.{name}").to_lowercase();
    let bare = name.to_lowercase();
    let find = |key: &str| map.iter().find(|(k, _)| k.to_lowercase() == key).map(|(_, v)| v);
    find(&full).or_else(|| find(&bare)).or_else(|| find("*"))
}

/// Keys of a `table=spec` map that match no table of the catalog (typos), for a warning or an error.
pub fn unmatched_keys<V>(map: &HashMap<String, V>, cat: &Catalog) -> Vec<String> {
    let mut out: Vec<String> = map.keys().filter(|k| k.as_str() != "*" && !cat.tables.iter().any(|t| {
        let k = k.to_lowercase();
        k == t.name.to_lowercase() || k == format!("{}.{}", t.schema, t.name).to_lowercase()
    })).cloned().collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------------------------
// jobs

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Job {
    /// `<schema>.<table>#<part>`, only `[A-Za-z0-9_.-]` plus the `#`; stable across runs.
    pub id: String,
    /// Index into `Catalog::tables`.
    pub table: usize,
    /// Human-readable `<table>:<value>`.
    pub label: String,
    /// DuckDB query producing exactly this job's rows, columns in catalog order.
    pub source_sql: String,
    /// T-SQL literal of the partition value (`N'BBB'`, `42`, `'2024-01-31'`) for `column:` jobs and Hive directories.
    pub partition_value: Option<String>,
    /// Column the partition value belongs to (catalog spelling for `column:`, the Hive key for `files:`).
    pub partition_column: Option<String>,
    pub est_rows: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobPlan {
    /// Largest first.
    pub jobs: Vec<Job>,
    /// Per table index of the catalog.
    pub strategy: Vec<Strategy>,
}

impl JobPlan {
    pub fn jobs_of(&self, table: usize) -> impl Iterator<Item = &Job> { self.jobs.iter().filter(move |j| j.table == table) }
    /// The jobs without a done marker in `state`.
    pub fn pending(&self, state: &StateDir) -> Vec<Job> { self.jobs.iter().filter(|j| !state.is_done(j)).cloned().collect() }
}

/// Plan the jobs of every table that has rows (or a `files:` split).
pub fn plan_jobs(con: &Connection, cat: &Catalog, splits: &HashMap<String, SplitSpec>, strategies: &HashMap<String, Strategy>,
                 default_strategy: Strategy, split_threshold: i64, parallel: usize) -> Result<JobPlan> {
    let mut jobs = vec![];
    let mut strategy = vec![];
    for (i, t) in cat.tables.iter().enumerate() {
        let st = lookup(strategies, &t.schema, &t.name).copied().unwrap_or(default_strategy);
        strategy.push(st);
        let spec = lookup(splits, &t.schema, &t.name).cloned().unwrap_or(SplitSpec::Auto);
        if t.row_count == 0 && !matches!(spec, SplitSpec::Files(_)) { continue; }
        let mut tj = table_jobs(con, t, i, &spec, split_threshold, parallel)
            .with_context(|| format!("planning jobs for {}.{}", t.schema, t.name))?;
        if st == Strategy::Switch { check_switch_values(t, &tj)?; }
        assign_ids(t, &mut tj);
        jobs.extend(tj.into_iter().map(|(_, j)| j));
    }
    jobs.sort_by(|a, b| b.est_rows.cmp(&a.est_rows).then_with(|| a.id.cmp(&b.id)));
    Ok(JobPlan { jobs, strategy })
}

/// Jobs of one table, each with the raw text its id is made from.
fn table_jobs(con: &Connection, t: &Table, ti: usize, spec: &SplitSpec, split_threshold: i64, parallel: usize) -> Result<Vec<(String, Job)>> {
    let fq = format!("{}.{}", quote_duck(&t.schema), quote_duck(&t.name));
    let base = select_sql(t);
    let job = |part: &str, label: String, source_sql: String, pv: Option<String>, pc: Option<String>, est_rows: i64| -> (String, Job) {
        (part.to_string(), Job { id: String::new(), table: ti, label, source_sql, partition_value: pv, partition_column: pc, est_rows })
    };
    let auto_n = || ((t.row_count / split_threshold.max(1)) as usize + 1).clamp(2, (parallel * 2).max(2));
    let range_jobs = |col: &Column, n: usize| -> Result<Vec<(String, Job)>> {
        let (mn, mx): (Option<i64>, Option<i64>) = duck::query_one(con, &format!("SELECT CAST(min({0}) AS BIGINT), CAST(max({0}) AS BIGINT) FROM {fq}", quote_duck(&col.name)), |r| Ok((r.get(0)?, r.get(1)?)))?;
        let rs = ranges(mn.unwrap_or(0) as i128, mx.unwrap_or(0) as i128, n);
        let k = rs.len() as i64;
        Ok(rs.iter().enumerate().map(|(i, r)| {
            let part = format!("r{i:03}of{k:03}");
            job(&part, format!("{}:{part}", t.name), format!("{base} WHERE {}", r.predicate(&quote_duck(&col.name))), None, None, t.row_count / k.max(1))
        }).collect())
    };
    let whole = || vec![job("all", t.name.clone(), base.clone(), None, None, t.row_count)];
    match spec {
        SplitSpec::Auto => {
            match auto_split_key(t) {
                Some(k) if t.row_count >= split_threshold && parallel > 1 => range_jobs(k, auto_n()),
                _ => Ok(whole()),
            }
        }
        SplitSpec::Range { col, n } => {
            let c = find_col(t, col)?;
            if !is_int_type(&c.duck_type) { bail!("range:{col}: column {} is {}, not an integer type; use hash:{col}:<n> or column:{col}", c.name, c.duck_type); }
            range_jobs(c, if *n == 0 { auto_n() } else { *n })
        }
        SplitSpec::Hash { cols, n } => {
            let qcols = cols.iter().map(|c| find_col(t, c).map(|c| quote_duck(&c.name))).collect::<Result<Vec<_>>>()?.join(", ");
            if *n <= 1 { return Ok(whole()); }
            let n = *n as i64;
            Ok((0..n).map(|i| {
                let part = format!("h{i:03}of{n:03}");
                job(&part, format!("{}:{part}", t.name), format!("{base} WHERE hash({qcols}) % {n} = {i}"), None, None, t.row_count / n)
            }).collect())
        }
        SplitSpec::Column(col) => {
            let c = find_col(t, col)?;
            let q = quote_duck(&c.name);
            let sql = format!("SELECT CAST({q} AS VARCHAR) AS v, count(*) AS n FROM {fq} GROUP BY {q} ORDER BY n DESC, v NULLS LAST");
            let values: Vec<(Option<String>, i64)> = duck::query_rows(con, &sql, |r| Ok((r.get(0)?, r.get(1)?)))?;
            if values.len() > MAX_COLUMN_VALUES {
                bail!("column:{col}: {} distinct values (more than the {MAX_COLUMN_VALUES} partitions SQL Server allows); use hash:{col}:<n> instead", values.len());
            }
            if values.len() > 1000 { crate::core::runlog::note(&format!("{}.{}: column:{col} gives {} jobs", t.schema, t.name, values.len())); }
            Ok(values.into_iter().map(|(v, n)| match v {
                None => job("NULL", format!("{}:NULL", t.name), format!("{base} WHERE {q} IS NULL"), None, Some(c.name.clone()), n),
                Some(v) => job(&v, format!("{}:{v}", t.name), format!("{base} WHERE {q} = {}", duck_literal(&v, &c.duck_type)),
                               tsql_literal(&v, &c.duck_type), Some(c.name.clone()), n),
            }).collect())
        }
        SplitSpec::Files(glob) => files_jobs(con, t, ti, glob),
    }
}

fn files_jobs(con: &Connection, t: &Table, ti: usize, glob: &str) -> Result<Vec<(String, Job)>> {
    let norm = glob.replace('\\', "/");
    let by_dir = norm.ends_with('/');
    let pattern = if by_dir { format!("{norm}**/*.parquet") } else { norm.clone() };
    let files: Vec<String> = duck::query_rows(con, &format!("SELECT file FROM glob({}) ORDER BY file", sql_str(&pattern)), |r| Ok(r.get::<_, String>(0)?))?
        .into_iter().map(|f| f.replace('\\', "/")).collect();
    if files.is_empty() { bail!("files:{glob}: no Parquet files match {pattern:?}"); }
    // static directory prefix of the glob (up to the last '/' before the first wildcard)
    let first_wild = norm.find(['*', '?', '[', '{']).unwrap_or(norm.len());
    let prefix = match norm[..first_wild].rfind('/') { Some(p) => norm[..=p].to_string(), None => String::new() };
    let depth = norm.trim_end_matches('/').split('/').count();
    let mut groups: Vec<(String, Vec<String>)> = vec![];
    for f in files {
        let key = if by_dir { f.split('/').take(depth).collect::<Vec<_>>().join("/") } else { f.clone() };
        match groups.iter_mut().find(|(k, _)| *k == key) { Some((_, v)) => v.push(f), None => groups.push((key, vec![f])) }
    }
    let inner_cols: Vec<String> = t.columns.iter().map(|c| format!("CAST({0} AS {1}) AS {0}", quote_duck(&c.name), c.duck_type)).collect();
    let outer_cols: Vec<&str> = t.columns.iter().map(|c| c.select_expr.as_str()).collect();
    let mut out = vec![];
    for (key, files) in groups {
        let list = format!("[{}]", files.iter().map(|f| sql_str(f)).collect::<Vec<_>>().join(", "));
        let src = format!("read_parquet({list}, hive_partitioning = true)");
        let est: i64 = duck::query_scalar(con, &format!("SELECT count(*) FROM {src}")).with_context(|| format!("counting rows of {key}"))?;
        let source_sql = format!("SELECT {} FROM (SELECT {} FROM {src}) AS src", outer_cols.join(", "), inner_cols.join(", "));
        // Hive value: last `k=v` directory component of the group (directory mode) or of the file's parent
        let dir = if by_dir { key.as_str() } else { key.rsplit_once('/').map(|(d, _)| d).unwrap_or("") };
        let hive = dir.split('/').rev().find_map(|c| c.split_once('=')).map(|(k, v)| (k.to_string(), percent_encoding::percent_decode_str(v).decode_utf8_lossy().to_string()));
        let (pv, pc) = match &hive {
            Some((k, v)) if v != "__HIVE_DEFAULT_PARTITION__" => {
                let col = t.columns.iter().find(|c| c.name.eq_ignore_ascii_case(k));
                (tsql_literal(v, col.map(|c| c.duck_type.as_str()).unwrap_or("VARCHAR")), Some(col.map(|c| c.name.clone()).unwrap_or(k.clone())))
            }
            Some((k, _)) => (None, Some(k.clone())),
            None => (None, None),
        };
        let part = match &hive { Some((_, v)) if by_dir => v.clone(), _ => key.strip_prefix(&prefix).unwrap_or(&key).trim_end_matches('/').to_string() };
        out.push((part.clone(), Job { id: String::new(), table: ti, label: format!("{}:{part}", t.name), source_sql, partition_value: pv, partition_column: pc, est_rows: est }));
    }
    Ok(out)
}

/// The SWITCH strategy needs one job per target partition. SQL Server compares strings
/// case-insensitively (default collations) and ignores trailing spaces, so DuckDB values that differ
/// only that way would land in the same partition (and the second SWITCH would fail).
fn check_switch_values(t: &Table, jobs: &[(String, Job)]) -> Result<()> {
    let mut seen: HashMap<String, &str> = HashMap::new();
    for (_, j) in jobs {
        let Some(v) = &j.partition_value else { continue };
        let k = v.trim_end_matches('\'').trim_end().to_lowercase();
        if let Some(prev) = seen.insert(k, v) {
            bail!("{}.{}: partition values {prev} and {v} differ only in case or trailing spaces; SQL Server treats them as one partition value, so the switch strategy cannot load them separately", t.schema, t.name);
        }
    }
    Ok(())
}

fn assign_ids(t: &Table, jobs: &mut [(String, Job)]) {
    let prefix = format!("{}.{}#", safe_part(&t.schema, true), safe_part(&t.name, true));
    let mut used: HashSet<String> = HashSet::new();
    for (raw, j) in jobs.iter_mut() {
        let mut id = format!("{prefix}{}", safe_part(raw, true));
        // deterministic and independent of the job order; collisions after hashing are practically impossible
        while !used.insert(id.to_lowercase()) { id = format!("{id}_{}", short_hash(&id)); }
        j.id = id;
    }
}

/// `[A-Za-z0-9_]` (plus `.` and `-` when `dots`); other characters become `_`. When that loses
/// information, or the text is longer than 100 characters, an 8-hex-digit hash of the original is
/// appended so different values never share a name.
pub(crate) fn safe_part(s: &str, dots: bool) -> String {
    let ok = |c: char| c.is_ascii_alphanumeric() || c == '_' || (dots && (c == '.' || c == '-'));
    let mapped: String = s.chars().map(|c| if ok(c) { c } else { '_' }).collect();
    let lossy = mapped != s || mapped.is_empty();
    let mut out: String = mapped.chars().take(100).collect();
    if lossy || out.len() < mapped.len() { out = format!("{out}_{}", short_hash(s)); }
    out
}

pub(crate) fn short_hash(s: &str) -> String {
    let h = sha2::Sha256::digest(s.as_bytes());
    hex::encode(&h[..4])
}

fn find_col<'a>(t: &'a Table, name: &str) -> Result<&'a Column> {
    let name = name.trim().trim_matches('"');
    t.columns.iter().find(|c| c.name == name).or_else(|| t.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
        .with_context(|| format!("{}.{} has no column {name:?} (columns: {})", t.schema, t.name, t.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")))
}

fn base_type(duck_type: &str) -> String {
    duck_type.split('(').next().unwrap_or("").trim().to_ascii_uppercase()
}

fn is_int_type(duck_type: &str) -> bool {
    matches!(base_type(duck_type).as_str(), "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT" | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" | "UHUGEINT" | "INT1" | "INT2" | "INT4" | "INT8")
}

fn is_string_type(duck_type: &str) -> bool {
    matches!(base_type(duck_type).as_str(), "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" | "ENUM")
}

/// The 0.1.0 split key: a single-column integer primary key, else an integer identity column.
fn auto_split_key(t: &Table) -> Option<&Column> {
    let int_types = ["INT", "BIGINT", "SMALLINT", "TINYINT"];
    let is_int = |c: &Column| int_types.contains(&c.sql_type.to_uppercase().as_str());
    if let Some(pk) = t.pk() { if pk.columns.len() == 1 { if let Some(c) = t.columns.iter().find(|c| c.name == pk.columns[0]) { if is_int(c) { return Some(c); } } } }
    t.columns.iter().find(|c| c.identity.is_some() && is_int(c))
}

fn sql_str(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// DuckDB literal for a value given as its `CAST(.. AS VARCHAR)` text.
pub fn duck_literal(text: &str, duck_type: &str) -> String {
    if is_string_type(duck_type) { sql_str(text) } else { format!("CAST({} AS {duck_type})", sql_str(text)) }
}

/// T-SQL literal for a value given as its DuckDB `CAST(.. AS VARCHAR)` text: numbers plain, strings
/// `N'..'`, dates and times quoted. `None` when there is no exact T-SQL literal (NaN, infinity).
pub fn tsql_literal(text: &str, duck_type: &str) -> Option<String> {
    let b = base_type(duck_type);
    let quoted = |s: &str| format!("'{}'", s.replace('\'', "''"));
    match b.as_str() {
        _ if is_int_type(duck_type) => text.parse::<i128>().ok().map(|v| v.to_string()).or_else(|| text.parse::<u128>().ok().map(|v| v.to_string())),
        "DECIMAL" | "NUMERIC" => text.parse::<rust_decimal::Decimal>().ok().map(|_| text.to_string()),
        "FLOAT" | "FLOAT4" | "REAL" | "DOUBLE" | "FLOAT8" => text.parse::<f64>().ok().filter(|f| f.is_finite()).map(|_| text.to_string()),
        "BOOLEAN" | "BOOL" => match text.to_ascii_lowercase().as_str() { "true" => Some("1".into()), "false" => Some("0".into()), _ => None },
        "DATE" => Some(quoted(text)),
        "TIME" | "TIMESTAMP" | "DATETIME" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_US" | "TIMESTAMP_NS" => Some(quoted(&trim_fraction(text, 7))),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Some(quoted(text)),
        "UUID" => Some(quoted(text)),
        _ => Some(format!("N{}", quoted(text))),
    }
}

/// Keep at most `digits` fractional-second digits (T-SQL datetime2 has 7).
fn trim_fraction(s: &str, digits: usize) -> String {
    match s.find('.') {
        Some(p) => {
            let frac_end = s[p + 1..].find(|c: char| !c.is_ascii_digit()).map(|e| p + 1 + e).unwrap_or(s.len());
            let keep = (p + 1 + digits).min(frac_end);
            format!("{}{}", &s[..keep], &s[frac_end..])
        }
        None => s.to_string(),
    }
}

// ---------------------------------------------------------------------------------------------
// state directory

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DoneMarker {
    pub rows: i64,
    pub seconds: f64,
    pub checksum: Option<String>,
    /// RFC 3339 UTC.
    pub finished: String,
    /// Hash of the job's `source_sql` when it finished (see [`StateDir::is_stale`]).
    #[serde(default)]
    pub source: Option<String>,
}

/// `<dir>/<schema>.<table>/<job id>.done` markers for restartable loads.
#[derive(Debug, Clone)]
pub struct StateDir {
    dir: PathBuf,
}

impl StateDir {
    pub fn open(p: &Path) -> Result<StateDir> {
        std::fs::create_dir_all(p).with_context(|| format!("creating state directory {}", p.display()))?;
        Ok(StateDir { dir: p.to_path_buf() })
    }

    pub fn path(&self) -> &Path { &self.dir }

    fn table_dir_of(&self, job: &Job) -> PathBuf {
        self.dir.join(job.id.split_once('#').map(|(t, _)| t).unwrap_or("_"))
    }

    fn marker(&self, job: &Job) -> PathBuf {
        self.table_dir_of(job).join(format!("{}.done", job.id))
    }

    pub fn read_done(&self, job: &Job) -> Option<DoneMarker> {
        let s = std::fs::read_to_string(self.marker(job)).ok()?;
        serde_json::from_str(&s).ok()
    }

    pub fn is_done(&self, job: &Job) -> bool { self.read_done(job).is_some() }

    /// True when the job is done but its source query changed since (different input or options).
    pub fn is_stale(&self, job: &Job) -> bool {
        matches!(self.read_done(job), Some(DoneMarker { source: Some(s), .. }) if s != short_hash(&job.source_sql))
    }

    /// Write the marker atomically (temporary file + rename).
    pub fn mark_done(&self, job: &Job, rows: i64, seconds: f64, checksum: Option<String>) -> Result<()> {
        let dir = self.table_dir_of(job);
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let m = DoneMarker { rows, seconds, checksum, finished: chrono::Utc::now().to_rfc3339(), source: Some(short_hash(&job.source_sql)) };
        let path = self.marker(job);
        let tmp = path.with_extension("done.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(&m)?).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    /// Forget every marker of one table (e.g. after the target table was recreated).
    pub fn clear(&self, schema: &str, table: &str) -> Result<()> {
        let d = self.dir.join(format!("{}.{}", safe_part(schema, true), safe_part(table, true)));
        if d.exists() { std::fs::remove_dir_all(&d).with_context(|| format!("removing {}", d.display()))?; }
        Ok(())
    }

    /// Number of done markers in the whole directory.
    pub fn done_jobs(&self) -> usize {
        let Ok(rd) = std::fs::read_dir(&self.dir) else { return 0 };
        rd.flatten().filter(|e| e.path().is_dir())
            .map(|e| std::fs::read_dir(e.path()).map(|r| r.flatten().filter(|f| f.path().extension().is_some_and(|x| x == "done")).count()).unwrap_or(0))
            .sum()
    }
}

// ---------------------------------------------------------------------------------------------
// shared queue

struct QueueInner {
    pending: Vec<Job>,
    in_flight: HashMap<usize, usize>,
    max_per_table: Option<usize>,
    table_limits: HashMap<usize, usize>,
    cancelled: bool,
}

/// One queue for the jobs of all tables, largest first. `max_jobs_per_table` keeps one table from
/// taking every worker; per-table limits (e.g. 1 for a `serial` table) can be set on top.
#[derive(Clone)]
pub struct JobQueue {
    inner: Arc<Mutex<QueueInner>>,
}

impl JobQueue {
    pub fn new(mut jobs: Vec<Job>, max_jobs_per_table: Option<usize>) -> JobQueue {
        jobs.sort_by(|a, b| b.est_rows.cmp(&a.est_rows).then_with(|| a.id.cmp(&b.id)));
        JobQueue { inner: Arc::new(Mutex::new(QueueInner { pending: jobs, in_flight: HashMap::new(), max_per_table: max_jobs_per_table.filter(|n| *n > 0), table_limits: HashMap::new(), cancelled: false })) }
    }

    /// At most `limit` concurrent jobs of table `table` (overrides `max_jobs_per_table` for it).
    pub fn set_table_limit(&self, table: usize, limit: usize) {
        self.lock().table_limits.insert(table, limit.max(1));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The largest pending job whose table is below its in-flight limit. `None` either when the queue
    /// is empty ([`Self::is_drained`]) or when every pending job's table is at its limit (sleep ~100 ms
    /// and retry, or use [`Self::next_blocking`]).
    pub fn next_for_worker(&self) -> Option<Job> {
        let mut q = self.lock();
        if q.cancelled { return None; }
        let pos = q.pending.iter().position(|j| {
            let limit = q.table_limits.get(&j.table).copied().or(q.max_per_table);
            limit.is_none_or(|l| q.in_flight.get(&j.table).copied().unwrap_or(0) < l)
        })?;
        let job = q.pending.remove(pos);
        *q.in_flight.entry(job.table).or_insert(0) += 1;
        Some(job)
    }

    /// Like `next_for_worker` but waits while jobs remain that are blocked by a limit; `None` only
    /// when nothing is left (or after [`Self::cancel`]). Every taken job must be `finish`ed, also on error.
    pub fn next_blocking(&self) -> Option<Job> {
        loop {
            if let Some(j) = self.next_for_worker() { return Some(j); }
            if self.is_drained() { return None; }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// Mark a job taken by `next_for_worker` as no longer in flight (success or failure).
    pub fn finish(&self, job: &Job) {
        let mut q = self.lock();
        if let Some(n) = q.in_flight.get_mut(&job.table) { *n = n.saturating_sub(1); }
    }

    /// Drop every pending job (after a failure); workers then stop after their current job.
    pub fn cancel(&self) {
        let mut q = self.lock();
        q.cancelled = true;
        q.pending.clear();
    }

    pub fn is_drained(&self) -> bool { let q = self.lock(); q.cancelled || q.pending.is_empty() }
    pub fn remaining(&self) -> usize { self.lock().pending.len() }
    pub fn in_flight(&self) -> usize { self.lock().in_flight.values().sum() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::catalog::{read_catalog, CatalogOptions};

    #[test]
    fn split_specs() {
        assert_eq!(parse_split("column:Mkt").unwrap(), SplitSpec::Column("Mkt".into()));
        assert_eq!(parse_split("files:out/big/Mkt=*/").unwrap(), SplitSpec::Files("out/big/Mkt=*/".into()));
        assert_eq!(parse_split("files:C:/data/*.parquet").unwrap(), SplitSpec::Files("C:/data/*.parquet".into()));
        assert_eq!(parse_split("hash:a,b:16").unwrap(), SplitSpec::Hash { cols: vec!["a".into(), "b".into()], n: 16 });
        assert_eq!(parse_split("hash:a:4").unwrap(), SplitSpec::Hash { cols: vec!["a".into()], n: 4 });
        assert_eq!(parse_split("range:id:8").unwrap(), SplitSpec::Range { col: "id".into(), n: 8 });
        assert_eq!(parse_split("range:id").unwrap(), SplitSpec::Range { col: "id".into(), n: 0 });
        assert_eq!(parse_split(" AUTO ").unwrap(), SplitSpec::Auto);
        for bad in ["", "column:", "hash:a", "hash::4", "hash:a:0", "range:id:x", "bogus:x", "files:", "column:a,b"] {
            assert!(parse_split(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn strategies_and_maps() {
        assert_eq!(parse_strategy("switch").unwrap(), Strategy::Switch);
        assert_eq!(parse_strategy("Heap-Then-Index").unwrap(), Strategy::HeapThenIndex);
        assert_eq!(parse_strategy("heap_then_index").unwrap(), Strategy::HeapThenIndex);
        assert_eq!(parse_strategy("serial").unwrap(), Strategy::Serial);
        assert!(parse_strategy("fast").is_err());
        assert_eq!(Strategy::HeapThenIndex.to_string(), "heap-then-index");

        let m = parse_table_spec_map(&["KPI=files:out/kpi/Mkt=*/".into(), "dbo.Other=column:x".into(), "*=serial".into()]).unwrap();
        assert_eq!(m["kpi"], "files:out/kpi/Mkt=*/");
        assert_eq!(lookup(&m, "main", "Kpi").unwrap(), "files:out/kpi/Mkt=*/");
        assert_eq!(lookup(&m, "DBO", "other").unwrap(), "column:x");
        assert_eq!(lookup(&m, "main", "other").unwrap(), "serial"); // schema differs -> wildcard
        assert_eq!(lookup(&m, "x", "y").unwrap(), "serial");
        assert!(parse_table_spec_map(&["nospec".into()]).is_err());
        assert!(parse_table_spec_map(&["=switch".into()]).is_err());
        assert!(parse_table_spec_map(&["a=switch".into(), "A=serial".into()]).is_err());
        let s = parse_strategies(&["kpi=switch".into()]).unwrap();
        assert_eq!(lookup(&s, "dbo", "KPI"), Some(&Strategy::Switch));
        assert!(lookup(&s, "dbo", "other").is_none());
        assert!(parse_splits(&["kpi=hash:a".into()]).is_err());
    }

    #[test]
    fn literals() {
        assert_eq!(tsql_literal("BBB", "VARCHAR").unwrap(), "N'BBB'");
        assert_eq!(tsql_literal("O'Brien", "VARCHAR").unwrap(), "N'O''Brien'");
        assert_eq!(tsql_literal("42", "INTEGER").unwrap(), "42");
        assert_eq!(tsql_literal("-7", "BIGINT").unwrap(), "-7");
        assert_eq!(tsql_literal("12.50", "DECIMAL(10,2)").unwrap(), "12.50");
        assert_eq!(tsql_literal("2024-01-31", "DATE").unwrap(), "'2024-01-31'");
        assert_eq!(tsql_literal("2024-01-31 10:00:00.123456789", "TIMESTAMP_NS").unwrap(), "'2024-01-31 10:00:00.1234567'");
        assert_eq!(tsql_literal("true", "BOOLEAN").unwrap(), "1");
        assert_eq!(tsql_literal("a", "ENUM('a','b')").unwrap(), "N'a'");
        assert!(tsql_literal("nan", "DOUBLE").is_none());
        assert!(tsql_literal("x", "INTEGER").is_none());
        assert_eq!(duck_literal("it's", "VARCHAR"), "'it''s'");
        assert_eq!(duck_literal("2024-01-31", "DATE"), "CAST('2024-01-31' AS DATE)");
        assert_eq!(trim_fraction("10:00:00", 7), "10:00:00");
        assert_eq!(trim_fraction("2024-01-01 10:00:00.123456789+02", 7), "2024-01-01 10:00:00.1234567+02");
    }

    #[test]
    fn safe_names() {
        assert_eq!(safe_part("AAA", true), "AAA");
        assert_eq!(safe_part("a.b-c_1", true), "a.b-c_1");
        let a = safe_part("a b", true);
        assert!(a.starts_with("a_b_") && a.len() == 12, "{a}");
        assert_ne!(safe_part("a b", true), safe_part("a_b", true));
        assert_ne!(safe_part("a/b", true), safe_part("a b", true));
        assert!(safe_part(&"x".repeat(300), true).len() <= 109);
    }

    fn job(table: usize, id: &str, est: i64) -> Job {
        Job { id: id.into(), table, label: id.into(), source_sql: format!("SELECT {est}"), partition_value: None, partition_column: None, est_rows: est }
    }

    #[test]
    fn state_dir_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let sd = StateDir::open(&tmp.path().join("state")).unwrap();
        let j1 = job(0, "main.kpi#AAA", 10);
        let j2 = job(0, "main.kpi#BBB", 5);
        let j3 = job(1, "main.other#all", 5);
        assert!(!sd.is_done(&j1));
        assert_eq!(sd.done_jobs(), 0);
        sd.mark_done(&j1, 10, 1.5, Some("abc".into())).unwrap();
        sd.mark_done(&j3, 5, 0.1, None).unwrap();
        assert!(sd.is_done(&j1) && !sd.is_done(&j2) && sd.is_done(&j3));
        let m = sd.read_done(&j1).unwrap();
        assert_eq!((m.rows, m.checksum.as_deref()), (10, Some("abc")));
        assert!(tmp.path().join("state/main.kpi/main.kpi#AAA.done").exists());
        assert_eq!(sd.done_jobs(), 2);
        assert!(!sd.is_stale(&j1));
        let mut changed = j1.clone();
        changed.source_sql.push_str(" WHERE 1=1");
        assert!(sd.is_stale(&changed));
        // reopening sees the same markers
        let sd2 = StateDir::open(&tmp.path().join("state")).unwrap();
        assert!(sd2.is_done(&j1));
        let plan = JobPlan { jobs: vec![j1.clone(), j2.clone(), j3.clone()], strategy: vec![Strategy::Serial; 2] };
        assert_eq!(plan.pending(&sd2), vec![j2.clone()]);
        sd2.clear("main", "kpi").unwrap();
        assert!(!sd2.is_done(&j1) && sd2.is_done(&j3));
        assert_eq!(sd2.done_jobs(), 1);
    }

    #[test]
    fn queue_limits() {
        let q = JobQueue::new(vec![job(0, "a1", 100), job(0, "a2", 90), job(0, "a3", 80), job(1, "b1", 10), job(2, "c1", 1)], Some(2));
        let a1 = q.next_for_worker().unwrap();
        let a2 = q.next_for_worker().unwrap();
        assert_eq!((a1.id.as_str(), a2.id.as_str()), ("a1", "a2"));
        // table 0 is at its limit: the next job comes from another table
        let b1 = q.next_for_worker().unwrap();
        assert_eq!(b1.id, "b1");
        let c1 = q.next_for_worker().unwrap();
        assert_eq!(c1.id, "c1");
        // only a3 left, blocked
        assert!(q.next_for_worker().is_none());
        assert!(!q.is_drained());
        assert_eq!(q.in_flight(), 4);
        q.finish(&a1);
        assert_eq!(q.next_for_worker().unwrap().id, "a3");
        assert!(q.is_drained());
        assert!(q.next_blocking().is_none());

        // per-table limit for a serial table, unlimited otherwise
        let q = JobQueue::new(vec![job(0, "a1", 3), job(0, "a2", 2), job(1, "b1", 1)], None);
        q.set_table_limit(0, 1);
        assert_eq!(q.next_for_worker().unwrap().id, "a1");
        assert_eq!(q.next_for_worker().unwrap().id, "b1");
        assert!(q.next_for_worker().is_none());
        // a blocked waiter wakes up once the in-flight job finishes
        let q2 = q.clone();
        let h = std::thread::spawn(move || q2.next_blocking().map(|j| j.id));
        std::thread::sleep(std::time::Duration::from_millis(150));
        q.finish(&job(0, "a1", 3));
        assert_eq!(h.join().unwrap().as_deref(), Some("a2"));
        let q = JobQueue::new(vec![job(0, "a1", 3)], None);
        q.cancel();
        assert!(q.next_blocking().is_none());
    }

    fn total(con: &Connection, sqls: &[&str]) -> i64 {
        sqls.iter().map(|s| duck::query_scalar::<i64>(con, &format!("SELECT count(*) FROM ({s})")).unwrap()).sum()
    }

    fn setup() -> (Connection, Catalog) {
        let con = duck::open_in_memory().unwrap();
        duck::exec(&con, "CREATE TABLE kpi (mkt VARCHAR(3) NOT NULL, id INTEGER NOT NULL, v DOUBLE, d DATE, PRIMARY KEY (mkt, id));
            INSERT INTO kpi SELECT CASE WHEN i % 10 < 5 THEN 'AAA' WHEN i % 10 < 8 THEN 'BBB' ELSE 'C''C' END, i, i * 1.5, DATE '2024-01-01' + (i % 3)::INT FROM range(1000) t(i);
            CREATE TABLE small (id INTEGER PRIMARY KEY, s VARCHAR); INSERT INTO small VALUES (1, 'x'), (2, NULL);
            CREATE TABLE empty_t (id INTEGER);
            CREATE TABLE big (id BIGINT PRIMARY KEY, s VARCHAR); INSERT INTO big SELECT i, 'r' || i FROM range(5000) t(i);").unwrap();
        let cat = read_catalog(&con, "test", &CatalogOptions::default()).unwrap();
        (con, cat)
    }

    fn ti(cat: &Catalog, name: &str) -> usize { cat.tables.iter().position(|t| t.name == name).unwrap() }

    #[test]
    fn plan_column_split() {
        let (con, cat) = setup();
        let splits = parse_splits(&["kpi=column:MKT".into()]).unwrap();
        let strategies = parse_strategies(&["kpi=switch".into()]).unwrap();
        let plan = plan_jobs(&con, &cat, &splits, &strategies, Strategy::Serial, 1_000_000, 4).unwrap();
        let k = ti(&cat, "kpi");
        assert_eq!(plan.strategy[k], Strategy::Switch);
        assert_eq!(plan.strategy[ti(&cat, "small")], Strategy::Serial);
        let kj: Vec<&Job> = plan.jobs_of(k).collect();
        assert_eq!(kj.len(), 3);
        assert_eq!(kj.iter().map(|j| j.est_rows).collect::<Vec<_>>(), vec![500, 300, 200]);
        assert_eq!(kj[0].partition_value.as_deref(), Some("N'AAA'"));
        assert_eq!(kj[2].partition_value.as_deref(), Some("N'C''C'"));
        assert_eq!(kj[0].partition_column.as_deref(), Some("mkt"));
        assert_eq!(kj[0].label, "kpi:AAA");
        assert_eq!(kj[0].id, "main.kpi#AAA");
        assert!(kj[2].id.starts_with("main.kpi#C_C_"), "{}", kj[2].id);
        for j in &kj { assert_eq!(total(&con, &[&j.source_sql]), j.est_rows, "{}", j.source_sql); }
        // whole plan: largest first, empty tables skipped, small tables one job
        assert!(plan.jobs.windows(2).all(|w| w[0].est_rows >= w[1].est_rows));
        assert!(plan.jobs_of(ti(&cat, "empty_t")).next().is_none());
        assert_eq!(plan.jobs_of(ti(&cat, "small")).count(), 1);
        // same plan twice -> same ids (restart)
        let again = plan_jobs(&con, &cat, &splits, &strategies, Strategy::Serial, 1_000_000, 4).unwrap();
        assert_eq!(plan.jobs.iter().map(|j| &j.id).collect::<Vec<_>>(), again.jobs.iter().map(|j| &j.id).collect::<Vec<_>>());
        // date column split and NULL value
        let p = plan_jobs(&con, &cat, &parse_splits(&["kpi=column:d".into(), "small=column:s".into()]).unwrap(), &HashMap::new(), Strategy::Serial, 1_000_000, 4).unwrap();
        assert!(p.jobs_of(k).all(|j| j.partition_value.as_deref().is_some_and(|v| v.starts_with("'2024-01-0"))));
        let sj: Vec<&Job> = p.jobs_of(ti(&cat, "small")).collect();
        assert_eq!(sj.len(), 2);
        let null_job = sj.iter().find(|j| j.label.ends_with(":NULL")).unwrap();
        assert!(null_job.partition_value.is_none() && null_job.source_sql.ends_with("\"s\" IS NULL"));
        assert_eq!(total(&con, &sj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 2);
        // unknown column
        assert!(plan_jobs(&con, &cat, &parse_splits(&["kpi=column:nope".into()]).unwrap(), &HashMap::new(), Strategy::Serial, 1, 4).is_err());
    }

    #[test]
    fn switch_rejects_case_only_values() {
        let con = duck::open_in_memory().unwrap();
        duck::exec(&con, "CREATE TABLE t (m VARCHAR, id INTEGER); INSERT INTO t VALUES ('ab', 1), ('AB', 2);").unwrap();
        let cat = read_catalog(&con, "test", &CatalogOptions::default()).unwrap();
        let splits = parse_splits(&["t=column:m".into()]).unwrap();
        assert!(plan_jobs(&con, &cat, &splits, &HashMap::new(), Strategy::Serial, 1, 4).is_ok());
        let e = plan_jobs(&con, &cat, &splits, &parse_strategies(&["*=switch".into()]).unwrap(), Strategy::Serial, 1, 4).unwrap_err();
        assert!(format!("{e:#}").contains("differ only in case"), "{e:#}");
    }

    #[test]
    fn plan_hash_and_range() {
        let (con, cat) = setup();
        let k = ti(&cat, "kpi");
        let plan = plan_jobs(&con, &cat, &parse_splits(&["kpi=hash:mkt,id:4".into()]).unwrap(), &HashMap::new(), Strategy::Serial, 1_000_000, 4).unwrap();
        let kj: Vec<&Job> = plan.jobs_of(k).collect();
        assert_eq!(kj.len(), 4);
        assert!(kj.iter().all(|j| j.source_sql.contains("hash(\"mkt\", \"id\") % 4 = ")));
        assert_eq!(total(&con, &kj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 1000);
        // every row exactly once: the union without duplicates has as many rows as the table
        let union = kj.iter().map(|j| j.source_sql.clone()).collect::<Vec<_>>().join(" UNION ALL ");
        let distinct: i64 = duck::query_scalar(&con, &format!("SELECT count(DISTINCT (mkt, id)) FROM ({union})")).unwrap();
        assert_eq!(distinct, 1000);

        // auto: big (5000 rows, BIGINT pk) splits into ranges at threshold 1000, kpi (composite pk) does not
        let plan = plan_jobs(&con, &cat, &HashMap::new(), &HashMap::new(), Strategy::Serial, 1000, 4).unwrap();
        let bj: Vec<&Job> = plan.jobs_of(ti(&cat, "big")).collect();
        assert_eq!(bj.len(), 6); // clamp(5000/1000+1, 2, 8)
        assert_eq!(total(&con, &bj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 5000);
        assert_eq!(plan.jobs_of(k).count(), 1);
        assert_eq!(plan.jobs_of(k).next().unwrap().id, "main.kpi#all");
        // no split with -j 1
        let plan = plan_jobs(&con, &cat, &HashMap::new(), &HashMap::new(), Strategy::Serial, 1000, 1).unwrap();
        assert_eq!(plan.jobs_of(ti(&cat, "big")).count(), 1);
        // explicit range
        let plan = plan_jobs(&con, &cat, &parse_splits(&["kpi=range:id:3".into()]).unwrap(), &HashMap::new(), Strategy::Serial, 1_000_000, 4).unwrap();
        let kj: Vec<&Job> = plan.jobs_of(k).collect();
        assert_eq!(kj.len(), 3);
        assert_eq!(total(&con, &kj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 1000);
        assert!(plan_jobs(&con, &cat, &parse_splits(&["kpi=range:mkt".into()]).unwrap(), &HashMap::new(), Strategy::Serial, 1, 4).is_err());
    }

    #[test]
    fn plan_files_split() {
        let (con, cat) = setup();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("kpi").to_string_lossy().replace('\\', "/");
        duck::exec(&con, &format!("COPY (SELECT * FROM kpi) TO '{dir}' (FORMAT PARQUET, PARTITION_BY (mkt));")).unwrap();
        let k = ti(&cat, "kpi");
        // per Hive directory
        let plan = plan_jobs(&con, &cat, &parse_splits(&[format!("kpi=files:{dir}/mkt=*/")]).unwrap(), &HashMap::new(), Strategy::Serial, 1_000_000, 4).unwrap();
        let kj: Vec<&Job> = plan.jobs_of(k).collect();
        assert_eq!(kj.len(), 3, "{kj:#?}");
        assert_eq!(kj.iter().map(|j| j.est_rows).collect::<Vec<_>>(), vec![500, 300, 200]);
        assert_eq!(kj[0].partition_value.as_deref(), Some("N'AAA'"));
        assert_eq!(kj[0].partition_column.as_deref(), Some("mkt"));
        assert_eq!(kj[2].partition_value.as_deref(), Some("N'C''C'"));
        assert_eq!(kj[0].id, "main.kpi#AAA");
        assert_eq!(total(&con, &kj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 1000);
        // the job query returns the table's columns in catalog order with the catalog types
        let (m, n, ty): (String, i64, String) = duck::query_one(&con, &format!("SELECT first(mkt), count(*), first(typeof(d)) FROM ({}) WHERE mkt = 'BBB'", kj[1].source_sql), |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!((m.as_str(), n, ty.as_str()), ("BBB", 300, "DATE"));
        let cols: Vec<String> = duck::query_rows(&con, &format!("DESCRIBE {}", kj[0].source_sql), |r| Ok(r.get::<_, String>(0)?)).unwrap();
        assert_eq!(cols, vec!["mkt", "id", "v", "d"]);
        // per file
        let plan = plan_jobs(&con, &cat, &parse_splits(&[format!("kpi=files:{dir}/*/*.parquet")]).unwrap(), &HashMap::new(), Strategy::Serial, 1_000_000, 4).unwrap();
        let kj: Vec<&Job> = plan.jobs_of(k).collect();
        assert_eq!(kj.len(), 3);
        assert_eq!(kj[0].partition_value.as_deref(), Some("N'AAA'"));
        assert!(kj[0].id.starts_with("main.kpi#mkt_AAA_data_0.parquet_"), "{}", kj[0].id);
        assert_eq!(total(&con, &kj.iter().map(|j| j.source_sql.as_str()).collect::<Vec<_>>()), 1000);
        assert!(plan_jobs(&con, &cat, &parse_splits(&[format!("kpi=files:{dir}/none*/")]).unwrap(), &HashMap::new(), Strategy::Serial, 1, 4).is_err());
    }
}
