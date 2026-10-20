//! Read the DuckDB catalog into the neutral model.
use super::model::*;
use super::typemap::{map_column_with, parse_type, quote_duck, StringMode, StringType, Target};
use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashMap;

const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "system", "temp"];

/// How string column sizes are chosen.
#[derive(Debug, Clone, Default)]
pub enum StringSizes {
    /// Measure the data (default).
    #[default]
    FromData,
    /// Always (N)VARCHAR(MAX) (keys are still sized).
    Max,
    /// "table.column" (or "schema.table.column"), lowercase -> exact SQL Server type text.
    File(HashMap<String, String>),
}

/// Parse a CSV `table,column,type` (optional header). `table` may be `schema.table` or bare.
pub fn load_string_sizes_file(path: &str) -> Result<StringSizes> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading string sizes file {path}"))?;
    let mut map = HashMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.splitn(3, ',').map(|p| p.trim().trim_matches('"')).collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            anyhow::bail!("{path}:{}: expected table,column,type", n + 1);
        }
        if n == 0 && parts[0].eq_ignore_ascii_case("table") && parts[1].eq_ignore_ascii_case("column") {
            continue;
        }
        map.insert(format!("{}.{}", parts[0], parts[1]).to_lowercase(), parts[2].to_string());
    }
    Ok(StringSizes::File(map))
}

pub struct CatalogOptions {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub string_mode: StringMode,
    pub target: Target,
    pub measure: bool,
    pub string_type: StringType,
    pub string_sizes: StringSizes,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self { include: vec![], exclude: vec![], string_mode: StringMode::Fit, target: Target::Mssql, measure: true, string_type: StringType::Nvarchar, string_sizes: StringSizes::FromData }
    }
}

pub fn open_readonly(path: &str) -> Result<Connection> {
    let cfg = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
    let con = Connection::open_with_flags(path, cfg).with_context(|| format!("opening DuckDB file {path}"))?;
    con.execute_batch("SET TimeZone='UTC';")?;
    Ok(con)
}

fn wanted(o: &CatalogOptions, schema: &str, name: &str) -> bool {
    let full = format!("{schema}.{name}").to_lowercase();
    let n = name.to_lowercase();
    if !o.include.is_empty() && !o.include.iter().any(|i| i.to_lowercase() == n || i.to_lowercase() == full) {
        return false;
    }
    !o.exclude.iter().any(|e| e.to_lowercase() == n || e.to_lowercase() == full)
}

pub fn read_catalog(con: &Connection, source: &str, o: &CatalogOptions) -> Result<Catalog> {
    let version: String = con.query_row("select version()", [], |r| r.get(0))?;
    let db: String = con.query_row("select current_database()", [], |r| r.get(0))?;

    // enum user types -> expanded ENUM('a','b') strings
    let mut enums: HashMap<String, String> = HashMap::new();
    {
        let mut st = con.prepare("select schema_name, type_name from duckdb_types() where logical_type='ENUM' and not internal and database_name=?")?;
        let rows: Vec<(String, String)> = st.query_map([&db], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;
        for (sch, tn) in rows {
            let sql = format!("select list_aggregate(list_transform(enum_range(NULL::{}.{}), x -> '''' || replace(x, '''', '''''') || ''''), 'string_agg', ', ')", quote_duck(&sch), quote_duck(&tn));
            let vals: String = con.query_row(&sql, [], |r| r.get(0))?;
            let e = format!("ENUM({vals})");
            enums.insert(tn.clone(), e.clone());
            enums.insert(format!("{sch}.{tn}"), e);
        }
    }

    // sequences
    let mut sequences = Vec::new();
    {
        let mut st = con.prepare("select schema_name, sequence_name, start_value, increment_by, last_value from duckdb_sequences() where database_name=? order by 1,2")?;
        for r in st.query_map([&db], |r| {
            Ok(Sequence { schema: r.get(0)?, name: r.get(1)?, start: r.get(2)?, increment: r.get(3)?, last_value: r.get(4)? })
        })? {
            sequences.push(r?);
        }
    }
    let seq_lookup = |key: &str| -> Option<&Sequence> {
        let key = key.replace('"', "");
        sequences.iter().find(|s| format!("{}.{}", s.schema, s.name) == key || s.name == key || key.ends_with(&format!(".{}", s.name)))
    };

    // tables
    let mut tables = Vec::new();
    {
        let mut st = con.prepare("select schema_name, table_name, comment from duckdb_tables() where database_name=? and not internal order by 1,2")?;
        let trows: Vec<(String, String, Option<String>)> = st.query_map([&db], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
        for (sch, tname, tcomment) in trows {
            if SYSTEM_SCHEMAS.contains(&sch.as_str()) || !wanted(o, &sch, &tname) {
                continue;
            }
            let mut cols = Vec::new();
            let mut cst = con.prepare("select column_name, data_type, is_nullable, column_default, comment from duckdb_columns() where database_name=? and schema_name=? and table_name=? order by column_index")?;
            for r in cst.query_map([&db, &sch, &tname], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, bool>(2)?, r.get::<_, Option<String>>(3)?, r.get::<_, Option<String>>(4)?))
            })? {
                let (name, mut dtype, nullable, default, comment) = r?;
                if let Some(e) = enums.get(&dtype) {
                    dtype = e.clone();
                }
                let identity = default.as_deref().and_then(nextval_target).and_then(|k| seq_lookup(&k)).map(|s| (s.start, s.increment));
                cols.push(Column {
                    name, duck_type: dtype, nullable, default, comment,
                    sql_type: String::new(), select_expr: String::new(), conv: Conv::Identity,
                    max_len: None, identity, enum_values: None, warnings: vec![],
                });
            }
            let mut constraints = Vec::new();
            let mut kst = con.prepare("select constraint_type, constraint_column_names, expression, constraint_name, referenced_table, referenced_column_names from duckdb_constraints() where database_name=? and schema_name=? and table_name=? order by constraint_index")?;
            for r in kst.query_map([&db, &sch, &tname], |r| {
                Ok((r.get::<_, String>(0)?, list_of_strings(r, 1), r.get::<_, Option<String>>(2)?, r.get::<_, Option<String>>(3)?, r.get::<_, Option<String>>(4)?, list_of_strings(r, 5)))
            })? {
                let (ctype, ccols, expr, cname, rtable, rcols) = r?;
                let kind = match ctype.as_str() {
                    "PRIMARY KEY" => ConstraintKind::PrimaryKey,
                    "UNIQUE" => ConstraintKind::Unique,
                    "FOREIGN KEY" => ConstraintKind::ForeignKey,
                    "CHECK" => ConstraintKind::Check,
                    _ => continue,
                };
                constraints.push(Constraint {
                    kind, name: cname, columns: ccols,
                    ref_schema: rtable.as_ref().map(|_| sch.clone()), ref_table: rtable, ref_columns: rcols,
                    expression: expr,
                });
            }
            let mut indexes = Vec::new();
            let mut ist = con.prepare("select index_name, is_unique, expressions, sql from duckdb_indexes() where database_name=? and schema_name=? and table_name=?")?;
            for r in ist.query_map([&db, &sch, &tname], |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?, list_of_strings(r, 2), r.get::<_, Option<String>>(3)?)))? {
                let (iname, uniq, exprs, isql) = r?;
                if let Some(cols) = index_columns(&exprs, isql.as_deref()) {
                    indexes.push(Index { name: iname, columns: cols, unique: uniq });
                }
            }
            tables.push(Table { schema: sch, name: tname, columns: cols, constraints, indexes, row_count: 0, comment: tcomment });
        }
    }

    // measurements + mapping
    let mode = if matches!(o.string_sizes, StringSizes::Max) { StringMode::Max } else { o.string_mode };
    let auto = o.string_type == StringType::Auto;
    for t in &mut tables {
        let mut ascii: HashMap<usize, bool> = HashMap::new();
        let fq = format!("{}.{}", quote_duck(&t.schema), quote_duck(&t.name));
        t.row_count = con.query_row(&format!("select count(*) from {fq}"), [], |r| r.get(0))?;
        if o.measure && t.row_count > 0 {
            let keys = t.key_columns();
            let idx: Vec<usize> = t.columns.iter().enumerate()
                .filter(|(_, c)| match parse_type(&c.duck_type).0.as_str() {
                    "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => mode == StringMode::Fit || auto || keys.contains(&c.name),
                    "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => true,
                    _ => false,
                })
                .map(|(i, _)| i).collect();
            if !idx.is_empty() {
                // strings: upper bound on UTF-16 code units (NVARCHAR(n) counts byte-pairs): chars + extra_bytes/3 >= chars + supplementary chars
                // big integers: digits without the sign
                let mut sel: Vec<String> = Vec::new();
                let mut ascii_at: Vec<(usize, usize)> = Vec::new(); // (column index, position in select list)
                for i in &idx {
                    let c = &t.columns[*i];
                    let q = quote_duck(&c.name);
                    match parse_type(&c.duck_type).0.as_str() {
                        "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => sel.push(format!("max(length(replace(CAST({q} AS VARCHAR), '-', '')))")),
                        _ => sel.push(format!("max(length({q}) + (strlen({q}) - length({q})) / 3)")),
                    }
                }
                if auto {
                    for i in &idx {
                        let c = &t.columns[*i];
                        if !matches!(parse_type(&c.duck_type).0.as_str(), "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM") {
                            let q = quote_duck(&c.name);
                            ascii_at.push((*i, sel.len()));
                            sel.push(format!("bool_and({q} IS NULL OR strlen({q}) = length({q}))"));
                        }
                    }
                }
                let sql = format!("select {} from {fq}", sel.join(", "));
                let n_len = idx.len();
                let (vals, flags): (Vec<Option<i64>>, Vec<Option<bool>>) = con.query_row(&sql, [], |r| {
                    let v = (0..n_len).map(|i| r.get(i)).collect::<std::result::Result<Vec<Option<i64>>, _>>()?;
                    let f = (n_len..sel.len()).map(|i| r.get(i)).collect::<std::result::Result<Vec<Option<bool>>, _>>()?;
                    Ok((v, f))
                })?;
                for (i, v) in idx.iter().zip(vals) {
                    t.columns[*i].max_len = Some(v.unwrap_or(0) as usize);
                }
                for (ci, pos) in ascii_at {
                    ascii.insert(ci, flags[pos - n_len].unwrap_or(true));
                }
            }
        }
        let keys = t.key_columns();
        let (tschema, tname) = (t.schema.to_lowercase(), t.name.to_lowercase());
        for (ci, c) in t.columns.iter_mut().enumerate() {
            let is_key = keys.contains(&c.name);
            let pinned = match &o.string_sizes {
                StringSizes::File(m) => {
                    let cn = c.name.to_lowercase();
                    m.get(&format!("{tschema}.{tname}.{cn}")).or_else(|| m.get(&format!("{tname}.{cn}"))).map(|s| s.as_str())
                }
                _ => None,
            };
            map_column_with(c, is_key, mode, o.target, o.string_type, ascii.get(&ci).copied(), pinned);
        }
    }

    // views
    let mut views = Vec::new();
    {
        let mut st = con.prepare("select schema_name, view_name, sql, comment from duckdb_views() where database_name=? and not internal order by 1,2")?;
        for r in st.query_map([&db], |r| Ok(View { schema: r.get(0)?, name: r.get(1)?, sql: r.get(2)?, comment: r.get(3)? }))? {
            let v = r?;
            if !SYSTEM_SCHEMAS.contains(&v.schema.as_str()) && wanted(o, &v.schema, &v.name) {
                views.push(v);
            }
        }
    }
    let mut schemas: Vec<String> = tables.iter().map(|t| t.schema.clone()).chain(views.iter().map(|v| v.schema.clone())).collect();
    schemas.sort();
    schemas.dedup();
    Ok(Catalog { source: source.into(), duckdb_version: version, schemas, tables, views, sequences, warnings: vec![] })
}

fn list_of_strings(r: &duckdb::Row, i: usize) -> Vec<String> {
    // DuckDB LIST(VARCHAR) comes back as a ListType value; simplest robust path is via its text form.
    match r.get_ref(i) {
        Ok(duckdb::types::ValueRef::List(list, idx)) => {
            use duckdb::arrow::array::Array;
            let arr: std::sync::Arc<dyn Array> = match list { duckdb::types::ListType::Regular(a) => a.value(idx), duckdb::types::ListType::Large(a) => a.value(idx) };
            let sa = arr.as_any().downcast_ref::<duckdb::arrow::array::StringArray>();
            match sa {
                Some(sa) => (0..sa.len()).filter(|k| !sa.is_null(*k)).map(|k| sa.value(k).to_string()).collect(),
                None => vec![],
            }
        }
        _ => vec![],
    }
}

fn nextval_target(default: &str) -> Option<String> {
    let low = default.to_lowercase();
    let i = low.find("nextval(")?;
    let rest = &default[i + 8..];
    let start = rest.find('\'')? + 1;
    let end = rest[start..].find('\'')? + start;
    Some(rest[start..end].to_string())
}

fn index_columns(exprs: &[String], sql: Option<&str>) -> Option<Vec<String>> {
    if !exprs.is_empty() {
        let mut out = Vec::new();
        for e in exprs {
            let e = e.trim().trim_matches('"');
            if e.chars().all(|c| c.is_alphanumeric() || c == '_' || c == ' ') {
                out.push(e.to_string());
            } else {
                return None; // expression index; cannot map 1:1
            }
        }
        return Some(out);
    }
    let sql = sql?;
    let open = sql.rfind('(')?;
    let close = sql.rfind(')')?;
    Some(sql[open + 1..close].split(',').map(|p| p.trim().trim_matches('"').to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_string_type_measures_ascii() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch("create table t(a varchar, b varchar); insert into t values ('abc','h\u{00e9}llo'),(NULL,'x');").unwrap();
        let o = CatalogOptions { string_type: StringType::Auto, ..Default::default() };
        let cat = read_catalog(&con, "mem", &o).unwrap();
        let t = &cat.tables[0];
        assert!(t.columns[0].sql_type.starts_with("VARCHAR("), "{}", t.columns[0].sql_type);
        assert!(t.columns[1].sql_type.starts_with("NVARCHAR("), "{}", t.columns[1].sql_type);
        assert!(t.columns[0].warnings.iter().any(|w| w.contains("auto string type: varchar")));
        assert!(t.columns[1].warnings.iter().any(|w| w.contains("non-ASCII")));
        // default: unchanged
        let cat = read_catalog(&con, "mem", &CatalogOptions::default()).unwrap();
        assert!(cat.tables[0].columns[0].sql_type.starts_with("NVARCHAR("));
        assert!(cat.tables[0].columns[0].warnings.is_empty());
    }

    #[test]
    fn pinned_and_max_sizes() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch("create table t(a varchar, b varchar); insert into t values ('abc','x');").unwrap();
        let mut m = HashMap::new();
        m.insert("t.a".to_string(), "varchar(12)".to_string());
        let o = CatalogOptions { string_sizes: StringSizes::File(m), ..Default::default() };
        let cat = read_catalog(&con, "mem", &o).unwrap();
        assert_eq!(cat.tables[0].columns[0].sql_type, "varchar(12)");
        assert_eq!(cat.tables[0].columns[1].sql_type, "NVARCHAR(50)");
        let o = CatalogOptions { string_sizes: StringSizes::Max, string_type: StringType::Varchar, ..Default::default() };
        let cat = read_catalog(&con, "mem", &o).unwrap();
        assert_eq!(cat.tables[0].columns[1].sql_type, "VARCHAR(MAX)");
    }

    #[test]
    fn sizes_file_parses() {
        let dir = std::env::temp_dir().join(format!("d2m_sizes_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("s.csv");
        std::fs::write(&f, "table,column,type\nmain.T1,Name,varchar(12)\nt2,C,nvarchar(50)\n").unwrap();
        match load_string_sizes_file(f.to_str().unwrap()).unwrap() {
            StringSizes::File(m) => {
                assert_eq!(m["main.t1.name"], "varchar(12)");
                assert_eq!(m["t2.c"], "nvarchar(50)");
            }
            _ => panic!(),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
