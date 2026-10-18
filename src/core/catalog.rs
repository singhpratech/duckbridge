//! Read the DuckDB catalog into the neutral model.
use super::model::*;
use super::typemap::{map_column, parse_type, quote_duck, StringMode, Target};
use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashMap;

const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "system", "temp"];

pub struct CatalogOptions {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub string_mode: StringMode,
    pub target: Target,
    pub measure: bool,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self { include: vec![], exclude: vec![], string_mode: StringMode::Fit, target: Target::Mssql, measure: true }
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
    for t in &mut tables {
        let fq = format!("{}.{}", quote_duck(&t.schema), quote_duck(&t.name));
        t.row_count = con.query_row(&format!("select count(*) from {fq}"), [], |r| r.get(0))?;
        if o.measure && t.row_count > 0 {
            let keys = t.key_columns();
            let idx: Vec<usize> = t.columns.iter().enumerate()
                .filter(|(_, c)| match parse_type(&c.duck_type).0.as_str() {
                    "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => o.string_mode == StringMode::Fit || keys.contains(&c.name),
                    "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => true,
                    _ => false,
                })
                .map(|(i, _)| i).collect();
            if !idx.is_empty() {
                // strings: upper bound on UTF-16 code units (NVARCHAR(n) counts byte-pairs): chars + extra_bytes/3 >= chars + supplementary chars
                // big integers: digits without the sign
                let sel: Vec<String> = idx.iter().map(|i| {
                    let c = &t.columns[*i];
                    let q = quote_duck(&c.name);
                    match parse_type(&c.duck_type).0.as_str() {
                        "HUGEINT" | "UHUGEINT" | "VARINT" | "BIGNUM" => format!("max(length(replace(CAST({q} AS VARCHAR), '-', '')))"),
                        _ => format!("max(length({q}) + (strlen({q}) - length({q})) / 3)"),
                    }
                }).collect();
                let sql = format!("select {} from {fq}", sel.join(", "));
                let vals: Vec<Option<i64>> = con.query_row(&sql, [], |r| (0..idx.len()).map(|i| r.get(i)).collect())?;
                for (i, v) in idx.iter().zip(vals) {
                    t.columns[*i].max_len = Some(v.unwrap_or(0) as usize);
                }
            }
        }
        let keys = t.key_columns();
        for c in &mut t.columns {
            let is_key = keys.contains(&c.name);
            map_column(c, is_key, o.string_mode, o.target);
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
