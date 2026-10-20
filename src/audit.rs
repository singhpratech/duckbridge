//! Case-insensitivity audit (requirement R7, work stream F): find data where SQL Server's case- and
//! trailing-space-insensitive comparisons would give a different answer than DuckDB's binary ones.
//!
//! * `case` / `trailing-space`: values of one column that SQL Server treats as equal (`lower(rtrim(v))`
//!   collides) but DuckDB keeps apart (GROUP BY / DISTINCT / joins split them).
//! * `literal`: string literals of a script (`WHERE col = 'Active'`) that match rows only case-insensitively.
use crate::transpile::tokens::{tokenize, Kind};
use anyhow::Result;
use duckdb::Connection;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AuditRow {
    pub table: String,
    pub column: String,
    /// "case" | "trailing-space" | "literal"
    pub kind: String,
    /// Up to 5 colliding keys (case/trailing-space) or the literal (literal).
    pub examples: Vec<String>,
    /// Number of colliding groups (case/trailing-space, at most 5 are sampled) or of rows matching only case-insensitively (literal).
    pub count: i64,
}

fn qi(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }
fn ql(s: &str) -> String { format!("'{}'", s.replace('\'', "''")) }

/// String columns: (schema, table, column).
fn string_columns(con: &Connection, tables: &[String], columns: &[String]) -> Result<Vec<(String, String, String)>> {
    let mut st = con.prepare(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns \
         WHERE table_catalog = current_database() AND table_schema NOT IN ('information_schema', 'pg_catalog') \
         AND (data_type = 'VARCHAR' OR data_type LIKE 'VARCHAR(%') ORDER BY table_schema, table_name, ordinal_position")?;
    let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?;
    let want_t: Vec<String> = tables.iter().map(|t| t.to_lowercase()).collect();
    let want_c: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
    let mut out = vec![];
    for r in rows {
        let (s, t, c) = r?;
        let (tl, full) = (t.to_lowercase(), format!("{}.{}", s, t).to_lowercase());
        if !want_t.is_empty() && !want_t.iter().any(|w| *w == tl || *w == full) { continue; }
        if !want_c.is_empty() && !want_c.iter().any(|w| *w == c.to_lowercase() || *w == format!("{tl}.{}", c.to_lowercase())) { continue; }
        out.push((s, t, c));
    }
    Ok(out)
}

/// String literals of a T-SQL script that contain a letter (candidates for case-insensitive matching).
pub fn script_literals(script: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for t in tokenize(script, true) {
        if t.kind != Kind::Str { continue; }
        let v = t.value();
        if v.chars().any(|c| c.is_alphabetic()) && !out.contains(&v) { out.push(v); }
    }
    out
}

/// Audit the selected string columns (all VARCHAR columns of `tables`, all tables when empty).
pub fn audit_ci(con: &Connection, tables: &[String], columns: &[String], script: Option<&str>) -> Result<Vec<AuditRow>> {
    let cols = string_columns(con, tables, columns)?;
    let lits = script.map(script_literals).unwrap_or_default();
    let mut out = vec![];
    for (s, t, c) in &cols {
        let rel = format!("{}.{}", qi(s), qi(t));
        let col = qi(c);
        // values equal under SQL Server's rules (case and trailing spaces ignored) but distinct in DuckDB
        let sql = format!(
            "WITH g AS (SELECT lower(rtrim({col}, ' ')) AS k, count(DISTINCT {col}) AS n, count(DISTINCT lower({col})) AS nl, count(DISTINCT rtrim({col}, ' ')) AS nr \
             FROM {rel} WHERE {col} IS NOT NULL GROUP BY 1 HAVING count(DISTINCT {col}) > 1) \
             SELECT k, nl < n OR nr >= n AS by_case, nr < n AS by_space, count(*) OVER () AS total FROM g ORDER BY k LIMIT 5");
        let mut st = con.prepare(&sql)?;
        let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?, r.get::<_, bool>(2)?, r.get::<_, i64>(3)?)))?;
        let (mut case_ex, mut space_ex, mut total) = (vec![], vec![], 0i64);
        for r in rows {
            let (k, by_case, by_space, n) = r?;
            total = n;
            if by_case { case_ex.push(k.clone()); }
            if by_space { space_ex.push(k); }
        }
        if !case_ex.is_empty() { out.push(AuditRow { table: format!("{s}.{t}"), column: c.clone(), kind: "case".into(), count: total, examples: case_ex }); }
        if !space_ex.is_empty() { out.push(AuditRow { table: format!("{s}.{t}"), column: c.clone(), kind: "trailing-space".into(), count: total, examples: space_ex }); }
        // literals of the script that match this column only case-/space-insensitively
        for lit in &lits {
            let sql = format!("SELECT count(*) FROM {rel} WHERE lower(rtrim({col}, ' ')) = lower(rtrim({l}, ' ')) AND {col} <> {l}", l = ql(lit));
            let n: i64 = con.query_row(&sql, [], |r| r.get(0))?;
            if n > 0 { out.push(AuditRow { table: format!("{s}.{t}"), column: c.clone(), kind: "literal".into(), examples: vec![lit.clone()], count: n }); }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_finds_case_space_and_literal_collisions() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(
            "CREATE TABLE customers (id INTEGER, status VARCHAR, city VARCHAR, code VARCHAR);
             INSERT INTO customers VALUES (1, 'Active', 'Paris', 'A1'), (2, 'active', 'Paris', 'A1'), (3, 'ACTIVE', 'paris ', 'B2'),
                                          (4, 'Closed', 'Lyon', 'C3'), (5, 'Closed ', 'Lyon', NULL);
             CREATE TABLE other (x VARCHAR); INSERT INTO other VALUES ('same'), ('same');").unwrap();
        let rows = audit_ci(&con, &["customers".into()], &[], Some("SELECT * FROM customers WHERE status = 'Active' AND city = N'PARIS' AND code = '123'")).unwrap();
        let find = |col: &str, kind: &str| rows.iter().find(|r| r.column == col && r.kind == kind).cloned();
        let st = find("status", "case").expect("status case collision");
        assert_eq!(st.examples, vec!["active".to_string()]);
        assert_eq!(st.count, 2); // 'active' and 'closed' groups collide
        assert_eq!(find("status", "trailing-space").unwrap().examples, vec!["closed".to_string()]);
        let city = find("city", "case").unwrap();
        assert_eq!(city.examples, vec!["paris".to_string()]);
        // 'Active' matches 'active' and 'ACTIVE' only case-insensitively
        let lit = rows.iter().find(|r| r.column == "status" && r.kind == "literal" && r.examples == vec!["Active".to_string()]).unwrap();
        assert_eq!(lit.count, 2);
        let lit = rows.iter().find(|r| r.column == "city" && r.kind == "literal" && r.examples == vec!["PARIS".to_string()]).unwrap();
        assert_eq!(lit.count, 3);
        // literals without letters are ignored; clean columns report nothing; other tables are not scanned
        assert!(rows.iter().all(|r| r.examples != vec!["123".to_string()]));
        assert!(find("code", "case").is_none());
        assert!(rows.iter().all(|r| r.table == "main.customers"));
        // column filter
        let only = audit_ci(&con, &[], &["city".into()], None).unwrap();
        assert!(only.iter().all(|r| r.column == "city") && !only.is_empty());
    }
}
