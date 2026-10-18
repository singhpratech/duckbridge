//! Our own SQL dialect translator: T-SQL <-> DuckDB SQL <-> PostgreSQL.
//!
//! Design: tokenize, split into statements, then run a recursive rewriter over each statement.
//! The rewriter understands parenthesised groups, function calls (name + argument list),
//! SELECT segments (for TOP/LIMIT/OFFSET placement) and dialect-specific statement forms.
//! Anything it cannot translate faithfully is left as-is and reported as a warning, never
//! silently mangled.
pub mod duck_to_pg;
pub mod duck_to_tsql;
pub mod engine;
pub mod pg_to_duck;
pub mod tokens;
pub mod tsql_to_duck;
pub mod types;

use std::collections::HashMap;
use tokens::{tokenize, Tok};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Tsql,
    DuckDb,
    Postgres,
}

impl Dialect {
    pub fn parse(s: &str) -> Option<Dialect> {
        match s.to_lowercase().as_str() {
            "tsql" | "mssql" | "sqlserver" | "sql-server" => Some(Dialect::Tsql),
            "duckdb" | "duck" => Some(Dialect::DuckDb),
            "postgres" | "pg" | "postgresql" => Some(Dialect::Postgres),
            _ => None,
        }
    }
    pub fn bracket_idents(self) -> bool {
        self == Dialect::Tsql
    }
}

#[derive(Debug, Clone, Default)]
pub struct Translated {
    pub sql: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("unsupported translation {0:?} -> {1:?}")]
    Unsupported(Dialect, Dialect),
}

#[derive(Debug, Clone)]
pub struct Options {
    /// SQL Server version the T-SQL output must run on (2019, 2022, 2025).
    pub target_version: u16,
    /// Emit the DuckDB session prelude (integer division, NULL order, collation, UTC) before a translated T-SQL script.
    pub prelude: bool,
    /// Treat string comparison as case-insensitive like SQL Server's default collation.
    pub nocase: bool,
    /// Rewrite DuckDB `/` as float division when producing T-SQL.
    pub float_division: bool,
}

impl Default for Options {
    fn default() -> Self { Options { target_version: 2019, prelude: true, nocase: true, float_division: true } }
}

/// Session settings that make DuckDB behave like SQL Server for translated T-SQL.
pub fn duckdb_prelude(nocase: bool) -> Vec<&'static str> {
    let mut v = vec![
        "SET integer_division = true",                                   // T-SQL int/int truncates
        "SET default_null_order = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC'",   // SQL Server NULL ordering
        "SET TimeZone = 'UTC'",
    ];
    if nocase { v.push("SET default_collation = 'nocase'"); }           // SQL Server default collations are CI
    v
}

/// Translate a script (one or more statements) with default options.
pub fn translate(sql: &str, src: Dialect, dst: Dialect, schema_map: &HashMap<String, String>) -> Result<Translated, TranslateError> {
    translate_with(sql, src, dst, schema_map, &Options { prelude: false, ..Options::default() })
}

pub fn translate_with(sql: &str, src: Dialect, dst: Dialect, schema_map: &HashMap<String, String>, opts: &Options) -> Result<Translated, TranslateError> {
    if src == dst {
        return Ok(Translated { sql: sql.to_string(), warnings: vec![] });
    }
    let toks = tokenize(sql, src.bracket_idents());
    let mut ctx = engine::Ctx::new(schema_map.clone());
    ctx.target_version = opts.target_version;
    ctx.nocase = opts.nocase;
    ctx.float_division = opts.float_division;
    let mut out = String::new();
    if opts.prelude && src == Dialect::Tsql && dst == Dialect::DuckDb {
        out.push_str("-- duckbridge prelude: make DuckDB behave like SQL Server for this script\n");
        for p in duckdb_prelude(opts.nocase) { out.push_str(p); out.push_str(";\n"); }
    }
    let stmts = tokens::statements(&toks);
    let n = stmts.len();
    for (i, st) in stmts.iter().enumerate() {
        // keep leading comments
        for tk in st.iter().take_while(|t| t.is_trivia()) {
            if tk.kind == tokens::Kind::Comment { out.push_str(&tk.text); out.push('\n'); }
        }
        let st = tokens::trim(st).to_vec();
        if st.is_empty() {
            continue;
        }
        let pieces = translate_stmt(&st, src, dst, &mut ctx)?;
        for p in pieces {
            let s = tokens::join(&p).trim().to_string();
            if s.is_empty() {
                continue;
            }
            out.push_str(&s);
            if !s.ends_with(';') && !s.starts_with("--") && !s.starts_with("/*") {
                out.push(';');
            }
            out.push('\n');
        }
        if i + 1 < n && !out.ends_with("\n\n") {
            // keep statements visually separated
        }
    }
    Ok(Translated { sql: out.trim_end().to_string(), warnings: ctx.take_warnings() })
}

/// Translate a single scalar/boolean expression (used for defaults and CHECK constraints).
pub fn translate_expr(expr: &str, src: Dialect, dst: Dialect) -> Result<Translated, TranslateError> {
    if src == dst {
        return Ok(Translated { sql: expr.to_string(), warnings: vec![] });
    }
    let toks = tokenize(expr, src.bracket_idents());
    let mut ctx = engine::Ctx::new(HashMap::new());
    let out = rewrite_expr(&toks, src, dst, &mut ctx)?;
    Ok(Translated { sql: tokens::join(&out).trim().to_string(), warnings: ctx.take_warnings() })
}

fn translate_stmt(st: &[Tok], src: Dialect, dst: Dialect, ctx: &mut engine::Ctx) -> Result<Vec<Vec<Tok>>, TranslateError> {
    use Dialect::*;
    Ok(match (src, dst) {
        (Tsql, DuckDb) => tsql_to_duck::statement(st, ctx),
        (DuckDb, Tsql) => duck_to_tsql::statement(st, ctx),
        (DuckDb, Postgres) => duck_to_pg::statement(st, ctx),
        (Postgres, DuckDb) => pg_to_duck::statement(st, ctx),
        (Tsql, Postgres) => {
            let mut out = vec![];
            for s in tsql_to_duck::statement(st, ctx) {
                out.extend(duck_to_pg::statement(&s, ctx));
            }
            out
        }
        (Postgres, Tsql) => {
            let mut out = vec![];
            for s in pg_to_duck::statement(st, ctx) {
                out.extend(duck_to_tsql::statement(&s, ctx));
            }
            out
        }
        (a, b) => return Err(TranslateError::Unsupported(a, b)),
    })
}

fn rewrite_expr(toks: &[Tok], src: Dialect, dst: Dialect, ctx: &mut engine::Ctx) -> Result<Vec<Tok>, TranslateError> {
    use Dialect::*;
    Ok(match (src, dst) {
        (Tsql, DuckDb) => tsql_to_duck::expression(toks, ctx),
        (DuckDb, Tsql) => duck_to_tsql::expression(toks, ctx),
        (DuckDb, Postgres) => duck_to_pg::expression(toks, ctx),
        (Postgres, DuckDb) => pg_to_duck::expression(toks, ctx),
        (Tsql, Postgres) => {
            let mid = tsql_to_duck::expression(toks, ctx);
            duck_to_pg::expression(&mid, ctx)
        }
        (Postgres, Tsql) => {
            let mid = pg_to_duck::expression(toks, ctx);
            duck_to_tsql::expression(&mid, ctx)
        }
        (a, b) => return Err(TranslateError::Unsupported(a, b)),
    })
}

/// For `CREATE [OR REPLACE] VIEW name AS <query>` return `<query>`; otherwise the input unchanged.
pub fn view_body(sql: &str, bracket_idents: bool) -> String {
    let toks = tokenize(sql, bracket_idents);
    let sig: Vec<usize> = toks.iter().enumerate().filter(|(_, t)| !t.is_trivia()).map(|(i, _)| i).collect();
    if sig.is_empty() || !toks[sig[0]].is_kw("CREATE") { return sql.trim().trim_end_matches(';').to_string(); }
    let mut depth = 0i32;
    for (i, t) in toks.iter().enumerate() {
        if t.is_punct("(") { depth += 1; } else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && t.is_kw("AS") {
            return tokens::join(&toks[i + 1..]).trim().trim_end_matches(';').to_string();
        }
    }
    sql.trim().trim_end_matches(';').to_string()
}
