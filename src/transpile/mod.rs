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
pub mod semantics;
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

/// Warning codes. Every warning carries one; free-text warnings use [`codes::GENERIC`].
pub mod codes {
    /// Decimal division (`int * 1.0 / int` style) whose DuckDB result (DOUBLE) differs from SQL Server's numeric rules.
    pub const DIV_DECIMAL: &str = "W-DIV-DECIMAL";
    /// `ROUND(x, n, 1)` (truncation) translated through DOUBLE arithmetic.
    pub const ROUND_TRUNC: &str = "W-ROUND-TRUNC";
    /// `FORMAT()` with a numeric/custom format left untranslated.
    pub const FORMAT_NUM: &str = "W-FORMAT-NUM";
    /// `TRY_CAST`/`TRY_CONVERT` to an integer type: SQL Server gives 0 for '', DuckDB NULL.
    pub const TRYCAST_EMPTY: &str = "W-TRYCAST-EMPTY";
    /// String comparison that keeps DuckDB's case-sensitive semantics (not rewritten).
    pub const CI_COMPARE: &str = "W-CI-COMPARE";
    /// String equality: SQL Server ignores trailing spaces, DuckDB does not.
    pub const TRAILING_SPACE: &str = "W-TRAILING-SPACE";
    /// Procedural T-SQL (IF/WHILE/BEGIN...END/cursors/dynamic SQL) left as a comment.
    pub const PROCEDURAL: &str = "W-PROCEDURAL";
    /// `POWER` on a decimal: SQL Server keeps the decimal type, DuckDB computes DOUBLE.
    pub const POWER_DOUBLE: &str = "W-POWER-DOUBLE";
    /// Any other translation note.
    pub const GENERIC: &str = "W-GENERIC";
    pub const ALL: &[&str] = &[DIV_DECIMAL, ROUND_TRUNC, FORMAT_NUM, TRYCAST_EMPTY, CI_COMPARE, TRAILING_SPACE, PROCEDURAL, POWER_DOUBLE, GENERIC];
}

/// A translation warning with a stable code and the 1-based source position (0:0 when unknown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    pub code: &'static str,
    pub line: usize,
    pub col: usize,
    pub message: String,
}

impl std::fmt::Display for Warning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (line {}:{}): {}", self.code, self.line, self.col, self.message)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Translated {
    pub sql: String,
    /// Rendered warnings (`Warning`'s Display form), kept as strings for existing callers.
    pub warnings: Vec<String>,
    /// The same warnings in structured form.
    pub diagnostics: Vec<Warning>,
}

impl Translated {
    fn with_sql(mut self, sql: &str) -> Self { self.sql = sql.to_string(); self }
    fn new(sql: String, diagnostics: Vec<Warning>) -> Self {
        Translated { sql, warnings: diagnostics.iter().map(|w| w.to_string()).collect(), diagnostics }
    }
}

fn render_list(ws: &[Warning]) -> String {
    ws.iter().map(|w| w.to_string()).collect::<Vec<_>>().join("\n")
}

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("unsupported translation {0:?} -> {1:?}")]
    Unsupported(Dialect, Dialect),
    /// `Options.strict` and at least one warning was produced; nothing should be executed.
    #[error("strict mode: {} translation warning(s):\n{}", .0.len(), render_list(.0))]
    Strict(Vec<Warning>),
}

/// How SQL Server's case-insensitive default collation is reproduced in DuckDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NocaseMode {
    /// `SET default_collation = 'nocase'` for the whole session (0.1.0 behaviour; affects every query).
    Global,
    /// Rewrite the script: LIKE -> ILIKE, `col = 'Lit'` -> `lower(rtrim(col, ' ')) = lower('Lit')`, IN lists likewise;
    /// everything else that compares strings is reported as W-CI-COMPARE.
    Rewrite,
    /// Keep DuckDB's binary comparisons.
    Off,
}

impl NocaseMode {
    pub fn parse(s: &str) -> Option<NocaseMode> {
        match s.to_ascii_lowercase().as_str() {
            "global" => Some(NocaseMode::Global),
            "rewrite" => Some(NocaseMode::Rewrite),
            "off" | "none" | "false" => Some(NocaseMode::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// SQL Server version the T-SQL output must run on (2019, 2022, 2025).
    pub target_version: u16,
    /// Emit the DuckDB session prelude (integer division, NULL order, UTC, optional collation, math macros) before a translated T-SQL script.
    pub prelude: bool,
    /// Legacy switch: treat string comparison as case-insensitive. Used only when `nocase_mode` is None
    /// (true -> Rewrite, false -> Off).
    pub nocase: bool,
    /// Explicit case-insensitivity mode; overrides `nocase`.
    pub nocase_mode: Option<NocaseMode>,
    /// Rewrite DuckDB `/` as float division when producing T-SQL.
    pub float_division: bool,
    /// Fail with `TranslateError::Strict` when any warning is produced.
    pub strict: bool,
    /// Use the mathcompat macros for exact SQL Server arithmetic (and add them to the prelude).
    pub mssql_math: bool,
    /// Lower-case column name (or `table.column`) -> SQL Server type ("decimal(18,4)", "int", "varchar(20)").
    pub types: HashMap<String, String>,
}

impl Default for Options {
    fn default() -> Self {
        Options { target_version: 2019, prelude: true, nocase: true, nocase_mode: None, float_division: true, strict: false, mssql_math: false, types: HashMap::new() }
    }
}

impl Options {
    /// The effective case-insensitivity mode.
    pub fn nocase_mode(&self) -> NocaseMode {
        self.nocase_mode.unwrap_or(if self.nocase { NocaseMode::Rewrite } else { NocaseMode::Off })
    }
}

/// Parse a `--types` value: `col=type,col2=type2` (type may contain a comma inside parentheses,
/// e.g. `amount=decimal(18,4),qty=int`). Keys are lower-cased.
pub fn parse_types(spec: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut items = vec![];
    for ch in spec.chars() {
        match ch {
            '(' => { depth += 1; cur.push(ch); }
            ')' => { depth -= 1; cur.push(ch); }
            ',' | ';' | '\n' if depth == 0 => items.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    items.push(cur);
    for it in items {
        if let Some((k, v)) = it.split_once('=').or_else(|| it.split_once(':')) {
            let (k, v) = (k.trim(), v.trim());
            if !k.is_empty() && !v.is_empty() { out.insert(k.to_lowercase(), v.to_lowercase()); }
        }
    }
    out
}

/// Session settings that make DuckDB behave like SQL Server for translated T-SQL.
/// `nocase = true` means the *global* collation (NocaseMode::Global); see [`duckdb_prelude_for`].
pub fn duckdb_prelude(nocase: bool) -> Vec<&'static str> {
    duckdb_prelude_for(if nocase { NocaseMode::Global } else { NocaseMode::Off })
}

/// Session prelude for a nocase mode: `default_collation` only for `Global`.
pub fn duckdb_prelude_for(mode: NocaseMode) -> Vec<&'static str> {
    let mut v = vec![
        "SET integer_division = true",                                   // T-SQL int/int truncates
        "SET default_null_order = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC'",   // SQL Server NULL ordering
        "SET TimeZone = 'UTC'",
    ];
    if mode == NocaseMode::Global { v.push("SET default_collation = 'nocase'"); } // SQL Server default collations are CI
    v
}

/// Translate a script (one or more statements) with default options.
pub fn translate(sql: &str, src: Dialect, dst: Dialect, schema_map: &HashMap<String, String>) -> Result<Translated, TranslateError> {
    translate_with(sql, src, dst, schema_map, &Options { prelude: false, ..Options::default() })
}

pub fn translate_with(sql: &str, src: Dialect, dst: Dialect, schema_map: &HashMap<String, String>, opts: &Options) -> Result<Translated, TranslateError> {
    if src == dst {
        return Ok(Translated::default().with_sql(sql));
    }
    let toks = tokenize(sql, src.bracket_idents());
    let mode = opts.nocase_mode();
    let mut ctx = engine::Ctx::new(schema_map.clone());
    ctx.target_version = opts.target_version;
    ctx.nocase_mode = mode;
    ctx.nocase = mode != NocaseMode::Off;
    ctx.float_division = opts.float_division;
    ctx.mssql_math = opts.mssql_math && src == Dialect::Tsql;
    ctx.types = opts.types.iter().map(|(k, v)| (k.to_lowercase(), v.to_lowercase())).collect();
    let mut out = String::new();
    if opts.prelude && src == Dialect::Tsql && dst == Dialect::DuckDb {
        out.push_str("-- duckbridge prelude: make DuckDB behave like SQL Server for this script\n");
        for p in duckdb_prelude_for(mode) { out.push_str(p); out.push_str(";\n"); }
        if opts.mssql_math {
            for m in crate::mathcompat::macros() { out.push_str(&m); out.push_str(";\n"); }
        }
    }
    let stmts = tokens::statements(&toks);
    if src == Dialect::Tsql {
        // column types declared in this script (CREATE TABLE / DECLARE @t TABLE) feed exact-arithmetic and CI decisions
        for st in &stmts {
            for (k, v) in semantics::declared_types(st) { ctx.types.entry(k).or_insert(v); }
        }
    }
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
        ctx.begin_statement(st.first());
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
    let diags = ctx.take_warnings();
    if opts.strict && !diags.is_empty() {
        return Err(TranslateError::Strict(diags));
    }
    Ok(Translated::new(out.trim_end().to_string(), diags))
}

/// Translate a single scalar/boolean expression (used for defaults and CHECK constraints).
pub fn translate_expr(expr: &str, src: Dialect, dst: Dialect) -> Result<Translated, TranslateError> {
    if src == dst {
        return Ok(Translated::default().with_sql(expr));
    }
    let toks = tokenize(expr, src.bracket_idents());
    let mut ctx = engine::Ctx::new(HashMap::new());
    let out = rewrite_expr(&toks, src, dst, &mut ctx)?;
    Ok(Translated::new(tokens::join(&out).trim().to_string(), ctx.take_warnings()))
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
