//! Generic recursive rewriter. Dialect pairs plug in a `Rules` implementation.
use super::tokens::*;
use super::{codes, NocaseMode, Warning};
use std::collections::{HashMap, HashSet};

pub struct Ctx {
    pub schema_map: HashMap<String, String>,
    warnings: Vec<Warning>,
    seen: HashSet<(String, String)>,
    /// Codes already reported for the current statement (for once-per-statement warnings).
    stmt_codes: HashSet<&'static str>,
    /// Source position of the token being rewritten (0 when unknown).
    pub cur_line: usize,
    pub cur_col: usize,
    /// Names declared as temp tables / table variables in this script (unprefixed).
    pub temp_tables: HashSet<String>,
    /// Scalar variables declared in this script (without '@').
    pub variables: HashSet<String>,
    /// Extra statements to emit *before* the current one (e.g. CREATE SEQUENCE).
    pub pre_statements: Vec<Vec<Tok>>,
    /// True while rewriting DDL (CREATE TABLE / DECLARE / ALTER), where bare type names are expected.
    pub in_ddl: bool,
    /// SQL Server target version for T-SQL output: 2019, 2022 or 2025.
    pub target_version: u16,
    /// Assume SQL Server's case-insensitive default collation (T-SQL LIKE -> ILIKE).
    pub nocase: bool,
    /// How SQL Server's case-insensitive comparisons are reproduced in DuckDB.
    pub nocase_mode: NocaseMode,
    /// Rewrite DuckDB `/` as float division in T-SQL (DuckDB `/` is always floating point).
    pub float_division: bool,
    /// Emit the mathcompat macros (mssql_div, mssql_round_trunc, mssql_try_int, ...) for exact SQL Server arithmetic.
    pub mssql_math: bool,
    /// Lower-case column name (or `table.column`) -> SQL Server type, e.g. "decimal(18,4)".
    pub types: HashMap<String, String>,
    /// Declared scalar variable types (name without '@', lower case) -> SQL Server type.
    pub var_types: HashMap<String, String>,
}

impl Ctx {
    pub fn new(schema_map: HashMap<String, String>) -> Self {
        Self {
            schema_map, warnings: vec![], seen: HashSet::new(), stmt_codes: HashSet::new(), cur_line: 0, cur_col: 0,
            temp_tables: HashSet::new(), variables: HashSet::new(), pre_statements: vec![], in_ddl: false, target_version: 2019,
            nocase: true, nocase_mode: NocaseMode::Rewrite, float_division: true, mssql_math: false, types: HashMap::new(), var_types: HashMap::new(),
        }
    }
    /// Free-text warning (code W-GENERIC) at the current position.
    pub fn warn(&mut self, msg: impl Into<String>) {
        self.warn_code(codes::GENERIC, msg);
    }
    /// Coded warning at the current position.
    pub fn warn_code(&mut self, code: &'static str, msg: impl Into<String>) {
        let (l, c) = (self.cur_line, self.cur_col);
        self.push_warning(code, l, c, msg.into());
    }
    /// Coded warning at the position of `tok` (falls back to the current position for synthesised tokens).
    pub fn warn_at(&mut self, code: &'static str, tok: &Tok, msg: impl Into<String>) {
        let (l, c) = if tok.has_pos() { (tok.line, tok.col) } else { (self.cur_line, self.cur_col) };
        self.push_warning(code, l, c, msg.into());
    }
    /// Coded warning reported at most once per statement (per code).
    pub fn warn_stmt_once(&mut self, code: &'static str, tok: &Tok, msg: impl Into<String>) {
        if self.stmt_codes.insert(code) { self.warn_at(code, tok, msg); }
    }
    fn push_warning(&mut self, code: &'static str, line: usize, col: usize, message: String) {
        if self.seen.insert((code.to_string(), message.clone())) {
            self.warnings.push(Warning { code, line, col, message });
        }
    }
    /// Remember the source position of `tok` as the current position (ignored for synthesised tokens).
    pub fn set_pos(&mut self, tok: &Tok) {
        if tok.has_pos() { self.cur_line = tok.line; self.cur_col = tok.col; }
    }
    /// Start a new statement: resets the once-per-statement bookkeeping.
    pub fn begin_statement(&mut self, first: Option<&Tok>) {
        self.stmt_codes.clear();
        if let Some(t) = first { self.set_pos(t); }
    }
    pub fn take_warnings(&mut self) -> Vec<Warning> {
        self.seen.clear();
        std::mem::take(&mut self.warnings)
    }
}

/// Keywords that are never function names even when followed by '('.
pub const NOT_FUNCS: &[&str] = &[
    "IN", "EXISTS", "VALUES", "AND", "OR", "NOT", "ON", "WHERE", "FROM", "JOIN", "OVER", "AS", "WITH", "SELECT", "THEN",
    "ELSE", "WHEN", "BY", "HAVING", "SET", "INTO", "UNION", "ALL", "DISTINCT", "TOP", "BETWEEN", "LIKE", "ILIKE", "IS",
    "NULL", "CASE", "END", "ANY", "SOME", "INSERT", "UPDATE", "DELETE", "TABLE", "VIEW", "INDEX", "RETURNING", "LIMIT",
    "OFFSET", "USING", "LATERAL", "APPLY", "PARTITION", "FETCH", "NEXT", "ROWS", "ONLY", "BEGIN", "WHILE", "DECLARE",
    "PRINT", "EXEC", "EXECUTE", "RETURN", "ESCAPE", "COLLATE", "GROUP", "ORDER", "WINDOW", "QUALIFY", "SAMPLE", "CROSS",
    "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "NATURAL", "ASOF", "POSITIONAL", "SEMI", "ANTI", "DEFAULT", "CHECK",
    "REFERENCES", "PRIMARY", "KEY", "UNIQUE", "FOREIGN", "CONSTRAINT", "ARRAY", "STRUCT", "MAP", "ROW", "INTERVAL",
    "FILTER", "WITHIN", "EXCLUDE", "REPLACE", "TABLESAMPLE", "MERGE", "MATCHED", "OUTPUT", "OPTION", "PIVOT", "UNPIVOT",
    "EXCEPT", "INTERSECT", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMIT", "ROLLBACK", "TRANSACTION",
    "SCHEMA", "DATABASE", "SEQUENCE", "TYPE", "FUNCTION", "PROCEDURE", "PROC", "TRIGGER", "ADD", "COLUMN", "RENAME", "TO",
    "WHERE", "LOOP", "DO", "ELSEIF", "RAISE", "CAST_AS_NEVER",
];

pub trait Rules {
    /// Rewrite a function call. `args` are already rewritten. Return None to keep as-is (with rewritten args).
    fn func(&self, name: &str, raw_args: &[Vec<Tok>], args: &[Vec<Tok>], ctx: &mut Ctx) -> Option<Vec<Tok>>;
    /// Rewrite a single non-structural token (identifier, variable, literal, operator).
    fn token(&self, tok: &Tok, prev: Option<&Tok>, next: Option<&Tok>, ctx: &mut Ctx) -> Vec<Tok>;
    /// Post-process a flat group (tokens at one paren depth, with nested groups already rewritten).
    fn group(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok>;
    /// Whether this group is a SELECT segment that should get LIMIT/TOP handling.
    fn select_segment(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok>;
}

/// Rewrite an expression or statement body recursively.
pub fn rewrite(toks: &[Tok], rules: &dyn Rules, ctx: &mut Ctx) -> Vec<Tok> {
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len());
    let mut i = 0;
    while i < toks.len() {
        let t = &toks[i];
        if t.is_punct("(") {
            let close = match_paren(toks, i);
            let inner = &toks[i + 1..close.max(i + 1)];
            // function call?
            let fname_idx = prev_sig(&out, out.len());
            let mut is_func = fname_idx.map(|k| out[k].kind == Kind::Ident && !out[k].is_any_kw(NOT_FUNCS)).unwrap_or(false);
            if is_func {
                // `CREATE TABLE t (...)`, `INSERT INTO s.t (...)`, `REFERENCES t (...)`, `AS alias (c1, c2)`: not a call
                let mut k = fname_idx.unwrap();
                loop {
                    match prev_sig(&out, k) {
                        Some(p) if out[p].is_punct(".") => match prev_sig(&out, p) { Some(q) => { k = q; } None => break },
                        Some(p) => { if out[p].is_any_kw(&["TABLE", "INTO", "FROM", "JOIN", "UPDATE", "VIEW", "INDEX", "REFERENCES", "AS", "SEQUENCE", "TYPE", "SCHEMA", "LATERAL", "USING"]) { is_func = false; } break; }
                        None => break,
                    }
                }
            }
            if is_func {
                let k = fname_idx.unwrap();
                let name = out[k].text.clone();
                let name_tok = out[k].clone();
                // drop the name (and trailing trivia) from out; rules decide how to render the call
                out.truncate(k);
                let raw_args: Vec<Vec<Tok>> = if inner.iter().all(|x| x.is_trivia()) { vec![] } else { split_top(inner, ",") };
                let args: Vec<Vec<Tok>> = raw_args.iter().map(|a| rewrite_arg(a, rules, ctx)).collect();
                ctx.set_pos(&name_tok);
                match rules.func(&name.to_uppercase(), &raw_args, &args, ctx) {
                    Some(rep) => out.extend(rep),
                    None => {
                        out.push(Tok::ident(&name).at(name_tok.line, name_tok.col));
                        out.push(Tok::punct("("));
                        for (ai, a) in args.iter().enumerate() {
                            if ai > 0 { out.push(Tok::punct(",")); out.push(Tok::ws()); }
                            out.extend(a.iter().cloned());
                        }
                        out.push(Tok::punct(")"));
                    }
                }
            } else {
                let body = rewrite(inner, rules, ctx);
                let body = if contains_top_kw(&body, "SELECT") { rules.select_segment(body, ctx) } else { body };
                out.push(Tok::punct("("));
                out.extend(body);
                out.push(Tok::punct(")"));
            }
            i = close + 1;
            continue;
        }
        let prev = prev_sig(&out, out.len()).map(|k| out[k].clone());
        let next = next_sig(toks, i + 1).map(|k| toks[k].clone());
        ctx.set_pos(t);
        out.extend(rules.token(t, prev.as_ref(), next.as_ref(), ctx));
        i += 1;
    }
    let out = rules.group(out, ctx);
    if contains_top_kw(&out, "SELECT") { rules.select_segment(out, ctx) } else { out }
}

fn rewrite_arg(a: &[Tok], rules: &dyn Rules, ctx: &mut Ctx) -> Vec<Tok> {
    let t = trim(a);
    rewrite(t, rules, ctx)
}

pub fn contains_top_kw(toks: &[Tok], kw: &str) -> bool {
    let mut depth = 0i32;
    for t in toks {
        if t.is_punct("(") { depth += 1; } else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && t.is_kw(kw) { return true; }
    }
    false
}

pub fn find_top_kw(toks: &[Tok], kw: &str, from: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (i, t) in toks.iter().enumerate().skip(from) {
        if t.is_punct("(") { depth += 1; } else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && t.is_kw(kw) { return Some(i); }
    }
    None
}

/// All top-level positions of any of `kws`.
pub fn find_all_top(toks: &[Tok], kws: &[&str]) -> Vec<usize> {
    let mut depth = 0i32;
    let mut out = vec![];
    for (i, t) in toks.iter().enumerate() {
        if t.is_punct("(") { depth += 1; } else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && t.is_any_kw(kws) { out.push(i); }
    }
    out
}

/// Start index of the operand ("atom") that ends just before position `end` (exclusive):
/// a parenthesised group, a function call, a dotted identifier chain, a literal or a variable.
pub fn atom_start(toks: &[Tok], end: usize) -> usize {
    let mut j = match prev_sig(toks, end) { Some(j) => j, None => return end };
    loop {
        if toks[j].is_punct(")") {
            // walk back to matching '('
            let mut depth = 0i32;
            let mut k = j;
            loop {
                if toks[k].is_punct(")") { depth += 1; } else if toks[k].is_punct("(") { depth -= 1; if depth == 0 { break; } }
                if k == 0 { break; }
                k -= 1;
            }
            j = k;
            // function name before '('?
            if let Some(p) = prev_sig(toks, j) {
                if toks[p].kind == Kind::Ident && !toks[p].is_any_kw(NOT_FUNCS) { j = p; } else { return j; }
            } else { return j; }
        } else if matches!(toks[j].kind, Kind::Ident | Kind::QIdent | Kind::Num | Kind::Str | Kind::Var | Kind::Temp) {
            // fallthrough to dotted-chain check
        } else {
            return j + 1;
        }
        // dotted chain: a.b.c
        match prev_sig(toks, j) {
            Some(p) if toks[p].is_punct(".") => {
                match prev_sig(toks, p) { Some(q) => { j = q; continue; } None => return j }
            }
            _ => return j,
        }
    }
}

/// End index (exclusive) of the operand that starts at `start`.
pub fn atom_end(toks: &[Tok], start: usize) -> usize {
    let mut j = match next_sig(toks, start) { Some(j) => j, None => return start };
    // unary minus
    if toks[j].is_op("-") || toks[j].is_op("+") {
        j = match next_sig(toks, j + 1) { Some(k) => k, None => return j + 1 };
    }
    loop {
        if toks[j].kind == Kind::Ident && !toks[j].is_any_kw(NOT_FUNCS) {
            if let Some(n) = next_sig(toks, j + 1) {
                if toks[n].is_punct("(") { j = match_paren(toks, n); }
            }
        } else if toks[j].is_punct("(") {
            j = match_paren(toks, j);
        } else if !matches!(toks[j].kind, Kind::QIdent | Kind::Num | Kind::Str | Kind::Var | Kind::Temp) {
            return j;
        }
        match next_sig(toks, j + 1) {
            Some(n) if toks[n].is_punct(".") => match next_sig(toks, n + 1) { Some(k) => { j = k; continue; } None => return n + 1 },
            _ => return j + 1,
        }
    }
}

pub fn t(s: &str) -> Vec<Tok> {
    tokenize(s, false)
}

pub fn t_br(s: &str) -> Vec<Tok> {
    tokenize(s, true)
}

/// Build tokens: `pieces` alternates literal SQL text and argument token lists via `{}` placeholders.
pub fn fmt(template: &str, args: &[&[Tok]]) -> Vec<Tok> {
    let mut out = vec![];
    let mut ai = 0;
    for (i, part) in template.split("{}").enumerate() {
        if i > 0 {
            if let Some(a) = args.get(ai) { out.extend(a.iter().cloned()); }
            ai += 1;
        }
        if !part.is_empty() { out.extend(t(part)); }
    }
    out
}

pub fn upper_text(toks: &[Tok]) -> String {
    join(trim(toks)).to_uppercase()
}

/// Is this token list a single string literal? Return its value.
pub fn as_str_lit(toks: &[Tok]) -> Option<String> {
    let s = strip_trivia(toks);
    if s.len() == 1 && s[0].kind == Kind::Str { Some(s[0].value()) } else { None }
}

pub fn as_ident(toks: &[Tok]) -> Option<String> {
    let s = strip_trivia(toks);
    if s.len() == 1 && matches!(s[0].kind, Kind::Ident | Kind::QIdent) { Some(s[0].value()) } else { None }
}

pub fn as_num(toks: &[Tok]) -> Option<String> {
    let s = strip_trivia(toks);
    if s.len() == 1 && s[0].kind == Kind::Num { Some(s[0].text.clone()) } else { None }
}

/// Statement view: full token list plus the positions of its significant (non-trivia) tokens.
pub struct Stmt<'a> {
    pub toks: &'a [Tok],
    pub sig: Vec<usize>,
}

impl<'a> Stmt<'a> {
    pub fn new(toks: &'a [Tok]) -> Self {
        let sig = toks.iter().enumerate().filter(|(_, t)| !t.is_trivia()).map(|(i, _)| i).collect();
        Stmt { toks, sig }
    }
    pub fn len(&self) -> usize { self.sig.len() }
    pub fn is_empty(&self) -> bool { self.sig.is_empty() }
    /// Upper-cased text of the n-th significant token ("" if absent).
    pub fn kw(&self, n: usize) -> String { self.sig.get(n).map(|&i| self.toks[i].text.to_uppercase()).unwrap_or_default() }
    pub fn tok(&self, n: usize) -> Option<&'a Tok> { self.sig.get(n).map(|&i| &self.toks[i]) }
    /// Index into `toks` of the n-th significant token.
    pub fn pos(&self, n: usize) -> usize { self.sig.get(n).copied().unwrap_or(self.toks.len()) }
    /// Tokens from the n-th significant token to the end.
    pub fn from(&self, n: usize) -> &'a [Tok] { &self.toks[self.pos(n).min(self.toks.len())..] }
    pub fn all(&self) -> Vec<Tok> { self.toks.to_vec() }
}

/// End index (exclusive) of a dotted identifier chain starting at the first significant token at/after `start`
/// (no function-call parentheses are consumed, unlike `atom_end`).
pub fn ident_end(toks: &[Tok], start: usize) -> usize {
    let mut j = match next_sig(toks, start) { Some(j) => j, None => return start };
    loop {
        if !matches!(toks[j].kind, Kind::Ident | Kind::QIdent | Kind::Temp | Kind::Var) { return j; }
        match next_sig(toks, j + 1) {
            Some(n) if toks[n].is_punct(".") => match next_sig(toks, n + 1) { Some(k) => { j = k; } None => return n + 1 },
            _ => return j + 1,
        }
    }
}
