//! Semantic checks for T-SQL -> DuckDB (requirement R7): SQL Server numeric typing for exact division,
//! case-insensitive / trailing-space string comparison, and the column types declared in a script.
//!
//! Everything here works on the token stream produced by the T-SQL -> DuckDB rules (DuckDB type names,
//! `getvariable('x')` for variables), so it can run as part of `Rules::group` or as a statement post-pass.
use super::codes;
use super::engine::*;
use super::tokens::*;
use super::NocaseMode;
use crate::mathcompat::{add_type, div_type, literal_type, mul_type};

/// SQL Server type class of an expression, as far as it can be inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ty {
    /// Integer type; `p` is the precision it gets when promoted to numeric (int 10, bigint 19, literal 7 -> 1).
    Int { p: u32 },
    Dec(u32, u32),
    /// Decimal of unknown precision/scale (money, or a decimal literal combined with an unknown operand).
    DecAny,
    Float,
    Str,
    /// Date/time/binary/boolean...
    Other,
    Unknown,
}

impl Ty {
    pub fn is_numeric(self) -> bool { matches!(self, Ty::Int { .. } | Ty::Dec(..) | Ty::DecAny | Ty::Float) }
    fn as_dec(self) -> Option<(u32, u32)> {
        match self { Ty::Int { p } => Some((p, 0)), Ty::Dec(p, s) => Some((p, s)), _ => None }
    }
}

/// Parse a SQL Server (or DuckDB) type name such as `decimal(18,4)`, `int`, `nvarchar(20)`.
pub fn parse_type(spec: &str) -> Ty {
    let s: String = spec.to_lowercase().chars().filter(|c| !c.is_whitespace()).collect();
    let (name, args) = match s.find('(') { Some(i) => (&s[..i], s[i + 1..].trim_end_matches(')')), None => (s.as_str(), "") };
    let nums: Vec<u32> = args.split(',').filter_map(|a| a.parse().ok()).collect();
    match name {
        "int" | "integer" | "int4" | "int32" => Ty::Int { p: 10 },
        "bigint" | "int8" | "int64" => Ty::Int { p: 19 },
        "smallint" | "int2" | "int16" => Ty::Int { p: 5 },
        "tinyint" | "utinyint" => Ty::Int { p: 3 },
        "decimal" | "numeric" | "dec" => match nums.as_slice() {
            [p, s] => Ty::Dec((*p).clamp(1, 38), (*s).min(*p)),
            [p] => Ty::Dec((*p).clamp(1, 38), 0),
            _ => Ty::Dec(18, 0),
        },
        "money" | "smallmoney" => Ty::DecAny,
        "float" | "real" | "double" | "float8" | "float4" | "doubleprecision" => Ty::Float,
        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext" | "sysname" | "string" | "bpchar" => Ty::Str,
        "" => Ty::Unknown,
        _ => Ty::Other,
    }
}

fn combine_dec(a: Ty, b: Ty, f: fn(u32, u32, u32, u32) -> (u32, u32), int_result: Ty) -> Ty {
    use Ty::*;
    match (a, b) {
        (Float, _) | (_, Float) => Float,
        (Int { .. }, Int { .. }) => int_result,
        (Dec(..) | Int { .. }, Dec(..) | Int { .. }) => {
            let ((p1, s1), (p2, s2)) = (a.as_dec().unwrap(), b.as_dec().unwrap());
            let (p, s) = f(p1, s1, p2, s2);
            Dec(p, s)
        }
        (DecAny | Dec(..), _) | (_, DecAny | Dec(..)) => DecAny,
        _ => Unknown,
    }
}

fn int_of(a: Ty, b: Ty) -> Ty {
    let p = |t: Ty| if let Ty::Int { p } = t { p } else { 10 };
    Ty::Int { p: if p(a) >= 19 || p(b) >= 19 { 19 } else { 10 } }
}

fn is_operand_end(t: &Tok) -> bool {
    match t.kind {
        Kind::Num | Kind::Str | Kind::QIdent | Kind::Var | Kind::Temp => true,
        Kind::Ident => !t.is_any_kw(&["AND", "OR", "NOT", "WHEN", "THEN", "ELSE", "SELECT", "WHERE", "ON", "AS", "IN", "IS", "LIKE", "ILIKE", "BETWEEN", "CASE", "RETURN", "BY", "SET", "HAVING"]),
        Kind::Punct => t.is_punct(")") || t.is_punct("]"),
        _ => false,
    }
}

/// Infer the SQL Server type of an expression (DuckDB-translated tokens).
pub fn infer(toks: &[Tok], ctx: &Ctx) -> Ty {
    let v = trim(toks);
    if v.is_empty() { return Ty::Unknown; }
    // whole expression in parentheses
    if v[0].is_punct("(") && match_paren(v, 0) == v.len() - 1 { return infer(&v[1..v.len() - 1], ctx); }
    // binary operators, lowest precedence first; the last one at depth 0 is the root (left associative)
    for ops in [&["||"][..], &["+", "-"][..], &["*", "/", "%"][..]] {
        let mut depth = 0i32;
        let mut root = None;
        for (i, t) in v.iter().enumerate() {
            if t.is_punct("(") { depth += 1; } else if t.is_punct(")") { depth -= 1; }
            if depth == 0 && t.kind == Kind::Op && ops.contains(&t.text.as_str()) {
                if let Some(p) = prev_sig(v, i) { if is_operand_end(&v[p]) { root = Some(i); } }
            }
        }
        if let Some(i) = root {
            if ops[0] == "||" { return Ty::Str; }
            let (a, b) = (infer(&v[..i], ctx), infer(&v[i + 1..], ctx));
            return match v[i].text.as_str() {
                "+" | "-" => {
                    if a == Ty::Str || b == Ty::Str { return Ty::Str; }
                    let ir = int_of(a, b);
                    combine_dec(a, b, add_type, ir)
                }
                "*" => { let ir = int_of(a, b); combine_dec(a, b, mul_type, ir) }
                "/" => { let ir = int_of(a, b); combine_dec(a, b, div_type, ir) }
                _ => match (a, b) { (Ty::Int { .. }, Ty::Int { .. }) => int_of(a, b), _ => Ty::Unknown },
            };
        }
    }
    // unary sign
    if v[0].is_op("-") || v[0].is_op("+") { return infer(&v[1..], ctx); }
    let first = &v[0];
    match first.kind {
        Kind::Num => {
            let t = &first.text;
            if t.contains(['e', 'E']) { return Ty::Float; }
            match literal_type(t) {
                Some((p, 0)) => { if t.trim_start_matches('0').len() <= 9 || t.parse::<i64>().map(|x| x <= i32::MAX as i64).unwrap_or(false) { Ty::Int { p } } else { Ty::Dec(p.min(38), 0) } }
                Some((p, s)) => Ty::Dec(p.min(38), s),
                None => Ty::Unknown,
            }
        }
        Kind::Str => Ty::Str,
        Kind::Var => ctx.var_types.get(&first.text.trim_start_matches('@').to_lowercase()).map(|t| parse_type(t)).unwrap_or(Ty::Unknown),
        Kind::Ident | Kind::QIdent => {
            // function call?
            if let Some(n) = next_sig(v, 1) {
                if first.kind == Kind::Ident && v[n].is_punct("(") && match_paren(v, n) == v.len() - 1 {
                    return infer_call(&first.text.to_uppercase(), &v[n + 1..v.len() - 1], ctx);
                }
            }
            if first.is_any_kw(&["NULL", "TRUE", "FALSE", "CASE"]) { return Ty::Unknown; }
            // dotted column chain
            let parts: Vec<String> = v.iter().filter(|t| !t.is_trivia() && !t.is_punct(".")).map(|t| t.value().to_lowercase()).collect();
            if v.iter().filter(|t| !t.is_trivia()).any(|t| !matches!(t.kind, Kind::Ident | Kind::QIdent) && !t.is_punct(".")) { return Ty::Unknown; }
            column_type(&parts, ctx)
        }
        _ => Ty::Unknown,
    }
}

/// Type of a (possibly qualified) column from `Options.types` / the script's CREATE TABLE statements.
pub fn column_type(parts: &[String], ctx: &Ctx) -> Ty {
    if parts.is_empty() { return Ty::Unknown; }
    let n = parts.len();
    if n >= 2 {
        if let Some(t) = ctx.types.get(&format!("{}.{}", parts[n - 2], parts[n - 1])) { return parse_type(t); }
    }
    ctx.types.get(&parts[n - 1]).map(|t| parse_type(t)).unwrap_or(Ty::Unknown)
}

fn infer_call(name: &str, inner: &[Tok], ctx: &Ctx) -> Ty {
    let args = split_top(inner, ",");
    let a0 = || args.first().map(|a| infer(a, ctx)).unwrap_or(Ty::Unknown);
    match name {
        "CAST" | "TRY_CAST" => match find_top_kw(inner, "AS", 0) { Some(k) => parse_type(&join(trim(&inner[k + 1..]))), None => Ty::Unknown },
        "ROUND" | "ABS" | "TRUNC" | "COALESCE" | "NULLIF" | "IFNULL" | "MIN" | "MAX" | "MSSQL_ROUND" | "MSSQL_ROUND_TRUNC" | "SIGN" => a0(),
        "FLOOR" | "CEILING" | "CEIL" => match a0() { Ty::Dec(p, _) => Ty::Dec(p, 0), t => t },
        "SUM" => match a0() { Ty::Dec(_, s) => Ty::Dec(38, s), Ty::Int { p } => Ty::Int { p: p.max(10) }, t => t },
        "AVG" => match a0() { Ty::Dec(_, s) => Ty::Dec(38, s.max(6)), t => t },
        "COUNT" | "LENGTH" | "STRPOS" | "DATE_DIFF" | "DATE_PART" | "MSSQL_TRY_INT" | "ORD" | "STRLEN" => Ty::Int { p: 10 },
        "GETVARIABLE" => match args.first().and_then(|a| as_str_lit(a)) { Some(n) => ctx.var_types.get(&n.to_lowercase()).map(|t| parse_type(t)).unwrap_or(Ty::Unknown), None => Ty::Unknown },
        "LOWER" | "UPPER" | "LTRIM" | "RTRIM" | "TRIM" | "CONCAT" | "CONCAT_WS" | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "REPLACE" | "STRFTIME"
        | "REPEAT" | "REVERSE" | "LPAD" | "RPAD" | "CHR" | "STRING_AGG" | "MSSQL_FORMAT_PCT" | "FORMAT" | "TRANSLATE" | "MD5" | "SHA256" => Ty::Str,
        "POWER" | "SQRT" | "EXP" | "LN" | "LOG" | "LOG10" | "RANDOM" | "PI" | "SIN" | "COS" | "TAN" => Ty::Float,
        _ => Ty::Unknown,
    }
}

/// Column types declared by `CREATE TABLE t (...)` / `DECLARE @t TABLE (...)` in a T-SQL statement:
/// `(column, type)` and `(table.column, type)`, lower case.
pub fn declared_types(st: &[Tok]) -> Vec<(String, String)> {
    let v = Stmt::new(st);
    let is_create = v.kw(0) == "CREATE" && v.kw(1) == "TABLE";
    let is_decl = v.kw(0) == "DECLARE" && v.tok(1).map(|t| t.kind == Kind::Var).unwrap_or(false) && v.kw(2) == "TABLE";
    if !is_create && !is_decl { return vec![]; }
    let open = match st.iter().position(|t| t.is_punct("(")) { Some(o) => o, None => return vec![] };
    let table: String = st[..open].iter().filter(|t| matches!(t.kind, Kind::Ident | Kind::QIdent | Kind::Temp | Kind::Var) && !t.is_any_kw(&["CREATE", "TABLE", "DECLARE"]))
        .map(|t| t.value().trim_start_matches(['#', '@']).to_lowercase()).last().unwrap_or_default();
    let close = match_paren(st, open);
    let mut out = vec![];
    for def in split_top(&st[open + 1..close], ",") {
        let d = strip_trivia(&def);
        if d.len() < 2 || !matches!(d[0].kind, Kind::Ident | Kind::QIdent) { continue; }
        if d[0].kind == Kind::Ident && d[0].is_any_kw(&["CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "INDEX", "PERIOD"]) { continue; }
        if d[1].is_kw("AS") || !matches!(d[1].kind, Kind::Ident | Kind::QIdent) { continue; }
        let mut ty = d[1].value().to_lowercase();
        if d.len() > 2 && d[2].is_punct("(") {
            let c = match_paren(&d, 2);
            ty.push_str(&join(&d[2..=c]).to_lowercase());
        }
        let col = d[0].value().to_lowercase();
        if !table.is_empty() { out.push((format!("{table}.{col}"), ty.clone())); }
        out.push((col, ty));
    }
    out
}

// ------------------------------------------------------------------ exact division (W-DIV-DECIMAL)

/// Start of the left operand of the binary operator at `op` (a multiplicative chain: `a * 1.0` in `a * 1.0 / b`).
fn mul_chain_start(v: &[Tok], op: usize) -> usize {
    let mut l = atom_start(v, op);
    while let Some(p) = prev_sig(v, l) {
        if v[p].kind == Kind::Op && matches!(v[p].text.as_str(), "*" | "/" | "%") {
            if prev_sig(v, p).map(|q| is_operand_end(&v[q])).unwrap_or(false) { l = atom_start(v, p); continue; }
        }
        break;
    }
    l
}

/// Handle every top-level `/` of a rewritten group: emit `mssql_div` (mssql_math with known types) or W-DIV-DECIMAL.
pub fn divisions(v: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
    let mut v = v;
    let mut i = 0;
    let mut depth = 0i32;
    while i < v.len() {
        if v[i].is_punct("(") { depth += 1; } else if v[i].is_punct(")") { depth -= 1; }
        if depth != 0 || !v[i].is_op("/") { i += 1; continue; }
        let l = mul_chain_start(&v, i);
        let r = atom_end(&v, i + 1);
        if l >= i || r <= i + 1 { i += 1; continue; }
        let (lt, rt) = (infer(&v[l..i], ctx), infer(&v[i + 1..r], ctx));
        let is_dec = |t: Ty| matches!(t, Ty::Dec(..) | Ty::DecAny);
        if !(is_dec(lt) || is_dec(rt)) || lt == Ty::Float || rt == Ty::Float { i += 1; continue; }
        let expr = join(trim(&v[l..r])).split_whitespace().collect::<Vec<_>>().join(" ");
        let op_tok = v[i].clone();
        match (lt.as_dec(), rt.as_dec()) {
            (Some((p1, s1)), Some((p2, s2))) => {
                let (p, s) = div_type(p1, s1, p2, s2);
                if ctx.mssql_math {
                    let left: Vec<Tok> = trim(&v[l..i]).to_vec();
                    let right: Vec<Tok> = trim(&v[i + 1..r]).to_vec();
                    let rep = fmt(&format!("CAST(mssql_div(CAST({{}} AS DECIMAL({p1},{s1})), CAST({{}} AS DECIMAL({p2},{s2})), {p}, {s}) AS DECIMAL({p},{s}))"), &[&left, &right]);
                    let n = rep.len();
                    v.splice(l..r, rep);
                    i = l + n;
                    continue;
                }
                ctx.warn_at(codes::DIV_DECIMAL, &op_tok, format!("{expr}: SQL Server computes numeric({p},{s}) truncated to {s} decimals, DuckDB divides in DOUBLE (use --mssql-math for exact results)"));
            }
            _ => {
                let why = if ctx.mssql_math { "mssql_div needs both operand types (pass --types col=type,...)" } else { "pass --types and --mssql-math for exact results" };
                ctx.warn_at(codes::DIV_DECIMAL, &op_tok, format!("{expr}: decimal division follows SQL Server numeric precision/scale rules there and DOUBLE in DuckDB; {why}"));
            }
        }
        i += 1;
    }
    v
}

// ------------------------------------------------------------------ string comparison (W-CI-COMPARE / W-TRAILING-SPACE)

const PRED_KW: &[&str] = &["WHERE", "ON", "HAVING", "WHEN", "QUALIFY"];
const VALUE_KW: &[&str] = &["SELECT", "SET", "THEN", "ELSE", "BY", "FROM", "VALUES", "RETURNING", "LIMIT", "OFFSET", "AS", "INTO", "USING", "JOIN", "UPDATE", "OUTPUT"];
const CLAUSE_END: &[&str] = &["HAVING", "ORDER", "LIMIT", "OFFSET", "QUALIFY", "WINDOW", "UNION", "EXCEPT", "INTERSECT", "FROM", "WHERE", "INTO"];

fn has_letter(s: &str) -> bool { s.chars().any(|c| c.is_alphabetic()) }

fn is_arith(t: &Tok) -> bool { t.kind == Kind::Op && matches!(t.text.as_str(), "+" | "-" | "*" | "/" | "%" | "||" | "&" | "|" | "^") }

fn lower_rtrim(x: &[Tok]) -> Vec<Tok> { fmt("lower(rtrim({}, ' '))", &[x]) }
fn lower_lit(s: &str) -> Vec<Tok> { let mut v = t("lower("); v.push(Tok::str_lit(s.trim_end_matches(' '))); v.push(Tok::punct(")")); v }

/// Post-pass over a translated DML/query statement: in `Rewrite` mode, `col = 'Lit'`, `col <> 'Lit'` and
/// `col IN ('a', 'b')` become `lower(rtrim(col, ' ')) = lower('Lit')` (only when a literal contains a letter);
/// other string comparisons are reported (W-CI-COMPARE once per statement, W-TRAILING-SPACE).
pub fn string_compare(v: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
    level(&v, false, ctx)
}

fn level(v: &[Tok], pred0: bool, ctx: &mut Ctx) -> Vec<Tok> {
    // pass 1: recurse into parentheses, track predicate context per output token
    let mut w: Vec<Tok> = Vec::with_capacity(v.len());
    let mut flags: Vec<bool> = Vec::with_capacity(v.len());
    let mut pred = pred0;
    let mut cases: Vec<bool> = vec![];
    let mut i = 0;
    while i < v.len() {
        let tk = &v[i];
        if tk.is_punct("(") {
            let close = match_paren(v, i);
            let is_call = prev_sig(&w, w.len()).map(|p| w[p].kind == Kind::Ident && !w[p].is_any_kw(NOT_FUNCS)).unwrap_or(false);
            let inner = level(&v[i + 1..close.max(i + 1)], pred && !is_call, ctx);
            w.push(tk.clone()); flags.push(pred);
            for x in inner { w.push(x); flags.push(false); }
            if close > i && close < v.len() { w.push(v[close].clone()); flags.push(pred); }
            i = close + 1;
            continue;
        }
        if tk.kind == Kind::Ident {
            if tk.is_kw("CASE") { cases.push(pred); }
            else if tk.is_kw("END") { if let Some(p) = cases.pop() { pred = p; } }
            else if tk.is_any_kw(PRED_KW) { pred = true; }
            else if tk.is_any_kw(VALUE_KW) { pred = false; }
        }
        w.push(tk.clone());
        flags.push(pred);
        i += 1;
    }
    // pass 2: comparisons at this level
    let mode = ctx.nocase_mode;
    let mut reps: Vec<(usize, usize, Vec<Tok>)> = vec![];
    let mut depth = 0i32;
    let mut i = 0;
    while i < w.len() {
        let tk = w[i].clone();
        if tk.is_punct("(") { depth += 1; } else if tk.is_punct(")") { depth -= 1; }
        if depth != 0 || reps.last().map(|r| i < r.1).unwrap_or(false) { i += 1; continue; }
        if tk.kind == Kind::Op && matches!(tk.text.as_str(), "=" | "<>" | "!=" | "<" | ">" | "<=" | ">=") && flags[i] {
            if let Some(rep) = comparison(&w, i, ctx) { reps.push(rep); }
        } else if tk.is_kw("IN") && flags[i] {
            if let Some(rep) = in_list(&w, i, ctx) { reps.push(rep); }
        } else if tk.is_kw("CASE") && mode == NocaseMode::Rewrite {
            // simple CASE x WHEN 'Lit' compares case-sensitively
            if let Some(n) = next_sig(&w, i + 1) {
                if !w[n].is_kw("WHEN") {
                    if let Some(wh) = find_top_kw(&w, "WHEN", n) {
                        if let Some(l) = next_sig(&w, wh + 1) {
                            if w[l].kind == Kind::Str && has_letter(&w[l].value()) {
                                ctx.warn_stmt_once(codes::CI_COMPARE, &w[l], "simple CASE ... WHEN 'literal' compares case-sensitively in DuckDB; write CASE WHEN lower(x) = lower('...')");
                            }
                        }
                    }
                }
            }
        } else if tk.is_kw("GROUP") && mode == NocaseMode::Rewrite {
            if let Some(b) = next_sig(&w, i + 1) {
                if w[b].is_kw("BY") {
                    let end = CLAUSE_END.iter().filter_map(|k| find_top_kw(&w, k, b + 1)).min().unwrap_or(w.len());
                    if split_top(&w[b + 1..end], ",").iter().any(|it| infer(it, ctx) == Ty::Str) {
                        ctx.warn_stmt_once(codes::CI_COMPARE, &tk, "GROUP BY on a string column groups case-sensitively in DuckDB (SQL Server's CI collation merges 'a' and 'A'); not rewritten");
                    }
                }
            }
        } else if tk.is_kw("DISTINCT") && mode == NocaseMode::Rewrite && prev_sig(&w, i).map(|p| w[p].is_kw("SELECT")).unwrap_or(false) {
            let end = find_top_kw(&w, "FROM", i + 1).unwrap_or(w.len());
            if split_top(&w[i + 1..end], ",").iter().any(|it| infer(it, ctx) == Ty::Str) {
                ctx.warn_stmt_once(codes::CI_COMPARE, &tk, "SELECT DISTINCT on a string column is case-sensitive in DuckDB; not rewritten");
            }
        }
        i += 1;
    }
    if reps.is_empty() { return w; }
    let mut out = Vec::with_capacity(w.len() + reps.len() * 8);
    let mut last = 0;
    for (a, b, rep) in reps {
        out.extend(w[last..a].iter().cloned());
        out.extend(rep);
        last = b;
    }
    out.extend(w[last..].iter().cloned());
    out
}

/// `lhs op rhs` at `i`. Returns a replacement span when it rewrites.
fn comparison(w: &[Tok], i: usize, ctx: &mut Ctx) -> Option<(usize, usize, Vec<Tok>)> {
    let mode = ctx.nocase_mode;
    let op = w[i].clone();
    let l = atom_start(w, i);
    let r = atom_end(w, i + 1);
    if l >= i || r <= i + 1 { return None; }
    let (lhs, rhs) = (trim(&w[l..i]).to_vec(), trim(&w[i + 1..r]).to_vec());
    let complex = prev_sig(w, l).map(|p| is_arith(&w[p])).unwrap_or(false) || next_sig(w, r).map(|n| is_arith(&w[n])).unwrap_or(false);
    let collate = next_sig(w, r).map(|n| w[n].is_kw("COLLATE")).unwrap_or(false);
    let equality = matches!(op.text.as_str(), "=" | "<>" | "!=");
    // `CASE ... END = 'x'`, `NULL = x`: the operand is not a simple atom
    let complex = complex || [&lhs, &rhs].iter().any(|s| s.first().map(|t| t.kind == Kind::Ident && t.is_any_kw(NOT_FUNCS)).unwrap_or(true));
    let (ll, rl) = (as_str_lit(&lhs), as_str_lit(&rhs));
    match (ll, rl) {
        (Some(a), Some(b)) => {
            // `x || 'a' = 'ba'`: a literal operand of a larger string expression
            if complex && (has_letter(&a) || has_letter(&b)) && equality {
                if mode == NocaseMode::Rewrite { ctx.warn_stmt_once(codes::CI_COMPARE, &op, "string comparison inside a larger expression is case-sensitive in DuckDB; not rewritten"); }
                ctx.warn_stmt_once(codes::TRAILING_SPACE, &op, "string equality: SQL Server ignores trailing spaces, DuckDB does not");
            }
            None
        }
        (None, None) => {
            let (a, b) = (infer(&lhs, ctx), infer(&rhs, ctx));
            if (a == Ty::Str || b == Ty::Str) && !a.is_numeric() && !b.is_numeric() {
                if mode == NocaseMode::Rewrite && !collate {
                    ctx.warn_stmt_once(codes::CI_COMPARE, &op, "comparison between string expressions is case-sensitive in DuckDB; not rewritten (only column-to-literal comparisons are)");
                }
                if equality { ctx.warn_stmt_once(codes::TRAILING_SPACE, &op, "string equality: SQL Server ignores trailing spaces, DuckDB does not"); }
            }
            None
        }
        (lit_l, lit_r) => {
            let lit_left = lit_l.is_some();
            let lit = lit_l.or(lit_r).unwrap();
            let other = if lit_left { &rhs } else { &lhs };
            let ot = infer(other, ctx);
            if ot.is_numeric() || ot == Ty::Other { return None; }
            let letter = has_letter(&lit);
            if !equality {
                if mode == NocaseMode::Rewrite && letter && !collate {
                    ctx.warn_stmt_once(codes::CI_COMPARE, &op, format!("ordering comparison {} with a string literal is case-sensitive in DuckDB; not rewritten", op.text));
                }
                return None;
            }
            let rewritable = mode == NocaseMode::Rewrite && letter && !collate && !complex;
            if rewritable {
                let rep = if lit_left {
                    let mut v = lower_lit(&lit); v.push(Tok::ws()); v.push(op.clone()); v.push(Tok::ws()); v.extend(lower_rtrim(other)); v
                } else {
                    let mut v = lower_rtrim(other); v.push(Tok::ws()); v.push(op.clone()); v.push(Tok::ws()); v.extend(lower_lit(&lit)); v
                };
                return Some((l, r, rep));
            }
            if mode == NocaseMode::Rewrite && letter && complex {
                ctx.warn_stmt_once(codes::CI_COMPARE, &op, "string comparison inside a larger expression is case-sensitive in DuckDB; not rewritten");
            }
            if letter || ot == Ty::Str {
                ctx.warn_stmt_once(codes::TRAILING_SPACE, &op, "string equality: SQL Server ignores trailing spaces, DuckDB does not");
            }
            None
        }
    }
}

/// `lhs [NOT] IN (...)` at `i`.
fn in_list(w: &[Tok], i: usize, ctx: &mut Ctx) -> Option<(usize, usize, Vec<Tok>)> {
    let mode = ctx.nocase_mode;
    let open = next_sig(w, i + 1).filter(|&o| w[o].is_punct("("))?;
    let close = match_paren(w, open);
    let not = prev_sig(w, i).filter(|&p| w[p].is_kw("NOT"));
    let lhs_end = not.unwrap_or(i);
    let l = atom_start(w, lhs_end);
    if l >= lhs_end { return None; }
    let lhs = trim(&w[l..lhs_end]).to_vec();
    let lt = infer(&lhs, ctx);
    if lt.is_numeric() || lt == Ty::Other { return None; }
    let inner = &w[open + 1..close];
    if contains_top_kw(inner, "SELECT") {
        if mode == NocaseMode::Rewrite && lt == Ty::Str { ctx.warn_stmt_once(codes::CI_COMPARE, &w[i], "IN (subquery) on a string column is case-sensitive in DuckDB; not rewritten"); }
        return None;
    }
    let items = split_top(inner, ",");
    let lits: Vec<Option<String>> = items.iter().map(|it| as_str_lit(it)).collect();
    let any_letter = lits.iter().flatten().any(|s| has_letter(s));
    if !any_letter { return None; }
    let complex = prev_sig(w, l).map(|p| is_arith(&w[p])).unwrap_or(false);
    if mode == NocaseMode::Rewrite && lits.iter().all(|x| x.is_some()) && !complex {
        let mut rep = lower_rtrim(&lhs);
        rep.push(Tok::ws());
        if not.is_some() { rep.extend(t("NOT ")); }
        rep.extend(t("IN ("));
        for (k, s) in lits.iter().flatten().enumerate() {
            if k > 0 { rep.extend(t(", ")); }
            rep.extend(lower_lit(s));
        }
        rep.push(Tok::punct(")"));
        return Some((l, close + 1, rep));
    }
    if mode == NocaseMode::Rewrite { ctx.warn_stmt_once(codes::CI_COMPARE, &w[i], "IN list mixing string literals and expressions is case-sensitive in DuckDB; not rewritten"); }
    ctx.warn_stmt_once(codes::TRAILING_SPACE, &w[i], "string equality (IN): SQL Server ignores trailing spaces, DuckDB does not");
    None
}
