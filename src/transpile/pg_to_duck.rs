//! PostgreSQL -> DuckDB SQL rules (DuckDB accepts most PostgreSQL syntax; this covers the gaps).
use super::engine::*;
use super::tokens::*;
use super::types::*;

pub struct R;

pub fn statement(st: &[Tok], ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    let st = trim(st);
    let v = Stmt::new(st);
    if v.is_empty() { return vec![]; }
    let first = v.kw(0);
    ctx.in_ddl = matches!(first.as_str(), "CREATE" | "ALTER");
    let body = match first.as_str() {
        "SET" | "RESET" | "VACUUM" | "ANALYZE" | "CLUSTER" | "REINDEX" | "LISTEN" | "NOTIFY" | "GRANT" | "REVOKE" | "COMMENT" | "DO" => {
            ctx.warn(format!("PostgreSQL-specific statement dropped: {first}")); t(&format!("-- (dropped) {}", join(st).trim().replace('\n', " ")))
        }
        _ => rewrite(st, &R, ctx),
    };
    ctx.in_ddl = false;
    let mut out = std::mem::take(&mut ctx.pre_statements);
    out.push(body);
    out
}

pub fn expression(toks: &[Tok], ctx: &mut Ctx) -> Vec<Tok> { rewrite(trim(toks), &R, ctx) }

impl Rules for R {
    fn func(&self, name: &str, raw: &[Vec<Tok>], a: &[Vec<Tok>], ctx: &mut Ctx) -> Option<Vec<Tok>> {
        let arg = |i: usize| -> &[Tok] { a.get(i).map(|v| v.as_slice()).unwrap_or(&[]) };
        let joined = || -> Vec<Tok> { let mut v = vec![]; for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.extend(x.iter().cloned()); } v };
        let call = |f: &str| -> Vec<Tok> { let mut v = t(f); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v };
        if ctx.in_ddl { if let Some((ty, w)) = pg_type_to_duck(name, raw) { if let Some(w) = w { ctx.warn(w); } return Some(ty); } }
        Some(match name {
            "TO_CHAR" => match as_str_lit(arg(1)) { Some(f) => fmt(&format!("strftime({{}}, '{}')", pg_to_strftime(&f)), &[arg(0)]), None => return None },
            "TO_DATE" => match as_str_lit(arg(1)) { Some(f) => fmt(&format!("CAST(strptime({{}}, '{}') AS DATE)", pg_to_strftime(&f)), &[arg(0)]), None => return None },
            "TO_TIMESTAMP" if a.len() == 2 => match as_str_lit(arg(1)) { Some(f) => fmt(&format!("strptime({{}}, '{}')", pg_to_strftime(&f)), &[arg(0)]), None => return None },
            "STRING_TO_ARRAY" => call("string_split"),
            "ARRAY_TO_STRING" => call("array_to_string"),
            "ARRAY_AGG" => call("list"),
            "CARDINALITY" | "ARRAY_LENGTH" => fmt("len({})", &[arg(0)]),
            "ARRAY_UPPER" => fmt("len({})", &[arg(0)]),
            "REGEXP_MATCH" => fmt("regexp_extract_all({}, {})", &[arg(0), arg(1)]),
            "PG_TYPEOF" => call("typeof"),
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => t("uuid()"),
            "NOW" | "CLOCK_TIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" => t("now()"),
            "AGE" if a.len() == 2 => fmt("age({}, {})", &[arg(0), arg(1)]),
            "JUSTIFY_DAYS" | "JUSTIFY_HOURS" | "JUSTIFY_INTERVAL" => return Some(arg(0).to_vec()),
            "CURRENT_SETTING" => { ctx.warn("current_setting() has no DuckDB equivalent; use getvariable()"); return None; }
            "SET_CONFIG" => { ctx.warn("set_config() has no DuckDB equivalent; use SET VARIABLE"); return None; }
            "TO_JSONB" | "TO_JSON" | "ROW_TO_JSON" => call("to_json"),
            "JSONB_BUILD_OBJECT" | "JSON_BUILD_OBJECT" => call("json_object"),
            "JSONB_BUILD_ARRAY" | "JSON_BUILD_ARRAY" => call("json_array"),
            "JSONB_AGG" | "JSON_AGG" => call("json_group_array"),
            "JSONB_ARRAY_LENGTH" | "JSON_ARRAY_LENGTH" => call("json_array_length"),
            "JSONB_PATH_QUERY_FIRST" => call("json_extract"),
            "CONVERT_FROM" => fmt("CAST({} AS VARCHAR)", &[arg(0)]),
            "ENCODE" if as_str_lit(arg(1)).map(|s| s.eq_ignore_ascii_case("hex")).unwrap_or(false) => fmt("hex({})", &[arg(0)]),
            "DECODE" if as_str_lit(arg(1)).map(|s| s.eq_ignore_ascii_case("hex")).unwrap_or(false) => fmt("unhex({})", &[arg(0)]),
            "SPLIT_PART" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "INITCAP" | "TRANSLATE" | "DATE_TRUNC" | "DATE_PART" | "EXTRACT" | "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "GENERATE_SERIES" | "UNNEST" | "STRPOS" | "POSITION" | "LENGTH" | "SUBSTRING" | "SUBSTR" | "MD5" | "RANDOM" | "FLOOR" | "CEIL" | "ROUND" | "ABS" | "POWER" | "LN" | "LOG" | "EXP" | "SQRT" => return None,
            _ => return None,
        })
    }

    fn token(&self, tok: &Tok, prev: Option<&Tok>, next: Option<&Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        match tok.kind {
            Kind::Ident => {
                let u = tok.text.to_uppercase();
                let next_is_dot = next.map(|n| n.is_punct(".")).unwrap_or(false);
                if next_is_dot { if let Some(m) = ctx.schema_map.get(&tok.text) { return vec![Tok::ident(m)]; } }
                let after_as = prev.map(|p| p.is_kw("AS")).unwrap_or(false);
                let after_cast = prev.map(|p| p.is_op("::")).unwrap_or(false);
                let next_is_paren = next.map(|n| n.is_punct("(")).unwrap_or(false);
                if (ctx.in_ddl || after_as || after_cast) && !next_is_paren {
                    if let Some((ty, w)) = pg_type_to_duck(&u, &[]) { if let Some(w) = w { ctx.warn(w); } return ty; }
                    if u == "CHARACTER" || u == "VARYING" { return if u == "CHARACTER" { t("VARCHAR") } else { vec![] }; }
                }
                match u.as_str() {
                    "LATERAL" => vec![tok.clone()],
                    _ => vec![tok.clone()],
                }
            }
            Kind::Var if tok.text.starts_with('$') => vec![tok.clone()],
            _ => vec![tok.clone()],
        }
    }

    fn group(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        let mut v = toks;
        // ARRAY[a, b] -> [a, b]
        let mut i = 0;
        while i < v.len() {
            if v[i].is_kw("ARRAY") && next_sig(&v, i + 1).map(|n| v[n].is_punct("[")).unwrap_or(false) { v.remove(i); continue; }
            i += 1;
        }
        // FOR UPDATE / FOR SHARE [NOWAIT | SKIP LOCKED] -> dropped
        let mut i = 0;
        while i < v.len() {
            if v[i].is_kw("FOR") {
                if let Some(n) = next_sig(&v, i + 1) {
                    if v[n].is_any_kw(&["UPDATE", "SHARE", "NO", "KEY"]) {
                        let mut e = n + 1;
                        while let Some(x) = next_sig(&v, e) { if v[x].is_any_kw(&["UPDATE", "SHARE", "KEY", "NOWAIT", "SKIP", "LOCKED", "OF"]) || (v[x].kind == Kind::Ident && v[x - 1].is_kw("OF")) { e = x + 1; } else { break; } }
                        ctx.warn("FOR UPDATE/SHARE row locking dropped");
                        while i > 0 && v[i - 1].is_trivia() { i -= 1; }
                        v.drain(i..e);
                        continue;
                    }
                }
            }
            i += 1;
        }
        v
    }

    fn select_segment(&self, toks: Vec<Tok>, _ctx: &mut Ctx) -> Vec<Tok> { toks }
}

fn pg_to_strftime(f: &str) -> String {
    let mut s = f.to_string();
    for (a, b) in [("YYYY", "%Y"), ("YY", "%y"), ("MM", "%m"), ("DD", "%d"), ("HH24", "%H"), ("HH12", "%I"), ("HH", "%I"), ("MI", "%M"), ("SS", "%S"), ("US", "%f"), ("MS", "%g"), ("FMMonth", "%B"), ("Month", "%B"), ("Mon", "%b"), ("FMDay", "%A"), ("Day", "%A"), ("Dy", "%a"), ("DDD", "%j"), ("AM", "%p"), ("PM", "%p"), ("TZ", "%Z"), ("OF", "%z")] {
        s = s.replace(a, b);
    }
    s.replace('\'', "''")
}
