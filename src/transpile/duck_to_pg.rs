//! DuckDB SQL -> PostgreSQL rules (the dialects are close; mostly function and type renames).
use super::engine::*;
use super::tokens::*;
use super::types::*;

pub struct R;

pub fn statement(st: &[Tok], ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    let st = trim(st);
    let v = Stmt::new(st);
    if v.is_empty() { return vec![]; }
    let first = v.kw(0);
    let second = v.kw(1);
    let third = v.kw(2);
    ctx.in_ddl = false;
    let mut out = vec![];
    let body = match first.as_str() {
        "SET" if second == "VARIABLE" => { ctx.warn("SET VARIABLE has no PostgreSQL equivalent outside DO blocks; converted to a session setting"); let name = v.tok(2).map(|t| t.value()).unwrap_or_default(); let eq = st.iter().position(|t| t.is_op("=")).unwrap_or(v.pos(3)); let e = rewrite(trim(&st[eq + 1..]), &R, ctx); fmt(&format!("SELECT set_config('d2m.{name}', ({{}})::text, false)"), &[&e]) }
        "PRAGMA" | "INSTALL" | "LOAD" | "ATTACH" | "DETACH" | "USE" | "CHECKPOINT" | "FORCE" | "EXPORT" | "IMPORT" | "DESCRIBE" | "SUMMARIZE" | "SHOW" => { ctx.warn(format!("DuckDB-specific statement dropped: {first}")); t(&format!("-- (dropped) {}", join(st).trim().replace('\n', " "))) }
        "CREATE" if second == "OR" && third == "REPLACE" && v.kw(3) == "TABLE" => {
            let name_start = v.pos(4);
            let name_end = ident_end(st, name_start);
            let name = rewrite(trim(&st[name_start..name_end]), &R, ctx);
            out.push(fmt("DROP TABLE IF EXISTS {} CASCADE", &[&name]));
            let mut w: Vec<Tok> = t("CREATE ");
            w.extend(st[v.pos(3)..].iter().cloned());
            ctx.in_ddl = true; let r = rewrite(&w, &R, ctx); ctx.in_ddl = false; r
        }
        "CREATE" | "ALTER" => { ctx.in_ddl = true; let r = rewrite(st, &R, ctx); ctx.in_ddl = false; r }
        _ => rewrite(st, &R, ctx),
    };
    out.append(&mut ctx.pre_statements);
    out.push(body);
    out
}

pub fn expression(toks: &[Tok], ctx: &mut Ctx) -> Vec<Tok> { rewrite(trim(toks), &R, ctx) }

impl Rules for R {
    fn func(&self, name: &str, raw: &[Vec<Tok>], a: &[Vec<Tok>], ctx: &mut Ctx) -> Option<Vec<Tok>> {
        let n = a.len();
        let arg = |i: usize| -> &[Tok] { a.get(i).map(|v| v.as_slice()).unwrap_or(&[]) };
        let joined = || -> Vec<Tok> { let mut v = vec![]; for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.extend(x.iter().cloned()); } v };
        let call = |f: &str| -> Vec<Tok> { let mut v = t(f); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v };
        if ctx.in_ddl { if let Some((ty, w)) = duck_type_to_pg(name, raw) { if let Some(w) = w { ctx.warn(w); } return Some(ty); } }
        let part_arg = |i: usize| -> Option<&'static str> { as_str_lit(arg(i)).and_then(|p| datepart(&p)) };
        Some(match name {
            "IFNULL" => fmt("COALESCE({}, {})", &[arg(0), arg(1)]),
            "IF" => fmt("CASE WHEN {} THEN {} ELSE {} END", &[arg(0), arg(1), arg(2)]),
            "TODAY" => t("CURRENT_DATE"),
            "GET_CURRENT_TIMESTAMP" | "CURRENT_LOCALTIMESTAMP" => t("now()"),
            "UUID" => t("gen_random_uuid()"),
            "LEN" => call("length"),
            "STRLEN" => call("octet_length"),
            "INSTR" => fmt("strpos({}, {})", &[arg(0), arg(1)]),
            "CONTAINS" => fmt("(strpos({}, {}) > 0)", &[arg(0), arg(1)]),
            "PREFIX" => call("starts_with"),
            "SUFFIX" | "ENDS_WITH" => fmt("(right({}, length({})) = {})", &[arg(0), arg(1), arg(1)]),
            "LIST_VALUE" | "LIST_PACK" => { let mut v = t("ARRAY["); v.extend(joined()); v.push(Tok::punct("]")); v }
            "LIST_AGGREGATE" | "ARRAY_AGGREGATE" => { ctx.warn("list_aggregate has no direct PostgreSQL equivalent"); return None; }
            "LIST_CONTAINS" | "ARRAY_CONTAINS" | "ARRAY_HAS" => fmt("({} = ANY({}))", &[arg(1), arg(0)]),
            "LEN_LIST" | "LIST_LENGTH" | "ARRAY_LENGTH" | "LEN_ARRAY" => fmt("cardinality({})", &[arg(0)]),
            "STRING_SPLIT" | "STR_SPLIT" | "SPLIT" => call("string_to_array"),
            "STRING_SPLIT_REGEX" => call("regexp_split_to_array"),
            "LIST_SORT" | "ARRAY_SORT" => fmt("(SELECT array_agg(x ORDER BY x) FROM unnest({}) AS x)", &[arg(0)]),
            "REGEXP_MATCHES" | "REGEXP_FULL_MATCH" => fmt("({} ~ {})", &[arg(0), arg(1)]),
            "REGEXP_EXTRACT" => fmt("(regexp_match({}, {}))[1]", &[arg(0), arg(1)]),
            "STRFTIME" => {
                let (x, f) = if as_str_lit(arg(1)).is_some() { (arg(0), as_str_lit(arg(1))) } else { (arg(1), as_str_lit(arg(0))) };
                match f { Some(f) => fmt(&format!("to_char({{}}, '{}')", strftime_to_pg(&f)), &[x]), None => return None }
            }
            "STRPTIME" => match as_str_lit(arg(1)) { Some(f) => fmt(&format!("to_timestamp({{}}, '{}')", strftime_to_pg(&f)), &[arg(0)]), None => return None },
            "DATE_DIFF" | "DATEDIFF" => match part_arg(0) {
                Some("day") => fmt("(({})::date - ({})::date)", &[arg(2), arg(1)]),
                Some("month") => fmt("((EXTRACT(YEAR FROM {1}) - EXTRACT(YEAR FROM {0})) * 12 + EXTRACT(MONTH FROM {1}) - EXTRACT(MONTH FROM {0}))::int".replace("{0}", "{}").replace("{1}", "{}").as_str(), &[arg(1), arg(2), arg(2), arg(1)]),
                Some("year") => fmt("(EXTRACT(YEAR FROM {}) - EXTRACT(YEAR FROM {}))::int", &[arg(2), arg(1)]),
                Some("hour") => fmt("(EXTRACT(EPOCH FROM date_trunc('hour', {}) - date_trunc('hour', {})) / 3600)::bigint", &[arg(2), arg(1)]),
                Some("minute") => fmt("(EXTRACT(EPOCH FROM date_trunc('minute', {}) - date_trunc('minute', {})) / 60)::bigint", &[arg(2), arg(1)]),
                Some("second") => fmt("(EXTRACT(EPOCH FROM date_trunc('second', {}) - date_trunc('second', {})))::bigint", &[arg(2), arg(1)]),
                _ => { ctx.warn("date_diff part not translated for PostgreSQL"); return None; }
            },
            "DATEPART" => call("date_part"),
            "DATETRUNC" => call("date_trunc"),
            "EPOCH" | "EPOCH_S" => fmt("EXTRACT(EPOCH FROM {})::bigint", &[arg(0)]),
            "EPOCH_MS" => fmt("(EXTRACT(EPOCH FROM {}) * 1000)::bigint", &[arg(0)]),
            "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "DAYOFYEAR" | "WEEK" | "ISODOW" | "DAYOFWEEK" | "MILLISECOND" | "MICROSECOND" | "ISOYEAR" => {
                let p = match name { "DAYOFYEAR" => "doy", "DAYOFWEEK" => "dow", "MILLISECOND" => "milliseconds", "MICROSECOND" => "microseconds", other => other };
                fmt(&format!("EXTRACT({p} FROM {{}})::int"), &[arg(0)])
            }
            "DAYNAME" => fmt("trim(to_char({}, 'Day'))", &[arg(0)]),
            "MONTHNAME" => fmt("trim(to_char({}, 'Month'))", &[arg(0)]),
            "LAST_DAY" => fmt("(date_trunc('month', {}) + interval '1 month - 1 day')::date", &[arg(0)]),
            "MAKE_TIMESTAMP" if n == 1 => fmt("to_timestamp(({})::double precision / 1000000)", &[arg(0)]),
            "TO_DAYS" => fmt("make_interval(days => ({})::int)", &[arg(0)]),
            "TO_HOURS" => fmt("make_interval(hours => ({})::int)", &[arg(0)]),
            "TO_MINUTES" => fmt("make_interval(mins => ({})::int)", &[arg(0)]),
            "TO_SECONDS" => fmt("make_interval(secs => ({})::double precision)", &[arg(0)]),
            "TO_MONTHS" => fmt("make_interval(months => ({})::int)", &[arg(0)]),
            "TO_YEARS" => fmt("make_interval(years => ({})::int)", &[arg(0)]),
            "TO_WEEKS" => fmt("make_interval(weeks => ({})::int)", &[arg(0)]),
            "TO_MILLISECONDS" => fmt("(({}) * interval '1 millisecond')", &[arg(0)]),
            "TO_MICROSECONDS" => fmt("(({}) * interval '1 microsecond')", &[arg(0)]),
            "QUANTILE_CONT" | "QUANTILE" => fmt("percentile_cont({}) WITHIN GROUP (ORDER BY {})", &[arg(1), arg(0)]),
            "QUANTILE_DISC" => fmt("percentile_disc({}) WITHIN GROUP (ORDER BY {})", &[arg(1), arg(0)]),
            "MEDIAN" => fmt("percentile_cont(0.5) WITHIN GROUP (ORDER BY {})", &[arg(0)]),
            "COUNT_IF" | "COUNTIF" => fmt("count(*) FILTER (WHERE {})", &[arg(0)]),
            "ANY_VALUE" | "ARBITRARY" | "FIRST" => fmt("(array_agg({}))[1]", &[arg(0)]),
            "LIST" | "ARRAY_AGG" => call("array_agg"),
            "HASH" => fmt("hashtext(({})::text)", &[arg(0)]),
            "TYPEOF" => fmt("pg_typeof({})", &[arg(0)]),
            "JSON_EXTRACT_STRING" => fmt("jsonb_path_query_first(({})::jsonb, {}) #>> '{{}}'", &[arg(0), &pgpath(arg(1))]),
            "JSON_EXTRACT" => fmt("jsonb_path_query_first(({})::jsonb, {})", &[arg(0), &pgpath(arg(1))]),
            "TO_JSON" => fmt("to_jsonb({})", &[arg(0)]),
            "JSON_VALID" => { ctx.warn("json_valid approximated"); fmt("(({})::jsonb IS NOT NULL)", &[arg(0)]) }
            "GETVARIABLE" => { let name = as_str_lit(arg(0)).unwrap_or_default(); t(&format!("current_setting('d2m.{name}', true)")) }
            "PRINTF" => call("format"),
            "NEXTVAL" | "CURRVAL" => return None,
            "GENERATE_SERIES" | "RANGE" => call("generate_series"),
            "STRUCT_PACK" | "STRUCT_EXTRACT" | "MAP" | "MAP_EXTRACT" | "UNION_VALUE" => { ctx.warn(format!("{name}: STRUCT/MAP have no PostgreSQL equivalent (use jsonb)")); return None; }
            _ => return None,
        })
    }

    fn token(&self, tok: &Tok, prev: Option<&Tok>, next: Option<&Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        match tok.kind {
            Kind::QIdent => {
                let v = tok.value();
                if let (Some(m), true) = (ctx.schema_map.get(&v), next.map(|n| n.is_punct(".")).unwrap_or(false)) { return vec![Tok::new(Kind::QIdent, format!("\"{m}\""))]; }
                vec![tok.clone()]
            }
            Kind::Ident => {
                let u = tok.text.to_uppercase();
                let next_is_dot = next.map(|n| n.is_punct(".")).unwrap_or(false);
                if next_is_dot { if let Some(m) = ctx.schema_map.get(&tok.text) { return vec![Tok::ident(m)]; } }
                let after_as = prev.map(|p| p.is_kw("AS")).unwrap_or(false);
                let next_is_paren = next.map(|n| n.is_punct("(")).unwrap_or(false);
                if (ctx.in_ddl || after_as || prev.map(|p| p.is_op("::")).unwrap_or(false)) && !next_is_paren {
                    if let Some((ty, w)) = duck_type_to_pg(&u, &[]) { if let Some(w) = w { ctx.warn(w); } return ty; }
                }
                match u.as_str() {
                    "QUALIFY" => { ctx.warn("QUALIFY has no PostgreSQL equivalent"); vec![tok.clone()] }
                    "EXCLUDE" if prev.map(|p| p.is_op("*")).unwrap_or(false) => { ctx.warn("SELECT * EXCLUDE has no PostgreSQL equivalent"); vec![tok.clone()] }
                    "ASOF" | "POSITIONAL" => { ctx.warn(format!("{u} JOIN has no PostgreSQL equivalent")); vec![tok.clone()] }
                    _ => vec![tok.clone()],
                }
            }
            Kind::Op => match tok.text.as_str() { "//" => t("/"), "**" => t("^"), _ => vec![tok.clone()] },
            _ => vec![tok.clone()],
        }
    }

    fn group(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        let mut v = toks;
        // [a, b] list literal -> ARRAY[a, b] ; x[1] indexing is the same
        let mut i = 0;
        while i < v.len() {
            if v[i].is_punct("[") {
                let prev_atom = prev_sig(&v, i).map(|p| matches!(v[p].kind, Kind::Ident | Kind::QIdent) || v[p].is_punct(")") || v[p].is_punct("]")).unwrap_or(false);
                if !prev_atom { v.splice(i..i, t("ARRAY")); i += 1; }
            }
            i += 1;
        }
        if let Some(g) = find_top_kw(&v, "GROUP", 0) { if let Some(b) = next_sig(&v, g + 1) { if v[b].is_kw("BY") { if let Some(a) = next_sig(&v, b + 1) { if v[a].is_kw("ALL") { ctx.warn("GROUP BY ALL has no PostgreSQL equivalent"); } } } } }
        if let Some(u) = find_top_kw(&v, "USING", 0) { if let Some(s) = next_sig(&v, u + 1) { if v[s].is_kw("SAMPLE") { ctx.warn("USING SAMPLE converted to TABLESAMPLE SYSTEM"); v.splice(u..=s, t("TABLESAMPLE SYSTEM")); } } }
        v
    }

    fn select_segment(&self, toks: Vec<Tok>, _ctx: &mut Ctx) -> Vec<Tok> { toks }
}

fn pgpath(p: &[Tok]) -> Vec<Tok> { p.to_vec() }

fn strftime_to_pg(f: &str) -> String {
    let mut out = String::new();
    let cs: Vec<char> = f.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        if cs[i] == '%' && i + 1 < cs.len() {
            let (spec, adv) = if cs[i + 1] == '-' && i + 2 < cs.len() { (cs[i + 2], 3) } else { (cs[i + 1], 2) };
            out.push_str(match spec { 'Y' => "YYYY", 'y' => "YY", 'm' => "MM", 'd' => "DD", 'H' => "HH24", 'I' => "HH12", 'M' => "MI", 'S' => "SS", 'f' => "US", 'g' => "MS", 'p' => "AM", 'B' => "FMMonth", 'b' => "Mon", 'A' => "FMDay", 'a' => "Dy", 'j' => "DDD", 'z' => "OF", '%' => "%", _ => "" });
            i += adv;
        } else { out.push(cs[i]); i += 1; }
    }
    out.replace('\'', "''")
}
