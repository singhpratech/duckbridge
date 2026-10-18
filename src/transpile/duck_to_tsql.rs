//! DuckDB SQL -> T-SQL (SQL Server 2019) rules.
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
    let mut out: Vec<Vec<Tok>> = vec![];
    let body: Vec<Tok> = match first.as_str() {
        "SET" if second == "VARIABLE" => {
            let name = v.tok(2).map(|t| t.value()).unwrap_or_default();
            let eq = st.iter().position(|t| t.is_op("=")).unwrap_or(v.pos(3));
            let expr = rewrite(trim(&st[eq + 1..]), &R, ctx);
            let decl = if ctx.variables.insert(name.clone()) {
                ctx.warn(format!("variable @{name} declared as SQL_VARIANT; set an explicit type"));
                format!("DECLARE @{name} SQL_VARIANT = {{}}")
            } else { format!("SET @{name} = {{}}") };
            fmt(&decl, &[&expr])
        }
        "RESET" if second == "VARIABLE" => t(&format!("SET @{} = NULL", v.tok(2).map(|t| t.value()).unwrap_or_default())),
        "SET" | "PRAGMA" | "INSTALL" | "LOAD" | "ATTACH" | "DETACH" | "USE" | "CHECKPOINT" | "FORCE" | "EXPORT" | "IMPORT" | "COPY" | "VACUUM" | "ANALYZE" => {
            ctx.warn(format!("DuckDB-specific statement dropped: {}", join(st).trim().split_whitespace().take(3).collect::<Vec<_>>().join(" ")));
            t(&format!("-- (dropped) {}", join(st).trim().replace('\n', " ")))
        }
        "DESCRIBE" | "SHOW" | "SUMMARIZE" | "EXPLAIN" => { ctx.warn(format!("{first} has no T-SQL equivalent")); t(&format!("-- (dropped) {}", join(st).trim().replace('\n', " "))) }
        "CREATE" if second == "OR" && third == "REPLACE" => {
            let kind = v.kw(3);
            if kind == "VIEW" || kind == "FUNCTION" || kind == "PROCEDURE" || kind == "MACRO" {
                let mut w = v.all(); w[v.pos(2)] = Tok::ident("ALTER");
                if kind != "VIEW" { ctx.warn("DuckDB macros/functions need manual conversion to T-SQL functions"); }
                rewrite(&w, &R, ctx)
            } else {
                let name_start = v.pos(4);
                let name_end = ident_end(st, name_start);
                let name = rewrite(trim(&st[name_start..name_end]), &R, ctx);
                out.push(fmt(&format!("DROP {kind} IF EXISTS {{}}"), &[&name]));
                let mut w: Vec<Tok> = t("CREATE ");
                w.extend(st[v.pos(3)..].iter().cloned());
                ddl(&w, ctx)
            }
        }
        "CREATE" if second == "TEMP" || second == "TEMPORARY" => {
            // CREATE TEMP TABLE t -> CREATE TABLE #t
            let mut w: Vec<Tok> = st.to_vec();
            let p = v.pos(1);
            w.remove(p);
            if p < w.len() && w[p].is_trivia() { w.remove(p); }
            if let Some(k) = w.iter().position(|t| t.is_kw("TABLE")) {
                if let Some(n) = next_sig(&w, k + 1) {
                    let name = w[n].value();
                    ctx.temp_tables.insert(name.clone());
                    w[n] = Tok::new(Kind::Temp, format!("#{name}"));
                }
            }
            ddl(&w, ctx)
        }
        "CREATE" if matches!(second.as_str(), "TABLE" | "SEQUENCE" | "INDEX" | "UNIQUE" | "SCHEMA") => ddl(st, ctx),
        "CREATE" if second == "MACRO" || second == "TYPE" => { ctx.warn(format!("CREATE {second} needs manual conversion")); t(&format!("/* UNTRANSLATED DuckDB:\n{}\n*/", join(st).trim())) }
        "ALTER" => ddl(st, ctx),
        "DELETE" => delete_using(st, ctx),
        "INSERT" => {
            if second == "OR" { ctx.warn("INSERT OR REPLACE/IGNORE has no T-SQL equivalent; converted to plain INSERT"); }
            let w: Vec<Tok> = remove_pair(st, &["OR", "REPLACE"]);
            let w: Vec<Tok> = remove_pair(&w, &["OR", "IGNORE"]);
            if find_top_kw(&w, "RETURNING", 0).is_some() { ctx.warn("RETURNING converted to OUTPUT inserted.*"); }
            if find_top_kw(&w, "CONFLICT", 0).is_some() { ctx.warn("ON CONFLICT clause has no T-SQL equivalent; dropped (consider MERGE)"); }
            let mut r = rewrite(&w, &R, ctx);
            if let Some(k) = find_top_kw(&r, "ON", 0) { if next_sig(&r, k + 1).map(|x| r[x].is_kw("CONFLICT")).unwrap_or(false) { r.truncate(k); } }
            if let Some(k) = find_top_kw(&r, "RETURNING", 0) {
                let cols: Vec<Tok> = r[k + 1..].to_vec();
                r.truncate(k);
                while r.last().map(|x| x.is_trivia()).unwrap_or(false) { r.pop(); }
                let star = strip_trivia(&cols).len() == 1 && cols.iter().any(|x| x.is_op("*"));
                let outp = if star { t("OUTPUT inserted.*") } else { let mut o = t("OUTPUT "); for (i, c) in split_top(trim(&cols), ",").iter().enumerate() { if i > 0 { o.extend(t(", ")); } o.extend(t("inserted.")); o.extend(trim(c).iter().cloned()); } o };
                let pos = find_top_kw(&r, "VALUES", 0).or_else(|| find_top_kw(&r, "SELECT", 0)).unwrap_or(r.len());
                let mut outp2 = outp; outp2.push(Tok::ws());
                r.splice(pos..pos, outp2);
            }
            r
        }
        _ => rewrite(st, &R, ctx),
    };
    out.append(&mut ctx.pre_statements);
    out.push(body);
    out
}

pub fn expression(toks: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    rewrite(trim(toks), &R, ctx)
}

fn remove_pair(s: &[Tok], seq: &[&str]) -> Vec<Tok> {
    let mut out = vec![];
    let mut i = 0;
    while i < s.len() {
        if s[i].is_kw(seq[0]) {
            let mut k = i; let mut ok = true; let mut last = i;
            for w in seq { match next_sig(s, k) { Some(p) if s[p].is_kw(w) => { last = p; k = p + 1; } _ => { ok = false; break; } } }
            if ok { i = last + 1; if i < s.len() && s[i].is_trivia() { i += 1; } continue; }
        }
        out.push(s[i].clone());
        i += 1;
    }
    out
}

fn ddl(s: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    ctx.in_ddl = true;
    let mut v: Vec<Tok> = vec![];
    let mut i = 0;
    while i < s.len() {
        if s[i].is_kw("IF") {
            if let (Some(a), Some(b)) = (next_sig(s, i + 1), next_sig(s, i + 1).and_then(|a| next_sig(s, a + 1))) {
                if s[a].is_kw("NOT") && s[b].is_kw("EXISTS") {
                    ctx.warn("IF NOT EXISTS dropped (wrap in IF OBJECT_ID(...) IS NULL manually)");
                    i = b + 1; if i < s.len() && s[i].is_trivia() { i += 1; } continue;
                }
            }
        }
        v.push(s[i].clone());
        i += 1;
    }
    // CREATE SEQUENCE: DuckDB START n INCREMENT n -> START WITH n INCREMENT BY n
    if Stmt::new(&v).kw(1) == "SEQUENCE" {
        let mut w = vec![];
        let mut j = 0;
        while j < v.len() {
            w.push(v[j].clone());
            if v[j].is_kw("START") && !next_sig(&v, j + 1).map(|p| v[p].is_kw("WITH")).unwrap_or(false) { w.extend(t(" WITH")); }
            if v[j].is_kw("INCREMENT") && !next_sig(&v, j + 1).map(|p| v[p].is_kw("BY")).unwrap_or(false) { w.extend(t(" BY")); }
            j += 1;
        }
        v = w;
    }
    let r = rewrite(trim(&v), &R, ctx);
    ctx.in_ddl = false;
    r
}

fn delete_using(st: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    // DELETE FROM t [AS a] USING o WHERE c  ->  DELETE a FROM t a, o WHERE c
    if let Some(u) = find_top_kw(st, "USING", 0) {
        let from = find_top_kw(st, "FROM", 0).unwrap_or(0);
        let target: Vec<Tok> = strip_trivia(&st[from + 1..u]).into_iter().filter(|x| !x.is_kw("AS")).collect();
        let alias = target.last().map(|t| t.value()).unwrap_or_default();
        let wh = find_top_kw(st, "WHERE", u).unwrap_or(st.len());
        let other: Vec<Tok> = st[u + 1..wh].to_vec();
        let mut v = t("DELETE ");
        v.push(Tok::ident(&alias));
        v.extend(t(" FROM "));
        for (i, x) in target.iter().enumerate() { if i > 0 && !x.is_punct(".") && !target[i - 1].is_punct(".") { v.push(Tok::ws()); } v.push(x.clone()); }
        v.extend(t(", "));
        v.extend(trim(&other).iter().cloned());
        if wh < st.len() { v.push(Tok::ws()); v.extend(st[wh..].iter().cloned()); }
        return rewrite(&v, &R, ctx);
    }
    rewrite(st, &R, ctx)
}

impl Rules for R {
    fn func(&self, name: &str, raw: &[Vec<Tok>], a: &[Vec<Tok>], ctx: &mut Ctx) -> Option<Vec<Tok>> {
        let n = a.len();
        let arg = |i: usize| -> &[Tok] { a.get(i).map(|v| v.as_slice()).unwrap_or(&[]) };
        let joined = || -> Vec<Tok> { let mut v = vec![]; for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.extend(x.iter().cloned()); } v };
        let call = |f: &str| -> Vec<Tok> { let mut v = t(f); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v };
        if ctx.in_ddl || matches!(name, "VARCHAR" | "DECIMAL" | "NUMERIC" | "CHAR") {
            if let Some((ty, w)) = duck_type_to_tsql_v(name, raw, ctx.target_version) { if let Some(w) = w { ctx.warn(w); } return Some(ty); }
        }
        let part_arg = |i: usize| -> Option<&'static str> { as_str_lit(arg(i)).and_then(|p| datepart(&p)) };
        Some(match name {
            "IFNULL" => fmt("ISNULL({}, {})", &[arg(0), arg(1)]),
            "IF" => fmt("IIF({}, {}, {})", &[arg(0), arg(1), arg(2)]),
            "NOW" | "CURRENT_TIMESTAMP" | "GET_CURRENT_TIMESTAMP" | "CURRENT_LOCALTIMESTAMP" | "TRANSACTION_TIMESTAMP" | "LOCALTIMESTAMP" => t("SYSDATETIME()"),
            "TODAY" | "CURRENT_DATE" | "CURRENT_LOCALTIME" => t("CAST(GETDATE() AS DATE)"),
            "UUID" | "GEN_RANDOM_UUID" => t("NEWID()"),
            "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => fmt("LEN({})", &[arg(0)]),
            "STRLEN" | "OCTET_LENGTH" => fmt("DATALENGTH({})", &[arg(0)]),
            "STRPOS" | "INSTR" | "POSITION" => fmt("CHARINDEX({}, {})", &[arg(1), arg(0)]),
            "CONTAINS" => fmt("(CHARINDEX({}, {}) > 0)", &[arg(1), arg(0)]),
            "STARTS_WITH" | "PREFIX" => fmt("(LEFT({}, LEN({})) = {})", &[arg(0), arg(1), arg(1)]),
            "ENDS_WITH" | "SUFFIX" => fmt("(RIGHT({}, LEN({})) = {})", &[arg(0), arg(1), arg(1)]),
            "SUBSTR" | "SUBSTRING" => if n == 2 { fmt("SUBSTRING({}, {}, LEN({}))", &[arg(0), arg(1), arg(0)]) } else { call("SUBSTRING") },
            "REPEAT" => fmt("REPLICATE({}, {})", &[arg(0), arg(1)]),
            "LPAD" => fmt("RIGHT(REPLICATE({}, {}) + {}, {})", &[arg(2), arg(1), arg(0), arg(1)]),
            "RPAD" => fmt("LEFT({} + REPLICATE({}, {}), {})", &[arg(0), arg(2), arg(1), arg(1)]),
            "UCASE" => call("UPPER"),
            "LCASE" => call("LOWER"),
            "STRING_AGG" | "LISTAGG" | "GROUP_CONCAT" => {
                // string_agg(x, sep ORDER BY y) -> STRING_AGG(x, sep) WITHIN GROUP (ORDER BY y)
                let r1 = raw.get(1).map(|v| v.as_slice()).unwrap_or(&[]);
                if let Some(k) = find_top_kw(r1, "ORDER", 0) {
                    let sep = rewrite(trim(&r1[..k]), self, ctx);
                    let ob = rewrite(trim(&r1[k..]), self, ctx);
                    fmt("STRING_AGG({}, {}) WITHIN GROUP ({})", &[arg(0), &sep, &ob])
                } else if n == 1 { fmt("STRING_AGG({}, ',')", &[arg(0)]) } else { call("STRING_AGG") }
            }
            "DATE_DIFF" | "DATEDIFF" => match part_arg(0) { Some(p) => fmt(&format!("DATEDIFF({}, {{}}, {{}})", tsql_part(p)), &[arg(1), arg(2)]), None => { ctx.warn("date_diff part must be a literal"); return None; } },
            "DATE_PART" | "DATEPART" | "EXTRACT" => match part_arg(0) { Some(p) => fmt(&format!("DATEPART({}, {{}})", tsql_part(p)), &[arg(1)]), None => { ctx.warn("date_part part must be a literal"); return None; } },
            "DATE_TRUNC" | "DATETRUNC" => match part_arg(0) {
                Some(p) if ctx.target_version >= 2022 => fmt(&format!("DATETRUNC({}, {{}})", if p == "week" { "ISO_WEEK" } else { tsql_part(p) }), &[arg(1)]),
                Some("day") => fmt("CAST(CAST({} AS DATE) AS DATETIME2)", &[arg(1)]),
                Some("hour") => fmt("CAST(CONVERT(VARCHAR(13), {}, 120) + ':00:00' AS DATETIME2)", &[arg(1)]),
                Some("minute") => fmt("CAST(CONVERT(VARCHAR(16), {}, 120) + ':00' AS DATETIME2)", &[arg(1)]),
                Some("second") => fmt("CAST(CONVERT(VARCHAR(19), {}, 120) AS DATETIME2)", &[arg(1)]),
                Some("millisecond") => fmt("CAST(CONVERT(VARCHAR(23), {}, 121) AS DATETIME2)", &[arg(1)]),
                Some("week") | Some("isoweek") => fmt("DATEADD(DAY, -((DATEPART(WEEKDAY, {}) + @@DATEFIRST - 2) % 7), CAST(CAST({} AS DATE) AS DATETIME2))", &[arg(1), arg(1)]),
                Some(p @ ("year" | "quarter" | "month")) => fmt(&format!("DATEADD({0}, DATEDIFF({0}, 0, {{}}), 0)", tsql_part(p)), &[arg(1)]),
                Some(_) => { ctx.warn("date_trunc part not supported on SQL Server 2019 (use --target-version 2022)"); return None; }
                None => { ctx.warn("date_trunc part must be a literal"); return None; }
            },
            "YEAR" | "MONTH" | "DAY" => return None,
            "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "DAYOFYEAR" | "MILLISECOND" | "MICROSECOND" => fmt(&format!("DATEPART({name}, {{}})"), &[arg(0)]),
            "DAYOFWEEK" => fmt("((DATEPART(WEEKDAY, {}) + @@DATEFIRST - 1) % 7)", &[arg(0)]),
            "ISODOW" => fmt("(((DATEPART(WEEKDAY, {}) + @@DATEFIRST + 5) % 7) + 1)", &[arg(0)]),
            "WEEK" | "WEEKOFYEAR" => fmt("DATEPART(ISO_WEEK, {})", &[arg(0)]),
            "ISOYEAR" => fmt("YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, {}), {}))", &[arg(0), arg(0)]),
            "DAYNAME" => fmt("DATENAME(WEEKDAY, {})", &[arg(0)]),
            "MONTHNAME" => fmt("DATENAME(MONTH, {})", &[arg(0)]),
            "LAST_DAY" => fmt("EOMONTH({})", &[arg(0)]),
            "EPOCH" | "EPOCH_S" => fmt("DATEDIFF_BIG(SECOND, '1970-01-01', {})", &[arg(0)]),
            "EPOCH_MS" => fmt("DATEDIFF_BIG(MILLISECOND, '1970-01-01', {})", &[arg(0)]),
            "TO_TIMESTAMP" => fmt("DATEADD(SECOND, {}, CAST('1970-01-01' AS DATETIME2))", &[arg(0)]),
            "MAKE_DATE" => call("DATEFROMPARTS"),
            "MAKE_TIMESTAMP" if n == 6 => { ctx.warn("make_timestamp fractional seconds truncated"); fmt("DATETIME2FROMPARTS({}, {}, {}, {}, {}, FLOOR({}), 0, 0)", &[arg(0), arg(1), arg(2), arg(3), arg(4), arg(5)]) }
            "STRFTIME" => {
                // DuckDB accepts both strftime(ts, fmt) and strftime(fmt, ts)
                let (x, f) = if as_str_lit(arg(1)).is_some() { (arg(0), as_str_lit(arg(1))) } else { (arg(1), as_str_lit(arg(0))) };
                match f {
                    Some(f) => { let (df, lossy) = strftime_to_dotnet(&f); if lossy { ctx.warn("strftime specifier without FORMAT() equivalent dropped"); } fmt(&format!("FORMAT({{}}, '{}')", df.replace('\'', "''")), &[x]) }
                    None => { ctx.warn("strftime format must be a literal"); return None; }
                }
            }
            "STRPTIME" => { ctx.warn("strptime converted to CONVERT(DATETIME2, x); format ignored"); fmt("CONVERT(DATETIME2, {})", &[arg(0)]) }
            "RANDOM" => t("RAND()"),
            "CEIL" => call("CEILING"),
            "TRUNC" => fmt("ROUND({}, 0, 1)", &[arg(0)]),
            "LN" => fmt("LOG({})", &[arg(0)]),
            "LOG" if n == 2 => fmt("LOG({}, {})", &[arg(1), arg(0)]),
            "LOG2" => fmt("LOG({}, 2)", &[arg(0)]),
            "POW" => call("POWER"),
            "GREATEST" if ctx.target_version >= 2022 => return None,
            "LEAST" if ctx.target_version >= 2022 => return None,
            "GREATEST" => { let mut v = t("(SELECT MAX(v) FROM (VALUES "); for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.push(Tok::punct("(")); v.extend(x.iter().cloned()); v.push(Tok::punct(")")); } v.extend(t(") AS g(v))")); v }
            "LEAST" => { let mut v = t("(SELECT MIN(v) FROM (VALUES "); for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.push(Tok::punct("(")); v.extend(x.iter().cloned()); v.push(Tok::punct(")")); } v.extend(t(") AS l(v))")); v }
            "TRIM" if n == 2 => fmt("TRIM({} FROM {})", &[arg(1), arg(0)]),
            "LTRIM" | "RTRIM" if n == 2 => { if ctx.target_version < 2022 { ctx.warn(format!("{name}(x, chars) needs SQL Server 2022 (compat 160); left as-is")); } return None; }
            "TIME_BUCKET" => { ctx.warn("time_bucket converted to DATE_BUCKET (SQL Server 2022+)"); return None; }
            "APPROX_QUANTILE" => { ctx.warn("approx_quantile converted to APPROX_PERCENTILE_CONT (2022+) WITHIN GROUP"); fmt("APPROX_PERCENTILE_CONT({}) WITHIN GROUP (ORDER BY {})", &[arg(1), arg(0)]) }
            "BASE64" | "TO_BASE64" if ctx.target_version >= 2025 => fmt("BASE64_ENCODE({})", &[arg(0)]),
            "FROM_BASE64" if ctx.target_version >= 2025 => fmt("BASE64_DECODE({})", &[arg(0)]),
            "MD5" => fmt("LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', {}), 2))", &[arg(0)]),
            "SHA256" => fmt("LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {}), 2))", &[arg(0)]),
            "SHA1" => fmt("LOWER(CONVERT(VARCHAR(40), HASHBYTES('SHA1', {}), 2))", &[arg(0)]),
            "GETVARIABLE" => { let name = as_str_lit(arg(0)).unwrap_or_default(); vec![Tok::new(Kind::Var, format!("@{name}"))] }
            "CAST" | "TRY_CAST" => {
                let r = raw.first().map(|v| v.as_slice()).unwrap_or(&[]);
                let as_pos = find_top_kw(r, "AS", 0)?;
                let expr = rewrite(trim(&r[..as_pos]), self, ctx);
                let ty = type_tokens(trim(&r[as_pos + 1..]), ctx);
                fmt(&format!("{name}({{}} AS {{}})"), &[&expr, &ty])
            }
            "TYPEOF" => { ctx.warn("typeof() approximated with SQL_VARIANT_PROPERTY"); fmt("SQL_VARIANT_PROPERTY(CAST({} AS SQL_VARIANT), 'BaseType')", &[arg(0)]) }
            "JSON_EXTRACT_STRING" | "JSON_EXTRACT_PATH_TEXT" => fmt("JSON_VALUE({}, {})", &[arg(0), arg(1)]),
            "JSON_EXTRACT" | "JSON_EXTRACT_PATH" => fmt("JSON_QUERY({}, {})", &[arg(0), arg(1)]),
            "JSON_VALID" => fmt("(ISJSON({}) = 1)", &[arg(0)]),
            "TO_JSON" | "JSON_OBJECT" | "JSON_ARRAY" | "JSON_GROUP_ARRAY" | "JSON_GROUP_OBJECT" => { ctx.warn(format!("{name}() converted to FOR JSON? no direct equivalent; left as-is")); return None; }
            "STRING_SPLIT" | "STR_SPLIT" | "SPLIT" | "STRING_TO_ARRAY" => { ctx.warn("string_split returns a list; in T-SQL use STRING_SPLIT as a table source"); fmt("STRING_SPLIT({}, {})", &[arg(0), arg(1)]) }
            "QUANTILE_CONT" | "QUANTILE_DISC" | "MEDIAN" => {
                let f = if name == "QUANTILE_DISC" { "PERCENTILE_DISC" } else { "PERCENTILE_CONT" };
                let p: Vec<Tok> = if name == "MEDIAN" { t("0.5") } else { arg(1).to_vec() };
                ctx.warn(format!("{name} converted to {f} ... WITHIN GROUP; it is a window function in T-SQL and needs OVER (PARTITION BY ...)"));
                fmt(&format!("{f}({{}}) WITHIN GROUP (ORDER BY {{}}) OVER ()"), &[&p, arg(0)])
            }
            "COUNT_IF" | "COUNTIF" => fmt("SUM(CASE WHEN {} THEN 1 ELSE 0 END)", &[arg(0)]),
            "BOOL_AND" => fmt("MIN(CASE WHEN {} THEN 1 ELSE 0 END)", &[arg(0)]),
            "BOOL_OR" | "ANY_VALUE" if name == "BOOL_OR" => fmt("MAX(CASE WHEN {} THEN 1 ELSE 0 END)", &[arg(0)]),
            "ANY_VALUE" | "ARBITRARY" | "FIRST" => fmt("MIN({})", &[arg(0)]),
            "LIST_VALUE" | "LIST_PACK" | "ARRAY_VALUE" => { ctx.warn("list literals have no T-SQL equivalent; left as-is"); return None; }
            "REGEXP_MATCHES" | "REGEXP_FULL_MATCH" if ctx.target_version >= 2025 => { let p = if name == "REGEXP_FULL_MATCH" { fmt("'^(?:' + {} + ')$'", &[arg(1)]) } else { arg(1).to_vec() }; fmt("REGEXP_LIKE({}, {})", &[arg(0), &p]) }
            "REGEXP_REPLACE" if ctx.target_version >= 2025 => { if !upper_text(arg(3)).contains('G') { ctx.warn("regexp_replace without 'g' replaces the first match in DuckDB but REGEXP_REPLACE replaces all in SQL Server"); } fmt("REGEXP_REPLACE({}, {}, {})", &[arg(0), arg(1), arg(2)]) }
            "REGEXP_EXTRACT" if ctx.target_version >= 2025 => fmt("REGEXP_SUBSTR({}, {})", &[arg(0), arg(1)]),
            "REGEXP_SPLIT_TO_ARRAY" if ctx.target_version >= 2025 => { ctx.warn("regexp_split_to_array returns a list; REGEXP_SPLIT_TO_TABLE is a table function"); fmt("REGEXP_SPLIT_TO_TABLE({}, {})", &[arg(0), arg(1)]) }
            "REGEXP_MATCHES" | "REGEXP_FULL_MATCH" | "REGEXP_REPLACE" | "REGEXP_EXTRACT" | "REGEXP_SPLIT_TO_ARRAY" => { ctx.warn(format!("{name}: SQL Server 2019/2022 have no regex support (2025 has REGEXP_*; use --target-version 2025); left as-is")); return None; }
            "NEXTVAL" => fmt("NEXT VALUE FOR {}", &[&t(&as_str_lit(arg(0)).unwrap_or_default())]),
            "HASH" => { ctx.warn("hash() replaced by CHECKSUM(); values differ"); call("CHECKSUM") }
            "PRINTF" | "FORMAT" => { ctx.warn("printf/format string formatting not translated; use FORMAT()/CONCAT manually"); return None; }
            "GENERATE_SERIES" | "RANGE" if ctx.target_version >= 2022 => { if name == "RANGE" { ctx.warn("range() stop is exclusive, GENERATE_SERIES stop is inclusive; verify"); } call("GENERATE_SERIES") }
            _ if name.starts_with("LIST_") || name.starts_with("ARRAY_") || name.starts_with("STRUCT_") || name.starts_with("MAP_") || name == "UNNEST" || name == "GENERATE_SERIES" || name == "RANGE" => {
                if name == "GENERATE_SERIES" || name == "RANGE" { ctx.warn("generate_series/range: SQL Server 2019 lacks GENERATE_SERIES (2022+, use --target-version 2022); left as-is"); }
                else { ctx.warn(format!("{name}(): nested types have no T-SQL equivalent; left as-is")); }
                return None;
            }
            _ => return None,
        })
    }

    fn token(&self, tok: &Tok, prev: Option<&Tok>, next: Option<&Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        match tok.kind {
            Kind::QIdent => {
                let v = tok.value();
                if let (Some(m), true) = (ctx.schema_map.get(&v), next.map(|n| n.is_punct(".")).unwrap_or(false)) { return vec![Tok::new(Kind::QIdent, format!("[{m}]"))]; }
                if ctx.temp_tables.contains(&v) { return vec![Tok::new(Kind::Temp, format!("#{v}"))]; }
                vec![Tok::new(Kind::QIdent, format!("[{}]", v.replace(']', "]]")))]
            }
            Kind::Ident => {
                let u = tok.text.to_uppercase();
                let next_is_dot = next.map(|n| n.is_punct(".")).unwrap_or(false);
                let prev_is_dot = prev.map(|n| n.is_punct(".")).unwrap_or(false);
                if next_is_dot { if let Some(m) = ctx.schema_map.get(&tok.text) { return vec![Tok::ident(m)]; } }
                if ctx.temp_tables.contains(&tok.text) && !prev_is_dot { return vec![Tok::new(Kind::Temp, format!("#{}", tok.text))]; }
                let after_as = prev.map(|p| p.is_kw("AS")).unwrap_or(false);
                let next_is_paren = next.map(|n| n.is_punct("(")).unwrap_or(false);
                if (ctx.in_ddl || after_as) && !next_is_paren && !prev_is_dot {
                    if let Some((ty, w)) = duck_type_to_tsql_v(&u, &[], ctx.target_version) { if let Some(w) = w { ctx.warn(w); } return ty; }
                }
                match u.as_str() {
                    "TRUE" if !prev_is_dot => t("1"),
                    "FALSE" if !prev_is_dot => t("0"),
                    "ILIKE" => { ctx.warn("ILIKE converted to LIKE; case sensitivity depends on the column collation"); t("LIKE") }
                    "CURRENT_TIMESTAMP" | "NOW" if !next_is_paren => t("SYSDATETIME()"),
                    "CURRENT_DATE" | "TODAY" if !next_is_paren => t("CAST(GETDATE() AS DATE)"),
                    "QUALIFY" => { ctx.warn("QUALIFY has no T-SQL equivalent; wrap the query in a subquery and filter"); vec![tok.clone()] }
                    "EXCLUDE" if prev.map(|p| p.is_op("*")).unwrap_or(false) => { ctx.warn("SELECT * EXCLUDE (...) has no T-SQL equivalent"); vec![tok.clone()] }
                    "LATERAL" => { ctx.warn("LATERAL join converted to CROSS APPLY syntax is not automatic; verify"); vec![tok.clone()] }
                    "ASOF" => { ctx.warn("ASOF JOIN has no T-SQL equivalent"); vec![tok.clone()] }
                    "USING" if prev.map(|p| p.is_kw("SAMPLE")).unwrap_or(false) => vec![tok.clone()],
                    _ => vec![tok.clone()],
                }
            }
            Kind::Str => {
                // promote non-ASCII literals to N'...'
                if !tok.text.starts_with('N') && tok.text.starts_with('\'') && !tok.text.is_ascii() { vec![Tok::new(Kind::Str, format!("N{}", tok.text))] }
                else if tok.text.starts_with("$$") { let body = &tok.text[2..tok.text.len() - 2]; vec![Tok::new(Kind::Str, format!("N'{}'", body.replace('\'', "''")))] }
                else { vec![tok.clone()] }
            }
            Kind::Op => match tok.text.as_str() {
                "||" => t("+"),
                "~" => { ctx.warn("regex operator ~ has no T-SQL equivalent"); vec![tok.clone()] }
                _ => vec![tok.clone()],
            },
            _ => vec![tok.clone()],
        }
    }

    fn group(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        let mut v = toks;
        // 1. x::type -> CAST(x AS type)
        let mut i = 0;
        while i < v.len() {
            if v[i].is_op("::") {
                let start = atom_start(&v, i);
                let ty_start = next_sig(&v, i + 1).unwrap_or(i + 1);
                let ty_end = type_end(&v, ty_start);
                let expr: Vec<Tok> = v[start..i].to_vec();
                let ty = type_tokens(&v[ty_start..ty_end], ctx);
                let rep = fmt("CAST({} AS {})", &[trim(&expr), &ty]);
                let rep_len = rep.len();
                v.splice(start..ty_end, rep);
                i = start + rep_len;
                continue;
            }
            i += 1;
        }
        // 2. date arithmetic: x + INTERVAL n unit | x + to_days(n) -> DATEADD(unit, n, x)
        let mut i = 0;
        while i < v.len() {
            if v[i].is_op("+") || v[i].is_op("-") {
                if let Some(r) = next_sig(&v, i + 1) {
                    let neg = v[i].is_op("-");
                    // (part, number tokens, end index exclusive)
                    let mut found: Option<(&'static str, Vec<Tok>, usize)> = None;
                    if v[r].is_kw("INTERVAL") {
                        if let Some(nn) = next_sig(&v, r + 1) {
                            if v[nn].kind == Kind::Str {
                                let sv = v[nn].value();
                                let mut it = sv.split_whitespace();
                                let num = it.next().unwrap_or("0").to_string();
                                let unit = it.next().unwrap_or("day").trim_end_matches('s').to_string();
                                if let Some(p) = datepart(&unit) { found = Some((p, t(&num), nn + 1)); }
                            } else {
                                let n_end = atom_end(&v, nn);
                                if let Some(u) = next_sig(&v, n_end) {
                                    let unit = v[u].text.trim_end_matches('s').to_string();
                                    if let Some(p) = datepart(&unit) { found = Some((p, trim(&v[nn..n_end]).to_vec(), u + 1)); }
                                }
                            }
                        }
                    } else if v[r].kind == Kind::Ident && v[r].text.to_lowercase().starts_with("to_") {
                        let unit = v[r].text.to_lowercase()[3..].trim_end_matches('s').to_string();
                        if let Some(p) = datepart(&unit) {
                            if let Some(o) = next_sig(&v, r + 1) {
                                if v[o].is_punct("(") {
                                    let c = match_paren(&v, o);
                                    found = Some((p, simplify_cast_bigint(trim(&v[o + 1..c])), c + 1));
                                }
                            }
                        }
                    }
                    if let Some((part, num, end)) = found {
                        let start = atom_start(&v, i);
                        let left: Vec<Tok> = trim(&v[start..i]).to_vec();
                        let num_s = if neg { if num.len() == 1 && num[0].kind == Kind::Num { t(&format!("-{}", num[0].text)) } else { fmt("-({})", &[&num]) } } else { num };
                        let rep = fmt(&format!("DATEADD({}, {{}}, {{}})", tsql_part(part)), &[&num_s, &left]);
                        v.splice(start..end, rep);
                        i = start;
                    }
                }
            }
            i += 1;
        }
        // 2b. DuckDB `/` is floating-point division; T-SQL int/int truncates -> CAST the left operand to FLOAT.
        //     `//` (integer division) becomes a plain `/`. Skipped when either operand is already a non-integer literal.
        let mut i = 0;
        while i < v.len() {
            if v[i].is_op("//") {
                v[i] = Tok::op("/");
                i += 1;
                continue;
            }
            if v[i].is_op("/") && ctx.float_division {
                let start = atom_start(&v, i);
                let r = next_sig(&v, i + 1).unwrap_or(i + 1);
                let end = atom_end(&v, r);
                let left: Vec<Tok> = trim(&v[start..i]).to_vec();
                let right: Vec<Tok> = trim(&v[r..end.min(v.len())]).to_vec();
                let is_float_lit = |x: &[Tok]| x.len() == 1 && x[0].kind == Kind::Num && (x[0].text.contains('.') || x[0].text.contains('e') || x[0].text.contains('E'));
                let already_cast = |x: &[Tok]| upper_text(x).starts_with("CAST(") && upper_text(x).contains(" AS FLOAT");
                if left.is_empty() || right.is_empty() || is_float_lit(&left) || is_float_lit(&right) || already_cast(&left) { i += 1; continue; }
                let rep = fmt("CAST({} AS FLOAT) / {}", &[&left, &right]);
                let l = rep.len();
                v.splice(start..end.min(v.len()), rep);
                i = start + l;
                continue;
            }
            i += 1;
        }
        // 3. ^ and ** -> POWER(a, b)
        let mut i = 0;
        while i < v.len() {
            if v[i].is_op("**") || v[i].is_op("^") {
                let start = atom_start(&v, i);
                let r = next_sig(&v, i + 1).unwrap_or(i + 1);
                let end = atom_end(&v, r);
                let a: Vec<Tok> = trim(&v[start..i]).to_vec();
                let b: Vec<Tok> = trim(&v[r..end]).to_vec();
                let rep = fmt("POWER({}, {})", &[&a, &b]);
                let l = rep.len();
                v.splice(start..end, rep);
                i = start + l;
                continue;
            }
            i += 1;
        }
        // 4. list literal [a, b] -> warn
        if v.iter().any(|x| x.is_punct("[")) { ctx.warn("list literal [...] has no T-SQL equivalent"); }
        // 5. = TRUE / = FALSE handled via token(); IS TRUE -> = 1
        let mut i = 0;
        while i + 1 < v.len() {
            if v[i].is_kw("IS") { if let Some(n) = next_sig(&v, i + 1) { if v[n].kind == Kind::Num && (v[n].text == "1" || v[n].text == "0") { v[i] = Tok::op("="); } } }
            i += 1;
        }
        // 6. GROUP BY ALL
        if let Some(g) = find_top_kw(&v, "GROUP", 0) { if let Some(b) = next_sig(&v, g + 1) { if v[b].is_kw("BY") { if let Some(a) = next_sig(&v, b + 1) { if v[a].is_kw("ALL") { ctx.warn("GROUP BY ALL has no T-SQL equivalent; list the columns"); } } } } }
        // 7. USING SAMPLE n -> TABLESAMPLE
        if let Some(u) = find_top_kw(&v, "USING", 0) { if let Some(s) = next_sig(&v, u + 1) { if v[s].is_kw("SAMPLE") { ctx.warn("USING SAMPLE converted to TABLESAMPLE (approximate)"); v.splice(u..=s, t("TABLESAMPLE")); } } }
        v
    }

    fn select_segment(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        let mut segments: Vec<(usize, usize)> = vec![];
        let mut depth = 0i32;
        let mut start = 0;
        for (i, tk) in toks.iter().enumerate() {
            if tk.is_punct("(") { depth += 1; } else if tk.is_punct(")") { depth -= 1; }
            if depth == 0 && tk.is_any_kw(&["UNION", "EXCEPT", "INTERSECT"]) { segments.push((start, i)); start = i + 1; }
        }
        segments.push((start, toks.len()));
        let mut out: Vec<Tok> = vec![];
        let mut last = 0;
        for (a, b) in segments {
            if a > last { if !out.last().map(|x| x.is_trivia()).unwrap_or(true) { out.push(Tok::ws()); } out.extend(toks[last..a].iter().cloned()); if !out.last().map(|x| x.is_trivia()).unwrap_or(true) { out.push(Tok::ws()); } }
            out.extend(limit_to_top(&toks[a..b], ctx));
            last = b;
        }
        out
    }
}

/// CAST(<number> AS BIGINT) -> <number>
fn simplify_cast_bigint(toks: &[Tok]) -> Vec<Tok> {
    let s = strip_trivia(toks);
    if s.len() == 6 && s[0].is_kw("CAST") && s[1].is_punct("(") && (s[2].kind == Kind::Num || (s[2].is_op("-"))) && s[3].is_kw("AS") || (s.len() == 7 && s[0].is_kw("CAST") && s[2].is_op("-") && s[3].kind == Kind::Num && s[4].is_kw("AS")) {
        if s.len() == 6 { return vec![s[2].clone()]; }
        return vec![Tok::num(format!("-{}", s[3].text))];
    }
    toks.to_vec()
}

fn type_end(v: &[Tok], start: usize) -> usize {
    let mut e = start + 1;
    if let Some(n) = next_sig(v, e) { if v[n].is_punct("(") { e = match_paren(v, n) + 1; } }
    // multi-word: WITH TIME ZONE / [] arrays
    if let Some(n) = next_sig(v, e) { if v[n].is_kw("WITH") { if let Some(z) = find_top_kw(v, "ZONE", n) { e = z + 1; } } }
    while let Some(n) = next_sig(v, e) { if v[n].is_punct("[") { e = n + 2; } else { break; } }
    e
}

fn type_tokens(raw: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    let s = strip_trivia(raw);
    if s.is_empty() { return vec![]; }
    let full = upper_text(raw).replace(' ', "");
    let name = if full.contains("WITHTIMEZONE") { if full.starts_with("TIME") && !full.starts_with("TIMESTAMP") { "TIMETZ".to_string() } else { "TIMESTAMPTZ".to_string() } } else { s[0].text.to_uppercase() };
    if name == "TIMETZ" { ctx.warn("TIME WITH TIME ZONE mapped to TIME(6); offset lost"); return t("TIME(6)"); }
    if full.ends_with("[]") { ctx.warn("array type has no T-SQL equivalent; mapped to NVARCHAR(MAX) JSON"); return t("NVARCHAR(MAX)"); }
    if name == "STRUCT" || name == "MAP" || name == "UNION" || name == "LIST" { ctx.warn(format!("{name} type mapped to NVARCHAR(MAX) JSON")); return t("NVARCHAR(MAX)"); }
    let args: Vec<Vec<Tok>> = if s.len() > 2 && s[1].is_punct("(") { split_top(&s[2..s.len() - 1], ",") } else { vec![] };
    match duck_type_to_tsql_v(&name, &args, ctx.target_version) { Some((ty, w)) => { if let Some(w) = w { ctx.warn(w); } ty } None => s.clone() }
}

fn limit_to_top(seg: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    let sel = match find_top_kw(seg, "SELECT", 0) { Some(s) => s, None => return seg.to_vec() };
    let mut v: Vec<Tok> = seg.to_vec();
    let mut limit: Option<Vec<Tok>> = None;
    let mut offset: Option<Vec<Tok>> = None;
    let mut percent = false;
    // LIMIT must be top-level and after any ORDER BY
    if let Some(l) = find_top_kw(&v, "LIMIT", 0) {
        let n = next_sig(&v, l + 1).unwrap_or(l + 1);
        let mut e = atom_end(&v, n);
        let mut lim: Vec<Tok> = v[n..e].to_vec();
        if let Some(p) = next_sig(&v, e) { if v[p].is_op("%") { percent = true; e = p + 1; } }
        if lim.len() == 1 && lim[0].kind == Kind::Num && lim[0].text.ends_with('%') { percent = true; lim[0].text.pop(); }
        if let Some(o) = next_sig(&v, e) {
            if v[o].is_kw("OFFSET") {
                let on = next_sig(&v, o + 1).unwrap_or(o + 1);
                let oe = atom_end(&v, on);
                offset = Some(v[on..oe].to_vec());
                e = oe;
            }
        }
        limit = Some(lim);
        let e = e.min(v.len());
        v.drain(l..e);
        while v.last().map(|x| x.is_trivia()).unwrap_or(false) { v.pop(); }
    } else if let Some(o) = find_top_kw(&v, "OFFSET", 0) {
        let on = next_sig(&v, o + 1).unwrap_or(o + 1);
        let oe = atom_end(&v, on);
        offset = Some(v[on..oe].to_vec());
        // LIMIT after OFFSET (DuckDB allows OFFSET n LIMIT m)
        let mut e = oe;
        if let Some(l) = next_sig(&v, oe) { if v[l].is_kw("LIMIT") { let n = next_sig(&v, l + 1).unwrap_or(l + 1); let le = atom_end(&v, n); limit = Some(v[n..le].to_vec()); e = le; } }
        v.drain(o..e.min(v.len()));
    }
    while v.last().map(|x| x.is_trivia()).unwrap_or(false) { v.pop(); }
    match (limit, offset) {
        (Some(l), None) => {
            let mut ins = sel + 1;
            if let Some(n) = next_sig(&v, sel + 1) { if v[n].is_any_kw(&["DISTINCT", "ALL"]) { ins = n + 1; } }
            let mut top = t(" TOP (");
            top.extend(trim(&l).iter().cloned());
            top.push(Tok::punct(")"));
            if percent { top.extend(t(" PERCENT")); }
            if !v.get(ins).map(|x| x.is_trivia()).unwrap_or(false) { top.push(Tok::ws()); }
            v.splice(ins..ins, top);
            if find_top_kw(&v, "ORDER", 0).is_none() { ctx.warn("LIMIT without ORDER BY: TOP returns arbitrary rows"); }
        }
        (l, Some(o)) => {
            if find_top_kw(&v, "ORDER", 0).is_none() { v.extend(t(" ORDER BY (SELECT NULL)")); }
            v.extend(t(" OFFSET "));
            v.extend(trim(&o).iter().cloned());
            v.extend(t(" ROWS"));
            if let Some(l) = l { v.extend(t(" FETCH NEXT ")); v.extend(trim(&l).iter().cloned()); v.extend(t(" ROWS ONLY")); }
        }
        (None, None) => {}
    }
    v
}
