//! T-SQL -> DuckDB SQL rules.
use super::engine::*;
use super::tokens::*;
use super::types::*;

pub struct R;

const HINTS: &[&str] = &["NOLOCK", "READUNCOMMITTED", "READCOMMITTED", "REPEATABLEREAD", "SERIALIZABLE", "ROWLOCK", "PAGLOCK", "TABLOCK",
    "TABLOCKX", "UPDLOCK", "XLOCK", "HOLDLOCK", "NOWAIT", "READPAST", "FORCESEEK", "FORCESCAN", "INDEX", "NOEXPAND", "SNAPSHOT"];
const STRING_FUNCS: &[&str] = &["LTRIM", "RTRIM", "TRIM", "UPPER", "LOWER", "SUBSTRING", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "CONCAT",
    "CONCAT_WS", "STR", "FORMAT", "REPLICATE", "REPEAT", "SPACE", "CHAR", "CHR", "NCHAR", "QUOTENAME", "REVERSE", "STUFF", "STRING_AGG",
    "TRANSLATE", "COALESCE", "ISNULL", "IFNULL", "STRFTIME", "RPAD", "LPAD"];

pub fn statement(st: &[Tok], ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    let st = trim(st);
    let v = Stmt::new(st);
    if v.is_empty() { return vec![]; }
    let first = v.kw(0);
    let second = v.kw(1);
    ctx.in_ddl = false;
    let mut out: Vec<Vec<Tok>> = vec![];
    let body: Vec<Tok> = match first.as_str() {
        "SET" if v.tok(1).map(|t| t.kind == Kind::Var).unwrap_or(false) => {
            // SET @x = expr  ->  SET VARIABLE x = expr
            let name = v.tok(1).unwrap().text.trim_start_matches('@').to_string();
            ctx.variables.insert(name.clone());
            let eq = st.iter().position(|t| t.is_op("=")).unwrap_or(v.pos(2));
            let expr = rewrite(trim(&st[eq + 1..]), &R, ctx);
            fmt(&format!("SET VARIABLE {name} = {{}}"), &[&expr])
        }
        "SET" => {
            ctx.warn(format!("session option dropped: {}", join(st).trim()));
            t(&format!("-- (dropped) {}", join(st).trim().replace('\n', " ")))
        }
        "USE" => t(&format!("-- (dropped) {}", join(st).trim())),
        "DECLARE" => return declare(&v, ctx),
        "PRINT" => fmt("SELECT {} AS message", &[&rewrite(trim(v.from(1)), &R, ctx)]),
        "BEGIN" if matches!(second.as_str(), "TRAN" | "TRANSACTION") => t("BEGIN TRANSACTION"),
        "BEGIN" => return procedural(st, ctx),
        "COMMIT" => t("COMMIT"),
        "ROLLBACK" => t("ROLLBACK"),
        "IF" => return if_statement(st, ctx),
        "WHILE" | "RETURN" | "RAISERROR" | "THROW" | "EXEC" | "EXECUTE" | "GOTO" | "WAITFOR" | "END" | "ELSE" | "BREAK" | "CONTINUE" | "OPEN" | "FETCH" | "CLOSE" | "DEALLOCATE" | "BULK" | "BACKUP" | "RESTORE" | "DBCC" | "GRANT" | "REVOKE" | "DENY" => return procedural(st, ctx),
        "CREATE" if matches!(second.as_str(), "PROCEDURE" | "PROC" | "FUNCTION" | "TRIGGER") => return procedural(st, ctx),
        "CREATE" if second == "OR" => {
            // CREATE OR ALTER VIEW -> CREATE OR REPLACE VIEW
            if matches!(v.kw(3).as_str(), "PROCEDURE" | "PROC" | "FUNCTION" | "TRIGGER") { return procedural(st, ctx); }
            let mut w = v.all();
            if v.kw(2) == "ALTER" { w[v.pos(2)] = Tok::ident("REPLACE"); }
            rewrite(&w, &R, ctx)
        }
        "CREATE" if second == "TABLE" => create_table(st, ctx),
        "CREATE" if matches!(second.as_str(), "INDEX" | "UNIQUE" | "CLUSTERED" | "NONCLUSTERED") => {
            let w: Vec<Tok> = st.iter().filter(|t| !t.is_any_kw(&["CLUSTERED", "NONCLUSTERED"])).cloned().collect();
            let mut r = rewrite(&w, &R, ctx);
            drop_trailing_with(&mut r);
            r
        }
        "ALTER" if second == "TABLE" => { ctx.in_ddl = true; let r = rewrite(st, &R, ctx); ctx.in_ddl = false; r }
        "SELECT" | "WITH" => select_into(st, ctx),
        "MERGE" => {
            ctx.warn("MERGE translated structurally; verify WHEN NOT MATCHED BY SOURCE/TARGET semantics");
            let w: Vec<Tok> = remove_seq(st, &["BY", "TARGET"]);
            rewrite(&w, &R, ctx)
        }
        "DELETE" if v.len() > 2 && !v.tok(1).unwrap().is_kw("FROM") && !v.tok(1).unwrap().is_kw("TOP") => delete_alias(st, ctx),
        "UPDATE" => update_from(st, ctx),
        _ => rewrite(st, &R, ctx),
    };
    out.append(&mut ctx.pre_statements);
    out.push(body);
    out
}

pub fn expression(toks: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    rewrite(trim(toks), &R, ctx)
}

fn procedural(st: &[Tok], ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    let text = join(st).trim().to_string();
    let head: String = text.split_whitespace().take(3).collect::<Vec<_>>().join(" ");
    ctx.warn(format!("procedural T-SQL has no DuckDB equivalent; left as a comment: {head} ..."));
    vec![t(&format!("/* UNTRANSLATED T-SQL (procedural):\n{}\n*/", text.replace("*/", "* /")))]
}

fn remove_seq(s: &[Tok], seq: &[&str]) -> Vec<Tok> {
    let mut out = vec![];
    let mut i = 0;
    while i < s.len() {
        if s[i].is_kw(seq[0]) {
            // match the sequence across trivia
            let mut k = i; let mut ok = true; let mut last = i;
            for w in seq { match next_sig(s, k) { Some(p) if s[p].is_kw(w) => { last = p; k = p + 1; } _ => { ok = false; break; } } }
            if ok { i = last + 1; continue; }
        }
        out.push(s[i].clone());
        i += 1;
    }
    out
}

fn drop_trailing_with(r: &mut Vec<Tok>) {
    // CREATE INDEX ... WITH (FILLFACTOR = 80) ON [PRIMARY]
    if let Some(i) = find_top_kw(r, "WITH", 0) { if next_sig(r, i + 1).map(|p| r[p].is_punct("(")).unwrap_or(false) { r.truncate(i); } }
    if let Some(i) = find_top_kw(r, "ON", 0) {
        if let Some(n) = next_sig(r, i + 1) { if r[n].kind == Kind::QIdent && r[n].value().eq_ignore_ascii_case("PRIMARY") { r.truncate(i); } }
    }
}

fn declare(v: &Stmt, ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    let mut out = vec![];
    for decl in split_top(v.from(1), ",") {
        let d = trim(&decl);
        let ds = Stmt::new(d);
        if ds.is_empty() || ds.tok(0).unwrap().kind != Kind::Var { continue; }
        let name = ds.tok(0).unwrap().text.trim_start_matches('@').to_string();
        if ds.kw(1) == "TABLE" {
            ctx.temp_tables.insert(name.clone());
            ctx.in_ddl = true;
            let cols = rewrite(trim(ds.from(2)), &R, ctx);
            ctx.in_ddl = false;
            out.push(fmt(&format!("CREATE TEMP TABLE {} {{}}", quote_if_needed(&name)), &[&cols]));
            continue;
        }
        if ds.kw(1) == "CURSOR" { ctx.warn("cursors are not supported in DuckDB"); continue; }
        ctx.variables.insert(name.clone());
        match d.iter().position(|t| t.is_op("=")) {
            Some(eq) => {
                let expr = rewrite(trim(&d[eq + 1..]), &R, ctx);
                out.push(fmt(&format!("SET VARIABLE {name} = {{}}"), &[&expr]));
            }
            None => out.push(t(&format!("SET VARIABLE {name} = NULL"))),
        }
    }
    out
}

fn if_statement(st: &[Tok], ctx: &mut Ctx) -> Vec<Vec<Tok>> {
    // IF OBJECT_ID('x') IS NOT NULL DROP TABLE x   /  IF EXISTS (...) DROP TABLE x
    if let Some(d) = find_top_kw(st, "DROP", 0) {
        let mut r = rewrite(trim(&st[d..]), &R, ctx);
        if let Some(k) = r.iter().position(|t| t.is_any_kw(&["TABLE", "VIEW", "INDEX", "SEQUENCE", "SCHEMA"])) {
            r.splice(k + 1..k + 1, t(" IF EXISTS"));
        }
        return vec![r];
    }
    procedural(st, ctx)
}

fn quote_if_needed(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') { name.to_string() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
}

fn create_table(st: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    ctx.in_ddl = true;
    let v: Vec<Tok> = st.to_vec();
    let temp = v.iter().any(|tk| tk.kind == Kind::Temp || (tk.kind == Kind::QIdent && tk.value().starts_with('#')));
    let tbl_idx = v.iter().position(|t| t.is_kw("TABLE")).unwrap_or(1);
    let open = v.iter().position(|t| t.is_punct("(")).unwrap_or(v.len());
    let tname: String = v[tbl_idx + 1..open].iter().filter(|t| !t.is_trivia()).map(|t| t.value()).filter(|x| x != ".").last().unwrap_or_default();
    let tname = tname.trim_start_matches('#').to_string();
    let mut i = 0;
    let mut out: Vec<Tok> = vec![];
    let mut col_name = String::new();
    let mut depth = 0i32;
    while i < v.len() {
        let tk = &v[i];
        if tk.is_punct("(") { depth += 1; } else if tk.is_punct(")") { depth -= 1; }
        if depth == 1 && (tk.is_punct("(") || tk.is_punct(",")) {
            if let Some(n) = next_sig(&v, i + 1) { col_name = v[n].value(); }
        }
        if tk.is_kw("IDENTITY") && depth == 1 {
            let (seed, inc) = match next_sig(&v, i + 1) {
                Some(p) if v[p].is_punct("(") => {
                    let close = match_paren(&v, p);
                    let args = split_top(&v[p + 1..close], ",");
                    let g = |k: usize| args.get(k).and_then(|a| as_num(a)).unwrap_or("1".into());
                    i = close;
                    (g(0), g(1))
                }
                _ => ("1".into(), "1".into()),
            };
            let seq = format!("seq_{}_{}", tname, col_name).chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect::<String>();
            ctx.pre_statements.push(t(&format!("CREATE SEQUENCE IF NOT EXISTS {seq} START {seed} INCREMENT {inc}")));
            out.extend(t(&format!("DEFAULT nextval('{seq}')")));
            i += 1;
            continue;
        }
        if tk.is_any_kw(&["CLUSTERED", "NONCLUSTERED"]) { while out.last().map(|x| x.is_trivia()).unwrap_or(false) { out.pop(); } i += 1; continue; }
        if tk.is_kw("COLLATE") { if let Some(n) = next_sig(&v, i + 1) { i = n + 1; continue; } }
        if depth == 0 && i > open && tk.is_any_kw(&["ON", "WITH", "TEXTIMAGE_ON"]) { break; }
        out.push(tk.clone());
        i += 1;
    }
    let mut r = rewrite(trim(&out), &R, ctx);
    ctx.in_ddl = false;
    if temp {
        ctx.temp_tables.insert(tname);
        if let Some(k) = r.iter().position(|t| t.is_kw("TABLE")) { r.splice(k..k, t("TEMP ")); }
    }
    r
}

fn select_into(st: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    // SELECT ... INTO [#]t FROM ...  ->  CREATE [TEMP] TABLE t AS SELECT ... FROM ...
    if let Some(into) = find_top_kw(st, "INTO", 0) {
        let before_sel = find_top_kw(st, "SELECT", 0).map(|k| k < into).unwrap_or(false);
        let is_insert = st.first().map(|t| t.is_kw("INSERT")).unwrap_or(false);
        if before_sel && !is_insert {
            let name_end = ident_end(st, into + 1);
            let name_toks = &st[into + 1..name_end];
            let is_temp = name_toks.iter().any(|t| t.kind == Kind::Temp || (t.kind == Kind::QIdent && t.value().starts_with('#')));
            let name: String = name_toks.iter().filter(|t| !t.is_trivia()).map(|t| t.value().trim_start_matches('#').to_string()).collect();
            let mut rest: Vec<Tok> = st[..into].to_vec();
            rest.extend(st[name_end..].iter().cloned());
            if is_temp { ctx.temp_tables.insert(name.clone()); }
            let body = rewrite(trim(&rest), &R, ctx);
            let kw = if is_temp { "CREATE TEMP TABLE" } else { "CREATE TABLE" };
            return fmt(&format!("{kw} {} AS {{}}", schema_mapped(&name, ctx)), &[&body]);
        }
    }
    rewrite(st, &R, ctx)
}

fn schema_mapped(name: &str, ctx: &Ctx) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 2 {
        if let Some(m) = ctx.schema_map.get(parts[0]) { return format!("{m}.{}", parts[1]); }
    }
    name.to_string()
}

fn wrap_conditions(v: &mut Vec<Tok>, cond: &[Tok], wh: &[Tok]) {
    let cond = trim(cond); let wh = trim(wh);
    if cond.is_empty() && wh.is_empty() { return; }
    v.extend(t(" WHERE "));
    if !cond.is_empty() { v.push(Tok::punct("(")); v.extend(cond.iter().cloned()); v.push(Tok::punct(")")); }
    if !cond.is_empty() && !wh.is_empty() { v.extend(t(" AND ")); }
    if !wh.is_empty() { v.push(Tok::punct("(")); v.extend(wh.iter().cloned()); v.push(Tok::punct(")")); }
}

fn delete_alias(st: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    // DELETE a FROM tbl a JOIN o ON cond WHERE w  ->  DELETE FROM tbl a USING o WHERE cond AND w
    let from = match find_top_kw(st, "FROM", 0) { Some(f) => f, None => return rewrite(st, &R, ctx) };
    let after = &st[from + 1..];
    let where_pos = find_top_kw(after, "WHERE", 0);
    let joins = find_all_top(after, &["JOIN"]);
    if joins.is_empty() {
        let mut v = t("DELETE FROM ");
        v.extend(trim(after).iter().cloned());
        return rewrite(&v, &R, ctx);
    }
    let first_join = joins[0];
    let target: Vec<Tok> = after[..first_join].iter().filter(|t| !t.is_any_kw(&["INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS"])).cloned().collect();
    let on = find_top_kw(after, "ON", first_join).unwrap_or(after.len());
    let using_tbl: Vec<Tok> = after[first_join + 1..on].to_vec();
    let cond_end = where_pos.unwrap_or(after.len());
    let cond: Vec<Tok> = if on < after.len() { after[on + 1..cond_end].to_vec() } else { vec![] };
    let wh: Vec<Tok> = where_pos.map(|w| after[w + 1..].to_vec()).unwrap_or_default();
    if joins.len() > 1 { ctx.warn("DELETE with multiple JOINs: only the first join was converted to USING; verify"); }
    let mut v = t("DELETE FROM ");
    v.extend(trim(&target).iter().cloned());
    v.extend(t(" USING "));
    v.extend(trim(&using_tbl).iter().cloned());
    wrap_conditions(&mut v, &cond, &wh);
    rewrite(&v, &R, ctx)
}

fn update_from(st: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    // UPDATE a SET ... FROM tbl a JOIN o ON c WHERE w -> UPDATE tbl AS a SET ... FROM o WHERE c AND w
    let set = match find_top_kw(st, "SET", 0) { Some(k) => k, None => return rewrite(st, &R, ctx) };
    let from = match find_top_kw(st, "FROM", set) { Some(f) => f, None => return rewrite(st, &R, ctx) };
    let target_alias = strip_trivia(&st[1..set]).iter().map(|t| t.value()).collect::<Vec<_>>().join("");
    let after = &st[from + 1..];
    let joins = find_all_top(after, &["JOIN"]);
    let where_pos = find_top_kw(after, "WHERE", 0);
    if joins.is_empty() { return rewrite(st, &R, ctx); }
    let first_join = joins[0];
    let first_tbl: Vec<Tok> = strip_trivia(&after[..first_join]).into_iter().filter(|t| !t.is_any_kw(&["INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS"])).collect();
    let first_alias = first_tbl.last().map(|t| t.value()).unwrap_or_default();
    if !first_alias.eq_ignore_ascii_case(&target_alias) {
        ctx.warn("UPDATE ... FROM with JOIN where the target is not the first table: rewritten best-effort; verify");
    }
    let on = find_top_kw(after, "ON", first_join).unwrap_or(after.len());
    let other: Vec<Tok> = after[first_join + 1..on].to_vec();
    let cond_end = where_pos.unwrap_or(after.len());
    let cond: Vec<Tok> = if on < after.len() { after[on + 1..cond_end].to_vec() } else { vec![] };
    let wh: Vec<Tok> = where_pos.map(|w| after[w + 1..].to_vec()).unwrap_or_default();
    if joins.len() > 1 { ctx.warn("UPDATE with multiple JOINs: only the first join was converted; verify"); }
    let mut v = t("UPDATE ");
    let tbl: Vec<Tok> = first_tbl[..first_tbl.len().saturating_sub(1)].to_vec();
    if tbl.is_empty() { v.extend(first_tbl.clone()); } else { for (i, x) in tbl.iter().enumerate() { if i > 0 && !x.is_punct(".") && !tbl[i - 1].is_punct(".") { v.push(Tok::ws()); } v.push(x.clone()); } v.extend(t(" AS ")); v.push(Tok::ident(&first_alias)); }
    v.extend(t(" SET "));
    v.extend(trim(&st[set + 1..from]).iter().cloned());
    v.extend(t(" FROM "));
    v.extend(trim(&other).iter().cloned());
    wrap_conditions(&mut v, &cond, &wh);
    rewrite(&v, &R, ctx)
}

impl Rules for R {
    fn func(&self, name: &str, raw: &[Vec<Tok>], a: &[Vec<Tok>], ctx: &mut Ctx) -> Option<Vec<Tok>> {
        let n = a.len();
        let arg = |i: usize| -> &[Tok] { a.get(i).map(|v| v.as_slice()).unwrap_or(&[]) };
        let joined = || -> Vec<Tok> { let mut v = vec![]; for (i, x) in a.iter().enumerate() { if i > 0 { v.extend(t(", ")); } v.extend(x.iter().cloned()); } v };
        let call = |f: &str| -> Vec<Tok> { let mut v = t(f); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v };
        // type used as a function-like name: NVARCHAR(50), DECIMAL(18,2) ...
        if ctx.in_ddl || name == "NVARCHAR" || name == "VARCHAR" || name == "NCHAR" || name == "CHAR" || name == "DECIMAL" || name == "NUMERIC" || name == "DATETIME2" || name == "VARBINARY" || name == "BINARY" || name == "DATETIMEOFFSET" || name == "TIME" || name == "FLOAT" {
            if let Some(ty) = tsql_type_to_duck(name, raw) {
                // CHAR(n) with a numeric arg is the chr() function outside DDL; TIME(n)/FLOAT(n) are types
                if !(name == "CHAR" && !ctx.in_ddl) { return Some(ty); }
            }
        }
        Some(match name {
            "ISNULL" => fmt("COALESCE({}, {})", &[arg(0), arg(1)]),
            "IIF" => fmt("CASE WHEN {} THEN {} ELSE {} END", &[arg(0), arg(1), arg(2)]),
            "GETDATE" | "SYSDATETIME" | "CURRENT_TIMESTAMP" => t("current_localtimestamp()"),
            "GETUTCDATE" | "SYSUTCDATETIME" => t("(now() AT TIME ZONE 'UTC')"),
            "SYSDATETIMEOFFSET" => t("now()"),
            "LEN" => fmt("length(rtrim({}))", &[arg(0)]),
            "DATALENGTH" => fmt("strlen({})", &[arg(0)]),
            "CHARINDEX" => {
                if n > 2 { ctx.warn("CHARINDEX start position argument ignored"); }
                fmt("strpos({}, {})", &[arg(1), arg(0)])
            }
            "PATINDEX" => { ctx.warn("PATINDEX has no direct DuckDB equivalent; converted to a LIKE-based position guess"); fmt("CASE WHEN {} LIKE {} THEN 1 ELSE 0 END", &[arg(1), arg(0)]) }
            "REPLICATE" => fmt("repeat({}, {})", &[arg(0), arg(1)]),
            "SPACE" => fmt("repeat(' ', {})", &[arg(0)]),
            "STUFF" => fmt("concat(substr({}, 1, {} - 1), {}, substr({}, {} + {}))", &[arg(0), arg(1), arg(3), arg(0), arg(1), arg(2)]),
            "STR" => { if n > 1 { ctx.warn("STR() length/decimals arguments ignored"); } fmt("CAST({} AS VARCHAR)", &[arg(0)]) }
            "CHAR" | "NCHAR" => fmt("chr({})", &[arg(0)]),
            "UNICODE" | "ASCII" => fmt("ord({})", &[arg(0)]),
            "QUOTENAME" => fmt("('\"' || replace({}, '\"', '\"\"') || '\"')", &[arg(0)]),
            "NEWID" => t("uuid()"),
            "NEWSEQUENTIALID" => { ctx.warn("NEWSEQUENTIALID() replaced by uuid()"); t("uuid()") }
            "SQUARE" => fmt("(({}) * ({}))", &[arg(0), arg(0)]),
            "LOG" => if n == 2 { fmt("log({}, {})", &[arg(1), arg(0)]) } else { fmt("ln({})", &[arg(0)]) },
            "RAND" => t("random()"),
            "COUNT_BIG" => call("count"),
            "ROUND" if n == 3 => { ctx.warn("ROUND(x, n, 1) truncation form converted to trunc"); fmt("(trunc(({}) * power(10, {})) / power(10, {}))", &[arg(0), arg(1), arg(1)]) }
            "ISNUMERIC" => fmt("(TRY_CAST({} AS DOUBLE) IS NOT NULL)", &[arg(0)]),
            "ISDATE" => fmt("(TRY_CAST({} AS TIMESTAMP) IS NOT NULL)", &[arg(0)]),
            "CHOOSE" => { let mut v = fmt("list_value(", &[]); for (i, x) in a.iter().skip(1).enumerate() { if i > 0 { v.extend(t(", ")); } v.extend(x.iter().cloned()); } v.extend(t(")[")); v.extend(arg(0).iter().cloned()); v.extend(t("]")); v }
            "EOMONTH" => if n == 2 { fmt("last_day({} + to_months(CAST({} AS BIGINT)))", &[arg(0), arg(1)]) } else { fmt("last_day({})", &[arg(0)]) },
            "DATEFROMPARTS" => call("make_date"),
            "DATETIMEFROMPARTS" | "SMALLDATETIMEFROMPARTS" => { let ms = if n >= 7 { arg(6).to_vec() } else { t("0") }; fmt("make_timestamp({}, {}, {}, {}, {}, {} + ({}) / 1000.0)", &[arg(0), arg(1), arg(2), arg(3), arg(4), arg(5), &ms]) }
            "DATETIME2FROMPARTS" => { ctx.warn("DATETIME2FROMPARTS fractions/precision simplified"); fmt("make_timestamp({}, {}, {}, {}, {}, {})", &[arg(0), arg(1), arg(2), arg(3), arg(4), arg(5)]) }
            "DATEADD" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p));
                match part.and_then(|p| duck_interval(p, arg(1))) {
                    Some(iv) => fmt("({} + {})", &[arg(2), &iv]),
                    None => { ctx.warn(format!("DATEADD part {:?} not translated", join(arg(0)))); return None; }
                }
            }
            "DATEDIFF" | "DATEDIFF_BIG" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p));
                match part {
                    Some("dayofyear") | Some("dayofweek") => fmt("date_diff('day', {}, {})", &[arg(1), arg(2)]),
                    Some(p) => fmt(&format!("date_diff('{p}', {{}}, {{}})"), &[arg(1), arg(2)]),
                    None => { ctx.warn("DATEDIFF part not recognised"); return None; }
                }
            }
            "DATEPART" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p));
                match part {
                    Some("dayofweek") => fmt("(dayofweek({}) + 1)", &[arg(1)]),
                    Some(p) => fmt(&format!("date_part('{p}', {{}})"), &[arg(1)]),
                    None => { ctx.warn("DATEPART part not recognised"); return None; }
                }
            }
            "DATENAME" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p));
                match part {
                    Some("month") => fmt("monthname({})", &[arg(1)]),
                    Some("dayofweek") => fmt("dayname({})", &[arg(1)]),
                    Some(p) => fmt(&format!("CAST(date_part('{p}', {{}}) AS VARCHAR)"), &[arg(1)]),
                    None => return None,
                }
            }
            "DATETRUNC" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p)).unwrap_or("day");
                fmt(&format!("date_trunc('{part}', {{}})"), &[arg(1)])
            }
            "CONVERT" | "TRY_CONVERT" => {
                let f = if name == "CONVERT" { "CAST" } else { "TRY_CAST" };
                let ty = type_tokens(raw.first().map(|v| v.as_slice()).unwrap_or(&[]));
                let is_string_type = raw.first().map(|v| { let u = upper_text(v); u.starts_with("VARCHAR") || u.starts_with("NVARCHAR") || u.starts_with("CHAR") || u.starts_with("NCHAR") }).unwrap_or(false);
                if n == 3 && is_string_type {
                    if let Some(style) = as_num(arg(2)).and_then(|s| convert_style(&s)) {
                        return Some(fmt(&format!("strftime({{}}, '{style}')"), &[arg(1)]));
                    }
                    ctx.warn("CONVERT style not recognised; style ignored");
                }
                fmt(&format!("{f}({{}} AS {{}})"), &[arg(1), &ty])
            }
            "CAST" | "TRY_CAST" => {
                // CAST(x AS type)
                let r = raw.first().map(|v| v.as_slice()).unwrap_or(&[]);
                let as_pos = find_top_kw(r, "AS", 0)?;
                let expr = rewrite(trim(&r[..as_pos]), self, ctx);
                let ty = type_tokens(trim(&r[as_pos + 1..]));
                fmt(&format!("{name}({{}} AS {{}})"), &[&expr, &ty])
            }
            "FORMAT" => {
                match as_str_lit(arg(1)) {
                    Some(f) if f.chars().any(|c| "yMdHhmsf".contains(c)) && !f.chars().any(|c| "#0,.CNPE".contains(c)) => {
                        let sf = dotnet_to_strftime(&f);
                        fmt(&format!("strftime({{}}, '{sf}')"), &[arg(0)])
                    }
                    _ => { ctx.warn("FORMAT() numeric/custom formats are not translated; verify output"); fmt("format('{{}}', {})", &[arg(0)]) }
                }
            }
            "STRING_SPLIT" => { if n == 3 { ctx.warn("STRING_SPLIT ordinal column not available; use unnest(...) WITH ORDINALITY manually"); } fmt("unnest(string_split({}, {}))", &[arg(0), arg(1)]) }
            "DATE_BUCKET" => {
                let part = as_ident(raw.first().map(|v| v.as_slice()).unwrap_or(&[])).and_then(|p| datepart(&p)).unwrap_or("day");
                let iv = duck_interval(part, arg(1))?;
                if n >= 4 { fmt("time_bucket({}, {}, {})", &[&iv, arg(2), arg(3)]) } else { fmt("time_bucket({}, {})", &[&iv, arg(2)]) }
            }
            "APPROX_PERCENTILE_CONT" | "APPROX_PERCENTILE_DISC" => { let mut v = t("approx_quantile"); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v }
            "JSON_OBJECT" => {
                // JSON_OBJECT('k': v, ...) -> json_object('k', v, ...)
                let mut v = t("json_object(");
                for (i, r) in raw.iter().enumerate() {
                    if i > 0 { v.extend(t(", ")); }
                    let parts = split_top(trim(r), ":");
                    if parts.len() == 2 { v.extend(rewrite(trim(&parts[0]), self, ctx)); v.extend(t(", ")); v.extend(rewrite(trim(&parts[1]), self, ctx)); } else { v.extend(a[i].iter().cloned()); }
                }
                v.push(Tok::punct(")")); v
            }
            "JSON_ARRAY" => call("json_array"),
            "JSON_PATH_EXISTS" => fmt("CASE WHEN json_exists({}, {}) THEN 1 ELSE 0 END", &[arg(0), arg(1)]),
            "REGEXP_LIKE" => if n >= 3 { fmt("regexp_matches({}, {}, {})", &[arg(0), arg(1), arg(2)]) } else { fmt("regexp_matches({}, {})", &[arg(0), arg(1)]) },
            "REGEXP_REPLACE" => { if n > 3 { ctx.warn("REGEXP_REPLACE start/occurrence arguments ignored"); } fmt("regexp_replace({}, {}, {}, 'g')", &[arg(0), arg(1), arg(2)]) }
            "REGEXP_SUBSTR" => { if n > 2 { ctx.warn("REGEXP_SUBSTR start/occurrence arguments ignored"); } fmt("regexp_extract({}, {})", &[arg(0), arg(1)]) }
            "REGEXP_COUNT" => fmt("len(regexp_extract_all({}, {}))", &[arg(0), arg(1)]),
            "REGEXP_INSTR" => { ctx.warn("REGEXP_INSTR approximated with strpos(regexp_extract())"); fmt("strpos({}, regexp_extract({}, {}))", &[arg(0), arg(0), arg(1)]) }
            "REGEXP_SPLIT_TO_TABLE" => fmt("unnest(regexp_split_to_array({}, {}))", &[arg(0), arg(1)]),
            "BASE64_ENCODE" => fmt("base64({})", &[arg(0)]),
            "BASE64_DECODE" => fmt("from_base64({})", &[arg(0)]),
            "LEFT_SHIFT" => fmt("({} << {})", &[arg(0), arg(1)]),
            "RIGHT_SHIFT" => fmt("({} >> {})", &[arg(0), arg(1)]),
            "BIT_COUNT" => call("bit_count"),
            "GENERATE_SERIES" | "GREATEST" | "LEAST" | "LTRIM" | "RTRIM" | "DATETRUNC_PASS" => return None,
            "JSON_VALUE" => fmt("json_extract_string({}, {})", &[arg(0), arg(1)]),
            "JSON_QUERY" => fmt("json_extract({}, {})", &[arg(0), arg(1)]),
            "ISJSON" => fmt("CASE WHEN json_valid({}) THEN 1 ELSE 0 END", &[arg(0)]),
            "HASHBYTES" => {
                match as_str_lit(arg(0)).map(|s| s.to_uppercase()).as_deref() {
                    Some("MD5") => { ctx.warn("HASHBYTES returns VARBINARY; md5() returns a hex string"); fmt("md5({})", &[arg(1)]) }
                    Some("SHA2_256") => { ctx.warn("HASHBYTES returns VARBINARY; sha256() returns a hex string"); fmt("sha256({})", &[arg(1)]) }
                    _ => { ctx.warn("HASHBYTES algorithm not supported in DuckDB"); return None; }
                }
            }
            "CHECKSUM" | "BINARY_CHECKSUM" => { ctx.warn("CHECKSUM replaced by hash(); values differ"); call("hash") }
            "OBJECT_ID" | "OBJECT_NAME" | "DB_NAME" | "SUSER_SNAME" | "USER_NAME" | "HOST_NAME" | "APP_NAME" | "SCOPE_IDENTITY" | "IDENT_CURRENT" | "ORIGINAL_LOGIN" | "SESSION_USER" | "SYSTEM_USER" | "SERVERPROPERTY" => {
                ctx.warn(format!("{name}() has no DuckDB equivalent; left as-is")); return None;
            }
            "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                // handled with WITHIN GROUP in group(); keep marker
                let f = if name == "PERCENTILE_CONT" { "quantile_cont" } else { "quantile_disc" };
                let mut v = t(f); v.push(Tok::punct("(")); v.extend(joined()); v.push(Tok::punct(")")); v
            }
            "STRING_AGG" => call("string_agg"),
            "TRIM" => {
                // TRIM(chars FROM x) is the same in DuckDB? DuckDB: trim(x, chars)
                let r = raw.first().map(|v| v.as_slice()).unwrap_or(&[]);
                if let Some(f) = find_top_kw(r, "FROM", 0) {
                    let chars = rewrite(trim(&r[..f]), self, ctx);
                    let x = rewrite(trim(&r[f + 1..]), self, ctx);
                    fmt("trim({}, {})", &[&x, &chars])
                } else { return None; }
            }
            "SUBSTRING" => call("substring"),
            "ATN2" => call("atan2"),
            "CEILING" => call("ceiling"),
            "SIGN" | "ABS" | "FLOOR" | "SQRT" | "POWER" | "EXP" | "LOG10" | "PI" | "COS" | "SIN" | "TAN" | "ACOS" | "ASIN" | "ATAN" | "DEGREES" | "RADIANS" => return None,
            "YEAR" | "MONTH" | "DAY" | "UPPER" | "LOWER" | "REPLACE" | "REVERSE" | "LEFT" | "RIGHT" | "CONCAT" | "CONCAT_WS" | "COALESCE" | "NULLIF" | "TRANSLATE" => return None,
            _ => return None,
        })
    }

    fn token(&self, tok: &Tok, prev: Option<&Tok>, next: Option<&Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        match tok.kind {
            Kind::QIdent => {
                let v = tok.value();
                if let (Some(m), true) = (ctx.schema_map.get(&v), next.map(|n| n.is_punct(".")).unwrap_or(false)) { return vec![Tok::ident(m)]; }
                if v.starts_with('#') { let name = v.trim_start_matches('#').to_string(); ctx.temp_tables.insert(name.clone()); return vec![Tok::ident(&quote_if_needed(&name))]; }
                vec![Tok::new(Kind::QIdent, format!("\"{}\"", v.replace('"', "\"\"")))]
            }
            Kind::Temp => {
                let name = tok.text.trim_start_matches('#').to_string();
                ctx.temp_tables.insert(name.clone());
                vec![Tok::ident(&quote_if_needed(&name))]
            }
            Kind::Var => {
                let name = tok.text.trim_start_matches('@').to_string();
                if tok.text.starts_with("@@") { ctx.warn(format!("{} has no DuckDB equivalent", tok.text)); return vec![tok.clone()]; }
                if ctx.temp_tables.contains(&name) { return vec![Tok::ident(&quote_if_needed(&name))]; }
                t(&format!("getvariable('{name}')"))
            }
            Kind::Str => {
                if tok.text.starts_with('N') || tok.text.starts_with('n') { vec![Tok::new(Kind::Str, tok.text[1..].to_string())] } else { vec![tok.clone()] }
            }
            Kind::Ident => {
                let u = tok.text.to_uppercase();
                let next_is_dot = next.map(|n| n.is_punct(".")).unwrap_or(false);
                if next_is_dot { if let Some(m) = ctx.schema_map.get(&tok.text) { return vec![Tok::ident(m)]; } }
                // bare type names in DDL or after AS (CAST ... AS INT)
                let after_as = prev.map(|p| p.is_kw("AS")).unwrap_or(false);
                let next_is_paren = next.map(|n| n.is_punct("(")).unwrap_or(false);
                if (ctx.in_ddl || after_as) && !next_is_paren && u != "TABLE" {
                    if let Some(ty) = tsql_type_to_duck(&u, &[]) { return ty; }
                }
                match u.as_str() {
                    "LIKE" if ctx.nocase => t("ILIKE"),
                    "CURRENT_TIMESTAMP" => t("current_localtimestamp()"),
                    "ISNULL" | "GETDATE" if !next_is_paren => vec![tok.clone()],
                    "IDENTITY" if ctx.in_ddl => vec![], // handled in create_table; stray marker
                    _ => vec![tok.clone()],
                }
            }
            Kind::Op => match tok.text.as_str() {
                "!<" => t(">="),
                "!>" => t("<="),
                _ => vec![tok.clone()],
            },
            _ => vec![tok.clone()],
        }
    }

    fn group(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        let mut v = toks;
        // 1. drop table hints: WITH (NOLOCK, ...)
        let mut i = 0;
        while i < v.len() {
            if v[i].is_kw("WITH") {
                if let Some(p) = next_sig(&v, i + 1) {
                    if v[p].is_punct("(") {
                        let close = match_paren(&v, p);
                        let inner: Vec<Tok> = strip_trivia(&v[p + 1..close]);
                        if !inner.is_empty() && inner.iter().all(|x| x.is_any_kw(HINTS) || x.kind == Kind::Punct || x.is_op("=") || x.kind == Kind::Num || x.kind == Kind::QIdent || x.kind == Kind::Ident) && inner.iter().any(|x| x.is_any_kw(HINTS)) {
                            v.drain(i..=close);
                            continue;
                        }
                    }
                }
            }
            // OPTION (...) query hints at the end
            if v[i].is_kw("OPTION") {
                if let Some(p) = next_sig(&v, i + 1) { if v[p].is_punct("(") { let close = match_paren(&v, p); v.drain(i..=close); continue; } }
            }
            // COLLATE name -> drop (DuckDB collations differ); keep NOCASE hint when CI
            if v[i].is_kw("COLLATE") {
                if let Some(p) = next_sig(&v, i + 1) {
                    let name = v[p].value().to_uppercase();
                    if name.contains("_CI") { let rep = t("COLLATE NOCASE"); let l = rep.len(); v.splice(i..=p, rep); i += l; } else { v.drain(i..=p); }
                    continue;
                }
            }
            i += 1;
        }
        // 2. CROSS APPLY / OUTER APPLY -> LATERAL joins
        let mut i = 0;
        while i < v.len() {
            if v[i].is_kw("APPLY") {
                if let Some(p) = prev_sig(&v, i) {
                    let outer = v[p].is_kw("OUTER");
                    if v[p].is_kw("CROSS") || outer {
                        let rep = if outer { t("LEFT JOIN LATERAL") } else { t("CROSS JOIN LATERAL") };
                        let rep_len = rep.len();
                        v.splice(p..=i, rep);
                        if outer {
                            // insert ON TRUE after the lateral operand (+ optional alias)
                            let start = p + rep_len;
                            let mut e = atom_end(&v, start);
                            if let Some(n) = next_sig(&v, e) { if v[n].is_kw("AS") { e = atom_end(&v, n + 1); } else if v[n].kind == Kind::Ident && !v[n].is_any_kw(NOT_FUNCS) && !v[n].is_any_kw(&["WHERE", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "JOIN", "CROSS", "OUTER", "ON", "HAVING", "UNION"]) { e = n + 1; } }
                            // alias with column list: alias(c1, c2)
                            if let Some(n) = next_sig(&v, e) { if v[n].is_punct("(") { e = match_paren(&v, n) + 1; } }
                            v.splice(e..e, t(" ON TRUE"));
                        }
                        i = p;
                    }
                }
            }
            i += 1;
        }
        // 3. WITHIN GROUP (ORDER BY ...) -> move ORDER BY inside the preceding aggregate call
        let mut i = 0;
        while i < v.len() {
            if v[i].is_kw("WITHIN") {
                if let (Some(g), Some(p)) = (next_sig(&v, i + 1), prev_sig(&v, i)) {
                    if v[g].is_kw("GROUP") && v[p].is_punct(")") {
                        if let Some(o) = next_sig(&v, g + 1) {
                            if v[o].is_punct("(") {
                                let close = match_paren(&v, o);
                                let order: Vec<Tok> = v[o + 1..close].to_vec();
                                v.drain(i..=close);
                                // inject before the ')' at p
                                let mut inj = vec![Tok::ws()];
                                inj.extend(trim(&order).iter().cloned());
                                // quantile_cont(p) WITHIN GROUP (ORDER BY x) -> quantile_cont(x, p)
                                let fstart = atom_start(&v, p + 1);
                                let fname = v[fstart].text.to_lowercase();
                                if fname.starts_with("quantile_") || fname == "approx_quantile" {
                                    let open = v.iter().enumerate().skip(fstart).find(|(_, x)| x.is_punct("(")).map(|(k, _)| k).unwrap();
                                    let pct: Vec<Tok> = v[open + 1..p].to_vec();
                                    let ob = trim(&order).to_vec();
                                    let by = find_top_kw(&ob, "BY", 0).map(|k| ob[k + 1..].to_vec()).unwrap_or(ob.clone());
                                    let mut newargs = trim(&by).to_vec();
                                    newargs.extend(t(", "));
                                    newargs.extend(trim(&pct).iter().cloned());
                                    v.splice(open + 1..p, newargs);
                                } else {
                                    v.splice(p..p, inj);
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            i += 1;
        }
        // 4. '+' string concatenation -> '||' when a neighbour is clearly a string
        let mut i = 0;
        while i < v.len() {
            if v[i].is_op("+") {
                let l = prev_sig(&v, i);
                let r = next_sig(&v, i + 1);
                let stringy = |k: Option<usize>, v: &Vec<Tok>, left: bool| -> bool {
                    match k {
                        Some(k) => {
                            if v[k].kind == Kind::Str { return true; }
                            if left && v[k].is_punct(")") { let s = atom_start(v, k + 1); return v[s].kind == Kind::Ident && v[s].is_any_kw(STRING_FUNCS) || (v[s].is_kw("CAST") && cast_is_string(&v[s..=k])); }
                            if !left && v[k].kind == Kind::Ident && v[k].is_any_kw(STRING_FUNCS) { return true; }
                            if !left && v[k].is_kw("CAST") { let e = atom_end(v, k); return cast_is_string(&v[k..e]); }
                            false
                        }
                        None => false,
                    }
                };
                if stringy(l, &v, true) || stringy(r, &v, false) {
                    v[i] = Tok::op("||");
                }
            }
            i += 1;
        }
        let _ = ctx;
        v
    }

    fn select_segment(&self, toks: Vec<Tok>, ctx: &mut Ctx) -> Vec<Tok> {
        // Split on top-level set operators and handle each SELECT segment.
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
            out.extend(top_to_limit(&toks[a..b], ctx));
            last = b;
        }
        out
    }
}

fn cast_is_string(toks: &[Tok]) -> bool {
    let u = upper_text(toks);
    u.contains(" AS VARCHAR") || u.contains(" AS NVARCHAR") || u.contains(" AS CHAR") || u.contains(" AS NCHAR") || u.contains(" AS TEXT")
}

fn type_tokens(raw: &[Tok]) -> Vec<Tok> {
    let s = strip_trivia(raw);
    if s.is_empty() { return vec![]; }
    let name = s[0].text.to_uppercase();
    let args: Vec<Vec<Tok>> = if s.len() > 2 && s[1].is_punct("(") { split_top(&s[2..s.len() - 1], ",") } else { vec![] };
    tsql_type_to_duck(&name, &args).unwrap_or_else(|| s.clone())
}

fn top_to_limit(seg: &[Tok], ctx: &mut Ctx) -> Vec<Tok> {
    let sel = match find_top_kw(seg, "SELECT", 0) { Some(s) => s, None => return seg.to_vec() };
    let mut v: Vec<Tok> = seg.to_vec();
    let mut limit: Option<Vec<Tok>> = None;
    let mut percent = false;
    // SELECT [DISTINCT|ALL] TOP n [PERCENT] [WITH TIES]
    let mut k = next_sig(&v, sel + 1);
    if let Some(kk) = k { if v[kk].is_any_kw(&["DISTINCT", "ALL"]) { k = next_sig(&v, kk + 1); } }
    if let Some(kk) = k {
        if v[kk].is_kw("TOP") {
            let (n_toks, mut end) = match next_sig(&v, kk + 1) {
                Some(n) if v[n].is_punct("(") => { let c = match_paren(&v, n); (v[n + 1..c].to_vec(), c + 1) }
                Some(n) => (vec![v[n].clone()], n + 1),
                None => (vec![], kk + 1),
            };
            if let Some(p) = next_sig(&v, end) { if v[p].is_kw("PERCENT") { percent = true; end = p + 1; } }
            if let Some(p) = next_sig(&v, end) { if v[p].is_kw("WITH") { if let Some(q) = next_sig(&v, p + 1) { if v[q].is_kw("TIES") { ctx.warn("TOP ... WITH TIES not supported; ties dropped"); end = q + 1; } } } }
            let end = if end < v.len() && v[end].is_trivia() { end + 1 } else { end };
            v.drain(kk..end);
            limit = Some(n_toks);
        }
    }
    // OFFSET n ROWS [FETCH NEXT m ROWS ONLY]
    let mut offset: Option<Vec<Tok>> = None;
    if let Some(o) = find_top_kw(&v, "OFFSET", 0) {
        if let Some(n) = next_sig(&v, o + 1) {
            let n_end = atom_end(&v, n);
            if v.get(next_sig(&v, n_end).unwrap_or(v.len())).map(|x| x.is_any_kw(&["ROWS", "ROW"])).unwrap_or(false) {
                let rows = next_sig(&v, n_end).unwrap();
                offset = Some(v[n..n_end].to_vec());
                let mut end = rows + 1;
                if let Some(f) = next_sig(&v, end) {
                    if v[f].is_kw("FETCH") {
                        let m = next_sig(&v, f + 1).and_then(|x| next_sig(&v, x + 1)); // NEXT|FIRST m
                        if let Some(m) = m {
                            let m_end = atom_end(&v, m);
                            limit = Some(v[m..m_end].to_vec());
                            let mut e = m_end;
                            while let Some(x) = next_sig(&v, e) { if v[x].is_any_kw(&["ROWS", "ROW", "ONLY"]) { e = x + 1; } else { break; } }
                            end = e;
                        }
                    }
                }
                v.drain(o..end);
            }
        }
    }
    // trim trailing whitespace before appending
    while v.last().map(|x| x.is_trivia()).unwrap_or(false) { v.pop(); }
    if let Some(l) = limit {
        v.extend(t(" LIMIT "));
        v.extend(trim(&l).iter().cloned());
        if percent { v.push(Tok::op("%")); }
    }
    if let Some(o) = offset {
        v.extend(t(" OFFSET "));
        v.extend(trim(&o).iter().cloned());
    }
    v
}
