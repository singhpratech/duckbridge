//! Translator unit tests: T-SQL <-> DuckDB <-> PostgreSQL.
use duckbridge::core::duck;
use duckbridge::transpile::{codes, translate, translate_expr, translate_with, Dialect, NocaseMode, Options, TranslateError, Warning};
use std::collections::HashMap;

fn t2d(sql: &str) -> (String, Vec<String>) {
    let mut m = HashMap::new();
    m.insert("dbo".to_string(), "main".to_string());
    let r = translate(sql, Dialect::Tsql, Dialect::DuckDb, &m).unwrap();
    (r.sql, r.warnings)
}
fn d2t(sql: &str) -> (String, Vec<String>) {
    let mut m = HashMap::new();
    m.insert("main".to_string(), "dbo".to_string());
    let r = translate(sql, Dialect::DuckDb, Dialect::Tsql, &m).unwrap();
    (r.sql, r.warnings)
}
fn d2p(sql: &str) -> String { translate(sql, Dialect::DuckDb, Dialect::Postgres, &HashMap::new()).unwrap().sql }
fn p2d(sql: &str) -> String { translate(sql, Dialect::Postgres, Dialect::DuckDb, &HashMap::new()).unwrap().sql }
fn norm(s: &str) -> String { s.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase().replace(", ", ",").replace(" (", "(").replace("( ", "(").replace(" )", ")") }
fn assert_sql(got: &str, want: &str) { assert_eq!(norm(got), norm(want), "\n got: {got}\nwant: {want}"); }

// ------------------------------------------------------------------ T-SQL -> DuckDB
#[test] fn top_to_limit() { assert_sql(&t2d("SELECT TOP 5 a FROM t ORDER BY a").0, "SELECT a FROM t ORDER BY a LIMIT 5;"); }
#[test] fn top_paren_distinct() { assert_sql(&t2d("SELECT DISTINCT TOP (10) a FROM t").0, "SELECT DISTINCT a FROM t LIMIT 10;"); }
#[test] fn top_percent() { assert_sql(&t2d("SELECT TOP 10 PERCENT a FROM t").0, "SELECT a FROM t LIMIT 10%;"); }
#[test] fn top_in_subquery() { assert_sql(&t2d("SELECT * FROM (SELECT TOP 1 x FROM t ORDER BY x) s").0, "SELECT * FROM (SELECT x FROM t ORDER BY x LIMIT 1) s;"); }
#[test] fn offset_fetch() { assert_sql(&t2d("SELECT a FROM t ORDER BY a OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY").0, "SELECT a FROM t ORDER BY a LIMIT 10 OFFSET 20;"); }
#[test] fn union_tops() { assert_sql(&t2d("SELECT TOP 1 a FROM t UNION ALL SELECT TOP 2 b FROM u").0, "SELECT a FROM t LIMIT 1 UNION ALL SELECT b FROM u LIMIT 2;"); }
#[test] fn brackets_and_schema() { assert_sql(&t2d("SELECT [a], [b c] FROM dbo.[t] WITH (NOLOCK)").0, "SELECT \"a\", \"b c\" FROM main.\"t\";"); }
#[test] fn hints_dropped() { assert_sql(&t2d("SELECT a FROM t WITH (NOLOCK, INDEX(ix)) OPTION (RECOMPILE)").0, "SELECT a FROM t;"); }
#[test] fn isnull_iif() { assert_sql(&t2d("SELECT ISNULL(a, 0), IIF(a > 1, 'y', 'n') FROM t").0, "SELECT COALESCE(a, 0), CASE WHEN a > 1 THEN 'y' ELSE 'n' END FROM t;"); }
#[test] fn getdate_len() { assert_sql(&t2d("SELECT GETDATE(), LEN(name), NEWID() FROM t").0, "SELECT current_localtimestamp(), length(rtrim(name)), uuid() FROM t;"); }
#[test] fn dateadd_datediff() {
    assert_sql(&t2d("SELECT DATEADD(day, 1, d), DATEADD(mm, -2, d), DATEDIFF(dd, a, b), DATEPART(yyyy, d) FROM t").0,
        "SELECT (d + to_days(CAST(1 AS BIGINT))), (d + to_months(CAST(-2 AS BIGINT))), date_diff('day', a, b), date_part('year', d) FROM t;");
}
#[test] fn convert_styles() {
    assert_sql(&t2d("SELECT CONVERT(VARCHAR(10), d, 112), CONVERT(INT, x), CAST(y AS NVARCHAR(MAX)), TRY_CONVERT(DATETIME2, z) FROM t").0,
        "SELECT strftime(d, '%Y%m%d'), CAST(x AS INTEGER), CAST(y AS VARCHAR), TRY_CAST(z AS TIMESTAMP) FROM t;");
}
#[test] fn charindex_swap() { assert_sql(&t2d("SELECT CHARINDEX('x', s) FROM t").0, "SELECT strpos(s, 'x') FROM t;"); }
#[test] fn string_concat_plus() {
    assert_sql(&t2d("SELECT first + ' ' + last, a + b, 'x' + CAST(n AS VARCHAR) FROM t").0,
        "SELECT first || ' ' || last, a + b, 'x' || CAST(n AS VARCHAR) FROM t;");
}
// 0.1.1: the default nocase mode is Rewrite, so `x = 'lit'` becomes a case/trailing-space-insensitive comparison
#[test] fn nstrings() { assert_sql(&t2d("SELECT N'héllo' FROM t WHERE x = N'a''b'").0, "SELECT 'héllo' FROM t WHERE lower(rtrim(x, ' ')) = lower('a''b');"); }
#[test] fn declare_set_variables() {
    assert_sql(&t2d("DECLARE @n INT = 5, @s NVARCHAR(10); SET @s = 'x'; SELECT * FROM t WHERE a = @n AND b = @s").0,
        "SET VARIABLE n = 5; SET VARIABLE s = NULL; SET VARIABLE s = 'x'; SELECT * FROM t WHERE a = getvariable('n') AND b = getvariable('s');");
}
#[test] fn temp_tables() {
    assert_sql(&t2d("SELECT a INTO #tmp FROM t; SELECT * FROM #tmp; DROP TABLE #tmp").0,
        "CREATE TEMP TABLE tmp AS SELECT a FROM t; SELECT * FROM tmp; DROP TABLE tmp;");
}
#[test] fn table_variable() { assert_sql(&t2d("DECLARE @t TABLE (id INT, n NVARCHAR(50)); INSERT INTO @t VALUES (1, 'a'); SELECT * FROM @t").0,
    "CREATE TEMP TABLE t (id INTEGER, n VARCHAR); INSERT INTO t VALUES (1, 'a'); SELECT * FROM t;"); }
#[test] fn if_object_id_drop() { assert_sql(&t2d("IF OBJECT_ID('dbo.t', 'U') IS NOT NULL DROP TABLE dbo.t").0, "DROP TABLE IF EXISTS main.t;"); }
#[test] fn go_batches_and_set_nocount() { let (s, w) = t2d("SET NOCOUNT ON;\nGO\nSELECT 1\nGO\nSELECT 2"); assert_sql(&s, "-- (dropped) SET NOCOUNT ON\nSELECT 1; SELECT 2;"); assert!(w.iter().any(|x| x.contains("session option"))); }
#[test] fn create_table_identity() {
    assert_sql(&t2d("CREATE TABLE dbo.t (id INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED, name NVARCHAR(100) NULL, amt DECIMAL(18,2), flag BIT, created DATETIME2(7) DEFAULT GETDATE(), uid UNIQUEIDENTIFIER) ON [PRIMARY]").0,
        "CREATE SEQUENCE IF NOT EXISTS seq_t_id START 1 INCREMENT 1; CREATE TABLE main.t (id INTEGER DEFAULT nextval('seq_t_id') NOT NULL PRIMARY KEY, name VARCHAR NULL, amt DECIMAL(18,2), flag BOOLEAN, created TIMESTAMP DEFAULT current_localtimestamp(), uid UUID);");
}
#[test] fn cross_apply() { assert_sql(&t2d("SELECT * FROM t CROSS APPLY (SELECT TOP 1 x FROM u WHERE u.id = t.id) s").0, "SELECT * FROM t CROSS JOIN LATERAL (SELECT x FROM u WHERE u.id = t.id LIMIT 1) s;"); }
#[test] fn outer_apply() { assert_sql(&t2d("SELECT * FROM t OUTER APPLY (SELECT x FROM u WHERE u.id = t.id) s WHERE 1=1").0, "SELECT * FROM t LEFT JOIN LATERAL (SELECT x FROM u WHERE u.id = t.id) s ON TRUE WHERE 1=1;"); }
#[test] fn string_agg_within_group() { assert_sql(&t2d("SELECT STRING_AGG(n, ',') WITHIN GROUP (ORDER BY n) FROM t").0, "SELECT string_agg(n, ',' ORDER BY n) FROM t;"); }
#[test] fn update_from_join() {
    assert_sql(&t2d("UPDATE a SET a.x = b.x FROM t a JOIN u b ON a.id = b.id WHERE b.y > 0").0,
        "UPDATE t AS a SET a.x = b.x FROM u b WHERE (a.id = b.id) AND (b.y > 0);");
}
#[test] fn delete_alias_join() { assert_sql(&t2d("DELETE a FROM t a JOIN u b ON a.id = b.id WHERE b.z = 1").0, "DELETE FROM t a USING u b WHERE (a.id = b.id) AND (b.z = 1);"); }
#[test] fn procedural_commented() { let (s, w) = t2d("IF @x > 1 BEGIN PRINT 'hi' END"); assert!(s.starts_with("/* UNTRANSLATED")); assert!(w.iter().any(|x| x.contains("procedural"))); }
#[test] fn print_and_collate() { assert_sql(&t2d("PRINT 'hello'; SELECT a FROM t WHERE n = 'x' COLLATE Latin1_General_CI_AS").0, "SELECT 'hello' AS message; SELECT a FROM t WHERE n = 'x' COLLATE NOCASE;"); }
#[test] fn expr_default_translation() { let r = translate_expr("(GETDATE())", Dialect::Tsql, Dialect::DuckDb).unwrap(); assert_sql(&r.sql, "(current_localtimestamp())"); }
#[test] fn comments_preserved() { assert_sql(&t2d("-- hello\nSELECT 1 /* c */ FROM t").0, "-- hello\nSELECT 1 /* c */ FROM t;"); }
#[test] fn cte_with_top() { assert_sql(&t2d("WITH c AS (SELECT TOP 3 a FROM t ORDER BY a) SELECT * FROM c").0, "WITH c AS (SELECT a FROM t ORDER BY a LIMIT 3) SELECT * FROM c;"); }
#[test] fn eomonth_format() { assert_sql(&t2d("SELECT EOMONTH(d), FORMAT(d, 'yyyy-MM-dd') FROM t").0, "SELECT last_day(d), strftime(d, '%Y-%m-%d') FROM t;"); }
#[test] fn json_funcs() { assert_sql(&t2d("SELECT JSON_VALUE(j, '$.a'), ISJSON(j) FROM t").0, "SELECT json_extract_string(j, '$.a'), CASE WHEN json_valid(j) THEN 1 ELSE 0 END FROM t;"); }
#[test] fn like_bracket_untouched_in_strings() { assert_sql(&t2d("SELECT 1 WHERE x LIKE '[a-z]%' AND y NOT LIKE 'Q%'").0, "SELECT 1 WHERE x ILIKE '[a-z]%' AND y NOT ILIKE 'Q%';"); }
#[test] fn like_stays_when_binary_collation() {
    let o = duckbridge::transpile::Options { nocase: false, prelude: false, ..Default::default() };
    let r = duckbridge::transpile::translate_with("SELECT 1 WHERE x LIKE 'a%'", Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &o).unwrap();
    assert_sql(&r.sql, "SELECT 1 WHERE x LIKE 'a%';");
}
#[test] fn prelude_emitted() {
    // 0.1.1: default nocase mode is Rewrite -> no global collation in the prelude (3 SET lines)
    let o = duckbridge::transpile::Options::default();
    let r = duckbridge::transpile::translate_with("SELECT 7 / 2", Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &o).unwrap();
    assert!(r.sql.contains("SET integer_division = true;"), "{}", r.sql);
    assert!(r.sql.contains("NULLS_FIRST_ON_ASC_LAST_ON_DESC"));
    assert!(!r.sql.contains("default_collation"));
    assert_eq!(r.sql.lines().filter(|l| l.starts_with("SET ")).count(), 3);
    assert!(r.sql.trim_end().ends_with("SELECT 7 / 2;"));
    let g = duckbridge::transpile::Options { nocase_mode: Some(NocaseMode::Global), ..Default::default() };
    let r = duckbridge::transpile::translate_with("SELECT 7 / 2", Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &g).unwrap();
    assert!(r.sql.contains("default_collation = 'nocase'"), "{}", r.sql);
}
#[test] fn sql2022_functions_to_duck() {
    assert_sql(&t2d("SELECT DATE_BUCKET(day, 7, d), GREATEST(a, b), DATETRUNC(month, d), REGEXP_LIKE(s, '^a'), JSON_OBJECT('k': v, 'n': 1), STRING_SPLIT(s, ',') FROM t").0,
        "SELECT time_bucket(to_days(CAST(7 AS BIGINT)), d), GREATEST(a, b), date_trunc('month', d), regexp_matches(s, '^a'), json_object('k', v, 'n', 1), unnest(string_split(s, ',')) FROM t;");
}
#[test] fn float_division_back() { assert_sql(&d2t("SELECT a / b, c // d, e / 2.0, 7 / 2 FROM t").0, "SELECT CAST(a AS FLOAT) / b, c / d, e / 2.0, CAST(7 AS FLOAT) / 2 FROM t;"); }
#[test] fn date_trunc_2019_idioms() { assert_sql(&d2t("SELECT date_trunc('hour', x), date_trunc('second', x) FROM t").0, "SELECT CAST(CONVERT(VARCHAR(13), x, 120) + ':00:00' AS DATETIME2), CAST(CONVERT(VARCHAR(19), x, 120) AS DATETIME2) FROM t;"); }
#[test] fn target_version_2022_and_2025() {
    let mk = |v: u16| duckbridge::transpile::Options { target_version: v, prelude: false, ..Default::default() };
    let sm = HashMap::new();
    let r22 = duckbridge::transpile::translate_with("SELECT greatest(a, b), date_trunc('hour', x), generate_series(1, 5) FROM t", Dialect::DuckDb, Dialect::Tsql, &sm, &mk(2022)).unwrap();
    assert_sql(&r22.sql, "SELECT greatest(a, b), DATETRUNC(HOUR, x), GENERATE_SERIES(1, 5) FROM t;");
    let r25 = duckbridge::transpile::translate_with("SELECT regexp_matches(s, 'a+'), regexp_replace(s, 'a', 'b', 'g') FROM t WHERE j::JSON IS NOT NULL", Dialect::DuckDb, Dialect::Tsql, &sm, &mk(2025)).unwrap();
    assert_sql(&r25.sql, "SELECT REGEXP_LIKE(s, 'a+'), REGEXP_REPLACE(s, 'a', 'b') FROM t WHERE CAST(j AS json) IS NOT NULL;");
    let r19 = duckbridge::transpile::translate_with("SELECT regexp_matches(s, 'a+') FROM t", Dialect::DuckDb, Dialect::Tsql, &sm, &mk(2019)).unwrap();
    assert!(r19.warnings.iter().any(|w| w.contains("regex")));
}
#[test] fn iso_parts_back() { assert_sql(&d2t("SELECT isoyear(d), isodow(d) FROM t").0, "SELECT YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, d), d)), (((DATEPART(WEEKDAY, d) + @@DATEFIRST + 5) % 7) + 1) FROM t;"); }

// ------------------------------------------------------------------ DuckDB -> T-SQL
#[test] fn limit_to_top() { assert_sql(&d2t("SELECT a FROM t ORDER BY a LIMIT 5").0, "SELECT TOP (5) a FROM t ORDER BY a;"); }
#[test] fn limit_offset_to_fetch() { assert_sql(&d2t("SELECT a FROM t ORDER BY a LIMIT 10 OFFSET 20").0, "SELECT a FROM t ORDER BY a OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;"); }
#[test] fn offset_without_order() { assert_sql(&d2t("SELECT a FROM t LIMIT 10 OFFSET 5").0, "SELECT a FROM t ORDER BY (SELECT NULL) OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY;"); }
#[test] fn quotes_and_schema_back() { assert_sql(&d2t("SELECT \"a b\", x FROM main.\"t\"").0, "SELECT [a b], x FROM dbo.[t];"); }
#[test] fn concat_and_cast_back() { assert_sql(&d2t("SELECT a || b, x::INTEGER, CAST(y AS VARCHAR), z::TIMESTAMP FROM t").0, "SELECT a + b, CAST(x AS INT), CAST(y AS NVARCHAR(MAX)), CAST(z AS DATETIME2(7)) FROM t;"); }
#[test] fn funcs_back() { assert_sql(&d2t("SELECT now(), length(s), strpos(s, 'x'), uuid(), ifnull(a, 0), if(a > 1, 1, 0) FROM t").0, "SELECT SYSDATETIME(), LEN(s), CHARINDEX('x', s), NEWID(), ISNULL(a, 0), IIF(a > 1, 1, 0) FROM t;"); }
#[test] fn date_funcs_back() { assert_sql(&d2t("SELECT date_diff('day', a, b), date_part('year', d), date_trunc('month', d), last_day(d) FROM t").0, "SELECT DATEDIFF(DAY, a, b), DATEPART(YEAR, d), DATEADD(MONTH, DATEDIFF(MONTH, 0, d), 0), EOMONTH(d) FROM t;"); }
#[test] fn interval_arith_back() { assert_sql(&d2t("SELECT d + INTERVAL 3 DAY, d - INTERVAL '2 months', d + to_hours(n) FROM t").0, "SELECT DATEADD(DAY, 3, d), DATEADD(MONTH, -2, d), DATEADD(HOUR, n, d) FROM t;"); }
#[test] fn strftime_back() { assert_sql(&d2t("SELECT strftime(d, '%Y-%m-%d %H:%M') FROM t").0, "SELECT FORMAT(d, 'yyyy-MM-dd HH:mm') FROM t;"); }
#[test] fn booleans_back() { assert_sql(&d2t("SELECT * FROM t WHERE flag = TRUE AND other = false").0, "SELECT * FROM t WHERE flag = 1 AND other = 0;"); }
#[test] fn temp_back() { assert_sql(&d2t("CREATE TEMP TABLE x AS SELECT 1 AS a; SELECT * FROM x").0, "CREATE TABLE #x AS SELECT 1 AS a; SELECT * FROM #x;"); }
#[test] fn create_or_replace_back() { assert_sql(&d2t("CREATE OR REPLACE TABLE t (a INTEGER, b VARCHAR, c BOOLEAN, d TIMESTAMP, e UUID, f DOUBLE)").0, "DROP TABLE IF EXISTS t; CREATE TABLE t (a INT, b NVARCHAR(MAX), c BIT, d DATETIME2(7), e UNIQUEIDENTIFIER, f FLOAT);"); }
#[test] fn variables_back() { let (s, _) = d2t("SET VARIABLE n = 5; SELECT * FROM t WHERE a = getvariable('n')"); assert_sql(&s, "DECLARE @n SQL_VARIANT = 5; SELECT * FROM t WHERE a = @n;"); }
#[test] fn string_agg_back() { assert_sql(&d2t("SELECT string_agg(n, ', ' ORDER BY n) FROM t").0, "SELECT STRING_AGG(n, ', ') WITHIN GROUP (ORDER BY n) FROM t;"); }
#[test] fn power_back() { assert_sql(&d2t("SELECT a ** 2, b ^ 3 FROM t").0, "SELECT POWER(a, 2), POWER(b, 3) FROM t;"); }
#[test] fn delete_using_back() { assert_sql(&d2t("DELETE FROM t USING u WHERE t.id = u.id").0, "DELETE t FROM t, u WHERE t.id = u.id;"); }
#[test] fn returning_back() { assert_sql(&d2t("INSERT INTO t (a) VALUES (1) RETURNING a").0, "INSERT INTO t (a) OUTPUT inserted.a VALUES (1);"); }
#[test] fn warns_on_regex() { let (_, w) = d2t("SELECT regexp_matches(s, 'a+') FROM t"); assert!(w.iter().any(|x| x.contains("regex"))); }
#[test] fn warns_on_qualify() { let (_, w) = d2t("SELECT a, row_number() OVER (ORDER BY a) r FROM t QUALIFY r = 1"); assert!(w.iter().any(|x| x.contains("QUALIFY"))); }

// ------------------------------------------------------------------ round trips & PG
#[test] fn roundtrip_simple() {
    let src = "SELECT TOP 10 a, ISNULL(b, 0) AS b FROM dbo.t WITH (NOLOCK) WHERE c > DATEADD(day, -7, GETDATE()) ORDER BY a";
    let (duck, _) = t2d(src);
    let (back, _) = d2t(&duck);
    assert_sql(&back, "SELECT TOP (10) a, COALESCE(b, 0) AS b FROM dbo.t WHERE c > (DATEADD(DAY, -7, SYSDATETIME())) ORDER BY a;");
}
#[test] fn duck_to_pg_basics() { assert_sql(&d2p("SELECT list_value(1,2), [3,4], strftime(d, '%Y-%m'), today(), uuid(), if(a,1,0) FROM t LIMIT 5"), "SELECT ARRAY[1, 2], ARRAY[3,4], to_char(d, 'YYYY-MM'), CURRENT_DATE, gen_random_uuid(), CASE WHEN a THEN 1 ELSE 0 END FROM t LIMIT 5;"); }
#[test] fn duck_to_pg_types() { assert_sql(&d2p("CREATE TABLE t (a UTINYINT, b DOUBLE, c BLOB, d TIMESTAMPTZ, e JSON, f VARCHAR, g DECIMAL(10,2))"), "CREATE TABLE t (a smallint, b double precision, c bytea, d timestamp with time zone, e jsonb, f text, g DECIMAL(10,2));"); }
#[test] fn pg_to_duck_basics() { assert_sql(&p2d("SELECT to_char(d, 'YYYY-MM-DD'), string_to_array(s, ','), ARRAY[1,2], x::bytea FROM t FOR UPDATE"), "SELECT strftime(d, '%Y-%m-%d'), string_split(s, ','), [1,2], x::BLOB FROM t;"); }
#[test] fn tsql_to_pg_chain() { assert_sql(&translate("SELECT TOP 3 ISNULL(a, 0), GETDATE() FROM dbo.t", Dialect::Tsql, Dialect::Postgres, &HashMap::from([("dbo".to_string(), "public".to_string())])).unwrap().sql, "SELECT COALESCE(a, 0), now() FROM public.t LIMIT 3;"); }

// ------------------------------------------------------------------ 0.1.1 (R7): coded warnings, nocase modes, strict, exact math
fn opts() -> Options { Options { prelude: false, ..Default::default() } }
fn t2d_with(sql: &str, o: &Options) -> (String, Vec<Warning>) {
    let r = translate_with(sql, Dialect::Tsql, Dialect::DuckDb, &HashMap::from([("dbo".to_string(), "main".to_string())]), o).unwrap();
    assert_eq!(r.warnings, r.diagnostics.iter().map(|w| w.to_string()).collect::<Vec<_>>());
    (r.sql, r.diagnostics)
}
fn typed(pairs: &[(&str, &str)]) -> Options { Options { types: pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), ..opts() } }
fn codes_of(w: &[Warning]) -> Vec<&'static str> { w.iter().map(|x| x.code).collect() }
fn has(w: &[Warning], code: &str) -> bool { w.iter().any(|x| x.code == code) }

#[test] fn warning_display_and_position() {
    let (_, w) = t2d_with("SELECT a\nFROM t WHERE x = ROUND(y, 2, 1)", &opts());
    let r = w.iter().find(|x| x.code == codes::ROUND_TRUNC).expect("W-ROUND-TRUNC");
    assert_eq!((r.line, r.col), (2, 18));
    assert!(r.to_string().starts_with("W-ROUND-TRUNC (line 2:18): ROUND(y, 2, 1)"), "{r}");
    let w0 = Warning { code: codes::DIV_DECIMAL, line: 12, col: 8, message: "m".into() };
    assert_eq!(w0.to_string(), "W-DIV-DECIMAL (line 12:8): m");
}
#[test] fn generic_code_for_free_text() {
    let (_, w) = t2d_with("SELECT 1;\nSET NOCOUNT ON", &opts());
    assert_eq!(codes_of(&w), vec![codes::GENERIC]);
    assert_eq!((w[0].line, w[0].col), (2, 1));
}
#[test] fn w_div_decimal() {
    // literal arithmetic: SQL Server types 7 * 1.0 / 3 as numeric(9,6)
    let (s, w) = t2d_with("SELECT 7 * 1.0 / 3", &opts());
    assert_sql(&s, "SELECT 7 * 1.0 / 3;");
    let d = w.iter().find(|x| x.code == codes::DIV_DECIMAL).unwrap();
    assert!(d.message.contains("numeric(9,6)"), "{d}");
    assert_eq!((d.line, d.col), (1, 16));
    // typed int columns: numeric(24,12)
    let (_, w) = t2d_with("SELECT qty * 1.0 / cnt FROM t", &typed(&[("qty", "int"), ("cnt", "int")]));
    assert!(w.iter().any(|x| x.code == codes::DIV_DECIMAL && x.message.contains("numeric(24,12)")), "{w:?}");
    // unknown operand types with decimal evidence
    let (_, w) = t2d_with("SELECT a * 1.0 / b FROM t", &opts());
    assert!(w.iter().any(|x| x.code == codes::DIV_DECIMAL && x.message.contains("--types")), "{w:?}");
    // integer division and float division are faithful: no warning
    assert!(t2d_with("SELECT a / b, 7 / 2, CAST(a AS FLOAT) / 3 FROM t", &opts()).1.is_empty());
    // column types from CREATE TABLE in the same script
    let (_, w) = t2d_with("CREATE TABLE t (amt DECIMAL(10,2), n INT); SELECT amt / n FROM t", &opts());
    assert!(w.iter().any(|x| x.code == codes::DIV_DECIMAL && x.message.contains("numeric(21,13)")), "{w:?}");
}
#[test] fn w_round_trunc_and_round_zero() {
    let (s, w) = t2d_with("SELECT ROUND(x, 2, 1), ROUND(x, 2, 0) FROM t", &opts());
    assert_sql(&s, "SELECT (trunc((x) * power(10, 2)) / power(10, 2)), round(x, 2) FROM t;");
    assert_eq!(codes_of(&w), vec![codes::ROUND_TRUNC]);
}
#[test] fn w_format_num() {
    let (s, w) = t2d_with("SELECT FORMAT(x, 'N2'), FORMAT(y, '0.0%'), FORMAT(d, 'yyyy') FROM t", &opts());
    assert_sql(&s, "SELECT format('{}', x), format('{}', y), strftime(d, '%Y') FROM t;");
    assert_eq!(w.iter().filter(|x| x.code == codes::FORMAT_NUM).count(), 2);
    assert!(w.iter().any(|x| x.message.contains("--mssql-math")));
}
#[test] fn w_trycast_empty() {
    let (s, w) = t2d_with("SELECT TRY_CAST(s AS INT), TRY_CONVERT(BIGINT, s), TRY_CAST(s AS DATE), TRY_CAST(n AS INT) FROM t", &typed(&[("n", "decimal(10,2)")]));
    assert_sql(&s, "SELECT TRY_CAST(s AS INTEGER), TRY_CAST(s AS BIGINT), TRY_CAST(s AS DATE), TRY_CAST(n AS INTEGER) FROM t;");
    assert_eq!(w.iter().filter(|x| x.code == codes::TRYCAST_EMPTY).count(), 2, "{w:?}");
}
#[test] fn w_procedural() {
    let (s, w) = t2d_with("WHILE @i < 10 SET @i = @i + 1", &opts());
    assert!(s.starts_with("/* UNTRANSLATED"));
    assert_eq!(codes_of(&w), vec![codes::PROCEDURAL]);
    let (_, w) = t2d_with("DECLARE c CURSOR FOR SELECT 1; DECLARE @c CURSOR", &opts());
    assert!(has(&w, codes::PROCEDURAL), "{w:?}");
    let (_, w) = t2d_with("EXEC('SELECT 1')", &opts());
    assert!(has(&w, codes::PROCEDURAL));
}
#[test] fn w_power_double() {
    let (_, w) = t2d_with("SELECT POWER(2.5, 2), POWER(amount, 2), POWER(n, 2) FROM t", &typed(&[("amount", "decimal(18,2)"), ("n", "int")]));
    assert_eq!(w.iter().filter(|x| x.code == codes::POWER_DOUBLE).count(), 2, "{w:?}");
}
#[test] fn nocase_rewrite_literal_comparisons() {
    let (s, w) = t2d_with("SELECT * FROM t WHERE name = 'Lit' AND 'Abc' = t.code AND s <> 'X  ' AND st IN ('A', 'b') AND k NOT IN ('Q') AND zip = '12345' AND d = '2024-01-01'", &opts());
    assert_sql(&s, "SELECT * FROM t WHERE lower(rtrim(name, ' ')) = lower('Lit') AND lower('Abc') = lower(rtrim(t.code, ' ')) AND lower(rtrim(s, ' ')) <> lower('X') \
        AND lower(rtrim(st, ' ')) IN (lower('A'), lower('b')) AND lower(rtrim(k, ' ')) NOT IN (lower('Q')) AND zip = '12345' AND d = '2024-01-01';");
    assert!(w.is_empty(), "{w:?}");
    // CASE WHEN, JOIN ON and subqueries are predicates too; SET/VALUES/SELECT-list assignments are not
    let (s, _) = t2d_with("UPDATE t SET name = 'New' WHERE kind = 'Old'", &opts());
    assert_sql(&s, "UPDATE t SET name = 'New' WHERE lower(rtrim(kind, ' ')) = lower('Old');");
    let (s, _) = t2d_with("SELECT CASE WHEN c = 'Y' THEN 'yes' END FROM a JOIN b ON b.k = 'K' WHERE EXISTS (SELECT 1 FROM u WHERE u.n = 'N')", &opts());
    assert_sql(&s, "SELECT CASE WHEN lower(rtrim(c, ' ')) = lower('Y') THEN 'yes' END FROM a JOIN b ON lower(rtrim(b.k, ' ')) = lower('K') WHERE EXISTS (SELECT 1 FROM u WHERE lower(rtrim(u.n, ' ')) = lower('N'));");
    let (s, _) = t2d_with("INSERT INTO t (a) VALUES ('X'); SELECT COALESCE(a, 'Z') AS a FROM t", &opts());
    assert_sql(&s, "INSERT INTO t (a) VALUES ('X'); SELECT COALESCE(a, 'Z') AS a FROM t;");
    // explicit COLLATE and numeric columns are left alone
    let (s, w) = t2d_with("SELECT 1 FROM t WHERE n = 'x' COLLATE Latin1_General_CI_AS AND id = 'abc'", &typed(&[("id", "int")]));
    assert_sql(&s, "SELECT 1 FROM t WHERE n = 'x' COLLATE NOCASE AND id = 'abc';");
    assert_eq!(codes_of(&w), vec![codes::TRAILING_SPACE]);
    // non-atomic operands are reported, never mangled
    let (s, w) = t2d_with("SELECT 1 FROM t WHERE CASE WHEN a = 1 THEN 'x' END = 'x' AND b + 'x' = 'yx'", &opts());
    assert_sql(&s, "SELECT 1 FROM t WHERE CASE WHEN a = 1 THEN 'x' END = 'x' AND b || 'x' = 'yx';");
    assert_eq!(codes_of(&w), vec![codes::CI_COMPARE, codes::TRAILING_SPACE]);
    // views read from SQL Server get the same treatment
    let (s, _) = t2d_with("CREATE VIEW v AS SELECT * FROM t WHERE status = 'Active'", &opts());
    assert_sql(&s, "CREATE VIEW v AS SELECT * FROM t WHERE lower(rtrim(status, ' ')) = lower('Active');");
}
#[test] fn nocase_rewrite_executes_like_sql_server() {
    let (s, _) = t2d_with("SELECT count(*) FROM t WHERE name = 'ALICE' OR name IN ('bob')", &opts());
    let con = duck::open_in_memory().unwrap();
    duck::exec(&con, "CREATE TABLE t (name VARCHAR); INSERT INTO t VALUES ('Alice'), ('alice  '), ('Bob'), ('carol')").unwrap();
    let n: i64 = duck::query_scalar(&con, s.trim_end_matches(';')).unwrap();
    assert_eq!(n, 3); // SQL Server (CI_AS): 'Alice', 'alice  ' and 'Bob'
}
#[test] fn w_ci_compare_not_rewritten() {
    let o = typed(&[("name", "varchar(50)"), ("city", "nvarchar(20)")]);
    let (s, w) = t2d_with("SELECT DISTINCT name FROM t", &o);
    assert_sql(&s, "SELECT DISTINCT name FROM t;");
    assert_eq!(codes_of(&w), vec![codes::CI_COMPARE]);
    let (_, w) = t2d_with("SELECT city, count(*) FROM t GROUP BY city", &o);
    assert_eq!(codes_of(&w), vec![codes::CI_COMPARE]);
    let (_, w) = t2d_with("SELECT * FROM a JOIN b ON a.name = b.name WHERE a.name > 'M'", &o);
    assert!(has(&w, codes::CI_COMPARE) && has(&w, codes::TRAILING_SPACE), "{w:?}");
    assert_eq!(w.iter().filter(|x| x.code == codes::CI_COMPARE).count(), 1, "once per statement: {w:?}");
    let (_, w) = t2d_with("SELECT CASE status WHEN 'Open' THEN 1 END FROM t", &opts());
    assert_eq!(codes_of(&w), vec![codes::CI_COMPARE]);
    // Off mode: binary semantics on purpose, only the trailing-space note
    let off = Options { nocase_mode: Some(NocaseMode::Off), ..o.clone() };
    let (s, w) = t2d_with("SELECT DISTINCT name FROM t WHERE city = 'Paris'", &off);
    assert_sql(&s, "SELECT DISTINCT name FROM t WHERE city = 'Paris';");
    assert_eq!(codes_of(&w), vec![codes::TRAILING_SPACE]);
}
#[test] fn w_trailing_space() {
    let (s, w) = t2d_with("SELECT 1 FROM t WHERE code = '123'", &typed(&[("code", "char(10)")]));
    assert_sql(&s, "SELECT 1 FROM t WHERE code = '123';");
    assert_eq!(codes_of(&w), vec![codes::TRAILING_SPACE]);
    let g = Options { nocase_mode: Some(NocaseMode::Global), ..opts() };
    let (s, w) = t2d_with("SELECT 1 FROM t WHERE x = 'abc'", &g);
    assert_sql(&s, "SELECT 1 FROM t WHERE x = 'abc';");
    assert_eq!(codes_of(&w), vec![codes::TRAILING_SPACE]);
}
#[test] fn legacy_nocase_flag_maps_to_modes() {
    assert_eq!(Options::default().nocase_mode(), NocaseMode::Rewrite);
    assert_eq!(Options { nocase: false, ..Default::default() }.nocase_mode(), NocaseMode::Off);
    assert_eq!(Options { nocase: false, nocase_mode: Some(NocaseMode::Global), ..Default::default() }.nocase_mode(), NocaseMode::Global);
    assert_eq!(NocaseMode::parse("REWRITE"), Some(NocaseMode::Rewrite));
    assert_eq!(duckbridge::transpile::duckdb_prelude_for(NocaseMode::Rewrite).len(), 3);
    assert_eq!(duckbridge::transpile::duckdb_prelude_for(NocaseMode::Off).len(), 3);
    assert_eq!(duckbridge::transpile::duckdb_prelude_for(NocaseMode::Global).len(), 4);
    assert_eq!(duckbridge::transpile::duckdb_prelude(true).len(), 4); // legacy bool = Global
    let t = duckbridge::transpile::parse_types("Amount=decimal(18,4), qty=int;t.name:varchar(20)");
    assert_eq!(t.get("amount").map(|s| s.as_str()), Some("decimal(18,4)"));
    assert_eq!(t.get("qty").map(|s| s.as_str()), Some("int"));
    assert_eq!(t.get("t.name").map(|s| s.as_str()), Some("varchar(20)"));
}
#[test] fn strict_mode() {
    let st = Options { strict: true, ..opts() };
    let e = translate_with("SELECT 1;\nSELECT 7 * 1.0 / 3;\nSET NOCOUNT ON", Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &st).unwrap_err();
    match &e {
        TranslateError::Strict(ws) => {
            assert_eq!(codes_of(ws), vec![codes::DIV_DECIMAL, codes::GENERIC]);
            assert_eq!(ws[0].line, 2);
        }
        other => panic!("{other:?}"),
    }
    let text = e.to_string();
    assert!(text.starts_with("strict mode: 2 translation warning(s):\nW-DIV-DECIMAL (line 2:"), "{text}");
    assert_eq!(text.lines().count(), 3);
    // clean scripts pass
    assert!(translate_with("SELECT a FROM t WHERE id = 1", Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &st).is_ok());
}
#[test] fn mssql_math_rewrites_and_runs() {
    let o = Options { mssql_math: true, ..Default::default() };
    let sql = "CREATE TABLE t (qty INT, cnt INT, price DECIMAL(10,3), s VARCHAR(10));\n\
               INSERT INTO t VALUES (10, 4, 2.567, ''), (7, 3, -2.567, '12.5');\n\
               SELECT qty * 1.0 / cnt AS r, ROUND(price, 2, 1) AS tr, ROUND(price, 2) AS rd, TRY_CAST(s AS INT) AS ti, FORMAT(qty * 1.0 / cnt / 100, '0.0%') AS pct, 7 * 1.0 / 3 AS lit FROM t ORDER BY qty DESC";
    let r = translate_with(sql, Dialect::Tsql, Dialect::DuckDb, &HashMap::new(), &o).unwrap();
    assert!(r.diagnostics.is_empty(), "{:?}", r.diagnostics);
    assert!(r.sql.contains("CREATE OR REPLACE MACRO mssql_div("), "macros in the prelude");
    assert!(r.sql.contains("CAST(mssql_div(CAST(qty * 1.0 AS DECIMAL(13,1)), CAST(cnt AS DECIMAL(10,0)), 24, 12) AS DECIMAL(24,12))"), "{}", r.sql);
    assert!(r.sql.contains("CAST(mssql_div(CAST(7 * 1.0 AS DECIMAL(4,1)), CAST(3 AS DECIMAL(1,0)), 9, 6) AS DECIMAL(9,6))"), "{}", r.sql);
    assert!(r.sql.contains("mssql_round_trunc(price, 2)") && r.sql.contains("mssql_round(price, 2)") && r.sql.contains("mssql_try_int(s)") && r.sql.contains("mssql_format_pct("), "{}", r.sql);
    let con = duck::open_in_memory().unwrap();
    let stmts: Vec<String> = duckbridge::transpile::tokens::statements(&duckbridge::transpile::tokens::tokenize(&r.sql, false))
        .iter().map(|s| duckbridge::transpile::tokens::join(s).trim().to_string()).filter(|s| !s.is_empty()).collect();
    let (last, init) = stmts.split_last().unwrap();
    for s in init { duck::exec(&con, s).unwrap(); }
    let rows: Vec<Vec<String>> = duck::query_rows(&con, &format!("SELECT CAST(r AS VARCHAR), CAST(tr AS VARCHAR), CAST(rd AS VARCHAR), COALESCE(CAST(ti AS VARCHAR), 'NULL'), pct, CAST(lit AS VARCHAR) FROM ({last}) ORDER BY r DESC"),
        |row| Ok((0..6).map(|i| row.get::<_, String>(i)).collect::<Result<Vec<_>, _>>()?)).unwrap();
    // SQL Server 2019: CAST(10 AS int)*1.0/CAST(4 AS int) = 2.500000000000; ROUND(2.567,2,1) = 2.560; ROUND(2.567,2) = 2.570;
    // TRY_CAST('' AS int) = 0; FORMAT(0.025, '0.0%') = 2.5%; 7*1.0/3 = 2.333333; 7*1.0/3 (int columns) = 2.333333333333;
    // ROUND(-2.567,2,1) = -2.560; ROUND(-2.567,2) = -2.570; TRY_CAST('12.5' AS int) = NULL; FORMAT(0.023333333333333, '0.0%') = 2.3%
    assert_eq!(rows[0], ["2.500000000000", "2.560", "2.570", "0", "2.5%", "2.333333"]);
    assert_eq!(rows[1], ["2.333333333333", "-2.560", "-2.570", "NULL", "2.3%", "2.333333"]);
}
#[test] fn mssql_math_needs_types_for_division() {
    let o = Options { mssql_math: true, ..opts() };
    let (s, w) = t2d_with("SELECT a * 1.0 / b FROM t", &o);
    assert_sql(&s, "SELECT a * 1.0 / b FROM t;");
    assert!(w.iter().any(|x| x.code == codes::DIV_DECIMAL && x.message.contains("mssql_div needs")), "{w:?}");
    let (s, w) = t2d_with("SELECT a * 1.0 / b FROM t", &Options { types: HashMap::from([("a".into(), "bigint".into()), ("b".into(), "bigint".into())]), ..o });
    assert_sql(&s, "SELECT CAST(mssql_div(CAST(a * 1.0 AS DECIMAL(22,1)), CAST(b AS DECIMAL(19,0)), 38, 17) AS DECIMAL(38,17)) FROM t;");
    assert!(w.is_empty());
}
