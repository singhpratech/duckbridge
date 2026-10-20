//! Exact SQL Server arithmetic for DuckDB (requirement R7, work stream F).
//!
//! DuckDB divides DECIMALs in DOUBLE, rounds `TRY_CAST('12.5' AS INTEGER)` to 13, returns NULL for
//! `TRY_CAST('' AS INTEGER)` and has no `FORMAT(x, '0.0%')`. The macros below reproduce SQL Server's results
//! with integer (HUGEINT) and DECIMAL arithmetic only, never through DOUBLE:
//!
//! | macro | SQL Server expression | result |
//! |---|---|---|
//! | `mssql_div(a, b, p, s)` | `a / b` on numerics, result type `numeric(p, s)` | VARCHAR text of the exact, truncated quotient; cast it to `DECIMAL(p, s)` (the translator does) |
//! | `mssql_round(x, n)` | `ROUND(x, n)` | half away from zero, keeps the scale of `x` |
//! | `mssql_round_trunc(x, n)` | `ROUND(x, n, 1)` | truncation toward zero, keeps the scale of `x` |
//! | `mssql_try_int(x)` | `TRY_CAST(x AS int)` | `''`/blank -> 0, `'12.5'` -> NULL, numeric input truncated |
//! | `mssql_format_pct(x, d)` | `FORMAT(x, '0.0%')` with `d` decimals | text such as `12.3%` |
//!
//! [`div_type`], [`mul_type`] and [`add_type`] implement SQL Server's result precision/scale rules
//! (including the scale reduction when the precision exceeds 38); the translator uses them to choose
//! `p` and `s` for `mssql_div`. [`verify_cases`] produces random expression pairs to check the macros
//! against a live SQL Server (`verify-math`).
use crate::core::duck::{self, Connection};
use anyhow::Result;

/// `CREATE OR REPLACE MACRO` statements (no trailing `;`) for the session.
pub fn macros() -> Vec<String> {
    [
        // 10^c as HUGEINT, exact (power() would go through DOUBLE)
        "CREATE OR REPLACE MACRO mssql_pow10(c) AS CAST('1' || repeat('0', CAST(c AS INTEGER)) AS HUGEINT)",
        // DECIMAL/integer -> {m: mantissa, s: scale}; DuckDB prints every scale digit of a DECIMAL
        "CREATE OR REPLACE MACRO mssql_dec_parts(x) AS list_transform([CASE WHEN typeof(x) IN ('DOUBLE', 'FLOAT') THEN error('mssql_div: operands must be DECIMAL or integer, not ' || typeof(x)) ELSE CAST(x AS VARCHAR) END], \
         lambda dp_t: {'m': CAST(replace(dp_t, '.', '') AS HUGEINT), 's': CASE WHEN strpos(dp_t, '.') > 0 THEN length(dp_t) - strpos(dp_t, '.') ELSE 0 END})[1]",
        // one long-division chunk: append min(e, k) quotient digits
        "CREATE OR REPLACE MACRO mssql_div_step(st) AS list_transform([st], lambda ds_w: {\
         'q': ds_w.q * mssql_pow10(least(ds_w.e, ds_w.k)) + (ds_w.r * mssql_pow10(least(ds_w.e, ds_w.k))) // ds_w.d, \
         'r': (ds_w.r * mssql_pow10(least(ds_w.e, ds_w.k))) % ds_w.d, 'e': ds_w.e - least(ds_w.e, ds_w.k), 'd': ds_w.d, 'k': ds_w.k})[1]",
        // trunc(n * 10^e / d) for n, d >= 0 in up to four HUGEINT chunks
        "CREATE OR REPLACE MACRO mssql_div_mant(n, d, e) AS list_transform([mssql_div_step(mssql_div_step(mssql_div_step(mssql_div_step(\
         {'q': n // d, 'r': n % d, 'e': greatest(e, 0), 'd': d, 'k': greatest(37 - length(CAST(d AS VARCHAR)), 1)}))))], \
         lambda dm_z: CASE WHEN dm_z.e > 0 THEN error('mssql_div: operands too large for exact division') ELSE dm_z.q END)[1]",
        // mantissa -> text with s decimals, overflow check against numeric(p, s)
        "CREATE OR REPLACE MACRO mssql_div_fmt(neg, m, p, s) AS list_transform([CAST(m AS VARCHAR)], lambda df_t: list_transform(\
         [CASE WHEN length(df_t) < s + 1 THEN repeat('0', s + 1 - length(df_t)) || df_t ELSE df_t END], lambda df_u: \
         CASE WHEN length(ltrim(left(df_u, length(df_u) - s), '0')) > p - s THEN error('Arithmetic overflow error converting numeric to data type numeric.') \
         ELSE (CASE WHEN neg AND m <> 0 THEN '-' ELSE '' END) || left(df_u, length(df_u) - s) || CASE WHEN s > 0 THEN '.' || right(df_u, s) ELSE '' END END)[1])[1]",
        "CREATE OR REPLACE MACRO mssql_div(a, b, p, s) AS list_transform([{'x': mssql_dec_parts(a), 'y': mssql_dec_parts(b)}], lambda dv_o: \
         CASE WHEN dv_o.x.m IS NULL OR dv_o.y.m IS NULL THEN NULL \
         WHEN dv_o.y.m = 0 THEN error('Divide by zero error encountered.') \
         WHEN s + dv_o.y.s - dv_o.x.s < 0 THEN mssql_div_fmt((dv_o.x.m < 0) != (dv_o.y.m < 0), abs(dv_o.x.m) // (abs(dv_o.y.m) * mssql_pow10(dv_o.x.s - s - dv_o.y.s)), p, s) \
         ELSE mssql_div_fmt((dv_o.x.m < 0) != (dv_o.y.m < 0), mssql_div_mant(abs(dv_o.x.m), abs(dv_o.y.m), s + dv_o.y.s - dv_o.x.s), p, s) END)[1]",
        // DuckDB round()/trunc() on DECIMAL are exact (half away from zero / toward zero) but drop the scale; `+ (x - x)` restores it
        "CREATE OR REPLACE MACRO mssql_round(x, n) AS round(x, n) + (x - x)",
        "CREATE OR REPLACE MACRO mssql_round_trunc(x, n) AS trunc(x, n) + (x - x)",
        // TRY_CAST(x AS int): spaces trimmed, '' / sign only -> 0, '12.5' / tabs / junk -> NULL, out of range -> NULL
        "CREATE OR REPLACE MACRO mssql_try_int(x) AS CASE WHEN typeof(x) IN ('VARCHAR', 'STRING_LITERAL') THEN \
         list_transform([regexp_extract(trim(CAST(x AS VARCHAR), ' '), '^([+-]?) *([0-9]*)$', ['sg', 'dg'])], lambda ti_r: \
         CASE WHEN CAST(x AS VARCHAR) IS NULL THEN NULL WHEN NOT regexp_full_match(trim(CAST(x AS VARCHAR), ' '), '[+-]? *[0-9]*') THEN NULL \
         WHEN ti_r.dg = '' THEN 0 ELSE TRY_CAST(ti_r.sg || ti_r.dg AS INTEGER) END)[1] \
         ELSE TRY_CAST(trunc(TRY_CAST(x AS DECIMAL(38,10))) AS INTEGER) END",
        // FORMAT(x, '0.0%'): x * 100 rounded half away from zero to d decimals (exact DECIMAL), no grouping, no '-0';
        // .NET quirk reproduced: a DECIMAL zero of scale 0 / 1 prints '000.0%' / '00.0%'
        "CREATE OR REPLACE MACRO mssql_format_pct(x, d) AS CASE WHEN typeof(x) LIKE 'DECIMAL%' AND x = 0 AND strpos(CAST(x AS VARCHAR), '.') = 0 THEN '00' \
         WHEN typeof(x) LIKE 'DECIMAL%' AND x = 0 AND length(CAST(x AS VARCHAR)) - strpos(CAST(x AS VARCHAR), '.') = 1 THEN '0' ELSE '' END \
         || CAST(round(CAST(x AS DECIMAL(35,15)) * CAST(100 AS DECIMAL(3,0)), d) AS VARCHAR) || '%'",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Names of the user-facing macros (the helpers `mssql_pow10`, `mssql_dec_parts`, `mssql_div_*` are internal).
pub const MACRO_NAMES: &[&str] = &["mssql_div", "mssql_round", "mssql_round_trunc", "mssql_try_int", "mssql_format_pct"];

/// Install the macros on a connection.
pub fn install(con: &Connection) -> Result<()> {
    for m in macros() { duck::exec(con, &m)?; }
    Ok(())
}

// ------------------------------------------------------------------ SQL Server numeric result types

const MAXP: u32 = 38;

/// Scale reduction for multiplication and division when the precision exceeds 38.
fn reduce_muldiv(p: u32, s: u32) -> (u32, u32) {
    if p <= MAXP { return (p, s); }
    let int = p - s;
    let s = if int < 32 { s.min(MAXP - int) } else { s.min(6) };
    (MAXP, s)
}

/// Result type of `numeric(p1,s1) / numeric(p2,s2)` in SQL Server.
pub fn div_type(p1: u32, s1: u32, p2: u32, s2: u32) -> (u32, u32) {
    let s = 6u32.max(s1 + p2 + 1);
    let p = p1 - s1 + s2 + s;
    reduce_muldiv(p, s)
}

/// Result type of `numeric(p1,s1) * numeric(p2,s2)` in SQL Server.
pub fn mul_type(p1: u32, s1: u32, p2: u32, s2: u32) -> (u32, u32) {
    reduce_muldiv(p1 + p2 + 1, s1 + s2)
}

/// Result type of `numeric(p1,s1) + / - numeric(p2,s2)` in SQL Server.
pub fn add_type(p1: u32, s1: u32, p2: u32, s2: u32) -> (u32, u32) {
    let s = s1.max(s2);
    let int = (p1 - s1).max(p2 - s2);
    let p = int + s + 1;
    if p <= MAXP { (p, s) } else { (MAXP, MAXP.saturating_sub(int)) }
}

/// Precision/scale of a numeric literal the way SQL Server types it (`0.05` -> (2,2), `007.50` -> (3,2), `7` -> (1,0)).
pub fn literal_type(lit: &str) -> Option<(u32, u32)> {
    let l = lit.trim();
    if l.is_empty() || !l.chars().all(|c| c.is_ascii_digit() || c == '.') || l.matches('.').count() > 1 { return None; }
    let (ip, fp) = l.split_once('.').unwrap_or((l, ""));
    let ip = ip.trim_start_matches('0');
    let s = fp.len() as u32;
    let p = (ip.len() as u32 + s).max(1).max(s);
    Some((p, s))
}

// ------------------------------------------------------------------ verification cases

/// One expression evaluated on both engines; both sides return text ('NULL' for NULL) to be compared verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MathCase {
    pub kind: &'static str,
    pub expr_tsql: String,
    pub expr_duck: String,
}

struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u32, hi: u32) -> u32 { lo + (self.next() % (hi - lo + 1) as u64) as u32 }
    fn digits(&mut self, n: u32) -> String { (0..n).map(|_| char::from(b'0' + (self.next() % 10) as u8)).collect() }
}

/// Random decimal text with at most `int` integral and exactly `frac` fractional digits.
fn rand_dec(r: &mut XorShift, int: u32, frac: u32, nonzero: bool) -> String {
    loop {
        let ni = if int == 0 { 0 } else { r.range(0, int) };
        let mut ip = r.digits(ni).trim_start_matches('0').to_string();
        if ip.is_empty() { ip = "0".into(); }
        let fp = r.digits(frac);
        let neg = r.next() % 3 == 0;
        let v = if frac > 0 { format!("{ip}.{fp}") } else { ip };
        if nonzero && v.chars().all(|c| c == '0' || c == '.') { continue; }
        return if neg { format!("-{v}") } else { v };
    }
}

fn wrap_t(e: &str) -> String { format!("COALESCE(CAST({e} AS varchar(100)), 'NULL')") }
fn wrap_d(e: &str) -> String { format!("COALESCE(CAST({e} AS VARCHAR), 'NULL')") }

/// Deterministic random cases for every macro (xorshift seeded by `seed`), `n` in total.
pub fn verify_cases(n: usize, seed: u64) -> Vec<MathCase> {
    let mut r = XorShift(seed.max(1) ^ 0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let case = match i % 5 {
            0 => {
                // numeric / numeric (or int)
                let (p1, s1) = { let p = r.range(1, 20); (p, r.range(0, p.min(10))) };
                let (p2, s2) = if r.next() % 4 == 0 { (10, 0) } else { let p = r.range(1, 18); (p, r.range(0, p.min(8))) };
                let a = rand_dec(&mut r, p1 - s1, s1, false);
                let b = rand_dec(&mut r, (p2 - s2).min(9), s2, true);
                let (p, s) = div_type(p1, s1, p2, s2);
                let (ta, da) = (format!("CAST('{a}' AS decimal({p1},{s1}))"), format!("CAST('{a}' AS DECIMAL({p1},{s1}))"));
                let (tb, db) = if (p2, s2) == (10, 0) { (format!("CAST('{b}' AS int)"), format!("CAST('{b}' AS INTEGER)")) } else { (format!("CAST('{b}' AS decimal({p2},{s2}))"), format!("CAST('{b}' AS DECIMAL({p2},{s2}))")) };
                MathCase { kind: "mssql_div", expr_tsql: wrap_t(&format!("{ta} / {tb}")), expr_duck: wrap_d(&format!("mssql_div({da}, {db}, {p}, {s})")) }
            }
            1 | 2 => {
                // ROUND(x, n) / ROUND(x, n, 1); keep one spare integral digit so rounding up cannot overflow numeric(p, s)
                let p = r.range(2, 18);
                let s = r.range(0, (p - 1).min(8));
                let x = rand_dec(&mut r, p - s - 1, s, false);
                let nd = r.range(0, s + 2) as i64 - 2;
                if i % 5 == 1 {
                    MathCase { kind: "mssql_round", expr_tsql: wrap_t(&format!("ROUND(CAST('{x}' AS decimal({p},{s})), {nd})")), expr_duck: wrap_d(&format!("mssql_round(CAST('{x}' AS DECIMAL({p},{s})), {nd})")) }
                } else {
                    MathCase { kind: "mssql_round_trunc", expr_tsql: wrap_t(&format!("ROUND(CAST('{x}' AS decimal({p},{s})), {nd}, 1)")), expr_duck: wrap_d(&format!("mssql_round_trunc(CAST('{x}' AS DECIMAL({p},{s})), {nd})")) }
                }
            }
            3 => {
                // TRY_CAST(text AS int)
                let pick = |r: &mut XorShift, xs: &[&str]| xs[(r.next() % xs.len() as u64) as usize].to_string();
                let lead = pick(&mut r, &["", "", " ", "  "]);
                let sign = pick(&mut r, &["", "", "-", "+"]);
                let mid = pick(&mut r, &["", "", " "]);
                let nd = r.range(0, 11);
                let digits = r.digits(nd);
                let tail = pick(&mut r, &["", "", "", " ", ".5", "x", " 3", "."]);
                let s = format!("{lead}{sign}{mid}{digits}{tail}");
                MathCase { kind: "mssql_try_int", expr_tsql: wrap_t(&format!("TRY_CAST('{s}' AS int)")), expr_duck: wrap_d(&format!("mssql_try_int('{s}')")) }
            }
            _ => {
                // FORMAT(x, '0.0%')
                let s = r.range(0, 6);
                let x = rand_dec(&mut r, 4, s, false);
                let d = r.range(0, 3);
                let fmt = if d == 0 { "0%".to_string() } else { format!("0.{}%", "0".repeat(d as usize)) };
                MathCase { kind: "mssql_format_pct", expr_tsql: format!("COALESCE(FORMAT(CAST('{x}' AS decimal(12,{s})), '{fmt}'), 'NULL')"), expr_duck: wrap_d(&format!("mssql_format_pct(CAST('{x}' AS DECIMAL(12,{s})), {d})")) }
            }
        };
        out.push(case);
    }
    out
}

/// The SQL Server query for one case (a single text column).
pub fn tsql_query(c: &MathCase) -> String { format!("SELECT {}", c.expr_tsql) }

/// Evaluate every case on DuckDB (installs the macros first). Errors are returned as `ERROR: ...` text.
pub fn duck_eval(con: &Connection, cases: &[MathCase]) -> Result<Vec<String>> {
    install(con)?;
    let mut out = Vec::with_capacity(cases.len());
    for c in cases {
        let v = duck::query_scalar::<String>(con, &format!("SELECT {}", c.expr_duck));
        out.push(match v { Ok(s) => s, Err(e) => format!("ERROR: {}", e.to_string().lines().next().unwrap_or("")) });
    }
    Ok(out)
}

/// A case whose DuckDB text differs from SQL Server's.
#[derive(Debug, Clone)]
pub struct Mismatch {
    pub index: usize,
    pub case: MathCase,
    pub duck: String,
    pub mssql: String,
}

/// Compare result texts index by index.
pub fn compare(cases: &[MathCase], duck: &[String], mssql: &[String]) -> Vec<Mismatch> {
    cases.iter().enumerate().filter_map(|(i, c)| {
        let (d, m) = (duck.get(i).cloned().unwrap_or_default(), mssql.get(i).cloned().unwrap_or_default());
        (d != m).then(|| Mismatch { index: i, case: c.clone(), duck: d, mssql: m })
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn con() -> Connection {
        let c = duck::open_in_memory().unwrap();
        install(&c).unwrap();
        c
    }
    fn q(c: &Connection, e: &str) -> String {
        duck::query_scalar::<String>(c, &format!("SELECT COALESCE(CAST({e} AS VARCHAR), 'NULL')")).unwrap()
    }

    // Every expected value below was produced by SQL Server 2019 (container on :14319) with
    // `SELECT CAST(<expr> AS varchar(100))`; result types with SQL_VARIANT_PROPERTY(<expr>, 'Precision'/'Scale').
    #[test]
    fn result_type_rules() {
        // 7 * 1.0 / 3: literals 7 -> numeric(1,0), 1.0 -> numeric(2,1); (1,0)*(2,1) = (4,1); (4,1)/(1,0) = (9,6)
        assert_eq!(literal_type("7"), Some((1, 0)));
        assert_eq!(literal_type("1.0"), Some((2, 1)));
        assert_eq!(literal_type("0.05"), Some((2, 2)));
        assert_eq!(literal_type("007.50"), Some((3, 2)));
        assert_eq!(literal_type(".5"), Some((1, 1)));
        assert_eq!(mul_type(1, 0, 2, 1), (4, 1));
        assert_eq!(div_type(4, 1, 1, 0), (9, 6));
        // int column * 1.0 / int column: (10,0)*(2,1) = (13,1); (13,1)/(10,0) = (24,12)
        assert_eq!(mul_type(10, 0, 2, 1), (13, 1));
        assert_eq!(div_type(13, 1, 10, 0), (24, 12));
        // reductions above precision 38
        assert_eq!(div_type(38, 10, 1, 0), (38, 10));
        assert_eq!(div_type(18, 4, 18, 4), (38, 20));
        assert_eq!(div_type(38, 0, 1, 0), (38, 6));
        assert_eq!(div_type(22, 1, 19, 0), (38, 17)); // bigint * 1.0 / bigint
        assert_eq!(div_type(38, 10, 38, 10), (38, 6));
        assert_eq!(div_type(10, 2, 5, 1), (17, 8));
        assert_eq!(add_type(38, 10, 38, 20), (38, 10));
        assert_eq!(add_type(10, 2, 5, 4), (13, 4));
        assert_eq!(add_type(38, 30, 38, 0), (38, 0));
    }

    #[test]
    fn div_matches_sql_server() {
        let c = con();
        let cases = [
            ("mssql_div(CAST(7*1.0 AS DECIMAL(4,1)), CAST(3 AS DECIMAL(1,0)), 9, 6)", "2.333333"),          // SELECT 7*1.0/3
            ("mssql_div(CAST(-7*1.0 AS DECIMAL(4,1)), CAST(3 AS DECIMAL(1,0)), 9, 6)", "-2.333333"),
            ("mssql_div(CAST(2.0 AS DECIMAL(2,1)), CAST(3 AS DECIMAL(1,0)), 7, 6)", "0.666666"),            // 2.0/3: truncated, not .666667
            ("mssql_div(CAST(10 AS INTEGER) * 1.0, CAST(4 AS INTEGER), 24, 12)", "2.500000000000"),         // CAST(10 AS int)*1.0/CAST(4 AS int)
            ("mssql_div(CAST(1 AS DECIMAL(10,2)), CAST(7 AS DECIMAL(5,1)), 17, 8)", "0.14285714"),
            ("mssql_div(CAST(-1 AS DECIMAL(10,2)), CAST(7 AS DECIMAL(5,1)), 17, 8)", "-0.14285714"),
            ("mssql_div(CAST(12345678901234.5678 AS DECIMAL(18,4)), CAST(3.3 AS DECIMAL(18,4)), 38, 20)", "3741114818555.92963636363636363636"),
            ("mssql_div(CAST(1 AS BIGINT) * 1.0, CAST(3 AS BIGINT), 38, 17)", "0.33333333333333333"),
            ("mssql_div(CAST(2 AS DECIMAL(38,10)), CAST(3 AS DECIMAL(38,10)), 38, 6)", "0.666666"),
            ("mssql_div(CAST(1 AS DECIMAL(38,10)), CAST(3 AS DECIMAL(1,0)), 38, 10)", "0.3333333333"),
            ("mssql_div(CAST(2 AS DECIMAL(18,4)), CAST(3 AS DECIMAL(18,4)), 38, 20)", "0.66666666666666666666"),
            ("mssql_div(CAST(7 AS INTEGER), CAST(2.0 AS DECIMAL(5,1)), 17, 6)", "3.500000"),
            ("mssql_div(CAST(5 AS DECIMAL(10,2)), CAST(0.3 AS DECIMAL(1,1)), 15, 6)", "16.666666"),
            ("mssql_div(CAST(-2 AS DECIMAL(10,0)), CAST(3.0 AS DECIMAL(2,1)), 18, 6)", "-0.666666"),
            ("mssql_div(NULL, 3, 10, 6)", "NULL"),
        ];
        for (e, want) in cases { assert_eq!(q(&c, e), want, "{e}"); }
        // a 28-digit dividend needs several HUGEINT chunks
        assert_eq!(q(&c, "mssql_div(CAST('1234567890123456789012345678.1234567890' AS DECIMAL(38,10)), CAST('9876543210987654321.12345' AS DECIMAL(38,10)), 38, 6)"), "124999998.860937");
        let err = duck::query_scalar::<String>(&c, "SELECT mssql_div(1, 0, 10, 6)").unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "{err}");
        let err = duck::query_scalar::<String>(&c, "SELECT mssql_div(1.5::DOUBLE, 2, 10, 6)").unwrap_err().to_string();
        assert!(err.contains("DECIMAL or integer"), "{err}");
    }

    #[test]
    fn round_trunc_try_int_format() {
        let c = con();
        let cases = [
            ("mssql_round(2.5::DECIMAL(2,1), 0)", "3.0"),           // ROUND(2.5,0)
            ("mssql_round(-2.5::DECIMAL(2,1), 0)", "-3.0"),         // ROUND(-2.5,0)
            ("mssql_round(2.345::DECIMAL(4,3), 2)", "2.350"),
            ("mssql_round(-2.345::DECIMAL(4,3), 2)", "-2.350"),
            ("mssql_round(1.005::DECIMAL(10,3), 2)", "1.010"),
            ("mssql_round(125::DECIMAL(10,0), -1)", "130"),
            ("mssql_round(CAST(-125 AS INTEGER), -1)", "-130"),
            ("mssql_round(9.5::DECIMAL(3,1), 0)", "10.0"),
            ("mssql_round_trunc(2.567::DECIMAL(4,3), 2)", "2.560"), // ROUND(2.567,2,1)
            ("mssql_round_trunc(-2.567::DECIMAL(4,3), 2)", "-2.560"),
            ("mssql_round_trunc(-1.005::DECIMAL(10,3), 2)", "-1.000"),
            ("mssql_round_trunc(129::DECIMAL(10,2), -1)", "120.00"),
            ("mssql_try_int('')", "0"),                              // TRY_CAST('' AS int)
            ("mssql_try_int(' ')", "0"),
            ("mssql_try_int('12.5')", "NULL"),
            ("mssql_try_int(' 12 ')", "12"),
            ("mssql_try_int('+12')", "12"),
            ("mssql_try_int('- 1')", "-1"),
            ("mssql_try_int('-')", "0"),
            ("mssql_try_int('12 3')", "NULL"),
            ("mssql_try_int('0012')", "12"),
            ("mssql_try_int('12.')", "NULL"),
            ("mssql_try_int('$12')", "NULL"),
            ("mssql_try_int('abc')", "NULL"),
            ("mssql_try_int('1e3')", "NULL"),
            ("mssql_try_int('2147483648')", "NULL"),
            ("mssql_try_int('-2147483648')", "-2147483648"),
            ("mssql_try_int(chr(9) || '12')", "NULL"),
            ("mssql_try_int(12.7)", "12"),
            ("mssql_try_int(NULL::VARCHAR)", "NULL"),
            ("mssql_format_pct(0.12345, 1)", "12.3%"),              // FORMAT(0.12345,'0.0%')
            ("mssql_format_pct(0.12355, 1)", "12.4%"),
            ("mssql_format_pct(-0.00049, 1)", "0.0%"),
            ("mssql_format_pct(-0.0005, 1)", "-0.1%"),
            ("mssql_format_pct(0.5, 1)", "50.0%"),
            ("mssql_format_pct(1.23456, 2)", "123.46%"),
            ("mssql_format_pct(-0.12345, 1)", "-12.3%"),
            ("mssql_format_pct(12.5, 0)", "1250%"),
            ("mssql_format_pct(1234.5, 1)", "123450.0%"),
            ("mssql_format_pct(0.125::DOUBLE, 0)", "13%"),
            ("mssql_format_pct(0.00125, 2)", "0.13%"),
            ("mssql_format_pct(CAST(0 AS DECIMAL(12,0)), 1)", "000.0%"),  // FORMAT(CAST(0 AS decimal(12,0)),'0.0%')
            ("mssql_format_pct(CAST(0 AS DECIMAL(12,1)), 1)", "00.0%"),
            ("mssql_format_pct(CAST(0 AS DECIMAL(12,2)), 1)", "0.0%"),
            ("mssql_format_pct(CAST(0 AS DECIMAL(12,0)), 0)", "000%"),
            ("mssql_format_pct(CAST(0 AS DOUBLE), 1)", "0.0%"),
            ("mssql_format_pct(CAST(0 AS INTEGER), 1)", "0.0%"),
            ("mssql_format_pct(CAST(-0.0001 AS DECIMAL(12,4)), 0)", "0%"),
        ];
        for (e, want) in cases { assert_eq!(q(&c, e), want, "{e}"); }
    }

    #[test]
    fn verify_cases_are_deterministic_and_run_on_duckdb() {
        let a = verify_cases(50, 7);
        assert_eq!(a, verify_cases(50, 7));
        assert_ne!(a, verify_cases(50, 8));
        assert_eq!(a.len(), 50);
        for k in MACRO_NAMES { assert!(a.iter().any(|c| c.kind == *k), "{k}"); }
        let c = duck::open_in_memory().unwrap();
        let out = duck_eval(&c, &a).unwrap();
        for (o, cs) in out.iter().zip(&a) { assert!(!o.starts_with("ERROR"), "{} -> {o}", cs.expr_duck); }
    }

    /// Live check against SQL Server:
    /// `DUCKBRIDGE_SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C -S localhost,14319 -U sa -P <pw>" cargo test --release --lib mathcompat -- --ignored`
    #[test]
    #[ignore]
    fn verify_cases_against_sql_server() {
        let Ok(cmd) = std::env::var("DUCKBRIDGE_SQLCMD") else { return };
        let n: usize = std::env::var("DUCKBRIDGE_MATH_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(500);
        let cases = verify_cases(n, 42);
        let duck = duck_eval(&duck::open_in_memory().unwrap(), &cases).unwrap();
        let mut script = String::from("SET NOCOUNT ON;\n");
        for c in &cases { script.push_str(&tsql_query(c)); script.push_str(";\n"); }
        let dir = std::env::temp_dir().join(format!("duckbridge-verify-math-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("cases.sql");
        std::fs::write(&f, script).unwrap();
        let mut parts = cmd.split_whitespace();
        let out = std::process::Command::new(parts.next().unwrap()).args(parts).args(["-d", "master", "-h", "-1", "-W", "-i"]).arg(&f).output().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let text = String::from_utf8_lossy(&out.stdout);
        let mssql: Vec<String> = text.lines().map(|l| l.to_string()).filter(|l| !l.is_empty()).collect();
        assert_eq!(mssql.len(), cases.len(), "{text}");
        let bad = compare(&cases, &duck, &mssql);
        let report: Vec<String> = bad.iter().map(|m| format!("{} -> duck {:?} mssql {:?}", m.case.expr_tsql, m.duck, m.mssql)).collect();
        assert!(bad.is_empty(), "{} mismatches:\n{}", bad.len(), report.join("\n"));
    }
}
