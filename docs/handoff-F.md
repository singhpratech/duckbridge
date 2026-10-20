# Hand-off: stream F (translator semantics, strict mode, audit, exact math), R7

Files: `src/transpile/{mod,engine,tokens,tsql_to_duck,semantics}.rs` (semantics.rs is new), `src/mathcompat.rs`,
`src/audit.rs`, `tests/transpile_tests.rs`.

**Outside my files:** one edit in `src/main.rs` `transpile_cmd` (the `Options` literal, now line ~503; it has since been committed with the G/H work). I added
`..transpile::Options::default()` to the `Options { .. }` literal because the new fields broke the bin build for
every stream. Behaviour is unchanged. `extension/src/lib.rs:338` (TranspileScalar) has the same full literal and
needs the same `..Default::default()` the next time the extension is built. I did not touch it.

`docs/requirements-large-load-safety.md` is not in the tree, so I worked from the stream F section of
`docs/plan-0.1.1.md` and the task brief. One deliberate difference from the plan: `Translated.warnings` stays
`Vec<String>` (the rendered form, as the brief asks), and the structured form is the new `Translated.diagnostics`.
So `warning_strings()` is not needed.

## API changes

### `transpile::Options`
| field | default | meaning |
|---|---|---|
| `nocase: bool` | `true` | legacy. Used only when `nocase_mode` is `None`: true -> Rewrite, false -> Off |
| `nocase_mode: Option<NocaseMode>` | `None` | `Global` / `Rewrite` / `Off`. `opts.nocase_mode()` returns the effective mode |
| `strict: bool` | `false` | any warning makes `translate_with` return `Err(TranslateError::Strict(Vec<Warning>))` |
| `mssql_math: bool` | `false` | emit `mssql_div` / `mssql_round` / `mssql_round_trunc` / `mssql_try_int` / `mssql_format_pct`, and add `mathcompat::macros()` to the prelude |
| `types: HashMap<String,String>` | empty | lower-case `column` or `table.column` -> SQL Server type (`decimal(18,4)`, `int`, `varchar(20)`) |

Column types from `CREATE TABLE` / `DECLARE @t TABLE` in the same script, and scalar `DECLARE @v type`, are
picked up automatically. Entries in `Options.types` take precedence.

### Other public items
- `Warning { code: &'static str, line, col, message }`. Display is `W-DIV-DECIMAL (line 12:8): message`
  (0:0 when unknown).
- `transpile::codes::*`: `W-DIV-DECIMAL`, `W-ROUND-TRUNC`, `W-FORMAT-NUM`, `W-TRYCAST-EMPTY`,
  `W-CI-COMPARE`, `W-TRAILING-SPACE`, `W-PROCEDURAL`, `W-POWER-DOUBLE`, and `W-GENERIC` for every older
  free-text warning (`ctx.warn(..)`).
- `Translated { sql, warnings: Vec<String> /* rendered */, diagnostics: Vec<Warning> }`. Existing callers
  compile unchanged.
- `TranslateError::Strict(Vec<Warning>)`. Display is `strict mode: N translation warning(s):` followed by one
  warning per line. Callers already use `?` before executing anything, so a strict failure runs nothing.
- `Tok` now has `line`/`col` (1-based; 0 for synthesised tokens). Token equality ignores positions.
- `Ctx::warn_code(code, msg)`, `Ctx::warn_at(code, &tok, msg)` and `Ctx::warn_stmt_once(code, &tok, msg)`.
- `transpile::parse_types("amount=decimal(18,4),qty=int")` parses `--types`. Commas inside parentheses are
  kept, `;` is also a separator, and `col:type` is accepted.
- `NocaseMode::parse("global"|"rewrite"|"off")`.
- `duckdb_prelude(nocase: bool)` is **unchanged** (true still means the global collation, 4 lines).
  `duckdb_prelude_for(mode)` is new: 3 lines, plus `default_collation` only for Global.
  `translate_with(.., prelude: true)` uses `duckdb_prelude_for(opts.nocase_mode())`, followed by the macros
  when `mssql_math` is set.

### Rewrite mode (the new default)
- `LIKE` -> `ILIKE` (unchanged).
- In predicates (WHERE / ON / HAVING / WHEN, including subqueries):
  - `col = 'Lit'` and `col <> 'Lit'` become `lower(rtrim(col, ' ')) = lower('Lit')`; `'Lit' = col` is mirrored.
  - `col [NOT] IN ('a','b')` becomes `lower(rtrim(col, ' ')) [NOT] IN (lower('a'), lower('b'))`.
  - Trailing spaces are stripped from the literal, which makes the comparison exact for SQL Server's padding
    rule too. This is why `rtrim` is there and why rewritten comparisons get no W-TRAILING-SPACE.
  - The rewrite applies only when the literal contains a letter. It does not apply when the other side is
    known to be numeric or a date, when there is an explicit COLLATE, or when the comparison is inside
    arithmetic or concatenation.
  - `SET a = 'x'`, `VALUES`, select-list `alias = expr` and function arguments are never touched.
- Not rewritten, reported as W-CI-COMPARE once per statement: GROUP BY / SELECT DISTINCT on string columns,
  column = column on strings (joins), `<` `>` `<=` `>=` with a letter literal, simple `CASE x WHEN 'Lit'`,
  `IN (subquery)`, and mixed IN lists. "String column" means a column known from `types` or the script.
- W-TRAILING-SPACE: string equalities that were not rewritten. That covers Off/Global modes, column = column,
  digit-only literals against a known char column, and comparisons with an explicit COLLATE.
- The pass runs on SELECT / WITH / INSERT / UPDATE / DELETE / MERGE / CREATE VIEW.

## How the integrator wires the CLI (stream H)

```
run … --strict --nocase <global|rewrite|off> --mssql-math --types <csv>
```
```rust
let mode = transpile::NocaseMode::parse(&a.nocase).ok_or_else(|| anyhow!("--nocase global|rewrite|off"))?;
let opts = transpile::Options { prelude: false, nocase_mode: Some(mode), strict: a.strict,
    mssql_math: a.mssql_math, types: transpile::parse_types(&a.types), ..transpile::Options::default() };
let tr = transpile::translate_with(sql, from, Dialect::DuckDb, &sm, &opts)?;   // Strict -> error, nothing executed
if from == Dialect::Tsql {
    for p in transpile::duckdb_prelude_for(mode) { con.execute_batch(p)?; }     // was duckdb_prelude(nocase)
    if a.mssql_math { duckbridge::mathcompat::install(&con)?; }               // run_script uses prelude:false
}
```
- Where to apply it: `src/main.rs` `run` (lines ~411–415) and `api::run_script` (which still calls
  `duckdb_prelude(nocase)`, i.e. Global). Both must switch to `duckdb_prelude_for(opts.nocase_mode())`,
  otherwise Rewrite users also get the global collation. That is harmless but defeats the purpose.
  `transpile_cmd` (main.rs ~503) needs `nocase_mode` / `strict` / `mssql_math` / `types` the same way.
- Strict mode exit code: map `TranslateError::Strict` to `Failure::Check` (exit 1). Print its Display, which
  is one warning per line.
- Warnings rows: `tr.warnings` (strings) still works. Use `tr.diagnostics` for structured output such as a
  `--log` JSON with `code`, `line` and `col`.
- `audit-ci <duckdb> --tables … --columns … [--script file]` calls
  `duckbridge::audit::audit_ci(&con, &tables, &columns, script.as_deref()) -> Result<Vec<AuditRow>>`.
  `AuditRow { table, column, kind: "case"|"trailing-space"|"literal", examples, count }`. Exit 1 when the
  result is non-empty, if you want it to act as a check. `audit::script_literals(script)` is public too.
- `verify-math --host … [--cases n]`:
  ```rust
  let cases = duckbridge::mathcompat::verify_cases(n, seed /* e.g. 42 */);
  let duck = duckbridge::mathcompat::duck_eval(&duck_con, &cases)?;          // installs the macros
  let ms: Vec<String> = /* for c in &cases: run mathcompat::tsql_query(c) via mssql::exec / query, column 0 as text */;
  let bad = duckbridge::mathcompat::compare(&cases, &duck, &ms);             // Vec<Mismatch{index, case, duck, mssql}>
  ```
  Every case returns one text column, with `'NULL'` for NULL on both sides. These are read-only SELECTs, so no
  guard is involved.
- `--plan`: `mathcompat::macros()` are DuckDB-side statements only. Nothing goes to SQL Server.

## Changed test expectations
- `tests/transpile_tests.rs`:
  - `nstrings` now expects `lower(rtrim(x, ' ')) = lower('a''b')` (Rewrite default).
  - `prelude_emitted` now expects no `default_collation` and 3 SET lines with default Options, and still 4
    with `NocaseMode::Global`.
  - All other 63 original tests are unchanged. There are 16 new tests (81 in total), one per warning code, plus strict mode, the Rewrite cases (including execution on DuckDB), legacy flag mapping and `mssql_math` end to end.
- `scripts/e2e_extension.sh:36` asserts `bridge_prelude(true)` = 4 lines. That still holds today because
  `duckdb_prelude(true)` keeps its Global meaning. If H changes `bridge_prelude` (extension/src/lib.rs:371) to
  the Rewrite default (`duckdb_prelude_for(NocaseMode::Rewrite)`), the expectation becomes **3**. The
  extension's `TranspileScalar` output for T-SQL with string literals will also show the `lower(rtrim(..))`
  rewrite and coded `-- WARNING: W-…` lines.

## Verified against SQL Server 2019 (localhost:14319, read-only SELECTs on master)
`7*1.0/3` = 2.333333 (numeric(9,6), because literals are typed by their digits); `CAST(10 AS int)*1.0/CAST(4 AS int)` =
2.500000000000 (24,12); `2.0/3` = .666666 (truncated, not rounded); (38,10)/3 -> (38,10); (18,4)/(18,4) ->
(38,20); bigint*1.0/bigint -> (38,17); `ROUND(2.5,0)` = 3.0, `ROUND(-2.5,0)` = -3.0, `ROUND(2.567,2,1)` = 2.560;
`TRY_CAST('' AS int)` = 0, `' '` -> 0, `'12.5'` -> NULL, `'- 1'` -> -1, tab -> NULL (DuckDB: NULL / 13);
`TRY_CAST('' AS decimal)` = NULL (so only int, bit, float and money get W-TRYCAST-EMPTY); `FORMAT(0.12345,'0.0%')` =
12.3%, `FORMAT(-0.00049,'0.0%')` = 0.0%; `POWER(2.5,2)` = 6.3. Every value is pinned in the
`mathcompat` unit tests and `transpile_tests::mssql_math_rewrites_and_runs`. The live random comparison is an ignored test. 3000 cases (`DUCKBRIDGE_MATH_CASES=3000`, seed 42) match SQL Server 2019 exactly, after reproducing a .NET quirk: `FORMAT(CAST(0 AS decimal(12,0)), '0.0%')` = `000.0%`, scale 1 gives `00.0%`, and scale 2 and up gives `0.0%`. Command:
`DUCKBRIDGE_SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C -S localhost,14319 -U sa -P <pw>" cargo test --release --lib mathcompat -- --ignored`.

## Open issues
- `mssql_div` returns **text**. DuckDB macros cannot take the type modifiers `p`, `s` as parameters, so the
  translator wraps it in `CAST(.. AS DECIMAL(p,s))`. Operands must be DECIMAL or integer (DOUBLE raises an
  error). Mantissas come from the DECIMAL text form and the division is exact HUGEINT long division in up to
  four chunks. A divisor with 37 or more significant digits, or more than four chunks, raises an error instead
  of giving a wrong result.
- Multiplication/addition scale reduction above precision 38 is modelled for typing (`mul_type`, `add_type`).
  The DuckDB-side product itself is DuckDB's own exact DECIMAL multiply, so it can differ only when SQL Server
  would round an intermediate product that needs more than 38 digits.
- Inference covers literals, typed columns, CAST, ROUND/ABS/FLOOR/CEILING/SUM/AVG/MIN/MAX/COALESCE, and
  declared variables. Other expressions are Unknown. A division with Unknown operands warns only when there
  is decimal evidence (a decimal literal or decimal column), so `a / b` on undeclared decimal columns is
  silent. Pass `--types`.
- `money` arithmetic follows SQL Server's money rules, not the numeric ones. It is treated as unknown decimal
  (W-DIV-DECIMAL, no rewrite).
- Not covered yet: ORDER BY on string columns (case-sensitive sort order), `AVG(int)` (int in SQL Server,
  DOUBLE in DuckDB), `CAST('' AS int)` (non-TRY: 0 vs error), BETWEEN with letter literals.
- W-TRAILING-SPACE / W-CI-COMPARE for joins and GROUP BY need known column types. Without `--types` or DDL in
  the script, only literal comparisons are analysed.
