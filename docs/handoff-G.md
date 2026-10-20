# Handoff: stream G (verification and swap, R9 and R10)

## What is implemented (only src/verify.rs and src/mssql/swap.rs changed)

### `verify` (R9)
- `VerifyLevel { Counts, Checksum, Except { sample } }`. `VerifyLevel::parse("counts" | "checksum" | "except" | "except:<n>")` and `.with_sample(n)`. Every level runs the counts. `checksum` adds the checksums. `except` adds both, plus row samples for each scope that does not match.
- `VerifyResult { table, partition: Option<i32>, ok, detail }` (Serialize, Display `OK  dbo.t partition 3: ...` / `FAIL ...`). `verify::check_results(&results) -> Result<()>` returns `Failure::Check` (exit 1) and lists every failed result.
- `pub async fn verify_table(con, c, duck, target, m, level) -> Result<Vec<VerifyResult>>`. This is **async** (the plan had it sync). The future is not `Send` because it holds `&duckdb::Connection`, so call it with `rt.block_on` on the current-thread runtime (`mssql::runtime()`). There is also `verify_table_from(con, c, src_sql, target, m, level)`, which takes any DuckDB FROM item, for `--source-parquet` (for example `read_parquet('out/t/*/*.parquet', hive_partitioning=1)`).
  - Counts: the target count comes from `target::partition_row_counts` (stream B), as a total and per partition. For a partitioned target, the DuckDB side is split by partition in a single pass. The query `SELECT canon(partcol), count(*) GROUP BY 1` (at most 5000 distinct values) gets the values, and SQL Server maps each one with `SELECT $PARTITION.fn(CAST(v AS <col type>)) FROM (VALUES ...)`. That gives collation-exact partition numbers. If there are more than 5000 values, if the partition column is not loaded, or if it is float or binary, only the totals are compared and the detail says why.
  - Checksum: `duck_checksum_sql(duck, m, Option<predicate>)` and `mssql_checksum_sql(t, m, Option<partition_number>)` both return one row: `n, c1..cN`, all as text, in mapping column order. A partitioned table is checksummed in **one scan per engine**: DuckDB groups by the partition value, SQL Server groups by `$PARTITION.fn(col)`, and the additive sums are combined per partition in Rust (i128). A mismatch reports the columns that differ. The public per-partition forms (`WHERE $PARTITION.fn(col) = n`, or a DuckDB predicate) are available, for example to check a staging table before SWITCH.
  - Except: for each failing scope (the table or one partition), both sides produce the canonical row text (columns joined by U+001F). Rust diffs the two as multisets and reports up to `sample` rows each way, with `NULL` and ` | ` shown. Above 1 M rows in a scope, both sides keep only `hash(key text) % ceil(rows/1M) = 0`. The key is the PK, or the clustered key, or the whole row when there is neither. The hash is the first 4 MD5 bytes and is the same in both engines.
  - Checksums of string columns need SQL Server 2019+ (the UTF-8 collation). On an older server, `verify_table` returns `Failure::Run` and says to use `--verify counts`.
- `pub async fn verify_indexes(c, before) -> Result<Vec<VerifyResult>>`: re-reads the table with `target::read_target_tables`. For every index in `before` (heap entries skipped), it compares type, PK, uniqueness, key columns and order (ASC/DESC), INCLUDE columns, filter, compression, fill factor, data space and the unique-constraint flag. It also checks `sys.indexes.is_disabled = 0`. The PK must still exist, the heap/clustered state must not change, every check constraint in `before` must exist with `is_not_trusted = 0` (new untrusted checks fail too), and foreign keys on the table must be enabled and trusted (`sys.foreign_keys`). Extra indexes are reported as OK.
- Every query is logged with `runlog::stmt("target"|"duckdb", "verify", ...)`. The queries only read, so they also run under `--plan`.

### Canonical text form (identical in both engines; the TARGET type decides)

| SQL Server type | SQL Server expression | DuckDB expression |
|---|---|---|
| tinyint/smallint/int/bigint | `CONVERT(varchar(20), x)` | `CAST(CAST(x AS BIGINT) AS VARCHAR)` |
| bit | `CONVERT(varchar(1), x)` | `CAST(CAST(CAST(x AS BOOLEAN) AS INTEGER) AS VARCHAR)` |
| decimal/numeric(p,s) | `CONVERT(varchar(50), CAST(x AS decimal(p,s)))` (e.g. `-0.50`, `12.50000`) | `CAST(CAST(x AS DECIMAL(p,s)) AS VARCHAR)` |
| money / smallmoney | as decimal(19,4) / decimal(10,4) | same |
| char/nchar | `RTRIM(x)` | `rtrim(x)` (spaces only on both sides) |
| varchar/nvarchar/text/ntext | as is | as is |
| date | `CONVERT(varchar(10), x, 23)` | `strftime(x, '%Y-%m-%d')` |
| datetime2(n) | `CONVERT(varchar(27), x, 121)`: n digits, no `.` for n=0 | epoch micros (nanos for TIMESTAMP_NS) rounded half up to n digits, `strftime(... '%Y-%m-%d %H:%M:%S.%f')` cut to n digits, `0` padded to 7 |
| datetime | `CONVERT(varchar(23), x, 121)` | micros rounded to the nearest 1/300 s tick, rendered as SQL Server ms (.000/.003/.007) |
| smalldatetime | `CONVERT(varchar(19), x, 120)` | rounded half up to the minute at 30.000 s. This matches datetime2 -> smalldatetime on SQL Server; the container gave 29.999 s -> down and 30 s -> up |
| time(n) | `CONVERT(varchar(16), x)` | as datetime2(n), time part only |
| datetimeoffset(n) | `CONVERT(varchar(27), CAST(SWITCHOFFSET(x, 0) AS datetime2(n)), 121)` (UTC) | `epoch_us(x)` as UTC, then as datetime2(n) |
| uniqueidentifier | `LOWER(CONVERT(varchar(36), x))` | `lower(CAST(x AS VARCHAR))` |
| binary(n)/varbinary/image | `LOWER(CONVERT(varchar(max), CAST(x AS varbinary(max)), 2))` | `lower(hex(x))`; binary(n) right-padded with `00` to n bytes |
| float/real/xml/sql_variant/spatial/hierarchyid | NULL vs non-NULL only (`'v'`) | same |

- NULL becomes `U+0001 + 'NULL'`.
- Hash: MD5 of the UTF-8 bytes. The first 8 bytes are read as an **unsigned** 64-bit integer.
  - SQL Server: `CAST(SUBSTRING(h,1,8) AS bigint)` as decimal(38,0), plus 2^64 when the first byte is >= 0x80.
  - DuckDB: `CAST('0x' || substr(md5(t),1,16) AS UBIGINT)` as HUGEINT.
- Both sides sum as DECIMAL(38,0) / HUGEINT. The maximum is about 1e28 for 1.2 bn rows.
- How each side gets UTF-8 bytes:
  - SQL Server string columns: `CONVERT(varchar(max), COALESCE(CAST(canon AS nvarchar(max)), NCHAR(1)+N'NULL') COLLATE Latin1_General_100_BIN2_UTF8)`. This was checked against `md5sum` for `äöü€😀`.
  - SQL Server non-string columns: the canonical text is ASCII, so it is hashed as plain varchar.
  - DuckDB: `md5()` hashes UTF-8 already.
- Floats are left out of the value checksums. The text rendering differs between the engines (shortest round-trip vs `CONVERT` style 3), and so does the summation order. Only their NULL pattern is checked.

### Parity proof
- Fixture: 18 columns × 5 rows (int, decimal(10,2), varchar with `Müller café`, nvarchar with `日本語 😀`, date incl. 0001-01-01 / 9999-12-31, datetime2(7), bit, uniqueidentifier, char(5), money at its limits, float, datetime, datetime2(3) rounding, datetimeoffset(3) with +14:00/-05:00, time(3), varbinary, binary(4), smalldatetime, one all-NULL row).
- It was created as `_agent_g.dbo.g_t` on the 2019 container (setup script below). `mssql_checksum_sql` and the canonical-text query (printed by the ignored test `verify::tests::print_fixture_mssql_sql`) ran there. The results are hard-coded in `verify::tests::{checksum_parity_with_sql_server, canonical_forms_match_sql_server_rendering}`, and the DuckDB-only tests reproduce them exactly.
- `_agent_g` was dropped afterwards.

### `mssql::swap` (R10)
- `pub async fn swap(c, pairs: &[(new, live)], lock_timeout_ms, keep_old) -> Result<Vec<String>>` (and `swap_at(.., now)`). Names may be `t`, `schema.t`, `[s].[t]` or `"s"."t"`; the default schema is `dbo`.
  1. Pre-checks, all before any change, all `Failure::Check` (exit 1):
     - each table appears in only one pair;
     - new and live exist (`OBJECT_ID(.., 'U')`);
     - `<live>__old_<yyyymmddhhmm>` does not exist yet;
     - a cross-schema move does not collide with an existing table;
     - no **schema-bound modules** reference the new or live tables (`sys.sql_expression_dependencies` + `sys.sql_modules.is_schema_bound`; views and functions are listed with what they reference);
     - no **foreign keys from other tables** reference them, since those would silently stay on the renamed table. This check is stricter than the spec and is intentional.
     - Non-schema-bound views on the live table only produce a `runlog::note` that suggests `sp_refreshview`.
  2. One batch: `SET LOCK_TIMEOUT n; SET XACT_ABORT ON; BEGIN TRY BEGIN TRANSACTION; EXEC sp_rename live -> live__old_ts; [ALTER SCHEMA live_schema TRANSFER new;] EXEC sp_rename new -> live; ... COMMIT; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; THROW; END CATCH`. It is sent through `exec_batch_as(c, "target", "swap", ..)`, so `--plan` lists it.
  3. If the batch fails (for example a lock timeout, error 1222), the whole batch is rolled back. The error is a `Failure::Run` (exit 2) that names the sessions holding or waiting for OBJECT locks on the tables (`sys.dm_tran_locks` + `sys.dm_exec_sessions`: session, login@host, program, mode/status, open transactions). Nothing is ever killed.
  4. Without `keep_old`, `DROP TABLE` runs for each `__old_` table after the commit (it asserts the name contains `__old_`).
  5. It returns the statements: the batch, then the drops.
- The timestamp is **UTC**, `%Y%m%d%H%M`. `old_name(live, now)` shortens `live` so the name fits in 128 characters. `parse_old_suffix(name)` returns the timestamp of a `…__old_<12 digits>` name.
- `pub async fn drop_old(c, schema: Option<&str>, before: NaiveDateTime) -> Result<Vec<String>>`. The plan had `(c, before: &str)`; the integrator parses the date. It drops `%__old_<yyyymmddhhmm>` tables whose suffix is earlier than `before` and returns the `schema.table` names.
- `SET LOCK_TIMEOUT` stays set on the connection after `swap`.

## Integrator wiring (stream H)
- `export ... --verify counts|checksum|except --sample n`:
  - `let level = verify::VerifyLevel::parse(&a.verify)?.with_sample(a.sample.unwrap_or(10));`
  - After each table has loaded, call `rt.block_on(verify::verify_table(&con, &mut client, duck_table, &target_table, &mapping, level))`.
  - After the heap-then-index or switch strategies, also call `rt.block_on(verify::verify_indexes(&mut client, &target_before))`, using the `TargetTable` captured before the load.
  - Print each result (Display), add `r.to_string()` to `SummaryEntry.checks` and `ok &= r.ok`, then call `verify::check_results(&all)?` at the end (exit 1).
  - R9 says counts are always on, so run `VerifyLevel::Counts` even without `--verify`.
- For `--source-parquet`, use `verify_table_from(&con, &mut c, "read_parquet('<glob>', hive_partitioning=1)", ...)`.
- `swap --host … --database … --pairs new1=live1,new2=live2 [--keep-old] [--lock-timeout ms]`:
  - Split the pairs on `,` and `=` into `(new, live)`, then call `rt.block_on(mssql::swap::swap(&mut c, &pairs, lock_timeout.unwrap_or(5000), keep_old))`.
  - `--keep-old` defaults to on. Add a `--no-keep-old` / `--drop-old-now` flag for false.
  - Print the returned statements.
- `drop-old --host … --database … --before <date> [--schema s]`:
  - Accept `YYYY-MM-DD` (meaning 00:00) or `YYYY-MM-DD HH:MM`, in UTC, then call `rt.block_on(mssql::swap::drop_old(&mut c, schema.as_deref(), before))`.
  - Print the dropped names.
- Extension: none needed.

## e2e cases for scripts/e2e_large_load.sh
1. Load the partitioned 10 M-row table and run `--verify checksum`. Expect exit 0, one OK per partition, and an OK for the total.
2. Change one row on the target (`UPDATE TOP (1) t SET col = 'x' WHERE Mkt = 'ABC'`), then run `--verify except --sample 3`. Expect exit 1. Only that partition should fail, naming `col`, with one row shown each way.
3. Delete one row on the target and run `--verify counts`. Expect exit 1 with `counts: duckdb=N target=N-1` for that partition.
4. After `heap-then-index`, run `verify_indexes`. Then `ALTER INDEX ix ON t DISABLE` and `ALTER TABLE t NOCHECK CONSTRAINT ck`, and rerun. Expect exit 1, with "is disabled" and "not trusted" reported.
5. `swap`:
   - Create `dbo.t_new` and `dbo.t` and run `swap --pairs t_new=t`. Expect `t__old_<ts>` and the new `t`.
   - With `CREATE VIEW v WITH SCHEMABINDING AS SELECT a FROM dbo.t`, expect exit 1, the view named, and no rename done.
   - Lock test: one session runs `BEGIN TRAN; SELECT * FROM t WITH (HOLDLOCK, TABLOCK)` and keeps it open. Run `swap --lock-timeout 2000`. Expect exit 2 within about 2 s, the holder session listed, both tables unchanged, and the holder still connected.
   - With two pairs where the second one's lock is held, the first rename must also be rolled back.
6. Run `drop-old --before <tomorrow>`: the `__old_` tables are dropped and other tables stay.

## SQL Server fixture used for the parity test
The `INSERT` with the 5 rows is in the test module, as the DuckDB `INSERT` (same literals). On SQL Server, each temporal value was `CAST(... AS datetime2(7))` and then stored, so SQL Server did its own rounding. To regenerate:

```sql
CREATE TABLE dbo.g_t (id int, d decimal(10,2), vc varchar(50), nv nvarchar(50), dt date, ts datetime2(7), b bit, g uniqueidentifier, ch char(5),
  m money, f float, dtm datetime, ts3 datetime2(3), dto datetimeoffset(3), tm time(3), bn varbinary(8), bf binary(4), sdt smalldatetime);
-- then: cargo test --release --lib verify::tests::print_fixture_mssql_sql -- --ignored --nocapture, run the two printed queries
```

## Open issues
- **Converter rounding**: the canonical forms assume the loader stores what SQL Server's own conversion would store. That means half-up rounding to datetime2(n<6), nearest 1/300 s tick for datetime, and half up at 30 s for smalldatetime. If stream C truncates instead, `--verify checksum` reports those columns. That is a real difference from the source, but it is worth checking the converter against these rules.
- smalldatetime: the documented "29.999 s rounds up" rule applies to string and datetime sources. Values that come from datetime2 round half up at 30 s, and that is what is implemented.
- If the DuckDB type is TIMESTAMPTZ and the target is datetime2 or date, the value is rendered as its UTC instant. That is correct only if the converter also writes UTC.
- Checksums of string columns need SQL Server 2019+ (UTF-8 collation). On 2016/2017 only counts work. A fallback would hash UTF-16LE, which needs a UTF-16 encoder in DuckDB; not done.
- varchar targets with a non-UTF-8 code page: characters that are not in the code page are already stopped by stream B's preflight (CODEPAGE). If one gets through anyway (as `?`), the checksum catches it.
- Float columns: only the NULL pattern is compared, and they are left out of `except` rows.
- Per-partition splitting of the DuckDB side needs at most 5000 distinct partition-column values. A table partitioned by a high-cardinality date or int falls back to totals plus a whole-table checksum. A range-based DuckDB predicate built from the boundaries would be possible, but collation semantics make it unsafe for strings.
- `except` keeps up to about 1 M canonical rows per side in memory (hundreds of MB for wide rows).
- `swap` refuses when foreign keys from other tables reference the tables. Constraint names (PK_live) move with the old table, so the new live table keeps its own constraint names. Rename them afterwards if names matter.
- `drop_old` matches any table that ends in `__old_<12 digits>`. A stream D `switch_in` old-partition table whose partition value happens to be a valid 12-digit timestamp would match too. That is unlikely, but the two naming schemes overlap.
- `cargo test --release verify swap` is not valid cargo syntax (only one filter is allowed), so run `cargo test --release --lib verify` (10 pass, 2 ignored) and `cargo test --release --lib swap` (3 pass). Both pass in the working tree, `cargo check --release --tests` gives zero warnings, and the guard source scan passes.
- Checked by hand on the 2019 container with sqlcmd:
  - the grouped per-partition checksum on a `RANGE RIGHT (3)` partitioned copy (the partition sums add up to the whole-table sums), the `$PARTITION` VALUES mapping (NULL -> partition 1), and the row-text and bucket-sampling query all run;
  - the swap batch with a second session holding `TABLOCK, HOLDLOCK` fails after 2 s with error 1222. The first pair's rename is rolled back, and the blocking-session query lists `52 sa@... (SQLCMD): S GRANT on dbo.b_live, open transactions 1`. With the lock gone, the same batch commits;
  - the schema-bound query finds `dbo.v_b (VIEW, references dbo.b_live)`.
  The Rust async paths (`verify_table`, `verify_indexes`, `swap`, `drop_old` over TDS) have not run end to end yet; that is left for the e2e script.
