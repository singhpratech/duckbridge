# DuckBridge: requirements for safe, large, production-grade loads

Status: **draft for review**. Branch `req/large-load-safety`. No code changes yet; this file is the spec.

## 1. Why

DuckBridge already moves data between SQL Server and DuckDB. To use it in a real weekly batch, it must
handle a workload like this one:

| | |
|---|---|
| Source | A busy SQL Server 2025 Standard database that other jobs write to during the week. Reading it must never block those jobs or be blocked by them, and it must not read half-written data. |
| Processing | DuckDB rebuilds about 20 result tables from about 16 input tables (2.5 M rows in). |
| Biggest output | **1.2 bn rows**, 17 columns. Clustered PK of 10 columns, mostly `varchar`, so there is no single integer key. `PAGE` compression. About 25 values of a `char(3)` market column. |
| Other outputs | 19 tables from a few rows to about 30 M rows. Each has a PK and one or two nonclustered indexes. |
| Target | Tables with **exact production DDL**: types, `varchar` vs `nvarchar`, collation, NOT NULL, compression, keys. Consumers (views, reports, other jobs) depend on that DDL. |
| Time budget | Load and index everything in **under 60 minutes**. The current SQL-only process takes about 12 hours; DuckDB processing takes about 30–60 minutes. |
| Recovery model | SIMPLE, so bulk loads with `TABLOCK` are minimally logged. |

Today DuckBridge would (a) disconnect other users of a database it is pointed at, (b) create its own tables
instead of using the production DDL, (c) load the 1.2 bn-row table on one connection, and (d) read the
source without lock protection. Sections 3–12 below define what has to change.

## 2. Principles

1. **Never harm a shared server.** No other session is ever disconnected, killed or blocked for longer than a
   bounded timeout. Database-level changes happen only on databases the operator marked as disposable.
2. **The target DDL is the contract.** DuckBridge fits the data to the existing tables and never the reverse.
   A mismatch is an error before any data moves, not a silent cast.
3. **Exact or fail.** No silent truncation, rounding, overflow or encoding change. Every conversion that could
   lose information is checked.
4. **Restartable.** A killed run can be resumed. Partial output is detected and cleaned, never trusted.
5. **Observable.** Every statement sent to the server can be shown before the run (`--plan`) and is logged
   during it, with per-job timings.

---

## 3. R1: Safety guard for database-level operations (blocker)

### Current behaviour
| Location | Statement |
|---|---|
| `src/mssql/load.rs:55` | `ALTER DATABASE … SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE` (`--drop-existing`) |
| `src/mssql/load.rs:58` | `ALTER DATABASE … SET COMPATIBILITY_LEVEL = 150` (**every** live load, even into an existing DB) |
| `src/mssql/load.rs:60` | `ALTER DATABASE … SET RECOVERY SIMPLE` |
| `src/main.rs:494`, `src/main.rs:525` | `SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE` (bak staging / verify) |
| `src/api.rs:313` | same |
| `src/mssql/backup.rs:102`, `:117` | same (restore verification DB) |
| `src/core/emit.rs:301` | compatibility level 150 in generated scripts with `--create-database` |

`ROLLBACK IMMEDIATE` rolls back and disconnects **every other session** in the database. The compatibility level change silently
downgrades a 2022/2025 database (160/170) and changes query plans for everyone using it.

### Required
1. **Disposable-database policy.** A new option `--disposable-db-pattern <regex>` (default `^_`). It is also read
   from `DUCKBRIDGE_DISPOSABLE_DB_PATTERN`.
   - `CREATE DATABASE`, `DROP DATABASE`, `ALTER DATABASE … SET RECOVERY`, and `ALTER DATABASE … SET COMPATIBILITY_LEVEL` are
     allowed **only** for a database whose name matches the pattern.
   - Anything else stops with an error **before connecting for writes**, and the error names the database and the rule.
   - The check lives in one function (`mssql::guard::assert_disposable(db)`) that every code path calls; a unit test
     greps the source for `DROP DATABASE` / `ALTER DATABASE` outside that module and fails if one is found.
2. **No `ROLLBACK IMMEDIATE`, ever.** Dropping a database:
   - checks `sys.dm_exec_sessions WHERE database_id = DB_ID(@db) AND session_id <> @@SPID`;
   - if any session is found, stops with an error listing the sessions (`session_id`, `login_name`, `host_name`, `program_name`);
   - otherwise runs `DROP DATABASE`.

   `--force-drop` is **not** provided.
3. **Do not change the compatibility level** of an existing database. When DuckBridge creates a disposable database, it
   leaves it at the server default unless `--compat-level <n>` is given explicitly.
4. **Recovery model.** Only change it on disposable databases, and only when `--recovery simple` is given (not by default).
   On a non-disposable target, read and report the model (`sys.databases.recovery_model_desc`). If it is FULL, warn
   that bulk loads will be fully logged.
5. **No implicit `CREATE DATABASE`** on live loads. If the target database does not exist, stop with an error unless `--create-database`
   is given *and* the name passes the disposable check.

### Acceptance
- A live load to a database called `Sales` with `--drop-existing` fails before connecting, with the message:
  `refusing DROP DATABASE on 'Sales': not a disposable database (pattern ^_)`.
- A live load to `_scratch` while another session is connected to `_scratch` fails and lists that session; the other
  session is still connected afterwards.
- A load into an existing database leaves `compatibility_level` and `recovery_model` unchanged (asserted in the e2e test).

---

## 4. R2: Load into existing tables (`--into-existing`) (blocker)

### Current behaviour
`prepare_schema` (`src/mssql/load.rs:50`) always issues `CREATE TABLE` from types mapped out of DuckDB
(`src/core/typemap.rs`). Strings become `NVARCHAR(n)` sized from the data. Then `finish_schema` adds PKs and indexes
of its own design.

### Required
1. New mode `export --to server --into-existing`:
   - Issues **no DDL** against the target tables (no CREATE, ALTER or DROP of the target tables).
   - Reads the target's metadata from `sys.columns`, `sys.types`, `sys.indexes`, `sys.partitions`,
     `sys.partition_schemes` and `sys.check_constraints` for every table in the job.
2. **Column mapping by name**, case-insensitive. Rules:
   - An extra DuckDB column is an error, unless `--ignore-extra-columns` is given.
   - A missing target column is an error unless it is nullable or has a default. In that case it is reported and left out
     of the bulk column list.
   - Column order on the wire follows the **target** (`column_id`).
3. **Type compatibility check before any data moves.** Each DuckDB column is checked against its target column:

   | Target type | Check |
   |---|---|
   | `char(n)` / `varchar(n)` | Run `max(length(col))` in DuckDB; it must be ≤ n. All values must be representable in the column's code page (run `max(octet_length) = max(length)` for non-UTF-8 collations, or check against the code page). A violation is an error that names the column, the longest value's length, and 5 sample keys. |
   | `nchar(n)` / `nvarchar(n)` | Check length in UTF-16 code units. |
   | `decimal(p,s)` | The source scale must be ≤ s. If it is >, stop unless `--allow-rescale` is given. Check `max(abs(col)) < 10^(p-s)` in DuckDB first. |
   | `int`/`smallint`/`tinyint`/`bigint` | Check min and max against the target range. `tinyint` is 0..255 (`to_column_data` currently does `as u8`, which wraps). |
   | `bit` | The source is BOOLEAN or an integer in {0,1}. |
   | `date`/`datetime2(n)`/`datetime` | Range and precision. `datetime` rounds to 1/300 s and must be warned. |
   | NOT NULL | Run `count(*) FILTER (WHERE col IS NULL) = 0` in DuckDB. |

   All checks run as **one DuckDB query per table** (aggregates for all columns). They are reported together, and
   they can be run alone with `--check-only`.
4. **Primary key duplicates.** Optionally (`--check-pk`), run `SELECT pk, count(*) … HAVING count(*) > 1 LIMIT 5` in DuckDB
   before loading. With `--check-pk=hash`, the check is done per partition/job so memory stays bounded.
5. **Collation awareness.** Read the target column collation. If the PK or a unique index contains a case-insensitive (`_CI_`)
   or trailing-space-insensitive collation, then:
   - check in DuckDB that `lower(rtrim(key))` is still unique (SQL Server compares `'a'` = `'A '`);
   - report case-only and trailing-space-only collisions as errors, because SQL Server would reject them in the PK.

### Acceptance
- Loading into a table with `varchar(3)` where DuckDB has a 4-character value fails **before** any bulk session opens. The
  error names the column, `max length 4 > 3`, and sample keys.
- Loading into an existing table leaves `sys.objects.modify_date` of the target unchanged (no DDL was executed).
- A table whose DDL has `PAGE` compression, a CI collation and a 10-column clustered PK loads with that DDL unchanged.

---

## 5. R3: String fidelity (`varchar` stays `varchar`)

### Current behaviour
In create mode, `src/core/typemap.rs` maps every DuckDB `VARCHAR` to `NVARCHAR(bucket)` (`StringMode::Fit`) or
`NVARCHAR(MAX)`. That doubles storage, changes comparison semantics, and breaks consumers that expect `varchar`.

### Required
1. Create mode gets `--string-type varchar|nvarchar|auto` (default `auto`). With `auto`:
   - `varchar` when every value in the column is ASCII (`bool_and(regexp_matches(col, '^[\x00-\x7F]*$'))`, or a byte/char length comparison);
   - `nvarchar` otherwise, and the decision is listed in `inspect` output.
2. `--string-sizes from-data|max|<file>`. `<file>` is a CSV of `table,column,type` that pins exact types (for
   reproducing a known DDL without `--into-existing`).
3. In `--into-existing` mode the target type is used as-is; R2's checks apply.
4. The bulk path sends `varchar` data as `varchar` with the target collation's code page. It does not rely on
   server-side `nvarchar → varchar` conversion, which can turn unknown characters into `?` silently.
   (The tiberius `ColumnData::String` path needs a test with a non-ASCII value against a `varchar` Latin1 column;
   the expected result is an error, not `?`.)

---

## 6. R4: Partition-aware parallel loading of very large tables (blocker for the 1.2 bn-row table)

### Current behaviour
`split_key` (`src/mssql/load.rs:187`) splits a table across workers only when its PK is a **single integer column** or
it has an identity column. The 1.2 bn-row table has a 10-column `varchar` PK, so it would stream on **one** connection.
Bulk loads use `TABLOCK`. Into a table with a clustered index, `TABLOCK` takes an exclusive table lock, so even split
jobs would serialize.

### Required
1. **Job sources.** One table can be split by any of these (`--split <table>=<spec>`):
   - `column:<col>`: one job per distinct value of a low-cardinality column (for example the market column). The values
     come from `SELECT DISTINCT col` in DuckDB, ordered by row count (largest first).
   - `files:<glob>`: one job per Parquet file or Hive folder, e.g. `out/big_table/Mkt=*/`. This avoids building one huge DuckDB
     table first.
   - `hash:<cols>:<n>`: `hash(cols) % n`, for tables with no natural split column.
   - `range:<int col>:<n>`: the existing integer-range behaviour.
2. **Target strategy per table** (`--strategy <table>=<s>`):

   | Strategy | When | How |
   |---|---|---|
   | `switch` (**required**) | Target is partitioned on the split column | For each job value: create a staging table `[schema].[<table>__stg_<value>]` on the **same filegroup** as the partition, with identical columns, the same clustered index (PK), the same compression and a `CHECK (col = value)` that matches the partition boundary. Bulk load with `TABLOCK`. Rebuild statistics if wanted. Run `ALTER TABLE stg SWITCH TO target PARTITION $PARTITION.<pf>(value)`. Drop the staging table. The partition must be empty first, otherwise it is an error unless `--replace-partitions` is given (which switches the old partition out to a `__old_<value>` table, kept until `--drop-old`). |
   | `heap-then-index` | Target is a heap, or the operator lets DuckBridge drop and recreate nonclustered indexes | Parallel bulk loads into a heap with `TABLOCK` (BU locks are compatible, so several sessions load at once with minimal logging). Then build the clustered and nonclustered indexes from the definitions captured before the load (`sys.indexes`, `sys.index_columns`, including `DATA_COMPRESSION`, `FILLFACTOR`, `INCLUDE` and filters). |
   | `serial` | Default for small tables | One bulk session per table. Many small tables still run in parallel with each other. |

   On Standard Edition, `CREATE INDEX` is single-threaded, so `switch` (clustered index built per partition in
   parallel sessions) is the only way to get parallel index builds. The documentation must say so.
3. **Generated staging DDL** comes from the target's own metadata. No hand-written type mapping is involved.
   - Before the SWITCH, run `sys.dm_db_partition_stats` and compare the staging row count with the job's DuckDB count.
   - SWITCH errors (e.g. 4972, check-constraint mismatch) are reported with the exact staging and target DDL difference.
4. **Concurrency control.**
   - `-j N` sets the number of concurrent bulk sessions (default `min(8, cores)`).
   - `--max-jobs-per-table` keeps one table from taking every slot.
   - The jobs of all tables share one queue, largest first.
5. **Batching.** `--batch-rows` controls the bulk batch size (`BATCHSIZE`). With `switch`, the whole staging load can be one batch;
   because the staging table is invisible to readers until the SWITCH, a partially loaded staging table is harmless and is dropped on retry.
6. **Restart.** Each job writes a done marker to `--state-dir <dir>` (`<table>/<job>.done`, containing row count, duration and
   the staging/target checksums if computed). On rerun:
   - completed jobs are skipped;
   - an unfinished job's staging table is dropped (it must match the `__stg_` naming and the disposable rule *for the staging table name*, never the target) and reloaded.

### Acceptance
- A 25-partition, 1.2 bn-row table loads with `-j 8`. All 25 partitions switch in, and total target rows equal the DuckDB count.
- Killing the process mid-run and rerunning with the same `--state-dir` finishes without duplicates and without
  reloading finished partitions.
- While the load runs, a `SELECT COUNT(*) FROM target WITH (NOLOCK)` from another session never sees a partially loaded
  partition (staging is invisible until the SWITCH).

---

## 7. R5: Throughput

### Current behaviour
- `to_column_data` (`src/mssql/load.rs:247`) runs per **value**:
  - `col.sql_type.to_uppercase()` plus a `split('(')`, each a string allocation;
  - for DECIMAL, `d.to_string().parse()`, i.e. formatting and parsing text per value;
  - for DECIMAL, the scale is parsed from the type string per value.
- Rows are fetched one by one through `rows.next()` with `get_ref_unwrap`.
- Import (`src/mssql/import.rs:397`) converts `Numeric` to `Value::Text` and appends text into a DECIMAL column, so DuckDB
  parses it again.

At 1.2 bn rows × 17 columns (about 20 bn values), the per-value overhead dominates.

### Required
1. **Precompiled column converters.**
   - For each column, build once a `Converter` enum (`Int32`, `Decimal { scale }`, `Varchar { max_len }`, …) from the
     target metadata (R2).
   - The hot loop does a `match` on the enum and does no string work for non-string types.
2. **DECIMAL without text.** DuckDB `ValueRef::Decimal` / `DECIMAL` vectors are i16/i32/i64/i128 mantissas with a known
   scale. Pass the mantissa straight to `tiberius::numeric::Numeric::new_with_scale(mantissa, scale)`. Rescale only when the
   target scale is larger, multiplying by `10^k` exactly. If the target scale is smaller, stop with an error (R2) instead of rounding.
3. **Columnar fetch.** Read DuckDB results as Arrow record batches or DataChunks (`query_arrow`) and convert column by
   column into `TokenRow`s. Do not use per-cell `get_ref` calls.
4. **Import side.** Map `Numeric` to `Value::Decimal` (or append through Arrow) instead of text. Use the DuckDB appender
   with Arrow batches.
5. **Pipelining.** Decouple the DuckDB read and the TDS send with a bounded channel (e.g. 4 batches), so CPU conversion and
   network I/O overlap.
6. **Benchmarks** (`benches/`): rows/s per session for a 17-column row (6 `varchar`, 2 `char(3)`, 5 `int`, 1 `decimal(8,5)`,
   …) and per table. These go in the README with hardware notes.

### Targets
- At least **300k rows/s per bulk session** for the row shape above on an 8-core client.
- **2 M+ rows/s aggregate** with 8 sessions, or whatever SQL Server can absorb.
- 1.2 bn rows in **≤ 15 minutes** of load time, excluding index builds, which the `switch` strategy makes part of each job.

---

## 8. R6: Safe reads from a busy source

### Current behaviour
`copy_one` (`src/mssql/import.rs:358`) runs a plain `SELECT` under default READ COMMITTED with no lock timeout, so it can
wait indefinitely behind writers and can block writers that need schema or exclusive locks. `--nolock` gives dirty reads
with no detection of concurrent change.

### Required
1. **Session settings for every import connection:**
   ```sql
   SET LOCK_TIMEOUT <ms>;            -- --lock-timeout, default 1000
   SET DEADLOCK_PRIORITY LOW;        -- we lose deadlocks, production jobs win
   SET TRANSACTION ISOLATION LEVEL <READ COMMITTED | READ UNCOMMITTED | SNAPSHOT>;  -- --isolation, default READ COMMITTED
   ```
   If the database has `READ_COMMITTED_SNAPSHOT` or `ALLOW_SNAPSHOT_ISOLATION` on, recommend `--isolation snapshot` in the
   pre-flight output, since it gives consistent reads without locks.
2. **Pre-flight lock check per table** (`--lock-check`, default on). Before reading a table, query
   `sys.dm_tran_locks WHERE resource_database_id = DB_ID() AND resource_associated_entity_id IN (object_id, its hobt_ids)
   AND request_session_id <> @@SPID`. If X/IX/SCH-M locks are held:
   - wait and retry up to `--lock-wait <s>`;
   - then skip the table (and report it) or fail (`--on-lock fail|skip`, default `fail`).
3. **Change fingerprint before and after** every table read:
   - rows from `sys.dm_db_partition_stats` (sum of `row_count` for index 0/1);
   - `max(last_user_update)` from `sys.dm_db_index_usage_stats` for the object;
   - `sys.objects.modify_date`.

   If any value differs after the read, re-read the table (up to `--max-rereads`, default 2); otherwise fail. The fingerprints
   go in the manifest.
4. **Manifest** (`<out>.manifest.csv`, plus a JSON copy): `table, rows_read, rows_source_stats, fingerprint_before, fingerprint_after,
   started, finished, seconds, isolation, rereads`. Downstream jobs can then confirm what was read.
5. **Read-only connection.** Use `ApplicationIntent=ReadOnly` when `--readonly` is given, which also routes to a readable
   secondary if one exists. Never issue DDL or DML on the source connection; assert this in code by having the import
   connection type expose only `query`.
6. **Gate wait (optional).** `--wait-for "<T-SQL returning one row, one bit>"` polls until the query returns 1 (e.g. an
   upstream job's completion flag), with `--wait-timeout`. This lets the import start right after its inputs are final.

### Acceptance
- With another session holding `BEGIN TRAN; UPDATE t …` open, `import` of `t` waits at most `--lock-wait`, then fails with a
  message naming the blocking session. That session is never blocked by DuckBridge (checked via
  `sys.dm_exec_requests.blocking_session_id`).
- An `INSERT` into the source table during the read is detected by the fingerprint, and the table is re-read.

---

## 9. R7: Translator, SQL Server semantics and an honest `--strict` mode

### Current behaviour
- `transpile::duckdb_prelude` (`src/transpile/mod.rs:69`) sets `integer_division = true` (good: T-SQL `int/int` truncates).
- It also sets `default_collation = 'nocase'` by default. In practice, on 1 bn-row workloads, a global `nocase` collation:
  - made hash `DISTINCT`/`GROUP BY` run single-threaded;
  - ran out of memory where the binary collation finished in minutes (measured on DuckDB 1.5.6, Windows, 2026-10).
- Other semantic gaps that cause **silent** result differences:

| T-SQL behaviour | Current translation | Risk |
|---|---|---|
| `int * 1.0 / int` gives `numeric(p,s)` with SQL Server's precision/scale rules (e.g. `(24,12)`), **truncated** | DuckDB `DECIMAL / DECIMAL` gives `DOUBLE` | Last-digit differences after rounding |
| `CAST(x AS numeric(p,s))` rounds half away from zero | DuckDB rounds half away from zero for DECIMAL, but values that went through DOUBLE first differ | Rounding differences |
| `ROUND(x, n, 1)` (truncate) | `trunc(x * power(10,n)) / power(10,n)` (`src/transpile/tsql_to_duck.rs:340`), i.e. DOUBLE arithmetic | Wrong last digit |
| `FORMAT(x, '0.0%')` / numeric formats | Not translated (`tsql_to_duck.rs:410`, warning only) | Different text |
| `TRY_CAST('' AS int)` gives `0` | DuckDB gives `NULL` | Different rows / joins |
| CI comparisons in `=`, `IN`, `GROUP BY`, `DISTINCT`, joins | Global `nocase` (see above) | Out of memory / slow, or case variants merged differently |
| Trailing-space-insensitive `=` (`'a' = 'a '` is true) | DuckDB: false | Missing join matches |
| Procedural T-SQL (`WHILE`, `IF … BEGIN … END`, dynamic SQL) | Left as a comment (`tsql_to_duck.rs:82`) | Statements silently skipped at run time |
| DuckDB `**` / `power()` | DOUBLE | Last-digit differences in decimal formulas |

### Required
1. **Prelude.** Do not set `default_collation = 'nocase'` by default; make it `--nocase=global` (opt-in), and document the
   performance cliff. The default `--nocase=rewrite`:
   - turns `LIKE` into `ILIKE`;
   - wraps comparisons of string literals with columns as `lower(col) = lower('lit')` only when needed;
   - adds a **data audit** (item 3 below).
2. **`--strict`.** Every construct in the table above (and any other approximation) is a warning with line and column
   (`W-DIV-DECIMAL`, `W-ROUND-TRUNC`, `W-FORMAT-NUM`, `W-TRYCAST-EMPTY`, `W-CI-COMPARE`, `W-TRAILING-SPACE`, `W-PROCEDURAL`,
   `W-POWER-DOUBLE`). Under `--strict`, any warning makes `transpile`/`run` exit non-zero **before executing anything**.
3. **Case and trailing-space audit helper.** `duckbridge audit-ci <db> --tables … --columns …` reports, per column, values
   that differ only by case or by trailing spaces. If a column has none, binary comparisons give the same result as
   SQL Server's CI_AS for that data, and the global `nocase` is unnecessary. The helper also lists every string literal in the
   script that matches a column value only case-insensitively. In practice this found 6 literals in a 1,100-line script,
   all fixable by rewriting the literal.
4. **Exact SQL Server arithmetic macros** (opt-in `--mssql-math`). Ship DuckDB macros that reproduce SQL Server results exactly,
   with integer/HUGEINT arithmetic and no DOUBLE:
   - `mssql_div(a, b, p, s)` truncates to the result scale of `numeric` division;
   - `mssql_round(x, n)` rounds half away from zero on DECIMAL;
   - `mssql_round_trunc(x, n)`;
   - `mssql_try_int(text)`, where `''` gives 0;
   - `mssql_format_pct(x, decimals)`.

   The translator uses them when the input types are known: from `CREATE TABLE` in the same script, the catalog of an
   attached DB, or `--types <csv>`.
5. **Verification kit.** `duckbridge verify-math --server … --cases <n>` generates random cases for each macro, evaluates
   them on SQL Server and in DuckDB, and reports mismatches. A test of this kind found 0 mismatches in 21,012 values after the
   macros were made exact (the `power()` DOUBLE issue was the last fix).

---

## 10. R8: Plan, observability, exit codes

1. `--plan` prints, for the whole job, every statement that would be sent to each server (source and target), the job list
   with estimated rows, and the guard decisions from R1. It connects for **metadata reads only**.
2. **Log file** (`--log <file>`):
   - one line per statement, with start time, end time, rows, session id and job id;
   - shared-read, append mode, so a tail does not lock it;
   - log writes never throw, and a failed log write never stops a load.
3. A progress line every `--progress-seconds` shows rows/s per job and in total, plus an ETA.
4. **Exit codes:** `0` all OK, `1` a check failed (types, PK, fingerprint, row counts), `2` could not run (connection,
   permissions), `3` stopped by guard (R1).
5. **Summary JSON** (`--summary <file>`): per table and per job, the rows, seconds, strategy and checks. Schedulers can then read
   the outcome.

---

## 11. R9: Verification after the load

1. **Row counts.** Per table and per partition, compare the target rows (`sys.dm_db_partition_stats`) with the DuckDB counts.
   Always on.
2. **Checksums** (`--verify checksum`). For each table or partition, compute in both engines:
   - `count(*)`;
   - per column, `sum` of a stable hash of the canonical text form.

   Canonical text form rules: `rtrim` for `char`; decimals as plain text at the target scale; dates ISO. They must give
   the same text in both engines. Differences are reported per column.
3. **Row diff** (`--verify except --sample <n>`). For a table/partition with a checksum mismatch, pull up to n rows of
   `EXCEPT ALL` in each direction and show them.
4. **Index completeness.** After `heap-then-index`/`switch`, every index captured before the load exists with the same
   definition (`sys.indexes` / `sys.index_columns` compared). The PK is enabled and trusted, and check constraints are trusted
   (`is_not_trusted = 0`).

---

## 12. R10: Cut-over helper (optional, for replacing live tables)

`duckbridge swap --pairs new1=live1,new2=live2 [--keep-old]`:
- in one transaction with `SET LOCK_TIMEOUT`, `sp_rename` live to `<live>__old_<yyyymmddhhmm>` and new to live, for all
  pairs (all or nothing);
- if a lock cannot be obtained within the timeout, roll back and report the blocking sessions; never kill them;
- `--keep-old` (default on) leaves the old tables, which a separate, explicit `duckbridge drop-old --before <date>` removes;
- dependent views with `SCHEMABINDING` are detected and reported **before** the swap.

---

## 13. CLI sketch

```text
# 1. Read inputs safely
duckbridge import work.duckdb --host SRC --database SRCDB --include t1,t2,... \
    --isolation snapshot --lock-timeout 1000 --lock-check --on-lock fail \
    --wait-for "SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM ctl.flags WHERE step='inputs' AND done=1) THEN 1 ELSE 0 END AS bit)" \
    --manifest in.manifest.csv -j 8

# 2. Process (translated or hand-written SQL)
duckbridge run work.duckdb steps/*.sql --strict --nocase=rewrite --mssql-math

# 3. Pre-flight against existing target DDL (no data moved)
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --check-only --check-pk=hash

# 4. Load
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --split big_table=column:Mkt --strategy big_table=switch \
    --strategy '*'=serial -j 8 --state-dir state/ --verify checksum --log load.log --summary load.json

# 5. Cut over (optional)
duckbridge swap --host TGT --database PRODDB --pairs ...
```

Parquet sources (`--source-parquet 'out/big_table/Mkt=*/'`) should work in place of a DuckDB file for `export`, so a
pipeline that already writes partitioned Parquet does not need to build a 1.2 bn-row DuckDB table first.

---

## 14. Implementation plan

| Step | Files | Size |
|---|---|---|
| 1. Guard module + remove `ROLLBACK IMMEDIATE` + no compat/recovery changes on existing DBs (R1) | new `src/mssql/guard.rs`; `load.rs`, `backup.rs`, `main.rs`, `api.rs`, `emit.rs` | S |
| 2. Target metadata reader + type/length/NULL/PK pre-flight (R2) | new `src/mssql/target.rs`; `load.rs` | M |
| 3. Precompiled converters + exact DECIMAL + Arrow fetch (R5) | `load.rs`, new `src/mssql/convert.rs` | M |
| 4. Job sources (`column`, `files`, `hash`) + shared queue + state dir (R4.1, R4.4, R4.6) | `load.rs`, `split.rs` | M |
| 5. `switch` strategy: staging DDL from metadata, SWITCH, checks (R4.2, R4.3) | new `src/mssql/switch.rs` | M |
| 6. `heap-then-index` with captured index DDL (R4.2) | `load.rs`, `target.rs` | S |
| 7. Safe reads: session settings, lock check, fingerprints, manifest, gate wait (R6) | `import.rs`, new `src/mssql/fingerprint.rs` | M |
| 8. Translator: prelude change, warning codes, `--strict`, `audit-ci`, `--mssql-math` macros + `verify-math` (R7) | `src/transpile/*`, new `src/mathcompat.rs` | L |
| 9. Plan/log/summary/exit codes (R8) | `main.rs`, new `src/runlog.rs` | S |
| 10. Verification: counts, checksums, except sample, index completeness (R9) | new `src/verify.rs` | M |
| 11. Swap helper (R10) | new `src/mssql/swap.rs` | S |

Recommended order: 1 → 2 → 3 → 4 → 5 → 7 → 9 → 10 → 6 → 8 → 11. Steps 1 and 2 make the tool safe to point at a
real server; 3–5 make it fast enough; 8 is the largest piece and can come later.

---

## 15. Test plan

### Unit
- Guard: the pattern is matched; an attempt on a non-matching name fails; a source-scan test finds no `DROP DATABASE` /
  `ALTER DATABASE` / `ROLLBACK IMMEDIATE` text outside `guard.rs`.
- Converters: round-trip for every type at boundary values (`decimal(38,s)` max, negative scale handling, `tinyint` 0/255/256
  as an error, `varchar(n)` at n and n+1, non-ASCII into `varchar`).
- Split: `column`/`hash`/`files` jobs cover every row exactly once (count and checksum of union = source).
- Translator: one fixture per warning code; `--strict` exits non-zero.

### Integration (CI, SQL Server 2025 container)
- A 10 M-row table, partitioned on a `char(3)` column with 25 values, 10-column `varchar` clustered PK, `PAGE`
  compression, a CI collation. Load with `switch -j 8`; verify counts, checksums and indexes.
- A kill at a random point in the load, then rerun: no duplicates, finished jobs skipped.
- Lock contention: a holder session with an open `UPDATE`; the import respects `--lock-wait` and never appears as a blocker.
- Fingerprint: insert during read, then re-read.
- Guard: every DB-level operation against `Sales` (non-disposable) fails with exit code 3; a connected session on `_x` stops the drop.
- Math: `verify-math` with 20,000 random cases, 0 mismatches.

### Scale (manual, documented)
- 1.2 bn rows, 17 columns, into a 25-partition table on an 8-core client. Record rows/s per session, total time, log
  growth (SIMPLE recovery) and CPU on both ends.

---

## 16. Out of scope (for now)
- PostgreSQL/MySQL targets for the new modes.
- Online index rebuilds / Enterprise-only features (parallel `CREATE INDEX`, online `SWITCH` with `WAIT_AT_LOW_PRIORITY`).
  These may be detected and used when available, but nothing may depend on them.
- Automatic translation of procedural T-SQL.

## 17. Acceptance summary

| ID | Must pass |
|---|---|
| R1 | No path can drop, disconnect or reconfigure a non-disposable database; other sessions are never disconnected. |
| R2 | Loads into existing production DDL with no DDL issued on the target; type, length, NULL and PK problems caught before data moves. |
| R3 | `varchar` targets receive `varchar` data; no silent `?` substitution. |
| R4 | 1.2 bn-row partitioned table loaded in parallel via staging + SWITCH; restartable; partial data never visible. |
| R5 | ≥ 300k rows/s per session; 1.2 bn rows ≤ 15 min load time on the reference setup. |
| R6 | Reads never block or wait indefinitely on production writers; concurrent change detected and re-read. |
| R7 | No silent semantic differences: every approximation is a warning, `--strict` blocks execution. |
| R8 | `--plan` shows every statement; logs never stop a run; meaningful exit codes. |
| R9 | Counts and checksums match in both engines; indexes identical to the captured definitions. |
| R10 | Swap is all-or-nothing, bounded by a lock timeout, keeps the old tables. |
