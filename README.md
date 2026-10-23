# DuckBridge

**DuckDB ⇄ SQL Server / PostgreSQL / Parquet bridge.** Keep your database of record where it is, do the
heavy processing in DuckDB, ship the result back as a genuine, restorable SQL Server `.bak`, a live
load, a PostgreSQL load, or Parquet files. Built to grow: each target is a self-contained module
(`src/mssql`, `src/pg`, `src/parquet.rs`) plus a dialect rule set in `src/transpile`, so adding
another engine means adding one of each.

One static binary (Linux, macOS, Windows). No ODBC driver, no Python, no `bcp` needed on the machine.

```
 SQL Server ──import──▶ work.duckdb ──run translated T-SQL──▶ processed ──export──▶ .bak / live load / script
                                                                                  ──▶ PostgreSQL / Parquet
```

## Commands

| Command | What it does |
|---|---|
| `import`    | Copy a SQL Server database (schema + data) into a DuckDB file over TDS, N streams in parallel; big tables are split into key ranges. |
| `transpile` | Translate SQL between `tsql`, `duckdb` and `postgres` with our own translator (no third-party SQL parser). |
| `run`       | Translate a T-SQL script and execute it on a DuckDB file, statement by statement. |
| `export`    | Write a DuckDB file to SQL Server as a T-SQL **script**, a **live load** (TDS bulk copy), or a **`.bak`** (load → `BACKUP DATABASE` → fetch → verify by restore). Also `--to postgres` and `--to parquet`. |
| `inspect`   | Show the catalog and the exact type mapping + loss warnings for every column before you commit. |
| `cp`        | Copy a file between local disk and `s3://` / `az://` / `gs://` with the built-in clients. |
| `swap`      | Cut over: rename live tables to `<live>__old_<timestamp>` and new tables to the live names, all or nothing (0.1.1). |
| `drop-old`  | Drop the `__old_` tables left by `swap` that are older than a date (0.1.1). |
| `audit-ci`  | Report values that differ only by case or trailing spaces, so you know whether case-insensitive handling is needed (0.1.1). |
| `verify-math` | Evaluate random arithmetic cases on SQL Server and in DuckDB (with the `mssql_*` macros) and report mismatches (0.1.1). |

Input files and outputs accept local paths or `s3://bucket/key`, `az://container/blob`, `gs://bucket/key`.
Cloud clients are built in (SigV4 / Azure Shared Key or SAS), credentials come from the usual environment variables.

## What changed in 0.1.2

0.1.2 is a correctness and DuckDB 2.0 release; no flag or default changed.

| Area | Change |
|---|---|
| `import` | `datetimeoffset` values were shifted by their offset (`10:00+02:00` became `10:00Z`); they now land as the UTC instant (`08:00Z`). |
| `import` | `datetime` values carried the raw 1/300 s tick (`.123333`); they are rounded to the millisecond SQL Server displays (`.123`). |
| DuckDB extension | One binary loads on DuckDB 1.5.6 and 2.x (stable v1 C API), and the community registry serves it on DuckDB 2.0. |
| DuckDB extension | A second, native DuckDB 2.0 build (`duckdb-neo`): `bridge_scan`, `bridge_tables`, `PRAGMA bridge_import`, `PRAGMA bridge_run`, `COPY ... TO 'mssql://...'`. See [DuckDB 2.0](#duckdb-20). |
| `run` | The result preview of a query (first 20 rows) prints values as text, the way the DuckDB CLI does, instead of the internal debug form (`Text([66, 111, 98])`). |
| `run` / catalog | DuckBridge no longer sets DuckDB's `lambda_syntax` option on the databases it opens; the SQL it generates uses the `lambda x:` form that DuckDB 1.5 and 2.0 both accept. |

## What changed in 0.1.1

0.1.1 makes loads into shared production servers safe (see [Loading into production tables safely](#loading-into-production-tables-safely)).
New features are new flags or subcommands; the behaviour changes below are on by default.

| Before (0.1.0) | Now (0.1.1) |
|---|---|
| `--drop-existing` ran `SET SINGLE_USER WITH ROLLBACK IMMEDIATE` and disconnected other sessions | `ROLLBACK IMMEDIATE` is never issued. A drop stops with the list of connected sessions. |
| A live load created the target database if it was missing | Needs `--create-database`, and the name must match the disposable pattern (default `^_`). |
| Every live load set compatibility level 150 and `RECOVERY SIMPLE` | Never changed unless `--compat-level` / `--recovery` are given, and only on disposable databases. |
| `run` / `transpile` set a global `nocase` collation | `--nocase rewrite` is the default (per-literal `lower()`/`rtrim`); `global` is opt-in because it is slow on big `GROUP BY` / `DISTINCT`. |
| Import read with the server's default lock behaviour | `LOCK_TIMEOUT 1000` ms, `DEADLOCK_PRIORITY LOW`, a lock check before each table, a change fingerprint before and after (up to 2 re-reads). |
| Strings above 4000 characters became `NVARCHAR(MAX)` | Above 2000 characters they become `NVARCHAR(MAX)` (the TDS bulk path cannot declare `nvarchar(2001..4000)`). |
| Row values converted one by one from text | Precompiled column converters, DECIMAL passed as mantissa, pipelined read/convert/send. Conversion runs at about 1.2 M rows/s; one bulk session reaches about 650 k rows/s on the dev container (17-column rows, local SQL Server 2019), up from about 500 k. Your numbers depend on hardware and network. |
| Exit code 1 for any error | 0 ok, 1 a check failed, 2 could not run, 3 stopped by the safety guard. |

## Quick start

```bash
export MSSQL_PASSWORD='...'

# 1. SQL Server -> DuckDB (8 parallel streams, tables >= 20M rows split into key ranges)
duckbridge import work.duckdb --host sql01 --database Sales -j 8          # Windows auth by default; --user sa --password … for SQL logins

# 2. run your existing T-SQL on DuckDB
duckbridge run work.duckdb nightly_kpis.sql --from tsql --memory-limit 40GB --threads 8

# 3. ship the result back as a restorable .bak (only the tables that changed)
duckbridge export work.duckdb --to bak --tables customer_kpi \
    --host sql01 --user sa --database Sales_kpi --drop-existing \
    --backup-dir /var/opt/mssql/backup --fetch local --out s3://my-bucket/kpi/Sales_kpi.bak \
    --verify-restore --drop-staging

# other targets
duckbridge export work.duckdb --to script --out Sales.sql --create-database   # sqlcmd -i Sales.sql
duckbridge export work.duckdb --to server --host sql01 --database Sales_copy   # live load, no .bak
duckbridge export work.duckdb --to postgres --pg-dsn "host=pg user=postgres password=x dbname=sales"
duckbridge export work.duckdb --to parquet --out s3://lake/sales/

# translate a script without running it (prelude included), or target a newer SQL Server on the way back
duckbridge transpile job.sql --from tsql --to duckdb -o job.duckdb.sql
duckbridge transpile report.duckdb.sql --from duckdb --to tsql --target-version 2022
```

### Connecting to SQL Server: Windows authentication by default

Connection flags (`--host --port --user --password`, env `MSSQL_*`) and the `mssql://` URLs used by
`cp` and the extension pick the login the same way:

| You give | Login used | Platforms |
|---|---|---|
| nothing (no `--user`) | **Integrated Windows authentication**: the identity running duckbridge (SSPI, Kerberos/NTLM) | Windows |
| `--user 'CORP\svc_etl' --password …` or `--user svc_etl@corp.local` | **Windows credentials** (NTLM) | Windows, Linux, macOS |
| `--user svc --password … --windows-auth` (or `?trusted_connection=yes` in a URL) | Windows credentials, no domain prefix | all |
| `--user sa --password …` | SQL Server login | all |

```bash
duckbridge import work.duckdb --host sql01 --database Sales                      # Windows: your own identity
duckbridge import work.duckdb --host sql01 --database Sales --user 'CORP\etl' --password "$PW"   # Linux box, domain account
SELECT * FROM bridge_import('mssql://CORP%5Cetl:p%40ss@sql01/Sales');             # extension; \ is %5C
```

No system Kerberos libraries are needed: Windows uses the OS SSPI, other platforms use a pure-Rust
NTLM implementation. `--trust-cert` (default on) accepts self-signed server certificates; TLS is
always on unless `--encrypt false`.

### Reading from a busy source

`import` protects the source server and the data it reads. Every import connection runs
`SET LOCK_TIMEOUT 1000; SET DEADLOCK_PRIORITY LOW;` (DuckBridge loses deadlocks, production jobs win), and the
source connection can only run reads. Before each table it checks for locks held by other sessions, and it
compares a fingerprint (row count, `last_user_update`, `modify_date`) before and after the read; if the
fingerprint changed, the table is read again.

| Flag | Default | Meaning |
|---|---|---|
| `--isolation read-committed\|read-uncommitted\|snapshot` | `read-committed` | `snapshot` needs `ALLOW_SNAPSHOT_ISOLATION` (with only `READ_COMMITTED_SNAPSHOT` on, statement-level versioning is used); otherwise the import stops. When the database has either option on, the import prints a hint to use it. `--nolock` is the same as `read-uncommitted`. |
| `--lock-timeout <ms>` | `1000` | `SET LOCK_TIMEOUT` on every source connection (`-1` waits forever). |
| `--lock-check <bool>` | `true` | Look for X / IX / SIX / Sch-M locks from other sessions on the table before reading it. |
| `--lock-wait <s>` | `30` | How long to wait for those locks to clear. |
| `--on-lock fail\|skip` | `fail` | After the wait: fail naming the blocking sessions, or skip the table and report it. |
| `--max-rereads <n>` | `2` | Re-read a table whose fingerprint changed during the read; fail after n re-reads. |
| `--manifest <file>` | none | Write what was read, as CSV plus a JSON copy (`x.csv` and `x.json`). |
| `--readonly` | off | Connect with `ApplicationIntent=ReadOnly` (routes to a readable secondary when there is one). |
| `--wait-for "<T-SQL>"` | none | Poll a query (first column true/1) before starting, for example an upstream job's completion flag. |
| `--wait-timeout <s>` | `3600` | Give up on `--wait-for` after this long. |

The manifest has one row per table with the columns `table, rows_read, rows_source_stats, fingerprint_before,
fingerprint_after, started, finished, seconds, isolation, rereads`. A skipped table has `rows_read = -1`.
Downstream jobs can read it to confirm what they were given.

```bash
duckbridge import work.duckdb --host SRC --database SRCDB --tables t1,t2 \
    --isolation snapshot --lock-timeout 1000 --on-lock fail --manifest in.manifest.csv -j 8 \
    --wait-for "SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM ctl.flags WHERE step='inputs' AND done=1) THEN 1 ELSE 0 END AS bit)"
```

`import` also accepts `--log <file>` and `--summary <file>` (see [Plan, log, summary](#plan-log-summary)).

### Making DuckDB behave like SQL Server

`run` (and `transpile --from tsql`, unless `--no-prelude`) applies a session prelude before your T-SQL:

```sql
SET integer_division = true;                                   -- 7 / 2 = 3, like T-SQL
SET default_null_order = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC';   -- SQL Server NULL ordering
SET TimeZone = 'UTC';
SET default_collation = 'nocase';                              -- SQL Server default collations are case-insensitive
```

Since 0.1.1 the `default_collation` line is only emitted with `--nocase global`; the default prelude (`--nocase rewrite`)
is the first three statements, and case-insensitivity is handled per comparison (see [Case-insensitive strings](#case-insensitive-strings)).
T-SQL `LIKE` becomes DuckDB `ILIKE` (DuckDB `LIKE` is case-sensitive). Use `--nocase off` for a binary-collated source. When translating DuckDB SQL **to** T-SQL, `a / b` becomes `CAST(a AS FLOAT) / b` because DuckDB
`/` is always floating point, and `//` becomes `/`.

### Target version

`--target-version 2019|2022|2025` (on `transpile`, `run`, `export`) controls which T-SQL is emitted and which types are used:

| Feature | 2019 (compat 150) | 2022 (160) | 2025 (170) |
|---|---|---|---|
| `GREATEST`/`LEAST`, `DATETRUNC`, `GENERATE_SERIES`, `DATE_BUCKET`, `LTRIM(x, chars)` | emulated / warned | native | native |
| `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `BASE64_ENCODE` | warned (no regex) | warned | native |
| DuckDB `JSON`, `VARIANT`, nested types | `NVARCHAR(MAX)` | `NVARCHAR(MAX)` | native `json` |
| DuckDB `FLOAT[n]` (n ≤ 1998) | `NVARCHAR(MAX)` | `NVARCHAR(MAX)` | `VECTOR(n)` |
| `BACKUP TO URL` (`--backup-to-url s3://…`) | not available | yes (needs a server `CREDENTIAL`) | yes |

A `.bak` written by 2019 restores on 2022 and 2025 (the database keeps compat level 150 until you raise it); a newer
version's backup never restores on an older one.

## How the load is made fast

* **Streaming everywhere.** Rows flow DuckDB → TDS bulk-copy (or TDS → DuckDB appender) in batches; memory is
  bounded by `batch_rows × parallel`, not by table size. Nothing is written to temp files.
* **Parallel streams.** `-j N` tables at once (default: CPU count). A table above `--split-threshold` rows with an
  integer key is cut into key ranges so a single 1.5-billion-row table also uses every stream.
* **Heap then key.** By default the live load creates tables *without* the clustered primary key, bulk-copies into
  the heap with `TABLOCK` and `KEEP_IDENTITY`, then adds the clustered PK once at the end (a single parallel sort in
  SQL Server). Secondary indexes, unique/foreign keys and views are created after the data. `--pk-inline` keeps the
  old behaviour; `--order-by-key` adds `ORDER BY key` per range when the DuckDB data is already sorted.
* **Identity preserved.** Identity values are copied as-is and the seed is reset to the maximum afterwards. On import,
  SQL Server identities become DuckDB sequences that start after `IDENT_CURRENT`.
* **Import of billions of rows.** `import --keys-after-load` creates DuckDB tables without constraints, streams the data,
  then adds the primary key and unique indexes (DuckDB cannot add foreign keys afterwards, so those are skipped with a warning).
  Tables above `--split-threshold` rows with an integer key are read in key ranges on parallel connections.
* **Staging database** uses `RECOVERY SIMPLE` for minimal logging; the backup uses `COMPRESSION` and `CHECKSUM`,
  optionally striped (`--stripes N`).

The ceiling is SQL Server's own bulk-copy ingest speed; the tool runs that path in parallel, which is as fast as SQL Server goes.

## Type mapping (DuckDB → SQL Server 2019)

Every lossy decision is reported per column by `inspect` and at the end of every export.

| DuckDB | SQL Server | Note |
|---|---|---|
| BOOLEAN | BIT | |
| TINYINT / UTINYINT | SMALLINT / TINYINT | DuckDB TINYINT is signed |
| SMALLINT, INTEGER, BIGINT | SMALLINT, INT, BIGINT | USMALLINT→INT, UINTEGER→BIGINT, UBIGINT→DECIMAL(20,0) |
| HUGEINT / UHUGEINT / VARINT | DECIMAL(38,0), or NVARCHAR text when any value exceeds 38 digits | measured, never truncated |
| FLOAT / DOUBLE | REAL / FLOAT(53) | NaN and ±Infinity become NULL (SQL Server cannot store them) |
| DECIMAL(p,s) | DECIMAL(p,s) | |
| VARCHAR | NVARCHAR(n) sized from data, or NVARCHAR(MAX) (`--string-mode max`) | key columns capped at NVARCHAR(450); above 2000 characters NVARCHAR(MAX); `--string-type varchar\|auto` in [create mode](#string-types-in-create-mode) |
| BLOB | VARBINARY(MAX) | |
| DATE / TIME | DATE / TIME(7) | |
| TIMESTAMP (_S/_MS/_NS) | DATETIME2(7) | TIMESTAMP_NS loses digits 8–9 |
| TIMESTAMP WITH TIME ZONE | DATETIMEOFFSET(7) | stored as UTC +00:00 |
| TIME WITH TIME ZONE | TIME(7) | offset dropped |
| INTERVAL | NVARCHAR(64) text | no interval type in SQL Server |
| UUID | UNIQUEIDENTIFIER | |
| JSON, LIST, STRUCT, MAP, UNION, ARRAY | NVARCHAR(MAX) JSON text | |
| ENUM | NVARCHAR(n) + CHECK constraint | |
| BIT (bitstring) | VARCHAR(MAX) text | |

SQL Server → DuckDB covers every system type; `hierarchyid`/`geography`/`geometry` are converted with `.ToString()`,
`sql_variant` and `xml` become VARCHAR, `rowversion` becomes an 8-byte BLOB, 2025 `json` becomes JSON and `vector(n)`
becomes `FLOAT[]`. `datetime2(7)`/`time(7)` keep microseconds (digit 7 is dropped, warned) and `datetimeoffset` is stored as a
UTC instant (offset not kept, warned). NVARCHAR sizes are measured in UTF-16 code units, so emoji and other supplementary
characters are counted correctly.

## SQL translation

`transpile`/`run` use a tokenizer and rule engine written for this tool. T-SQL → DuckDB covers `TOP`/`OFFSET…FETCH`,
`[brackets]`, `N'…'`, table hints, `ISNULL`/`IIF`, `GETDATE`, `DATEADD`/`DATEDIFF`/`DATEPART`/`DATENAME`, `CONVERT`
styles, `CAST` types, `CHARINDEX`, `STUFF`, `FORMAT`, `EOMONTH`, `STRING_AGG … WITHIN GROUP`, `CROSS/OUTER APPLY`,
`DECLARE/SET @var`, table variables, `#temp` tables, `SELECT … INTO`, `UPDATE … FROM JOIN`, `DELETE alias FROM JOIN`,
`IF OBJECT_ID(…) DROP`, `IDENTITY` → sequences, string `+` → `||`, `GO` batches, and more. The reverse direction
covers `LIMIT`/`OFFSET`, `::` casts, `||`, interval arithmetic → `DATEADD`, `strftime` → `FORMAT`, `getvariable`,
temp tables, `RETURNING` → `OUTPUT`, and so on. Anything that cannot be translated faithfully (procedural `IF`/`WHILE`,
cursors, regex on 2019, `QUALIFY`, nested types…) is left in place or commented out **and reported as a warning**, never
silently changed.
### Warnings and `--strict`

Constructs that DuckDB cannot reproduce exactly are reported with a code, line and column, for example
`W-DIV-DECIMAL (line 12:8): ...`. Free-text warnings from older rules carry `W-GENERIC`.

| Code | Meaning |
|---|---|
| `W-DIV-DECIMAL` | T-SQL `numeric / numeric` yields a truncated `numeric(p,s)`; DuckDB yields `DOUBLE`, so the last digits can differ. |
| `W-ROUND-TRUNC` | `ROUND(x, n, 1)` (truncate) is done with `DOUBLE` arithmetic. |
| `W-FORMAT-NUM` | `FORMAT(x, '0.0%')` and other numeric formats are not translated. |
| `W-TRYCAST-EMPTY` | `TRY_CAST('' AS int)` is `0` in T-SQL and `NULL` in DuckDB. |
| `W-CI-COMPARE` | A string comparison, `GROUP BY` or `DISTINCT` that depends on a case-insensitive collation and was not rewritten. |
| `W-TRAILING-SPACE` | `'a' = 'a '` is true in T-SQL and false in DuckDB. |
| `W-PROCEDURAL` | `WHILE`, `IF ... BEGIN ... END`, dynamic SQL: left as a comment, so the statement is not executed. |
| `W-POWER-DOUBLE` | `power()` / `**` use `DOUBLE`; decimal formulas can differ in the last digit. |

`--strict` (on `transpile` and `run`) turns any warning into an error: nothing is executed and the exit code is 1.

```bash
duckbridge run work.duckdb steps/nightly.sql --strict --nocase rewrite --mssql-math
```

### Case-insensitive strings

`--nocase rewrite|global|off` (default `rewrite`):

| Mode | What it does |
|---|---|
| `rewrite` | `LIKE` becomes `ILIKE`. A comparison of a column with a letter-containing literal becomes `lower(rtrim(col, ' ')) = lower('Lit')` (also `<>` and `IN (...)`). Joins, `GROUP BY`, `DISTINCT` and ordering comparisons are not rewritten and get `W-CI-COMPARE`. |
| `global` | `SET default_collation = 'nocase'`. Everything is case-insensitive, but on large inputs it makes hash `GROUP BY` / `DISTINCT` single-threaded and can run out of memory where the binary collation finishes in minutes. |
| `off` | Binary comparisons, like a `_BIN` / `_CS` source. |

To see whether you need any of this, run the audit. If a column has no values that differ only by case or trailing
spaces, binary comparison gives the same result as SQL Server's `CI_AS` for that data:

```bash
duckbridge audit-ci work.duckdb --tables customers,orders --columns name,country --script nightly.sql
```

It lists, per column, the values that collide by case or trailing space, and every string literal in the script that
matches a column value only case-insensitively (rewrite the literal). `--json` gives machine-readable output.

### Exact SQL Server arithmetic

`--mssql-math` installs DuckDB macros that reproduce SQL Server results with integer/HUGEINT arithmetic instead of
`DOUBLE`, and the translator uses them where it knows the column types:

| Macro | T-SQL it stands for |
|---|---|
| `mssql_div(a, b, p, s)` | `numeric` division, truncated to the result scale |
| `mssql_round(x, n)` | `ROUND(x, n)`, half away from zero on DECIMAL |
| `mssql_round_trunc(x, n)` | `ROUND(x, n, 1)` |
| `mssql_try_int(text)` | `TRY_CAST(text AS int)`, with `''` giving `0` |
| `mssql_format_pct(x, decimals)` | `FORMAT(x, '0.0%')` |

Types come from `CREATE TABLE` / `DECLARE` in the same script, or from `--types`:

```bash
duckbridge transpile kpi.sql --mssql-math --types 'amount=decimal(18,4),qty=int' -o kpi.duckdb.sql
duckbridge verify-math --host sql01 --user sa --cases 20000      # random cases on both engines, reports mismatches
```

## Loading into production tables safely

For a weekly batch into a shared SQL Server with fixed production DDL, use `export --to server` with the options
below. The tool only touches objects it is allowed to, loads into the DDL you already have, checks the data
before it moves, and can resume after a kill.

### Safety guard

Database-level statements (`CREATE`/`DROP DATABASE`, `ALTER DATABASE ... SET RECOVERY`, `SET COMPATIBILITY_LEVEL`) are
allowed only for databases whose name matches the **disposable-database pattern**: `--disposable-db-pattern <regex>`,
env `DUCKBRIDGE_DISPOSABLE_DB_PATTERN`, default `^_` (names starting with an underscore).

| Situation | Result |
|---|---|
| `--drop-existing` on `Sales` (not disposable) | refused before connecting: `refusing DROP DATABASE on 'Sales': not a disposable database (pattern ^_)` |
| Dropping `_scratch` while other sessions are connected | refused, the sessions are listed (id, login, host, program); nobody is disconnected |
| Create, drop or alter any other database | refused |
| `ROLLBACK IMMEDIATE` | never issued, no override flag exists |
| Compatibility level, recovery model of an existing database | untouched. Set only with `--compat-level <n>` / `--recovery simple\|full\|bulk_logged`, on disposable databases |
| Live load into a database that does not exist | error, unless `--create-database` is given and the name matches the pattern |

Exit codes: `0` ok, `1` a check failed (types, lengths, PK, row counts, fingerprints), `2` could not run (connection,
permissions), `3` stopped by the guard.

### `--into-existing`

`export --to server --into-existing` treats the target table's DDL as the contract. No `CREATE`, `ALTER` or `DROP`
touches the target table (its `modify_date` does not change). The tool reads the target's columns, indexes, partitioning and check
constraints from the catalog and maps DuckDB columns to them **by name**, case-insensitively, in the target's column order.

* A DuckDB column with no target column is an error, unless `--ignore-extra-columns`.
* A target column with no DuckDB column is an error, unless it is nullable or has a default; it is then left out of the load.
* All checks run before any bulk session opens: one aggregate DuckDB query per table. Findings are reported together,
  with the longest value and sample keys. `--check-only` runs them and moves no data.

| Code | What is checked |
|---|---|
| `LENGTH` | `char`/`varchar(n)` (bytes for `_UTF8` collations), `nchar`/`nvarchar(n)` (UTF-16 code units), `binary`/`varbinary(n)` |
| `CODEPAGE` | every character is representable in the column's code page (no silent `?`) |
| `NOT_NULL` | NULLs in a `NOT NULL` target column |
| `RANGE` | `tinyint` 0..255, `smallint`, `int`, `bigint`, `money`, `real`; NaN/Infinity |
| `SCALE` / `PRECISION` | `decimal(p,s)`: source scale above `s` is an error unless `--allow-rescale`; values must fit `p-s` integer digits |
| `BIT` | boolean, or integer in {0,1} |
| `DATE_RANGE` | `date`, `datetime` (1753..9999), `smalldatetime`, `datetime2`; precision loss is a warning (`DATETIME_PRECISION`) |
| `PK_DUP` | duplicate primary keys, trailing spaces ignored. Only with `--check-pk full\|hash` |
| `CI_COLLISION` | keys that collide under a `_CI` collation (`'a'` vs `'A'`, or trailing spaces), which SQL Server would reject |

`TYPE` (incompatible pair, bad UUID or number text) and `EXTRA_COLUMN` / `MISSING_COLUMN` also appear.

```bash
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --check-only --check-pk hash
```

### Splitting and strategies

A table is cut into jobs with `--split <table>=<spec>` (repeatable) and loaded with a strategy given by
`--strategy <table|*>=<strategy>` (repeatable). All jobs of all tables share one queue, largest first, run by `-j N` bulk sessions.

| `--split` spec | One job per |
|---|---|
| `column:<col>` | distinct value of a low-cardinality column (for example the market column) |
| `files:<glob>` | Parquet file or Hive folder, e.g. `files:out/big/Mkt=*/` (no huge DuckDB table needed) |
| `hash:<cols>:<n>` | `hash(cols) % n`, for tables without a natural split column |
| `range:<col>:<n>` | integer key range (the 0.1.0 behaviour; also the automatic split above `--split-threshold`) |

| `--strategy` | Use when | What happens |
|---|---|---|
| `switch` | The target is partitioned on the split column | Per job: create `<table>__stg_<value>` from the target's own metadata (same columns, collation, clustered key, compression, filegroup, a `CHECK` on the partition value), bulk load it with `TABLOCK`, build the nonclustered indexes, check the row count, then `ALTER TABLE ... SWITCH` it in. Readers never see a partial partition. |
| `heap-then-index` | The target is a heap, or dropping and recreating its indexes is acceptable | The nonclustered and clustered indexes are captured from `sys.indexes`, dropped, the table is bulk loaded in parallel (`TABLOCK` bulk-update locks are compatible), and the indexes are recreated from the captured DDL (compression, fill factor, `INCLUDE`, filters). Captured DDL is written to the run log in case a run dies in between. |
| `serial` | Small tables (default) | One bulk session per table. Many tables still load in parallel with each other. |

On Standard Edition `CREATE INDEX` is single-threaded, so `heap-then-index` builds indexes one at a time;
`switch` builds each partition's clustered index in its own session and is the only way to get parallel index builds there.

```bash
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --split big_table=column:Mkt --strategy big_table=switch --strategy '*'=serial -j 8 \
    --max-jobs-per-table 6 --state-dir state/ --replace-partitions --drop-old
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --source-parquet 'big_table=out/big_table/Mkt=*/' --strategy big_table=switch   # a Parquet source implies a files: split, one job per folder
```

| Flag | Meaning |
|---|---|
| `--max-jobs-per-table <n>` | one table cannot take every `-j` slot |
| `--state-dir <dir>` | each finished job writes `<dir>/<schema>.<table>/<job>.done` (rows, seconds). Rerun with the same directory after a kill: finished jobs are skipped, an unfinished job's staging table (never visible to readers) is dropped and reloaded. A job killed after its `SWITCH` but before its marker fails on rerun with "partition N already holds X rows; pass --replace-partitions" rather than duplicating data. |
| `--replace-partitions` | switch: a non-empty target partition is switched out to `<table>__old_<value>` instead of failing |
| `--drop-old` | drop those `<table>__old_<value>` tables after a successful load |
| `--source-parquet table=<glob>` | read that table from Parquet (Hive folders supported) instead of the DuckDB file |

Staging tables are dropped only if their name contains `__stg_` or `__old_`; the target table is never dropped.

### Verification

`--verify counts|checksum|except` (default `counts`, which always runs), `--sample <n>` (default 10).

| Level | Checks |
|---|---|
| `counts` | target rows (per partition for partitioned tables) against the DuckDB count |
| `checksum` | per column, the sum of MD5 hashes of a canonical text form (`rtrim` for `char`, decimals at the target scale, ISO dates, lower-case GUIDs, hex binary, NULL marker), computed with the same rules in DuckDB and in SQL Server. A mismatch names the columns. Float columns are compared by NULL pattern only. Needs SQL Server 2019+. |
| `except` | `checksum`, plus up to `--sample` differing rows in each direction for each table or partition that mismatches |

After `heap-then-index` or `switch`, every index captured before the load must exist with the same definition
(type, key columns and order, `INCLUDE`, filter, compression, fill factor, data space), and the PK must not be disabled.

### Plan, log, summary

* `--plan` prints every statement that would be sent to each server and the list of bulk jobs with estimated rows, and executes
  nothing. It connects only for metadata reads.
* `--log <file>` appends one line per statement and event; a failed log write never stops the run.
* `--summary <file>` writes JSON: per table and job, rows, seconds, strategy and checks.
* `--progress-seconds <n>` (default 10) sets the progress line interval (rows/s per job and in total).

`import` accepts `--log` and `--summary` too.

### Cut-over

```bash
duckbridge swap --host TGT --database PRODDB --pairs kpi_new=kpi,dim_new=dbo.dim --lock-timeout 5000
duckbridge drop-old --host TGT --database PRODDB --before 2026-11-01
```

`swap` renames in one transaction with `SET LOCK_TIMEOUT` (default 5000 ms): each live table becomes
`<live>__old_<yyyymmddhhmm>` (UTC) and each new table takes the live name. If a lock cannot be obtained, everything rolls
back and the blocking sessions are listed; nobody is killed. It refuses, before changing anything, when a schema-bound view
or function references the tables, when other tables have foreign keys to them, or when a name collides.
The old tables are kept (`--keep-old` is on by default; `--keep-old false` drops them after the commit). `drop-old --before`
removes `__old_` tables whose timestamp is earlier than the date (`YYYY-MM-DD` or `"YYYY-MM-DD HH:MM"`, UTC); `--schema` limits it.

### String types in create mode

Without `--into-existing`, DuckDB `VARCHAR` columns are created as `NVARCHAR` by default.

| Flag | Values |
|---|---|
| `--string-type` | `nvarchar` (default), `varchar`, `auto` (`varchar` when every value of the column is ASCII, `nvarchar` otherwise; the decision shows in `inspect`) |
| `--string-sizes` | `from-data` (default, measured), `max`, or a CSV file `table,column,type` that pins exact types |

`inspect` takes the same two flags, so you can preview the DDL. Explicit `varchar` with non-ASCII data can lose characters in the
target code page; use `auto`, or `--into-existing`, whose `CODEPAGE` check stops instead.

### End to end

```bash
# 1. read the inputs safely
duckbridge import work.duckdb --host SRC --database SRCDB --tables t1,t2 \
    --isolation snapshot --lock-timeout 1000 --lock-check true --on-lock fail \
    --manifest in.manifest.csv -j 8

# 2. process; stop before running anything if a construct cannot be translated exactly
duckbridge run work.duckdb steps/nightly.sql --strict --nocase rewrite --mssql-math

# 3. pre-flight against the existing target DDL (no data moves)
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --check-only --check-pk hash

# 4. load and verify
duckbridge export work.duckdb --to server --host TGT --database _stage_db --into-existing \
    --split big_table=column:Mkt --strategy big_table=switch --strategy '*'=serial -j 8 \
    --state-dir state/ --verify checksum --log load.log --summary load.json

# 5. cut over (optional)
duckbridge swap --host TGT --database PRODDB --pairs kpi_new=kpi
```

## Roadmap

DuckBridge is organised so that every engine is a pair: a **target module** (catalog reader, bulk
loader, backup/restore) and a **dialect rule set** for the translator. SQL Server and PostgreSQL are
done; the plan for the rest, in priority order:

| Phase | Engine / feature | What it adds | Status |
|---|---|---|---|
| 1 | SQL Server 2019 / 2022 / 2025 | import, live load, `.bak`, T-SQL ⇄ DuckDB | done |
| 1 | PostgreSQL | export via `COPY`, DuckDB ⇄ PostgreSQL SQL | export done; import next |
| 1 | Parquet | export, any engine can absorb it | done |
| 2 | **DuckDB extension** (`duckbridge`) | `bridge_import`, `bridge_export`, `bridge_run`, `bridge_transpile` straight from DuckDB SQL | done; one binary for DuckDB 1.5.6 and 2.x (stable v1 C API); native 2.0 build (`duckdb-neo`): `bridge_scan`/`bridge_tables`/`PRAGMA bridge_import`/`PRAGMA bridge_run`/`COPY TO` |
| 2 | PostgreSQL import | binary `COPY TO STDOUT`, key-range parallel streams, identity → sequence | planned |
| 3 | MySQL / MariaDB | `LOAD DATA LOCAL INFILE`, MySQL dialect rules | planned |
| 3 | Oracle | `SQL*Loader`-style direct path over OCI-free wire protocol, PL/SQL-lite rules | planned |
| 3 | Snowflake / BigQuery / Databricks | Parquet stage + `COPY INTO`, dialect rules | planned |
| 3 | SQLite | direct file read, dialect rules | planned |
| 1 | Safe large loads into existing DDL, partition `SWITCH`, safe reads, verification, cut-over | done in 0.1.1 |
| 4 | Change-data pipelines | incremental import by watermark / CDC tables, partitioned `.bak` shipping | planned |
| 4 | Procedural T-SQL | IF / WHILE / cursors to a DuckDB job runner | research |

### DuckDB extension

The same engine ships as a loadable DuckDB extension (`extension/`), so everything above can be
driven from inside DuckDB itself, with no CLI process:

Install: download `duckbridge-<platform>.duckdb_extension.zip` for your platform from the
[Releases](https://github.com/singhpratech/duckbridge/releases) page (`linux_amd64`, `linux_arm64`, `osx_amd64`, `osx_arm64`,
`windows_amd64`), unzip it, and keep the file name `duckbridge.duckdb_extension`: DuckDB derives
the extension's entry point from the file name, so a renamed file will not load. The build is
pinned to DuckDB 1.5.6 and is not yet signed:

```bash
unzip duckbridge-linux_amd64.duckdb_extension.zip        # -> duckbridge.duckdb_extension
duckdb -unsigned work.duckdb                              # or SET allow_unsigned_extensions = true
```

```sql
LOAD '/path/to/duckbridge.duckdb_extension';

-- SQL Server -> this DuckDB database (parallel key-range streams, identity -> sequence)
SELECT * FROM bridge_import('mssql://sa:p%40ss@sql01:1433/Sales', parallel := 8, keys_after_load := true);

-- run the team's T-SQL against DuckDB; the SQL Server compatibility prelude is applied to
-- the extension's own session only, your session's settings are untouched
SELECT * FROM bridge_run('SELECT c.id, SUM(o.amount) AS total INTO dbo.kpi FROM dbo.customers c
                          JOIN dbo.orders o ON o.customer_id = c.id GROUP BY c.id;');

-- ship it back: live load into a heap, clustered PK after, then a verified, restorable .bak
SELECT * FROM bridge_export('mssql://sa:p%40ss@sql01:1433/Sales_kpi', tables := 'kpi',
                            bak := 's3://backups/Sales_kpi.bak', fetch_via := 'local', verify_restore := true);

SELECT * FROM bridge_export('postgres://u:p@pg/analytics', tables := 'kpi');
SELECT * FROM bridge_export('parquet:s3://lake/sales/', parquet_compression := 'zstd');
SELECT * FROM bridge_export('script:/tmp/Sales.sql', database := 'Sales', create_database := true);

-- text in, text out
SELECT bridge_transpile('tsql', 'duckdb', 'SELECT TOP 5 ISNULL(a, 0) FROM dbo.t');
SELECT bridge_transpile('duckdb', 'tsql', 'SELECT greatest(a, b) FROM t', 2022);   -- target version
SELECT bridge_prelude(true);                                                       -- the SET statements
```

Every table function returns `(object, kind, rows, note)` rows: one per table/view, a `summary`,
and any `warning`s. Named parameters mirror the CLI flags (`tables`, `exclude`, `schema_map`,
`parallel`, `split_threshold`, `batch_rows`, `drop_existing`, `schema_only`, `overwrite`, `target_version`,
`string_mode`, `method`, `pk_inline`, `order_by_key`, `bak`, `backup_dir`, `fetch_via`, `stripes`,
`compression`, `verify_restore`, `drop_staging`, `backup_to_url`, `create_database`,
`parquet_compression`; `bridge_run` takes `from`, `nocase`, `schema_map`, `continue_on_error`). The 0.1.1 parameters:

| Function | New named parameters (same meaning as the CLI flag) |
|---|---|
| `bridge_import` | `isolation`, `lock_timeout`, `lock_check`, `lock_wait`, `on_lock`, `max_rereads`, `manifest`, `readonly`, `wait_for`, `wait_timeout`, `log_file`, `summary_file` |
| `bridge_export` | `disposable_db_pattern`, `compat_level`, `recovery`, `plan`, `log_file`, `summary_file`, `string_type`, `string_sizes`, `into_existing`, `ignore_extra_columns`, `allow_rescale`, `check_only`, `check_pk`, `split`, `strategy`, `max_jobs_per_table`, `state_dir`, `replace_partitions`, `drop_old`, `verify`, `sample`, `source_parquet` |
| `bridge_run` | `nocase_mode` (`rewrite`, `global`, `off`), `strict`, `mssql_math`, `types` |

`split`, `strategy` and `source_parquet` take the CLI form, for example `split := 'big_table=column:Mkt'`; several entries are comma-separated.
`bridge_prelude(true)` now returns the 3-line rewrite-mode prelude (no `default_collation`).

```sql
SELECT * FROM bridge_export('mssql://sa:p%40ss@tgt/_stage_db', into_existing := true, check_only := true, check_pk := 'hash');
SELECT * FROM bridge_export('mssql://sa:p%40ss@tgt/_stage_db', into_existing := true,
                            split := 'big_table=column:Mkt', strategy := 'big_table=switch', state_dir := 'state/', verify := 'checksum');
```
URL-encode special characters in passwords (`!` → `%21`, `#` → `%23`, `@` → `%40`).

Build and test:

```bash
cd extension && make release        # -> build/duckbridge.duckdb_extension
make test                           # scripts/e2e_extension.sh against the local SQL Server container
```

How it works: the extension opens a pool of 64 connections to the host database when loaded
and runs every operation on those, so parallel workers get their own connections and nothing
touches your transaction. Two consequences worth knowing: `parallel` is capped at 32 (each bulk-load worker uses two pooled connections), and the
database stays open until the process exits, so a `.wal` file may remain next to the database
after a session that used the bridge (DuckDB replays it on the next open; the bridge checkpoints
after each operation to keep it small). The extension is built on DuckDB's C extension API via
`duckdb-rs` against the *stable* v1 API table (floor `v1.5.6`, footer `C_STRUCT`, see
`extension/Makefile`), so one binary loads on DuckDB 1.5.6 and on DuckDB 2.x, whose loader keeps
serving that table. CI builds Linux x86_64/arm64, macOS Intel/ARM and Windows. The extension is in
the DuckDB community registry
([duckdb/community-extensions](https://github.com/duckdb/community-extensions/tree/main/extensions/duckbridge),
built from [duckbridge-extension](https://github.com/singhpratech/duckbridge-extension)), so on DuckDB 2.0
`INSTALL duckbridge FROM community; LOAD duckbridge;` is all it takes (Linux x86_64/arm64, macOS ARM,
Windows). The registry builds only for DuckDB 2.0; on DuckDB 1.5.6, or for macOS Intel, download the
`duckbridge-<platform>.duckdb_extension.zip` from the
[releases](https://github.com/singhpratech/duckbridge/releases) and load it with `duckdb -unsigned` or
`SET allow_unsigned_extensions = true`.

### DuckDB 2.0

The standard build above runs on DuckDB 2.0 unchanged, through the v1 API table 2.0 still serves,
with every function of the 1.x section (`scripts/e2e_extension.sh` passes 33/33 on both engines).
DuckDB 2.0 also has a new, v2 C API, and an extension written against it gets no connection back to
the database that loaded it, so the 1.x design (the extension opens a pool and runs SQL on it) cannot
exist there. DuckBridge therefore has a second, native 2.0 build (`duckdb-neo`, `make release-neo`,
see `docs/plan-2.0-extension.md`) for the day the v1 table goes away. It turns the arrow around:
DuckDB calls into DuckBridge through table functions (rows out), a `COPY` function (rows in) and
pragmas that return SQL text for DuckDB to run. The names differ from the 1.x section above, and
`bridge_transpile` and `bridge_prelude` are unchanged:

```sql
LOAD duckbridge;

SELECT * FROM bridge_tables('mssql://sa:p%40ss@sql01:1433/Sales', tables := 'dbo.*', exclude := 'dbo.v_*');   -- catalog: schema, name, kind, rows, columns, pk, import_sql
SELECT * FROM bridge_scan('mssql://sa:p%40ss@sql01:1433/Sales', 'dbo.orders');          -- one table, streamed over TDS
SELECT * FROM bridge_scan(dsn, 'SELECT id, total FROM dbo.orders WHERE total > 100');   -- or any query
CREATE TABLE orders AS FROM bridge_scan(dsn, 'dbo.orders', nolock := true);

PRAGMA bridge_import('mssql://sa:p%40ss@sql01:1433/Sales', tables = 'dbo.*', schema_map = 'dbo=main', keys = true);   -- pragmas take name = value
PRAGMA bridge_run('UPDATE dbo.orders SET total = qty * price WHERE total IS NULL', strict = true);

COPY orders TO 'mssql://sa:p%40ss@sql01:1433/Sales/dbo.orders' (FORMAT duckbridge);
COPY (SELECT * FROM kpi) TO 'mssql://sa:p%40ss@sql01:1433/Sales/dbo.kpi' (FORMAT duckbridge, CREATE true, BATCH_ROWS 50000, SORT_BY 'id');
```

Table functions keep DuckDB's `name := value` syntax. PRAGMA arguments are written `name = value`, because
DuckDB's pragma grammar has no `:=`. `PRAGMA bridge_import` returns a script that DuckDB runs in place of
the pragma: the importer's own DDL (schemas, identity sequences, tables with NOT NULL, defaults, primary,
unique and foreign keys), one `INSERT INTO t (cols) SELECT cols FROM bridge_scan(...)` per table in foreign
key order, the translated views, and a closing summary `SELECT` in the 1.x `(object, kind, rows, note)`
shape. `keys = false` drops all constraints. (`bridge_tables` returns the simpler
`CREATE TABLE ... AS FROM bridge_scan(...)` per table in `import_sql`, empty for views.) `PRAGMA bridge_run`
translates the T-SQL and runs it in the current database, prelude first. DuckDB 2.0 refuses a pragma
expansion in which a row-returning statement is followed by other statements, so `bridge_run` leaves out
the `SELECT`s that come before the last statement (a comment in the script marks them) and returns the
rows of the last one; the prelude settings stay on the session unless `reset = true`, which keeps the last
statement's rows in a temp table, resets the settings and then selects them. The table filters take names,
`schema.name` and `*` / `?` globs. `money` and `smallmoney` scan as `DECIMAL(19,4)` and `DECIMAL(10,4)`.
The COPY path is `mssql://user:pw@host:port/Database/schema.table`; the table must exist unless
`CREATE true`, and the database is never created by `COPY`. `COPY` rejects nested types (`LIST`, `ARRAY`,
`STRUCT`, `MAP`, `UNION`) at bind, naming the column; use `to_json(col)`. Unknown COPY options are errors.
The 1.x names `bridge_import(...)`, `bridge_export(...)` and `bridge_run(...)` as table functions still
exist on 2.0 and fail with a message naming the replacement.

| Call | Named parameters / options (default) |
|---|---|
| `bridge_tables` | `tables` (all), `exclude` (none), `schema_map` (none), and the `bridge_scan` options |
| `bridge_scan` | `nolock` (false), `batch_rows` (50000), `lock_timeout` ms (1000), `isolation` (`read-committed`, `read-uncommitted`, `snapshot`), `readonly` (false) |
| `PRAGMA bridge_import` | `tables` (all), `exclude` (none), `schema_map` (none), `keys` (true), `keys_after_load` (false), `overwrite` (false), `views` (true), `schema_only` (false), and the `bridge_scan` options |
| `PRAGMA bridge_run` | `dialect` (`tsql`; `from` is a reserved word in pragma arguments), `nocase_mode` (`rewrite`, `global`, `off`), `strict` (false), `mssql_math` (false), `types` (a spec such as `'col=decimal(18,4)'`), `schema_map` (none), `reset` (false) |
| `COPY ... TO 'mssql://...'` | `FORMAT duckbridge`, `CREATE` (false; `CREATE_TABLE` also works), `DROP_EXISTING` (false), `KEEP_IDENTITY` (false), `TABLE_LOCK` (true), `BATCH_ROWS` (100000), `CHECK_CONSTRAINTS` (true), `SORT_BY` (the bulk ORDER hint; `ORDER_BY` is DuckDB's own COPY option), `STRING_TYPE` (`nvarchar`; `auto`, `varchar`), `TARGET_VERSION` (2019), `TABLE` (overrides the last path segment) |

Not available on 2.0 without a host connection, use the `duckbridge` CLI for these: export targets
other than SQL Server (`.bak`, parquet, postgres, scripts), parallel and split imports, the import
manifest, the lock pre-check and the fingerprint re-reads, and plan mode for `COPY`.

## Build

Prebuilt binaries for Linux, macOS and Windows are on the [Releases](https://github.com/singhpratech/duckbridge/releases) page. With a
Rust toolchain, `cargo install duckbridge` builds the CLI from [crates.io](https://crates.io/crates/duckbridge)
(the bundled DuckDB takes a few minutes to compile). From source:

```bash
cargo build --release            # ~3 min; DuckDB is compiled in
cargo test --release             # translator unit tests
scripts/e2e_mssql.sh             # full round trip against a SQL Server (see env vars in the script)
```

Cross-platform binaries are produced by `.github/workflows/release.yml` (Linux x86_64/aarch64, macOS Intel/ARM, Windows).

## Notes and limits

* A `.bak` can only be written by SQL Server itself, so `--to bak` needs a SQL Server to stage into (the 2019 target, a
  Docker container, or the local 2025 instance). A 2019 `.bak` restores on 2019, 2022 and 2025; never the other way round.
* SQL Server 2019 cannot back up directly to S3 (2022+ can); the tool backs up locally and uploads.
* `--fetch docker:<container>` copies the file out of a container; `--fetch local` when the server runs on this machine;
  `--fetch none` leaves it on the server and prints the path.
* PostgreSQL loads use `COPY … FROM STDIN` without TLS (local/VPC use); Parquet is read by PostgreSQL via extensions, by
  SQL Server 2022+ via `OPENROWSET`, not by 2019.
