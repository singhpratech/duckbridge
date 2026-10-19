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

Input files and outputs accept local paths or `s3://bucket/key`, `az://container/blob`, `gs://bucket/key`.
Cloud clients are built in (SigV4 / Azure Shared Key or SAS), credentials come from the usual environment variables.

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

### Making DuckDB behave like SQL Server

`run` (and `transpile --from tsql`, unless `--no-prelude`) applies a session prelude before your T-SQL:

```sql
SET integer_division = true;                                   -- 7 / 2 = 3, like T-SQL
SET default_null_order = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC';   -- SQL Server NULL ordering
SET TimeZone = 'UTC';
SET default_collation = 'nocase';                              -- SQL Server default collations are case-insensitive
```

Under `nocase`, T-SQL `LIKE` becomes DuckDB `ILIKE` (DuckDB `LIKE` is case-sensitive). Use `--nocase false` for a
binary-collated source. When translating DuckDB SQL **to** T-SQL, `a / b` becomes `CAST(a AS FLOAT) / b` because DuckDB
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
| VARCHAR | NVARCHAR(n) sized from data, or NVARCHAR(MAX) (`--string-mode max`) | key columns capped at NVARCHAR(450) |
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

## Roadmap

DuckBridge is organised so that every engine is a pair: a **target module** (catalog reader, bulk
loader, backup/restore) and a **dialect rule set** for the translator. SQL Server and PostgreSQL are
done; the plan for the rest, in priority order:

| Phase | Engine / feature | What it adds | Status |
|---|---|---|---|
| 1 | SQL Server 2019 / 2022 / 2025 | import, live load, `.bak`, T-SQL ⇄ DuckDB | done |
| 1 | PostgreSQL | export via `COPY`, DuckDB ⇄ PostgreSQL SQL | export done; import next |
| 1 | Parquet | export, any engine can absorb it | done |
| 2 | **DuckDB extension** (`duckbridge`) | `bridge_import`, `bridge_export`, `bridge_run`, `bridge_transpile` straight from DuckDB SQL | done (see below); community-extension submission next |
| 2 | PostgreSQL import | binary `COPY TO STDOUT`, key-range parallel streams, identity → sequence | planned |
| 3 | MySQL / MariaDB | `LOAD DATA LOCAL INFILE`, MySQL dialect rules | planned |
| 3 | Oracle | `SQL*Loader`-style direct path over OCI-free wire protocol, PL/SQL-lite rules | planned |
| 3 | Snowflake / BigQuery / Databricks | Parquet stage + `COPY INTO`, dialect rules | planned |
| 3 | SQLite | direct file read, dialect rules | planned |
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
`parquet_compression`; `bridge_run` takes `from`, `nocase`, `schema_map`, `continue_on_error`).
URL-encode special characters in passwords (`!` → `%21`, `#` → `%23`, `@` → `%40`).

Build and test:

```bash
cd extension && make release        # -> build/duckbridge.duckdb_extension
make test                           # scripts/e2e_extension.sh against the local SQL Server container
```

How it works: the extension opens a pool of 64 connections to the host database when loaded
and runs every operation on those, so parallel workers get their own connections and nothing
touches your transaction. Two consequences worth knowing: `parallel` is capped at 64, and the
database stays open until the process exits, so a `.wal` file may remain next to the database
after a session that used the bridge (DuckDB replays it on the next open; the bridge checkpoints
after each operation to keep it small). The extension is built on DuckDB's C extension API via
`duckdb-rs`, which currently requires the *unstable* struct, so each binary is pinned to one
DuckDB release (`v1.5.6` today, see `extension/Makefile`). CI builds Linux x86_64/arm64,
macOS Intel/ARM and Windows. The community-extension submission
([duckdb/community-extensions#3147](https://github.com/duckdb/community-extensions/pull/3147), built from
[duckbridge-extension](https://github.com/singhpratech/duckbridge-extension)) will make it
`INSTALL duckbridge FROM community`; until it is merged, load the file with `duckdb -unsigned` or
`SET allow_unsigned_extensions = true`.

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
