# Hand-off: work stream B (target metadata + pre-flight, R2 / R3 checks)

Owner file: `src/mssql/target.rs`. The shared types are unchanged; every function in the plan is
implemented and has the planned signature. Nothing in the module sends DDL. The DDL helpers only
build statement text, and callers send it through `mssql::exec_batch`.

## What is implemented

| Function | Notes |
|---|---|
| `read_target_tables(c, names)` | About 8 catalog queries per chunk of 500 tables, filtered to the object ids: name resolution (`OBJECT_ID` + `sys.objects`, `modify_date` as ISO 8601), `sys.columns`+`sys.types`, `sys.indexes`+`sys.data_spaces`+`sys.partitions` (compression of partition 1), `sys.index_columns`, partition scheme/function, `sys.partition_range_values`, `sys.destination_data_spaces`, `sys.check_constraints`, row counts. A missing table returns `Failure::Check("target table(s) not found: dbo.x, …")` (exit 1). |
| `partition_row_counts(c, t)` | `sys.dm_db_partition_stats`, index 0/1, grouped by partition. Falls back to `sys.partitions.rows` when the login lacks VIEW DATABASE STATE. Uses `OBJECT_ID(name)`, so it still works after a swap. |
| `map_columns(duck, target, ignore_extra)` | Matches names case-insensitively and returns columns in target `column_id` order. Fails with `Failure::Check` containing `EXTRA_COLUMN: …` / `MISSING_COLUMN: …`. Computed, `GENERATED ALWAYS` and `rowversion` target columns are always left out; a DuckDB column with that name is skipped with a note. Nullable, defaulted and identity columns with no DuckDB column are left out with a note. A mapped identity column gets a note "needs KEEPIDENTITY". |
| `preflight(con, duck, m, o)` | Runs **one** DuckDB aggregate query per table (`count(*) FILTER (WHERE <violation>)` plus a statistic per check). Then it runs one small `LIMIT o.samples` query per blocking finding for samples, an optional PK GROUP BY, and an optional CI GROUP BY per CI unique key. |
| `preflight_sql(duck, m, o) -> String` | **New**, pure. It returns the aggregate query (tests, `--plan`). |
| `is_blocking(f) -> bool` | **New**. It is false for `DATETIME_PRECISION` and for any message that starts with `warning:`. |
| `index_ddl(t, on_table)` | Builds PK / UNIQUE constraints as `ALTER TABLE … ADD CONSTRAINT`. Other indexes become `CREATE [UNIQUE] CLUSTERED/NONCLUSTERED INDEX … INCLUDE … WHERE … WITH (DATA_COMPRESSION, FILLFACTOR) ON [scheme]([col]) / ON [fg]`. Clustered (and clustered columnstore) indexes come first, then the rest in index_id order. HEAP is skipped, and FILLFACTOR is left out when it is 0. Columnstore indexes are rendered too. |
| `drop_index_ddl(t)` | Order: nonclustered indexes (`DROP INDEX`), then nonclustered unique constraints, then the clustered non-PK index, then the PK last (`ALTER TABLE … DROP CONSTRAINT`). |
| `staging_ddl(t, staging, partition_value)` | Returns `CREATE TABLE` (same columns, types, collations and NULLability; identity becomes a plain column; a heap keeps its `DATA_COMPRESSION`), the clustered PK / index with the same key order, compression and fill factor, copies of every target CHECK constraint, and `CHECK ([col] = <literal>[ AND [col] IS NOT NULL])`. Everything is `ON` the partition's filegroup. The first statement starts with the comment `-- partition N of dbo.t: SELECT $PARTITION.[pf](<literal>)`, where N is `?` when it was not resolved locally. |
| `partition_number_sql(t, literal)` | **New**. It returns `SELECT $PARTITION.[pf](<literal>)`, or `None` when the table is not partitioned. |
| `staging_ddl_for_partition(t, staging, literal, partition_number)` | **New**. Same as `staging_ddl`, with a partition number the caller got from the server. |
| `staging_nonclustered_ddl(t, staging, partition_value, partition_number)` | **New**. The nonclustered indexes and NC PK / UNIQUE constraints of the target, rebuilt on the staging table on the same filegroup. |
| `render_type(c)` | **New** (pub). It returns the SQL Server type text from metadata, for example `varchar(10)`, `nvarchar(max)`, `decimal(18,2)`, `datetime2(7)` or `rowversion`. |

### Pre-flight checks (codes)

| Target | Source | Check |
|---|---|---|
| any NOT NULL | nullable DuckDB column | `NOT_NULL`: `col IS NULL`. |
| char/varchar(n) | anything but BLOB | `LENGTH`: `length(x) > n`. For a `_UTF8` collation it is `strlen(x) > n`, because varchar counts bytes there. The message is `max length 4 > 3 of char(3) (1 values)`. |
| char/varchar/text, non-UTF-8 | strings | `CODEPAGE`. For code page 1252 collations (`Latin1_General_*`, `SQL_*_CP1_*`, French, German_PhoneBook, Finnish_Swedish, Danish_Norwegian, Icelandic, Spanish), a value fails only when it has a character outside Windows-1252 (an RE2 class), so `café` passes. Any other non-UTF-8 collation fails on any non-ASCII value (`strlen <> length`). |
| nchar/nvarchar(n) | anything but BLOB | `LENGTH` in exact UTF-16 code units. A supplementary character counts 2; the regex runs only on non-ASCII rows. |
| tinyint/smallint/int/bigint | ints, decimals, floats, strings | `RANGE` (skipped when the source type's range fits). `SCALE` for a fractional part unless `allow_rescale`. For a string source, `TYPE` when the value is not an integer (DuckDB would round `'1.5'`, SQL Server rejects it). |
| bit | int/decimal/float, string | `BIT`: values other than 0/1; for strings, other than 0/1/true/false. |
| decimal/numeric(p,s), money, smallmoney | ints, decimals, floats, strings | `SCALE`: source scale > s, reported even when no row loses digits, unless `allow_rescale`. `PRECISION`: \|v\| ≥ 10^(p-s), compared exactly against a DECIMAL literal and after rounding when rescaling. Money also gets an exact `RANGE` check. For strings, `TYPE` (`TRY_CAST … AS DECIMAL(p,s)`). |
| float/real | double, string | `RANGE`: NaN/Infinity, or beyond the real range for `real`. For strings, `TYPE`. |
| date/datetime2/datetimeoffset/datetime/smalldatetime | date, timestamp*, string | `DATE_RANGE` (datetime 1753-01-01..9999-12-31 23:59:59.997, smalldatetime 1900-01-01..2079-06-06). The **warning** `DATETIME_PRECISION` covers datetime 1/300 s rounding, datetime2(n<6/7) digits, smalldatetime seconds, and a time part dropped by `date`. |
| time(n) | time, string | `DATETIME_PRECISION` warning when n < 6. For strings, `TYPE`. |
| binary/varbinary(n) | blob, string | `LENGTH` in bytes. |
| uniqueidentifier | uuid, string, blob | `TYPE` for a bad UUID string or a blob that is not 16 bytes. |
| (any) | clearly incompatible pair (BLOB→int, DATE→int, TIME→date, LIST→decimal, …) | `TYPE`, decided from the types alone without a query. |
| PK (`check_pk != None`) | – | `PK_DUP`: `GROUP BY` the PK, with `rtrim(x, ' ')` on string keys because SQL Server ignores trailing spaces, then `HAVING count(*) > 1 LIMIT samples`. The sample is `(k1, k2, …) xN`. |
| PK / unique index with a `_CI` string key (`collation_check`) | – | `CI_COLLISION`: `GROUP BY lower(rtrim(key))` (plus `strip_accents` for `_AI`) `HAVING count(DISTINCT raw) > 1`. The samples list the colliding raw values, for example `('AAA', '2', 'y') / ('aaa', '2', 'Y')`. |
| unmapped columns | – | `EXTRA_COLUMN` / `MISSING_COLUMN` as **warnings** (the mapping already failed or accepted them). |

Samples show the target PK values `(k1, k2, …)` when every PK column is loaded. Otherwise they show
the offending value, cut to 60 characters. Typed checks read the native DuckDB column. String
checks read `select_expr`, or the quoted column when `select_expr` is empty, because that is the
text that will be sent.

## Deviations from the plan / things the integrator must know

1. **SWITCH also needs the nonclustered indexes and the CHECK constraints.** I checked this on the
   2019 container. Without the NC indexes it fails with Msg 4947 ("There is no identical index in
   source table"). Without a copy of a target CHECK constraint it fails with Msg 4971. Without
   `IS NOT NULL` on a nullable partition column it fails with Msg 4972. `staging_ddl` copies the
   CHECK constraints and adds `IS NOT NULL`. NC indexes come from the new
   `staging_nonclustered_ddl`; stream D runs it after the bulk load and before `SWITCH`. The
   identity column as a plain column and the equality CHECK `([mkt] = 'AAA')` were both accepted
   by SWITCH.
2. **Filegroup of a partition.** `staging_ddl(t, s, Some(lit))` works out the partition locally for
   numeric and ISO-date boundaries, and also when every partition is on the same filegroup
   (`ALL TO ([PRIMARY])`). Otherwise, for example with string boundaries on different filegroups,
   it returns an error. Stream D should then run `partition_number_sql`, read the number, and call
   `staging_ddl_for_partition`. Each staging table must hold exactly one partition value.
3. **Constraint names are schema-scoped.** The staging PK, UNIQUE and CHECK constraints are named
   `PK_<staging>`, `UQ_<staging>_<index_id>`, `CK_<staging>_<n>` and `CK_<staging>_partition`
   (cut to 128 characters). `index_ddl(t, other_table)` keeps the **original** constraint names,
   which is right after `drop_index_ddl` on the same table. On a different table it collides while
   the original still exists.
4. **Columnstore.** `TargetTable::clustered()` / `is_heap()` (contract code, unchanged) only match
   `type_desc = "CLUSTERED"`, so a clustered columnstore table counts as a heap there. The B
   functions handle `CLUSTERED COLUMNSTORE` themselves. XML and spatial indexes are not read; a PK
   with an XML index on top cannot be dropped by `drop_index_ddl` and fails loudly.
5. **Partition-only index columns are left out.** When the partition column is not a key column,
   SQL Server lists it in `sys.index_columns` with `key_ordinal = 0` and `is_included_column = 0`.
   It is **not** in `TargetIndex.columns`, so the DDL does not add it to the key. The partition
   column is in `TargetPartitioning.column`. Columnstore columns are listed (they are flagged as
   included).
6. **Compression is captured per index from partition 1 only**, as the type says. Per-partition
   differences are not reproduced.
7. **Computed / `GENERATED ALWAYS` columns** are folded into `TargetColumn.is_computed`, and
   `staging_ddl` refuses such tables because the type has no definition field. `rowversion` is
   kept as a `rowversion` column in staging and omitted by `map_columns`.
8. **Filtered indexes need `QUOTED_IDENTIFIER ON`** in the session that runs `index_ddl` /
   `staging_nonclustered_ddl`. tiberius sessions already have it (checked: the filtered index was
   recreated through `exec_batch`). `sqlcmd` needs `-I`, or it fails with Msg 1934.
9. `PkCheck::Hash` runs the same query as `Full`, and DuckDB's hash aggregate spills under its
   memory limit. Bounded per-job checking means calling `preflight` per job (with a `Table` that
   points at a per-job view). The caller does that.
10. The CI check fires only for keys with a `_CI` collation, as planned. Trailing-space-only
    duplicates under CS/BIN collations are caught by `PK_DUP`, since it rtrims string keys, but
    only when `--check-pk` is given.
11. `CODEPAGE` only knows code page 1252 exactly. For other non-UTF-8 collations it is
    conservative: any non-ASCII value fails.

## SQL-Server-backed cases for `scripts/e2e_large_load.sh`

Run the fixtures in a disposable database (`_e2e_b` below) with `sqlcmd -I` (QUOTED_IDENTIFIER ON).

```sql
CREATE PARTITION FUNCTION pf (char(3)) AS RANGE RIGHT FOR VALUES ('AAA','BBB');
CREATE PARTITION SCHEME ps AS PARTITION pf ALL TO ([PRIMARY]);
CREATE TABLE dbo.sales (
  mkt char(3) NOT NULL,                                          -- db default collation SQL_Latin1_General_CP1_CI_AS
  id int NOT NULL,
  code varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  amount decimal(18,2) NULL,
  note nvarchar(max) NULL,
  ts datetime NULL,
  rid int IDENTITY(1,1) NOT NULL,
  dflt int NOT NULL CONSTRAINT df_sales_dflt DEFAULT 0,
  CONSTRAINT PK_sales PRIMARY KEY CLUSTERED (mkt, id DESC, code) WITH (DATA_COMPRESSION = PAGE, FILLFACTOR = 90) ON ps(mkt),
  CONSTRAINT CK_sales_amount CHECK (amount >= 0)
) ON ps(mkt);
CREATE NONCLUSTERED INDEX ix_sales_code ON dbo.sales (code) INCLUDE (amount) WHERE amount IS NOT NULL WITH (DATA_COMPRESSION = ROW) ON ps(mkt);
ALTER TABLE dbo.sales ADD CONSTRAINT UQ_sales_rid UNIQUE NONCLUSTERED (rid, mkt) ON ps(mkt);
CREATE PARTITION FUNCTION pfi (int) AS RANGE LEFT FOR VALUES (10, 20);
CREATE PARTITION SCHEME psi AS PARTITION pfi ALL TO ([PRIMARY]);
CREATE TABLE dbo.h (k int NULL, v varchar(5) NULL) ON psi(k);
```

Expected results:

1. **Metadata** (`read_target_tables` for `dbo.sales`, `dbo.h`). `dbo.sales` has 8 columns. `rid`
   is identity and `dflt` has_default. The PK `PK_sales` is CLUSTERED with columns
   `mkt ASC, id DESC, code ASC`, PAGE compression, fill factor 90, data space `ps`, partitioned.
   `ix_sales_code` has 2 columns: `code` (key) and `amount` (included). The partition-only `mkt` is
   not listed. Its filter is `([amount] IS NOT NULL)` and its compression ROW. Partitioning is
   `ps`/`pf`, column `mkt`, range_right, boundaries `['AAA', 'BBB']`, 3 filegroups `PRIMARY`. Checks
   are `CK_sales_amount` with `([amount]>=(0))`. `dbo.h` has a HEAP index with `pfi`, range_right
   false, boundaries `[10, 20]`. `read_target_tables` for `dbo.nope` fails with exit 1 and names
   `dbo.nope`.
2. **No DDL on the target.** Record `sys.objects.modify_date` for `dbo.sales`, run
   `export --into-existing --check-only`, and check that the date is unchanged.
3. **LENGTH acceptance.** DuckDB `sales(MKT VARCHAR, id INT, code VARCHAR, amount DECIMAL(18,2), note VARCHAR, ts TIMESTAMP)`
   with the row `('ABCD', 2, 'café', 2, NULL, NULL)`. Expect a failure with exit 1 before any bulk
   session: `LENGTH dbo.sales.mkt: max length 4 > 3 of char(3) (1 values) (samples: (ABCD, 2, café))`.
   Expect no CODEPAGE finding, because `é` is in 1252.
4. **CODEPAGE.** The row `('BBB', 3, 'z中', …)` gives
   `CODEPAGE dbo.sales.code: 1 values have characters outside code page 1252 …`, with sample
   `(BBB, 3, z中)`.
5. **NOT_NULL.** A NULL `MKT` gives `NOT_NULL dbo.sales.mkt`.
6. **PK_DUP** (`--check-pk`). The rows `('AAA',1,'x')` and `('AAA',1,'x ')` give a `PK_DUP` sample
   starting `(AAA, 1, x` and ending ` x2`. **CI_COLLISION**: the rows `('AAA',2,'y')` and
   `('aaa',2,'Y')` give a `CI_COLLISION` that names `PK_sales`, with sample
   `('AAA', '2', 'y') / ('aaa', '2', 'Y')`. Also load the same data without the checks and confirm
   SQL Server rejects it with a PK violation, which shows the checks are needed.
7. **DATETIME_PRECISION** is a warning only. `ts = '2020-01-01 00:00:00.001'` gives
   `warning: 1 values are rounded to datetime's 1/300 s …`, and the load still runs.
8. **SWITCH round trip** (stream D path). Generate `staging_ddl(sales, "sales__stg_AAA", Some("'AAA'"))`
   and run it. Bulk-load the rows for 'AAA', run `staging_nonclustered_ddl(...)`, then run
   `ALTER TABLE dbo.sales__stg_AAA SWITCH TO dbo.sales PARTITION 2`. It must succeed;
   `SELECT $PARTITION.pf('AAA')` = 2. For `dbo.h` with `Some("15")`, the CHECK is
   `([k] = 15 AND [k] IS NOT NULL)` and `SWITCH TO dbo.h PARTITION 2` must succeed.
9. **Heap-then-index.** Run `drop_index_ddl(sales)`, which drops ix_sales_code, then UQ_sales_rid,
   then PK_sales, and then `index_ddl(sales, "[dbo].[sales]")`. Afterwards, `sys.indexes` and
   `sys.partitions.data_compression_desc` must match the snapshot taken before (stream G's
   `verify_indexes`).

The ignored unit test `target_read_live_metadata` already covers case 1 against a live server:
`DUCKBRIDGE_TEST_MSSQL='localhost,14319,sa,<pw>,<db>' cargo test --release --lib target_read_live_metadata -- --ignored --nocapture`.
The database needs the fixtures above plus
`CREATE TABLE dbo.cs (a int NOT NULL, b varchar(10) NULL, c date NULL) ON psi(a); CREATE CLUSTERED COLUMNSTORE INDEX cci_cs ON dbo.cs ON psi(a);`.
With `DUCKBRIDGE_TEST_MSSQL_WRITE=1` (disposable database only) it also runs parts of cases 8 and 9
through `exec_batch`. It truncates partition 3 of `dbo.sales`, runs `staging_ddl` for `'BBB'`,
inserts a row, runs `staging_nonclustered_ddl`, checks that `$PARTITION` = 3, runs `SWITCH`, and
checks that `partition_row_counts` gives `(3, 1)`. Then it runs `drop_index_ddl` and `index_ddl`
and compares the index shape before and after. It passed against the 2019 container.
