# Hand-off: work stream D (job sources, strategies, state dir, SWITCH, heap-then-index; R4)

Owner files: `src/mssql/jobs.rs` and `src/mssql/switch.rs`. Every signature in the plan is kept. The
additions are listed below. Unit tests: `cargo test --release jobs switch` (16 pass, plus 1 ignored
live test). `cargo check` and `clippy` report no warnings in these files.

## What is implemented

### `mssql::jobs`

| Item | Notes |
|---|---|
| `SplitSpec`, `parse_split` | `column:<col>`, `files:<glob>` (everything after the first `:`, so `files:C:/x/*.parquet` works), `hash:<a,b>:<n>`, `range:<col>:<n>`, `range:<col>` (n = auto formula), `auto`. Bad specs are errors. |
| `Strategy`, `parse_strategy` | `switch`, `heap-then-index` (also `heap`, `heap_then_index`), `serial`. Has `as_str()` and `Display` for the summary. |
| `parse_table_spec_map(&[String])` | Parses `table=spec`, splitting at the first `=` (so `files:out/t/Mkt=*/` survives). Keys are lowercased. A duplicate or empty key is an error. **Added:** `parse_splits`, `parse_strategies` (map plus parse of each value), `lookup(map, schema, name)` (tries `schema.name`, then `name`, then `*`, case-insensitive), and `unmatched_keys(map, cat)` for typo warnings. |
| `Job` | Fields from the plan, plus **`partition_column: Option<String>`**: the split column (`column:`) or the Hive key (`files:`). `prepare_staging` refuses a job whose split column is not the target's partition column. |
| `JobPlan` | Has `jobs` (largest first, ties broken by id) and `strategy[table index]`. **Added:** `jobs_of(t)` and `pending(&StateDir)`. |
| `plan_jobs(..)` | Skips tables with 0 rows, unless they have a `files:` split. **auto** uses the 0.1.0 rule: a single integer PK or identity column, `rows >= threshold`, `-j > 1`, then `n = clamp(rows/threshold+1, 2, 2j)` ranges. Otherwise it makes one job, `#all`. **column** runs one `GROUP BY` and makes one job per value, largest first. The job filter is `col = '<text>'` for strings and `col = CAST('<text>' AS <duck type>)` otherwise. NULL gives `col IS NULL` with `partition_value = None`. More than 15 000 values is an error. **files** globs in DuckDB. With a trailing `/` it uses `<glob>**/*.parquet` and groups by the matched directory. Without one it makes one job per file. The job SQL is `SELECT <select_expr…> FROM (SELECT CAST(col AS <catalog type>) AS col, … FROM read_parquet([<files>], hive_partitioning = true))`, so columns come out in catalog order with catalog types. `est_rows` is `count(*)` over the files. The Hive `k=v` value is percent-decoded and `__HIVE_DEFAULT_PARTITION__` maps to `None`. **hash** gives `WHERE hash(a, b) % n = i`. **range** gives min/max plus `split::ranges`. |
| Literals | `tsql_literal(text, duck_type)`: numbers are plain, strings `N'..'` with `''` doubling, dates `'YYYY-MM-DD'`, timestamps quoted with at most 7 fractional digits, booleans `1`/`0`. NaN and Inf give `None`. `duck_literal` is the DuckDB counterpart. |
| Job ids | `<schema>.<table>#<part>`, where part is the value, `all`, `r000of006`, `h003of016` or the Hive value. Characters outside `[A-Za-z0-9_.-]` become `_`. When that loses information, or the part is longer than 100 characters, an 8-hex SHA-256 suffix is appended. So `A B` and `A_B` never collide, and ids are the same on every run. |
| Switch value check | With `Strategy::Switch`, `plan_jobs` fails when two values differ only in case or trailing spaces. SQL Server would put both in one partition, so the second SWITCH could never succeed. |
| `StateDir` | `open`, `is_done`, `mark_done` (atomic: tmp file plus rename), `clear(schema, table)`, `done_jobs()`. **Added:** `read_done`, `is_stale(job)` (the marker stores a hash of `source_sql`; `is_stale` is true when it changed, for a warning), and `path()`. The marker is `<dir>/<schema>.<table>/<job id>.done` holding `{rows, seconds, checksum, finished, source}`. |
| `JobQueue` | `new(jobs, max_jobs_per_table)` (`Some(0)` means no limit), `next_for_worker()`, `next_blocking()` (sleeps 100 ms while jobs are blocked by a limit, and returns `None` only when the queue is drained), `finish(&job)` (call it on error too), `cancel()`, `set_table_limit(table, n)` (for example 1 for a `serial` table), `is_drained`, `remaining`, `in_flight`. `Clone` shares the queue. |

### `mssql::switch`

| Item | Notes |
|---|---|
| `staging_name(table, value)` / **`old_name`** | `<table>__stg_<value>` / `<table>__old_<value>`, `[A-Za-z0-9_]` only, at most 128 characters. Lossy sanitising or truncation adds a hash suffix. `value` is the raw value; `literal_value("N'O''B'")` gives `O'B`. |
| `Staging` | `name, schema, partition_number` plus **`partition_literal, ddl, nc_ddl, job`**, and **`table_ref()`** = `[schema].[name]` for the bulk load. |
| `prepare_staging(c, t, job)` | Fails unless the job has a `partition_value` and the target has `partitioning` (`Failure::Check`), and fails when the job's `partition_column` is not the partition column. `adapt_literal` drops the `N` of `N'..'` for `char`/`varchar` partition columns. An implicit conversion in the CHECK would otherwise risk Msg 4972. It reads the partition number with `target::partition_number_sql`. It refuses (`Failure::Guard`) a name without `__stg_`, drops a leftover table with `IF OBJECT_ID(..) IS NOT NULL DROP TABLE`, and then creates the table with `target::staging_ddl_for_partition` (the filegroup comes from the server's number, see handoff-B item 2). It also prepares `target::staging_nonclustered_ddl`. |
| **`finish_staging(c, t, &mut s)`** | Runs after the bulk load and before `switch_in`. It builds the target's nonclustered indexes on the staging table (`target::staging_nonclustered_ddl(t, name, Some(lit), Some(n))`; `SET QUOTED_IDENTIFIER ON` first) and sets `s.nc_built`. That way the load goes into a table with only the clustered key. It is optional, because `switch_in` builds the indexes itself when `nc_built` is false. Note: the real B signatures are `staging_nonclustered_ddl(t, staging, partition_value, partition_number)` and `staging_ddl_for_partition(t, staging, literal, partition_number)`. They differ slightly from the coordinator's summary, and this code uses the real ones. |
| `switch_in(c, t, s, expected, replace)` | 1. Builds the staging NC indexes unless `finish_staging` already did. 2. Staging row count from `sys.dm_db_partition_stats` must equal `expected` (`Failure::Check` names both numbers). 3. Counts the target partition. If it is not empty and `!replace`, it fails with `Failure::Check`. If `replace`, it fails when `__old_<v>` already exists ("drop it with --drop-old first"). Otherwise it creates `__old_<v>` with the same DDL plus NC indexes and **drops its CHECK constraints**: switching out into a table with `CHECK (mkt='BBB')` fails with 4972, which I verified on 2019, because the RANGE RIGHT partition is wider. Then `SET XACT_ABORT ON; BEGIN TRAN; SWITCH PARTITION n TO old; SWITCH stg TO target PARTITION n; COMMIT`, so readers never see an empty partition. 4. The SWITCH. On error: `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION`, plus context with the server error number, a hint (4972 CHECK/type/collation, 4904 not empty, 4947 indexes, 4939 filegroup), the target's partition range, filegroup, clustered key and compression, the NC index names and the full staging DDL. In `--plan` mode it only records the statements and skips the counts. |
| `drop_staging(c, s)` | Requires `__stg_` or `__old_` in the name (`Failure::Guard` otherwise). |
| `drop_old_tables(c, schema, table)` | Lists `sys.tables` for `<table>__old_%` (LIKE-escaped) and drops each table. Returns the names. |
| `capture_and_drop_indexes(c, t)` | Captures `target::index_ddl(t, "[schema].[table]")` **before** dropping. Each statement goes to the run log as a NOTE, so a killed run can be finished by hand. If the table has indexes but no DDL could be generated, it drops nothing. Then it runs `drop_index_ddl` and returns the DDL. |
| `recreate_indexes(c, ddl, job)` | `SET QUOTED_IDENTIFIER ON`, then each statement in order (clustered first). |

All DDL and DML goes through `exec_batch_as(c, "target", job_id, ..)`. Metadata reads use
`simple_query`, so they also run under `--plan`.

## How the integrator drives a job

```rust
let plan = jobs::plan_jobs(&con, &cat, &splits, &strategies, default, o.split_threshold, o.parallel)?;
let state = jobs::StateDir::open(state_dir)?;
for j in &plan.jobs { if state.is_stale(j) { runlog::note(&format!("{}: done marker is from different input", j.id)); } }
let queue = jobs::JobQueue::new(plan.pending(&state), o.max_jobs_per_table);
// optional: for (i, s) in plan.strategy.iter().enumerate() { if *s == Strategy::Serial { queue.set_table_limit(i, 1) } }

// heap-then-index tables, once before the workers start (one connection):
let ddl = switch::capture_and_drop_indexes(&mut c, &target_table).await?;   // persist `ddl` (state dir) too

// each worker:
while let Some(job) = queue.next_blocking() {
    let t0 = Instant::now();
    let r = async {
        match plan.strategy[job.table] {
            Strategy::Switch => {
                let mut s = switch::prepare_staging(&mut client, &tt, &job).await?;
                let rows = bulk::bulk_load(&con, &mut client, &s.table_ref(), &cols, &convs, &job.source_sql,
                                           &BulkOptions { table_lock: true, .. }, &progress).await?;
                switch::finish_staging(&mut client, &tt, &mut s).await?;          // NC indexes, after the load
                switch::switch_in(&mut client, &tt, &s, rows /* or job.est_rows for column: jobs */, o.replace_partitions).await?;
                switch::drop_staging(&mut client, &s).await?;
                Ok(rows)
            }
            _ => bulk::bulk_load(&con, &mut client, &table_ref, &cols, &convs, &job.source_sql, &opts, &progress).await,
        }
    }.await;
    queue.finish(&job);
    match r { Ok(rows) => state.mark_done(&job, rows, t0.elapsed().as_secs_f64(), None)?, Err(e) => { queue.cancel(); return Err(e) } }
}

// after all jobs of a heap-then-index table: switch::recreate_indexes(&mut c, &ddl, &format!("{}#indexes", tt.full_name())).await?;
// --drop-old: switch::drop_old_tables(&mut c, schema, table).await?;
```

For the `expected_rows` of `switch_in`: pass the number of rows the bulk load **sent**. That number
is exact. `job.est_rows` is exact for `column:` and `files:` jobs but only an estimate for `hash:` and
`range:` jobs. Never switch with `expected_rows = job.est_rows` for those. They have no partition
value anyway.

Rerun behaviour: done jobs are filtered out by `plan.pending(&state)`. A job killed during the bulk
load leaves `<table>__stg_<v>`. `prepare_staging` drops it on the rerun. A job killed after the SWITCH
but before `mark_done` leaves a full partition. The rerun then fails with "partition N already holds
X rows; pass --replace-partitions". That is safe: no duplicates, and the operator decides. The
integrator could instead treat "partition count == job rows and no staging left" as done. I did not
build that, because it cannot tell a finished job from different data.

## e2e cases for `scripts/e2e_large_load.sh` (2019 container, disposable database `_e2e_large`)

Setup:

```sql
CREATE PARTITION FUNCTION pf_mkt (char(3)) AS RANGE RIGHT FOR VALUES ('M01','M02',…,'M25');
CREATE PARTITION SCHEME ps_mkt AS PARTITION pf_mkt ALL TO ([PRIMARY]);
CREATE TABLE dbo.kpi (mkt char(3) NOT NULL, id bigint NOT NULL, v varchar(20), amount decimal(18,4),
  CONSTRAINT pk_kpi PRIMARY KEY CLUSTERED (mkt, id) WITH (DATA_COMPRESSION = PAGE)) ON ps_mkt(mkt);
CREATE INDEX ix_kpi_v ON dbo.kpi(v) WHERE v IS NOT NULL WITH (DATA_COMPRESSION = ROW) ON ps_mkt(mkt);
```

DuckDB: `CREATE TABLE kpi AS SELECT printf('M%02d', 1 + i % 25) AS mkt, i AS id, 'v' || i AS v, (i % 1000) / 7.0 AS amount FROM range(10000000) t(i)`.
Optionally `COPY kpi TO 'out/kpi' (FORMAT PARQUET, PARTITION_BY (mkt))` for the `files:` variant.

1. **Switch, column split**: `export … --into-existing --split kpi=column:mkt --strategy kpi=switch -j 8 --state-dir st`.
   Expect 25 done markers, `SELECT COUNT(*) FROM dbo.kpi` = 10 000 000, every partition 2..26
   holding 400 000 rows (`sys.dm_db_partition_stats`), no `kpi__stg_%` tables left, and
   `ix_kpi_v` still present.
2. **Switch, files split**: the same with `--split kpi=files:out/kpi/mkt=*/` into a truncated target.
   Same expectations.
3. **Kill and rerun**: start case 1 on a truncated target with `--state-dir st2`. `kill -9` it once
   about 8 markers exist. Count markers (K) and staging tables (usually 1–8 left). Rerun the same
   command. Expect exit 0, 25 markers, total = 10 000 000, no duplicates
   (`SELECT mkt, id FROM dbo.kpi GROUP BY mkt, id HAVING COUNT(*) > 1` is empty, and the PK enforces it
   anyway), no `__stg_` tables, and the log shows only 25 − K bulk loads. If the kill landed between
   SWITCH and marker, expect exit 1 with "already holds"; rerun with `--replace-partitions`, expect
   exit 0, then `--drop-old` removes the `kpi__old_*` table.
4. **Rerun without changes**: a third run with the same state dir loads nothing (0 jobs) and exits 0.
5. **Partition not empty**: delete one marker and rerun. Expect exit 1 and "partition N … already
   holds 400000 rows". With `--replace-partitions`: exit 0, `kpi__old_<v>` holds 400 000 rows, and
   `--drop-old` drops it.
6. **NOLOCK reader**: during case 1, loop `SELECT COUNT(*) FROM dbo.kpi WITH (NOLOCK)` in sqlcmd.
   Every value must be a multiple of 400 000 (staging is invisible until the SWITCH).
7. **Strategy errors**: `--strategy kpi=switch` with `--split kpi=hash:id:8` gives exit 1 ("needs
   a partition value"). A switch into a non-partitioned table gives exit 1.
8. **heap-then-index**: a non-partitioned copy `dbo.kpi2` with the same PK and NC index,
   `--split kpi2=hash:mkt,id:8 --strategy kpi2=heap-then-index -j 8`. Expect 10 000 000 rows, and
   PK plus NC index recreated with the same compression and filter (`sys.indexes`,
   `sys.partitions.data_compression_desc`).
9. **`--plan`** of case 1: the plan lists the 25 × (`IF OBJECT_ID … DROP`, `CREATE TABLE …__stg_…`,
   `CREATE INDEX`, `ALTER TABLE … SWITCH`, `DROP`) and creates nothing on the server.

A smaller version of 1, 3 (leftover staging), 5 and 8 already ran on the 2019 container through
`switch::tests::switch_live`
(`DUCKBRIDGE_TEST_SWITCH='localhost:14319:sa:<pw>:_agent_d' cargo test --release switch_live -- --ignored`;
it needs the `dbo.t` from the task description and works with an extra filtered NC index too).

## Open issues

1. **SWITCH after the job's SWITCH but before `mark_done`** (see "Rerun behaviour") ends as an
   explicit error, not an automatic skip.
2. **NULL partition keys** cannot use `switch` (no partition value). Such a table needs
   `heap-then-index`/`serial`, or the integrator falls back for the NULL job only.
3. **Case-insensitive file systems** (Windows, macOS) with `serial`/`hash` jobs whose values differ
   only in case: ids differ only in case, so their markers would share a file. `switch` tables are
   protected by the value check. A fix would add a hash suffix when a value has lowercase letters.
   I left it out to keep the names readable.
4. `files:` with a directory glob needs files ending in `.parquet`. Several Hive levels
   (`Mkt=*/Year=*/`) group by the deepest matched level. The partition value comes from the last
   `k=v` of that directory.
5. Timestamp-with-time-zone partition values are passed through as DuckDB text (`+00` offsets).
   A `datetimeoffset` partition column may need `+00:00`. This was not tested.
6. `switch_in` does not set `LOCK_TIMEOUT`. The SWITCH needs a short SCH-M lock on the target, and
   long readers block it. The integrator may want `SET LOCK_TIMEOUT` on the worker sessions with a
   retry.
7. `capture_and_drop_indexes` returns the DDL in memory and in the run log only. The integrator
   should write it into the state dir (for example `<dir>/<schema>.<table>/indexes.sql`) so a killed
   heap-then-index run can recreate the indexes on rerun.
