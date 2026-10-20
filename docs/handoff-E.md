# Hand-off: stream E (safe reads, R6)

Files: `src/mssql/fingerprint.rs` (new module), `src/mssql/import.rs` (catalog read and copy loop). No other
files were touched. Unit tests: `cargo test --release --lib -- fingerprint import` (11 tests). Live checks
were run against the 2019 container on a scratch database (see "Verified live" below).

## What is implemented

`mssql::fingerprint`:

- `ReadSafety` (with `Default`), `Isolation { ReadCommitted, ReadUncommitted, Snapshot }` with `parse` / `name` /
  `sql`, `OnLock { Fail, Skip }` with `parse`.
- `apply_session_settings(c, o)`: `SET LOCK_TIMEOUT n; SET DEADLOCK_PRIORITY LOW; SET TRANSACTION ISOLATION LEVEL ..`
  through `exec_batch_as(c, "source", "", ..)`. With `Snapshot`, it uses SNAPSHOT when ALLOW_SNAPSHOT_ISOLATION is on,
  READ COMMITTED when only READ_COMMITTED_SNAPSHOT is on (statement-level versioning, noted in the log), and fails with
  `Failure::Check` when neither is on.
- `recommend_isolation(c, current) -> Option<String>`: the spec's advice when the database has RCSI or snapshot
  isolation on and `current` is not `Snapshot`.
- `held_locks(c, schema, table) -> Vec<LockInfo>`: granted X / IX / SIX / Sch-M locks of other sessions on the object
  id or its hobt_ids (`HELD_LOCKS_SQL`), with login / host / program from `sys.dm_exec_sessions`. U locks are left out
  because they do not block readers.
- `wait_for_unlocked(c, schema, table, o) -> LockOutcome { Clear, Skipped(locks) }`: polls every 2 s up to `lock_wait_s`.
  Then `Fail` returns `Failure::Check` naming the sessions ("session 59 (sa@host, SQLCMD) holds IX on OBJECT, X on KEY"),
  and `Skip` returns `Skipped`. Under `Snapshot` / `ReadUncommitted` only Sch-M counts as blocking (`blocks_reader`),
  because X locks do not block those readers.
- `Fingerprint { rows, last_user_update, modify_date }` with `PartialEq` and `Display`
  (`rows=.. last_update=..|none modified=..`), and `take(c, schema, table)`. Without VIEW SERVER STATE it falls back to
  `sys.partitions` rows and no `last_user_update`, and logs a note.
- `ManifestRow`, `write_manifest(path, rows)`: a CSV file and a JSON copy (`x.csv` -> `x.json`, any other name -> `<path>.json`).
- `wait_for(c, sql, timeout)`: polls every 10 s until the first column is true (bit, non-zero number, or 'true'/'1'/'yes').
  It logs every poll with `runlog::note` and returns `Failure::Check` on timeout. A lock timeout (1222) counts as "not
  yet"; any other error stops the run.
- `readonly_config(target, db)`: the normal config plus `Config::readonly(true)`. tiberius 0.13 has this setter, so
  `readonly_supported()` returns `true`. `connect(target, db, o)` uses it when `readonly_intent` is set, follows one AG
  routing redirect (`Error::Routing`), and applies the session settings.
- `SourceClient(MsClient)`: exposes only `query` / `simple_query`, plus the read helpers (`take`, `held_locks`,
  `wait_for_unlocked`, `wait_for`, `recommend_isolation`). `query` and `simple_query` call `assert_read_only`, which
  refuses any statement that contains a write keyword (INSERT, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, TRUNCATE, EXEC,
  INTO, GRANT, ...) outside literals, bracketed or quoted identifiers and comments, with `Failure::Guard` (exit 3).
  `SourceClient::connect` / `SourceClient::wrap` apply the settings before wrapping.

`mssql::import`:

- `read_catalog_with(c, o, &safety, warnings)` applies the session settings to the catalog connection, waits for the
  `wait_for` gate when it is set, and pushes the `recommend_isolation` advice into `warnings`. `read_catalog(c, o, w)`
  keeps its signature and calls it with `ReadSafety::default()`.
- `copy_data_with(con, target, tables, o, &safety, progress)`: each worker and the job-planning connection is a
  `SourceClient` (settings applied). `copy_data(..)` keeps its signature and uses `ReadSafety::default()`. Per-table
  bookkeeping lives in `Shared.tables: Mutex<HashMap<usize, TableState { phase, pending_jobs, fingerprint_before,
  started, rereads, rows }>>`:
  - The first job of a table that a worker picks moves the table to `Starting`. That worker runs `wait_for_unlocked` (when
    `lock_check` is set) and `take`, then sets the table to `Running`. Other workers skip that table's jobs while it is
    starting.
  - With `Skipped`, the table's remaining jobs are removed from the queue. A warning is added, and the manifest row gets
    `rows_read = -1`.
  - The worker that finishes a table's last job takes the "after" fingerprint. If it is equal, the table is done. If it
    differs, the worker runs `DELETE FROM` on the DuckDB table and puts every job of that table back in the queue. The next
    pass starts from `Pending`, so it gets a new lock check and a new "before" fingerprint. This repeats up to
    `max_rereads` times; after that the result is `Failure::Check`.
  - Workers exit only when the queue is empty and every table is `Done` or `Skipped`, because a re-read can queue jobs
    again. Any worker error or panic sets `abort` so the other workers stop.
  - If the lock check itself cannot run (for example, no VIEW SERVER STATE), the table is read anyway, the lock timeout
    still applies, and one warning is added.
- Empty tables now get a single job too. They are fingerprinted and listed in the manifest (this costs one empty SELECT).
- `ImportReport.manifest: Vec<ManifestRow>` (sorted by table). It is written to `safety.manifest` when that is set. There
  is one `runlog::summary_add` per table.
- A lock timeout (1222) during a data read gets an extra hint in its error: raise `--lock-timeout` or use
  `--isolation snapshot`.
- `nolock`: the `WITH (NOLOCK)` hint is unchanged. `effective_safety(o, s)` also makes the isolation `ReadUncommitted`
  whenever `o.nolock` is set, which affects the session settings, the lock-mode filter and the manifest.

## Deviation from the plan: `ImportOptions.safety`

`ImportOptions` does **not** gain a `safety` field yet. `api.rs` and `main.rs` build it with struct literals, and I may
not edit those files, so adding a field would have broken the shared tree for every stream. The safety settings go in as
an explicit argument instead:

```rust
use duckbridge::mssql::fingerprint::{self, ReadSafety, Isolation, OnLock};
use duckbridge::mssql::import::{read_catalog_with, copy_data_with};
let mut c = fingerprint::connect(&target, &database, &safety).await?;      // read-only intent + settings
read_catalog_with(&mut c, &opts, &safety, &mut warnings).await?;           // gate + advice
let rep = copy_data_with(&con, &target, &tables, &opts, &safety, progress)?; // rep.manifest
```

If you prefer the field, add `pub safety: ReadSafety` to `ImportOptions`, set `safety: Default::default()` in the two
literals, and replace `read_catalog`/`copy_data` bodies with `read_catalog_with(c, o, &o.safety, ..)` and
`copy_data_with(.., &o.safety, ..)`. That is a 4-line change in `import.rs`.

## Integrator wiring (stream H)

`ReadSafety` fields -> CLI flags (`ImportArgs`) / `ImportRequest` fields -> extension `bridge_import` named parameters:

| `ReadSafety` field | CLI flag | default | extension parameter |
|---|---|---|---|
| `isolation: Isolation` | `--isolation <read-committed\|read-uncommitted\|snapshot>` -> `Isolation::parse` | read-committed | `isolation VARCHAR` |
| `lock_timeout_ms: i64` | `--lock-timeout <ms>` (-1 = wait forever) | 1000 | `lock_timeout BIGINT` |
| `lock_check: bool` | `--lock-check` / `--no-lock-check` | true | `lock_check BOOLEAN` |
| `lock_wait_s: u64` | `--lock-wait <s>` | 30 | `lock_wait BIGINT` |
| `on_lock: OnLock` | `--on-lock <fail\|skip>` -> `OnLock::parse` | fail | `on_lock VARCHAR` |
| `max_rereads: u32` | `--max-rereads <n>` | 2 | `max_rereads BIGINT` |
| `manifest: Option<PathBuf>` | `--manifest <file>` | none | `manifest VARCHAR` |
| `readonly_intent: bool` | `--readonly` | false | `readonly BOOLEAN` |
| `wait_for: Option<String>` | `--wait-for <sql>` | none | `wait_for VARCHAR` |
| `wait_timeout_s: u64` | `--wait-timeout <s>` | 3600 | `wait_timeout BIGINT` |

- Keep `--nolock`. Map it to `Isolation::ReadUncommitted` when `--isolation` is not given; `effective_safety` does that
  anyway. The `nolock` field stays in `ImportOptions` for the hint.
- `main.rs` import: replace `target.connect(&a.database)` for the catalog with `fingerprint::connect(&target, &a.database,
  &safety)` so `--readonly` covers the catalog connection too (the workers already use it). Use `read_catalog_with` and
  `copy_data_with`, and print `rep.manifest` or the manifest path at the end.
- `api.rs` `import_mssql`: same calls. A manifest row could also become a result `Row` (rows_read, rereads).
- Exit codes: lock failure, snapshot not available, fingerprint still changing and gate timeout are all
  `Failure::Check` (exit 1). A write statement on the source is `Failure::Guard` (exit 3).
- `--plan`: `apply_session_settings` uses `exec_batch_as`, so in plan mode the SETs are recorded and not sent. The
  metadata reads still run.

## E2E cases for `scripts/e2e_large_load.sh`

Use a disposable database (here `_agent_e`). The fixture is `dbo.t (id INT IDENTITY PRIMARY KEY, v NVARCHAR(50))` with
200 000 rows, plus `dbo.small (k INT PRIMARY KEY, s VARCHAR(10))` with 2 rows. Run with `--split-threshold 50000 -j 4` so
`dbo.t` is read by 5 range jobs on several workers.

1. **Lock holder fails after `--lock-wait`.** In the background:
   `sqlcmd -Q "BEGIN TRAN; UPDATE dbo.small SET s='z' WHERE k=1; WAITFOR DELAY '00:00:20'; COMMIT"`. After 2 s, run
   `duckbridge import x.duckdb ... --lock-wait 6`. Expect exit 1 after about 6 s and a stderr line containing
   `dbo.small is locked by other sessions after waiting 6s: session <spid> (sa@<host>, SQLCMD) holds IX on OBJECT, IX on PAGE, X on KEY`.
   While it waits, `SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0` returns 0, so the holder is
   never blocked.
2. **`--on-lock skip`.** Same holder, then `--on-lock skip --lock-wait 3 --manifest m.csv`. Expect exit 0, a warning
   `dbo.small: skipped, still locked after 3s`, `dbo.small` empty in DuckDB, and the manifest row
   `dbo.small,-1,2,rows=2 ...`.
3. **Insert during the read triggers a re-read.** The read must last long enough for an insert to land in it: use about
   5 M rows with `-j 2`, or slow the read down. Insert in the background:
   `sleep 1.5; sqlcmd -Q "INSERT INTO dbo.t (v) VALUES ('new1'),('new2'),('new3')"`. Expect the stderr line
   `dbo.t: changed during the read (before [rows=N ...], after [rows=N+3 ...]); re-reading (1/2)`, exit 0, DuckDB `t`
   with N+3 rows, and a manifest row with `rereads = 1` and `rows_read = rows_source_stats = N+3`. I verified this with a
   harness that paused 4 s inside the first batch of `dbo.t` (the progress callback). The CLI has no such hook, so the
   script needs the larger table.
4. **Re-reads exhausted.** Same as case 3 with `--max-rereads 0`. Expect exit 1 and
   `dbo.t changed while it was read (1 read(s)): before [...], after [...]`.
5. **Snapshot.** With `sqlcmd -Q "ALTER DATABASE [_agent_e] SET ALLOW_SNAPSHOT_ISOLATION ON"`, a default import prints
   the warning `the source database has ALLOW_SNAPSHOT_ISOLATION on: --isolation snapshot gives consistent reads without locks`.
   With the holder from case 1 and `--isolation snapshot`, the import finishes right away with the committed values
   (`s='z'`, not the holder's uncommitted value). With snapshot isolation off and no RCSI, `--isolation snapshot` exits 1
   with `neither ALLOW_SNAPSHOT_ISOLATION nor READ_COMMITTED_SNAPSHOT`.
6. **Gate.** Create `dbo.flag (done BIT)` with 0. `--wait-for "SELECT done FROM dbo.flag" --wait-timeout 3` exits 1:
   `the gate query did not return 1 within 3s`. If an `UPDATE dbo.flag SET done = 1` runs after 4 s, the import starts
   on the next 10 s poll. `--wait-for "DELETE FROM dbo.flag"` exits 3 (read-only source connection).
7. **Manifest.** `--manifest in.manifest.csv` writes `in.manifest.csv` and `in.manifest.json` with one row per table,
   including empty ones.

## Verified live (2019 container, scratch DB `_agent_e`, created and dropped with sqlcmd)

Cases 1 to 7 were run with a scratch harness (`copy_data_with` plus an explicit `ReadSafety`), and the plain CLI import
was run with defaults. All results matched the expectations above. Case 3 re-read 200 003 rows and found 3 new rows.

## Notes and limits

- On a re-read, the whole table is deleted from DuckDB and read again. A table that other DuckDB tables reference with a
  FOREIGN KEY (created when `--keys-after-load` is off) cannot be deleted while child rows exist. The re-read then fails
  with the DuckDB error in context. With `--keys-after-load`, which creates no FKs, this does not happen.
- `last_user_update` comes from `sys.dm_db_index_usage_stats`. It is reset when the server restarts or the database goes
  offline. A reset during a read would cause one unnecessary re-read, never a missed change: rows and modify_date are
  still compared.
- The job queue order (pop from the end) and the data path in `copy_one` are unchanged, apart from taking `&mut
  SourceClient`, the lock-timeout hint and a `runlog::stmt` line per data read.
