# Hand-off: stream C (converters, exact DECIMAL, columnar fetch, pipelining)

Files: `src/mssql/convert.rs`, `src/mssql/bulk.rs`. No other files touched. Nothing committed.
`benches/bulk.rs` was not created: Cargo.toml cannot change, so the benchmark is an ignored test
(see "Benchmarks").

## What is implemented

### `mssql::convert::Converter` (R5.1, R5.2, R3.4)

The enum matches the plan: `Bit, TinyInt, SmallInt, Int, BigInt, Real, Float, Decimal{precision,scale},
Char{max_chars,unicode,fixed}, Date, Time{scale}, DateTime, SmallDateTime, DateTime2{scale},
DateTimeOffset{scale}, Guid, Binary{max_len}, Text`. It derives `Debug, Clone, PartialEq, Eq` and
`Display` (`decimal(8,5)`, `varchar(20)`, ...).

- `for_target(&TargetColumn, duck_type)` maps sys.types names. `float` with `max_length` 4 becomes `Real`.
  `money` is (19,4) and `smallmoney` is (10,4). `nchar`/`nvarchar` use `max_length/2`, and -1 means MAX.
  `text`, `ntext`, `xml`, `json` and anything unknown map to `Text`.
- `for_sql_type(sql_type, duck_type)` parses the create-mode type text, for example `NVARCHAR(50)`,
  `DECIMAL(18, 3)`, `DATETIME2(7)`, `VARBINARY(MAX)`, `FLOAT(24|53)`, `INT`, `MONEY`, `SYSNAME`, `json` and
  `VECTOR(n)`. Invalid text such as `DECIMAL(39,2)`, `DATETIME2(8)` or `VARCHAR(abc)` returns an error.
- `convert(ValueRef)`:
  - NULL becomes the typed `None`.
  - Integers are range-checked and never wrapped. A tinyint outside 0..=255 errors and the message names
    the value. A DECIMAL, float or text value with a fractional part going into an int column is an
    error, not a truncation.
  - `bit` accepts a Boolean, 0/1 or `true`/`false` text. Any other value is an error.
  - DECIMAL takes the DuckDB mantissa directly. In duckdb-rs 1.10506, `ValueRef::Decimal` is
    `duckdb::types::Decimal { width, scale, value: i128 }`, not rust_decimal.
    - Scaling up multiplies exactly by 10^k.
    - Scaling down is exact when the dropped digits are zero. Otherwise it errors with
      `value 1234.56789: scale 5 > target scale 2; use --allow-rescale`.
    - Precision is checked: |mantissa| must be below 10^p.
    - Integer sources become the mantissa times 10^s.
    - Text sources go through an exact 38-digit parser, with rust_decimal as the fallback for exponents.
    - float/double sources use the shortest round-trip text, rounded half away from zero (float has no
      exact scale).
  - `varchar`/`char` (`unicode: false`) accept ASCII only. Anything else errors with
    `non-ASCII value '...' for varchar column; SQL Server would substitute '?'`. `nvarchar`/`nchar` count
    UTF-16 units, so one emoji counts 2.
  - Length is checked against `max_chars`. When the only overflow is trailing spaces, they are cut,
    exactly as SQL Server does. Any other overflow errors with `length n > max m`.
  - `date`/`time`/`datetime2`/`datetimeoffset` use 100 ns tick math and are sent at the column's scale.
    tiberius does not rescale `time` or `datetimeoffset` itself. Rounding is half up, with carry into
    the next day.
  - `datetime` rounds to the nearest 1/300 s over 1753-01-01..9999-12-31. `smalldatetime` rounds to the
    nearest minute (30.000 s rounds up, the same as SQL Server's datetime2 -> smalldatetime) over
    1900-01-01..2079-06-06.
  - Out-of-range dates are errors, not tiberius panics.
  - Text fallbacks are parsed like load.rs does today.
  - `uuid` comes from text or 16 bytes. Binary length is checked.
  - `Text` uses DuckDB-style formatting. ints, decimals, booleans (`true`), dates and blobs (hex) print
    properly instead of 0.1.0's `Debug` output.
  - No string work for non-string targets, and no String allocation except for string or binary targets.
- Behaviour fixes compared with `load::to_column_data`:
  - The datetimeoffset text path now sends the UTC instant plus the offset. 0.1.0 sent local wall-clock
    time as UTC.
  - `tinyint`/`int` no longer wrap.
  - DECIMAL no longer formats and re-parses text.
  - `time(n)`/`datetimeoffset(n)` are sent at scale n instead of always 7.
- Crate-internal fast paths used by bulk.rs: `convert_mantissa(i128, scale)` and `convert_str(&str)`.

### `mssql::bulk` (R5.3, R5.5)

- `BulkOptions { batch_rows, keep_identity, table_lock, prefetch_batches, order_by }`. `Default` is
  100 000 / false / true / 4 / None.
- `bulk_load(con, client, table_ref, columns, convs, sql, o, progress) -> Result<i64>` works in five steps:
  1. **Plan mode:** under `runlog::planning()` it records an `INSERT BULK ...` line and returns 0. In a
     real run the same line goes to `runlog::stmt` at the end, with the row count.
  2. **Metadata pre-check:** it reads the target's column metadata once (`client.column_metadata`) and
     checks it against tiberius 0.13's bulk path (see Open issues). It fails early, with a clear
     message, for:
     - an identity column without `keep_identity`;
     - computed columns;
     - `time(n<7)` / `datetimeoffset(n<7)`;
     - `nvarchar`/`nchar(2001..4000)`;
     - `xml`.
     It sends `smalldatetime NULL` columns as `datetime` values that are already rounded to the minute.
  3. **Reader thread:** a std thread with its own DuckDB connection (`core::conn::acquire`). It runs the
     query as a *streaming* query (`stream_arrow` + `Statement::step`, not `query_arrow`). Both
     `query`/`query_arrow` materialize the whole result in DuckDB first, which load.rs does today, so
     TB-scale tables currently need their full result in memory.
     - Each Arrow chunk is converted column by column: one downcast per column per 2048-row chunk, then a
       typed loop.
     - Fast paths cover Int8/16/32/64, UInt8/16/32/64, Float32/64, Boolean, Utf8/LargeUtf8/Utf8View,
       Binary/LargeBinary/BinaryView/FixedSizeBinary, Date32, Time64 (us, ns), Timestamp (s/ms/us/ns,
       with or without tz), Decimal32/64/128 (mantissa straight through; HUGEINT arrives as
       Decimal128(38,0)), Interval and Null.
     - UUID arrives as Utf8 by default.
     - When any result column has no fast path (LIST/STRUCT/MAP/ENUM dictionary/UNION), the whole query
       falls back to the row path (`raw_query` on the same streaming result + `get_ref`). select_sql
       already casts all of those to VARCHAR/JSON.
  4. **Channel:** batches of `min(batch_rows, 8192)` rows go over `tokio::sync::mpsc::channel(prefetch_batches)`
     using `blocking_send`.
  5. **Async side:** it feeds `bulk_insert_with_options(table_ref, cols, KeepIdentity|TableLock, ORDER hints)`
     and finalizes and restarts every `batch_rows` rows, exactly as 0.1.0 does. `progress(rows_so_far)`
     is called after every commit and at the end.

  The reader connection is released with `core::conn::release` on success and on error. When the writer
  fails, the DuckDB query is interrupted (`interrupt_handle`) so the reader stops at once.
- Errors carry the table, column name and source row, for example
  `dbo.c_bad: column v (varchar(10)), source row 3001: non-ASCII value 'café' for varchar column; SQL Server would substitute '?' ...`.
  A failed load does **not** commit the open batch. Earlier batches stay committed. The `MsClient` is
  left mid-request, so drop it and reconnect.
- Column names are bracket-quoted by bulk_load (pass raw names; `[already quoted]` names are kept). This
  fixes 0.1.0, which passed raw names into `INSERT BULK`, so names with spaces failed.
- `order_by`: `"col [ASC|DESC], ..."` uses target column names from `columns`. The DuckDB query is wrapped
  as `SELECT * FROM (sql) ORDER BY <ordinal> ASC NULLS FIRST ...` to match SQL Server's NULL order, and
  `INSERT BULK ... ORDER(...)` is sent. Only use it for integer or date keys. Text collations may sort
  differently and then the server rejects the hint.
- `pub(crate) fn read_rows(con, sql, convs, batch_rows, sink)` is the reader pipeline. Its errors are
  `bulk::ConvertError { column, row, message }`.

## Measured (this box, 20 threads, release build)

| what | rows/s |
|---|---|
| DuckDB streaming read alone, 17-col spec row shape, 2 M rows | 7.3-7.9 M |
| **Conversion** (read + Arrow -> TokenRow, rows dropped on the reader thread), 2 M rows | **1.22-1.31 M** |
| Conversion with rows handed to a consumer thread (the bulk_load shape) | 1.01-1.14 M |
| End to end, one session, 1 M rows into a heap with TABLOCK on the local 2019 container | 616-657 k |
| Same 1 M rows through the 0.1.0 path (`load::to_column_data`, materialized query) | 484-502 k |

The R5 target of 300k rows/s per session is met with about 2x headroom. With pipelining, the end-to-end
limit is tiberius' TDS encoding plus the server, not the converter. A run while other builds loaded the
machine gave 344-369k (new) vs 270k (0.1.0); the ratio held.

## Benchmarks / how to run

```
cargo test --release --lib -- convert bulk                          # 18 unit tests (no SQL Server)
cargo test --release --lib -- --ignored bench --nocapture           # conversion benchmark (DUCKBRIDGE_BENCH_ROWS, default 2 M)
# live, needs a database named _agent_c (create/drop it with sqlcmd):
DUCKBRIDGE_TEST_MSSQL='localhost:14319:sa:<password>' cargo test --release --lib -- --ignored bulk_live --nocapture --test-threads 1
```

`cargo test --release convert bulk` (two names) is rejected by cargo itself, since it accepts only one
filter. Use `-- convert bulk`.

The live test `bulk_live_sql_server` checks the following, and passed on the 2019 container:
- 25k rows, 19 types, KEEP_IDENTITY, ORDER hint, batch 4000;
- counts, exact decimal sums, money, unicode lengths;
- our datetime, smalldatetime, datetime2(3) and time rounding are identical to SQL Server's own CAST from
  datetime2(7);
- datetimeoffset UTC;
- every column of one row;
- the non-ASCII error with table/column/row and an uncommitted open batch;
- an empty source;
- MAX strings/binaries, char/nchar padding, decimal(38,10), smallmoney max, binary(4), ntext;
- the early refusals for time(3), nvarchar(3000), xml and an identity column.

## How the integrator calls it from load.rs

```rust
use crate::mssql::bulk::{bulk_load, BulkOptions};
use crate::mssql::convert::Converter;

// create mode
let convs: Vec<Converter> = t.columns.iter().map(|c| Converter::for_sql_type(&c.sql_type, &c.duck_type)).collect::<Result<_>>()?;
let cols: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
// into-existing (target::TableMapping m, target column_id order)
let convs: Vec<Converter> = m.columns.iter().map(|cm| Converter::for_target(&cm.target, &cm.duck.duck_type)).collect::<Result<_>>()?;
let cols: Vec<String> = m.columns.iter().map(|cm| cm.target.name.clone()).collect();
// the SELECT must return exactly these columns in this order (select_sql(t) / the mapping's select)

let o = BulkOptions { batch_rows: lo.batch_rows, keep_identity: t.has_identity(), table_lock: true, prefetch_batches: 4, order_by: None };
let tag = format!("{}.{}", t.schema, t.name);
let n = bulk_load(con, &mut client, &em.table_ref(&t.schema, &t.name), &cols, &convs, &sql, &o,
                  &|rows| progress(&tag, rows)).await.with_context(|| format!("loading {tag}"))?;
```

- Build the converters once per table and reuse them for every job or range of that table.
- `con` is the worker's DuckDB connection. bulk_load acquires a *second* connection for its reader, so a
  worker needs 2 connections from the extension's pool.
- Leave out computed columns. Leave out identity columns unless `keep_identity` is set.
- After an error, drop the client.
- With `--allow-rescale`, cast in the DuckDB SELECT (`CAST(x AS DECIMAL(p,s))`), so values arrive at the
  target scale. The converter never rounds DECIMAL.

## Open issues

1. **tiberius 0.13 `INSERT BULK` type text** (`MetaDataColumn` Display). We cannot work around this
   without patching tiberius, so bulk_load refuses these cases up front:
   - `time` and `datetimeoffset` are declared without their scale, so `time(n<7)` and `datetimeoffset(n<7)`
     fail with server error 4875.
   - `nvarchar(n)`/`nchar(n)` are declared with the *byte* length, so `nvarchar(2001..4000)` becomes
     `nvarchar(max)` and the server rejects it (error 4816).
   - `xml` is rejected (4816).
   - Nullable `smalldatetime` is declared as `datetime`. bulk_load handles this by sending datetime values.

   **Create mode is affected too:** typemap's largest string bucket is `NVARCHAR(4000)`, so any column with
   2001-4000 characters fails today, in 0.1.0 as well. Stream I should emit `NVARCHAR(MAX)` above 2000, or
   tiberius should be patched (`ctx.len()/2` for NVarchar/NChar, and scale for Timen/DatetimeOffsetn).
2. **The varchar check is ASCII-only, as specified.** Latin1 data such as `é` into a `varchar` Latin1
   column is refused even though the code page can hold it. tiberius already encodes varchar with the
   column collation's code page and fails on unmappable characters (`Error::Encoding`), so it never
   substitutes `?`. Relaxing the converter for code-page-representable data would be safe if R3 allows it.
3. NaN/Infinity into `real`/`float` become NULL, as in 0.1.0. SQL Server cannot store them, and preflight
   should flag them.
4. UHUGEINT values above i128::MAX: DuckDB's Arrow export carries them as Decimal128, which cannot hold
   them. Values that large are beyond decimal(38) anyway.
5. Intra-session parallel conversion (columns across threads) is not done. One reader thread converts
   about 1.2 M rows/s, and the TDS writer is the slower stage. Parallelism comes from running several
   jobs at once.
6. If the reader thread panics (it should not; every path returns errors), its pooled connection is not
   returned to the extension pool.
