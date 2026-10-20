#!/usr/bin/env bash
# End-to-end test against a real SQL Server (default: the local 2019 Docker container on port 14319).
# 1. export --to script  -> run with sqlcmd -> verify counts
# 2. export --to bak     -> BACKUP, verify by RESTORE under another name, fetch the .bak
# 3. import              -> SQL Server -> new DuckDB file -> compare row counts & checksums
# 4. transpile + run     -> T-SQL job executed on DuckDB
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/duckbridge}
HOST=${MSSQL_HOST:-localhost}; PORT=${MSSQL_PORT:-14319}; USER=${MSSQL_USER:-sa}
export DUCKBRIDGE_DISPOSABLE_DB_PATTERN='^d2m_'   # the guard (spec R1) only touches databases matching this
export MSSQL_PASSWORD=${MSSQL_PASSWORD:-'D2m!Strong#2019'}
CONTAINER=${MSSQL_CONTAINER:-d2m-mssql2019}
SQLCMD=${SQLCMD:-/opt/mssql-tools18/bin/sqlcmd}
DUCK=${DUCK:-$HOME/.local/bin/duckdb}
WORK=${WORK:-$(mktemp -d)}
FIX=tests/fixtures/sample.duckdb
[ -f "$FIX" ] || "$DUCK" "$FIX" < tests/fixtures/make_sample.sql
q() { "$SQLCMD" -S "$HOST,$PORT" -U "$USER" -P "$MSSQL_PASSWORD" -C -h -1 -W -Q "$1" | sed '/rows affected/d;/^$/d'; }
pass=0; fail=0
check() { if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; pass=$((pass+1)); else echo "  FAIL $1: got '$2' want '$3'"; fail=$((fail+1)); fi; }

echo "== 1. script export =="
$BIN export "$FIX" --to script --out "$WORK/sample.sql" --database d2m_script --create-database --drop-existing -q
"$SQLCMD" -S "$HOST,$PORT" -U "$USER" -P "$MSSQL_PASSWORD" -C -b -i "$WORK/sample.sql" -o "$WORK/script_run.log" || { echo "script failed:"; tail -20 "$WORK/script_run.log"; exit 1; }
grep -i "WARNING" "$WORK/script_run.log" || true
check "script customers" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.dbo.customers')" "3"
check "script orders"    "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.dbo.orders')" "1000"
check "script events"    "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.dbo.events')" "200000"
check "script regions"   "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.sales.regions')" "3"
check "script view"      "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.dbo.v_customer_totals')" "3"
check "script unicode"   "$(q "SET NOCOUNT ON; SELECT name FROM d2m_script.dbo.customers WHERE id = 1")" "Alice O'Neil"
check "script decimal"   "$(q 'SET NOCOUNT ON; SELECT CAST(balance AS VARCHAR(30)) FROM d2m_script.dbo.customers WHERE id = 1')" "1234.5678"
check "script huge"      "$(q 'SET NOCOUNT ON; SELECT CAST(huge AS VARCHAR(50)) FROM d2m_script.dbo.orders WHERE id = 1')" "170141183460469231731687303715884105726"
check "script ts_ns"     "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(30), ts_ns, 121) FROM d2m_script.dbo.orders WHERE id = 1')" "2024-06-01 01:02:03.1234567"
check "script tz"        "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(40), ordered_at, 121) FROM d2m_script.dbo.orders WHERE id = 1')" "2024-01-01 10:01:00.0000000 +00:00"
check "script enum"      "$(q 'SET NOCOUNT ON; SELECT m FROM d2m_script.dbo.orders WHERE id = 1')" "ok"
check "script json"      "$(q 'SET NOCOUNT ON; SELECT JSON_VALUE(payload, '"'"'$.k'"'"') FROM d2m_script.dbo.orders WHERE id = 7')" "7"
check "script list"      "$(q 'SET NOCOUNT ON; SELECT tags FROM d2m_script.dbo.orders WHERE id = 7')" '["a","b","7"]'
check "script blob"      "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(10), blob) FROM d2m_script.dbo.orders WHERE id = 42')" "42"
check "script inf->null" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.dbo.customers WHERE score IS NULL')" "2"
check "script pk"        "$(q "SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.sys.key_constraints WHERE type='PK'")" "4"
check "script fk"        "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.sys.foreign_keys')" "1"
check "script identity"  "$(q "SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_script.sys.identity_columns WHERE object_id = OBJECT_ID('d2m_script.dbo.orders')")" "1"
check "script reseed"    "$(q "SET NOCOUNT ON; SELECT CAST(IDENT_CURRENT('d2m_script.dbo.orders') AS INT)")" "1000"

echo "== 2. live load + .bak + verify restore =="
$BIN export "$FIX" --to bak --host "$HOST" --port "$PORT" --user "$USER" --database d2m_bak --drop-existing --create-database \
     --backup-dir /var/opt/mssql/data --fetch "docker:$CONTAINER" --out "$WORK/d2m_bak.bak" --verify-restore -q
check "bak file exists"  "$([ -s "$WORK/d2m_bak.bak" ] && echo yes)" "yes"
check "bak orders"       "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_bak.dbo.orders')" "1000"
check "bak events sum"   "$(q 'SET NOCOUNT ON; SELECT SUM(event_id) FROM d2m_bak.dbo.events')" "20000100000"
check "bak identity"     "$(q "SET NOCOUNT ON; SELECT CAST(IDENT_CURRENT('d2m_bak.dbo.orders') AS INT)")" "1000"
check "bak uuid"         "$(q "SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_bak.dbo.customers WHERE uid IS NOT NULL")" "3"
check "bak ts_ns"        "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(30), ts_ns, 121) FROM d2m_bak.dbo.orders WHERE id = 1')" "2024-06-01 01:02:03.1234567"
check "bak decimal"      "$(q 'SET NOCOUNT ON; SELECT CAST(amount AS VARCHAR(30)) FROM d2m_bak.dbo.orders WHERE id = 3')" "3.03"
check "bak time"         "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(30), t, 121) FROM d2m_bak.dbo.orders WHERE id = 1')" "12:34:56.1234560"
check "bak pk clustered" "$(q "SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_bak.sys.indexes WHERE object_id = OBJECT_ID('d2m_bak.dbo.events') AND is_primary_key = 1 AND type = 1")" "1"
check "bak date"         "$(q 'SET NOCOUNT ON; SELECT CONVERT(VARCHAR(10), born, 23) FROM d2m_bak.dbo.customers WHERE id = 1')" "1980-02-29"
# restore the fetched file into a third database name from scratch to prove portability
docker cp "$WORK/d2m_bak.bak" "$CONTAINER:/var/opt/mssql/data/portable.bak"
docker exec -u 0 "$CONTAINER" chown mssql:root /var/opt/mssql/data/portable.bak
q "IF DB_ID('d2m_portable') IS NOT NULL BEGIN ALTER DATABASE d2m_portable SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE d2m_portable; END" >/dev/null
q "RESTORE DATABASE d2m_portable FROM DISK = N'/var/opt/mssql/data/portable.bak' WITH MOVE N'd2m_bak' TO N'/var/opt/mssql/data/d2m_portable.mdf', MOVE N'd2m_bak_log' TO N'/var/opt/mssql/data/d2m_portable_log.ldf', REPLACE" | grep -v "RESTORE DATABASE successfully" || true
check "portable restore" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_portable.dbo.events')" "200000"

echo "== 3. import SQL Server -> DuckDB =="
rm -f "$WORK/back.duckdb"
$BIN import "$WORK/back.duckdb" --host "$HOST" --port "$PORT" --user "$USER" --database d2m_bak -q
check "import orders"    "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c 'select count(*) from orders')" "1000"
check "import events"    "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c 'select sum(event_id) from events')" "20000100000"
check "import regions"   "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c 'select count(*) from sales.regions')" "3"
check "import pk"        "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select count(*) from duckdb_constraints() where constraint_type='PRIMARY KEY'")" "4"
check "import identity"  "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select count(*) from duckdb_sequences()")" "1"
check "import seq next"  "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select start_value from duckdb_sequences()")" "1001"
check "import ts"        "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select strftime(ts_ms, '%Y-%m-%d %H:%M:%S.%g') from orders where id=1")" "2024-06-01 01:02:03.456"
check "import decimal"   "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select balance from customers where id=1")" "1234.5678"
check "import unicode"   "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select name from customers where id=2")" "Bob 𝔘nicode"
check "import view"      "$($DUCK -readonly "$WORK/back.duckdb" -noheader -list -c "select count(*) from v_customer_totals")" "3"

echo "== 4. transpile + run a T-SQL job on DuckDB =="
cat > "$WORK/job.sql" <<'SQL'
SET NOCOUNT ON;
GO
IF OBJECT_ID('dbo.customer_kpi', 'U') IS NOT NULL DROP TABLE dbo.customer_kpi;
GO
DECLARE @since DATETIME2 = '2024-01-01';
SELECT TOP 100 PERCENT c.id, c.name,
       COUNT_BIG(e.event_id) AS n_events,
       SUM(ISNULL(e.value, 0)) AS total_value,
       MAX(e.at_ts) AS last_seen,
       DATEDIFF(day, @since, MAX(e.at_ts)) AS days_active,
       CONVERT(VARCHAR(10), MIN(e.at_ts), 112) AS first_day,
       c.name + ' (' + CAST(c.id AS VARCHAR(10)) + ')' AS label
INTO dbo.customer_kpi
FROM dbo.customers c WITH (NOLOCK)
LEFT JOIN dbo.events e ON e.customer_id = c.id
WHERE e.at_ts >= @since
GROUP BY c.id, c.name;
GO
SELECT TOP 3 * FROM dbo.customer_kpi ORDER BY n_events DESC;
SQL
cp "$WORK/back.duckdb" "$WORK/job.duckdb"
$BIN run "$WORK/job.duckdb" "$WORK/job.sql" -q > "$WORK/job.out" 2> "$WORK/job.err" || { cat "$WORK/job.err"; exit 1; }
check "job rows"         "$($DUCK -readonly "$WORK/job.duckdb" -noheader -list -c 'select count(*) from customer_kpi')" "3"
check "job sum"          "$($DUCK -readonly "$WORK/job.duckdb" -noheader -list -c 'select sum(n_events) from customer_kpi')" "200000"
check "job label"        "$($DUCK -readonly "$WORK/job.duckdb" -noheader -list -c "select label from customer_kpi where id=1")" "Alice O'Neil (1)"
check "job first_day"    "$($DUCK -readonly "$WORK/job.duckdb" -noheader -list -c "select first_day from customer_kpi where id=1")" "20240101"

echo "== 5. ship the processed table back as .bak (only changed table) =="
$BIN export "$WORK/job.duckdb" --to bak --tables customer_kpi --host "$HOST" --port "$PORT" --user "$USER" --database d2m_kpi --drop-existing --create-database \
     --backup-dir /var/opt/mssql/data --fetch "docker:$CONTAINER" --out "$WORK/d2m_kpi.bak" --verify-restore --drop-staging -q
check "kpi bak exists"   "$([ -s "$WORK/d2m_kpi.bak" ] && echo yes)" "yes"

echo; echo "passed: $pass  failed: $fail  (work dir: $WORK)"
[ "$fail" -eq 0 ]
