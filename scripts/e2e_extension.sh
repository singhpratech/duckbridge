#!/usr/bin/env bash
# End-to-end test of the DuckDB extension against the local SQL Server 2019 container
# (and PostgreSQL if reachable). Needs: the duckdb CLI matching extension/Makefile's
# DUCKDB_VERSION, sqlcmd, docker. Builds the extension if build/ is missing.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f private/env.sh ] && . private/env.sh   # optional, gitignored: local overrides for the variables below
HOST=${MSSQL_HOST:-localhost}; PORT=${MSSQL_PORT:-14319}; USER=${MSSQL_USER:-sa}
export MSSQL_PASSWORD=${MSSQL_PASSWORD:-'D2m!Strong#2019'}
CONTAINER=${MSSQL_CONTAINER:-d2m-mssql2019}
SQLCMD=${SQLCMD:-/opt/mssql-tools18/bin/sqlcmd}
DUCK=${DUCK:-$HOME/.local/bin/duckdb}
EXT=${EXT:-extension/build/duckbridge.duckdb_extension}
WORK=${WORK:-$(mktemp -d)}
PG_CONTAINER=${PG_CONTAINER:-postgres}; PG_USER=${PG_USER:-postgres}; PG_PASSWORD=${PG_PASSWORD:-postgres}; PG_PORT=${PG_PORT:-5432}
PW_ENC=$(printf '%s' "$MSSQL_PASSWORD" | sed 's/!/%21/g; s/#/%23/g; s/@/%40/g; s/:/%3A/g; s/\//%2F/g')
DSN="mssql://$USER:$PW_ENC@$HOST:$PORT"
q() { "$SQLCMD" -S "$HOST,$PORT" -U "$USER" -P "$MSSQL_PASSWORD" -C -h -1 -W -Q "$1" | sed '/rows affected/d;/^$/d'; }
d() { "$DUCK" -unsigned -noheader -list "$WORK/ext.duckdb" -c "LOAD '$EXT'; $1" 2>&1; }
pass=0; fail=0
check() { if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; pass=$((pass+1)); else echo "  FAIL $1: got '$2' want '$3'"; fail=$((fail+1)); fi; }

[ -f "$EXT" ] || (cd extension && make release)
[ -f tests/fixtures/sample.duckdb ] || "$DUCK" tests/fixtures/sample.duckdb < tests/fixtures/make_sample.sql

echo "== 0. source database on SQL Server =="
if [ "$(q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'd2m_extsrc'")" != "1" ]; then
  target/release/duckbridge export tests/fixtures/sample.duckdb --to server --host "$HOST" --port "$PORT" --user "$USER" --database d2m_extsrc --drop-existing -q
fi

echo "== 1. load + transpile =="
check "transpile tsql->duck" "$(d "SELECT bridge_transpile('tsql','duckdb','SELECT TOP 2 ISNULL(a,0) FROM dbo.t')")" "SELECT COALESCE(a, 0) FROM main.t LIMIT 2;"
check "transpile duck->tsql 2022" "$(d "SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2022)")" "SELECT greatest(a, b) FROM t;"
check "transpile duck->tsql 2019" "$(d "SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2019)")" "SELECT (SELECT MAX(v) FROM (VALUES (a), (b)) AS g(v)) FROM t;"
check "prelude lines" "$(d "SELECT length(string_split(bridge_prelude(true), chr(10)))")" "4"
check "bad dialect errors" "$(d "SELECT bridge_transpile('oracle','duckdb','x')" | grep -c "unknown source dialect")" "1"

echo "== 2. bridge_import =="
d "SELECT * FROM bridge_import('$DSN/d2m_extsrc', parallel := 4)" > "$WORK/import.txt"
check "import rows" "$(grep '|summary|' "$WORK/import.txt" | cut -d'|' -f3)" "201006"
check "import tables" "$(d "SELECT count(*) FROM duckdb_tables() WHERE NOT internal")" "4"
check "import views" "$(d "SELECT count(*) FROM duckdb_views() WHERE NOT internal AND schema_name IN ('main','sales')")" "2"
check "identity sequence" "$(d "SELECT nextval('seq_orders_id')")" "1001"
check "hugeint text" "$(d "SELECT huge FROM orders WHERE id = 1")" "$(q "SET NOCOUNT ON; SELECT huge FROM d2m_extsrc.dbo.orders WHERE id = 1")"
check "unicode" "$(d "SELECT name FROM customers WHERE id = 2")" "Bob 𝔘nicode"
check "schema_only + exclude + overwrite" "$(d "SELECT count(*) FROM bridge_import('$DSN/d2m_extsrc', schema_only := true, exclude := 'events,regions', schema_map := 'dbo=staging', overwrite := true) WHERE kind = 'table'")" "2"
check "import without overwrite fails" "$(d "SELECT * FROM bridge_import('$DSN/d2m_extsrc', schema_only := true)" | grep -c 'already exists' | awk '{print ($1>0)?"yes":"no"}')" "yes"

echo "== 3. bridge_run (T-SQL on DuckDB) =="
d "SELECT * FROM bridge_run('IF OBJECT_ID(''dbo.kpi'') IS NOT NULL DROP TABLE dbo.kpi;
SELECT c.id, c.name, COUNT(o.id) AS n, ISNULL(SUM(o.amount),0) AS total, 7/2 AS intdiv INTO dbo.kpi FROM dbo.customers c LEFT JOIN dbo.orders o ON o.customer_id = c.id GROUP BY c.id, c.name;
UPDATE dbo.kpi SET name = UPPER(name) WHERE id = 1;
SELECT TOP 1 * FROM dbo.kpi ORDER BY total DESC;')" > "$WORK/run.txt"
check "run summary" "$(grep '|summary|' "$WORK/run.txt" | cut -d'|' -f4)" "4 statement(s) succeeded, 0 failed"
check "run update count" "$(grep 'UPDATE main.kpi' "$WORK/run.txt" | cut -d'|' -f3)" "1"
check "run integer division" "$(d "SELECT intdiv FROM kpi LIMIT 1")" "3"
check "run upper" "$(d "SELECT name FROM kpi WHERE id = 1")" "ALICE O'NEIL"
check "settings not leaked" "$(d "SELECT current_setting('integer_division')")" "false"
check "run error surfaces" "$(d "SELECT * FROM bridge_run('SELECT * FROM dbo.nope')" | grep -c "nope" | awk '{print ($1>0)?"yes":"no"}')" "yes"
check "continue_on_error" "$(d "SELECT kind FROM bridge_run('SELECT * FROM dbo.nope; SELECT 1;', continue_on_error := true) WHERE kind IN ('ok','failed') ORDER BY kind" | tr '\n' ',')" "failed,ok,"

echo "== 4. bridge_export -> SQL Server live + .bak + verify =="
q "IF DB_ID('d2m_extout') IS NOT NULL BEGIN ALTER DATABASE d2m_extout SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE d2m_extout; END" >/dev/null
d "SELECT * FROM bridge_export('$DSN/d2m_extout', parallel := 4, bak := '$WORK/d2m_extout.bak', fetch_via := 'docker:$CONTAINER', verify_restore := true)" > "$WORK/export.txt"
cat "$WORK/export.txt" | grep -v '^$' | sed 's/^/    /' | head -14
check "export summary rows" "$(grep '|summary|' "$WORK/export.txt" | cut -d'|' -f3)" "201009"
check "export verify" "$(grep -c '|verify|' "$WORK/export.txt")" "1"
check "bak fetched" "$([ -s "$WORK/d2m_extout.bak" ] && echo yes)" "yes"
check "events on server" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_extout.dbo.events')" "200000"
check "kpi on server" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_extout.dbo.kpi')" "3"
check "identity current" "$(q "SET NOCOUNT ON; SELECT CAST(IDENT_CURRENT('d2m_extout.dbo.orders') AS INT)")" "1000"
check "clustered pk" "$(q "SET NOCOUNT ON; SELECT type_desc FROM d2m_extout.sys.indexes WHERE object_id = OBJECT_ID('d2m_extout.dbo.orders') AND is_primary_key = 1")" "CLUSTERED"
check "view on server" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_extout.dbo.v_customer_totals')" "3"

echo "== 5. restore the fetched .bak elsewhere =="
docker cp "$WORK/d2m_extout.bak" "$CONTAINER:/var/opt/mssql/backup/ext_portable.bak" >/dev/null
docker exec -u root "$CONTAINER" chown mssql /var/opt/mssql/backup/ext_portable.bak
q "RESTORE DATABASE d2m_extport FROM DISK = N'/var/opt/mssql/backup/ext_portable.bak' WITH MOVE 'd2m_extout' TO '/var/opt/mssql/data/d2m_extport.mdf', MOVE 'd2m_extout_log' TO '/var/opt/mssql/data/d2m_extport.ldf', REPLACE" >/dev/null
check "portable restore" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_extport.dbo.orders')" "1000"

echo "== 6. parquet + script =="
d "SELECT * FROM bridge_export('parquet:$WORK/pq', tables := 'main.kpi,main.customers')" > "$WORK/pq.txt"
check "parquet files" "$(grep -c '|parquet|' "$WORK/pq.txt")" "2"
check "parquet readable" "$("$DUCK" -noheader -list -c "SELECT count(*) FROM '$WORK/pq/main.kpi.parquet'")" "3"
d "SELECT * FROM bridge_export('script:$WORK/kpi.sql', database := 'd2m_extscript', create_database := true, tables := 'kpi')" > "$WORK/script.txt"
"$SQLCMD" -S "$HOST,$PORT" -U "$USER" -P "$MSSQL_PASSWORD" -C -b -i "$WORK/kpi.sql" -o "$WORK/script_run.log"
check "script runs" "$(q 'SET NOCOUNT ON; SELECT COUNT(*) FROM d2m_extscript.dbo.kpi')" "3"

echo "== 7. postgres (optional) =="
if docker ps --format '{{.Names}}' | grep -q "^$PG_CONTAINER\$"; then
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -qc "DROP DATABASE IF EXISTS d2m_extpg" -c "CREATE DATABASE d2m_extpg" >/dev/null
  d "SELECT * FROM bridge_export('postgres://$PG_USER:$PG_PASSWORD@localhost:$PG_PORT/d2m_extpg', tables := 'kpi', drop_existing := true)" > "$WORK/pg.txt"
  check "pg rows" "$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d d2m_extpg -tAc 'SELECT count(*) FROM public.kpi')" "3"
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -qc "DROP DATABASE d2m_extpg" >/dev/null
else
  echo "  skipped (no postgres container)"
fi

echo "== cleanup =="
for db in d2m_extout d2m_extport d2m_extscript; do q "IF DB_ID('$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END" >/dev/null; done
docker exec "$CONTAINER" rm -f /var/opt/mssql/backup/ext_portable.bak /var/opt/mssql/backup/d2m_extout.bak
echo
echo "passed: $pass  failed: $fail  (work dir: $WORK)"
[ "$fail" -eq 0 ]
