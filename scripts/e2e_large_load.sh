#!/usr/bin/env bash
# End-to-end test of the 0.1.1 large-load safety features against the local SQL Server 2019 container.
#   1 guard (R1)            5 verification (R9)            9 plan + exit codes (R8)
#   2 pre-flight (R2/R3)    6 safe reads (R6)
#   3 switch load + kill/rerun (R4)   7 translator (R7)
#   4 heap-then-index / serial (R4.2) 8 swap + drop-old (R10)
# Databases d2m_large (target) and d2m_large_src (import source) are created and dropped with sqlcmd.
# Env: BIN, MSSQL_HOST/PORT/USER/PASSWORD, DUCK, SQLCMD, KILL_AFTER (seconds before the kill test SIGKILLs the load, default 12),
#      ROWS (default 2000000), KEEP=1 to keep the databases.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/duckbridge}
HOST=${MSSQL_HOST:-localhost}; PORT=${MSSQL_PORT:-14319}; USER=${MSSQL_USER:-sa}
export DUCKBRIDGE_DISPOSABLE_DB_PATTERN='^d2m_'
export MSSQL_PASSWORD=${MSSQL_PASSWORD:-'D2m!Strong#2019'}
SQLCMD=${SQLCMD:-/opt/mssql-tools18/bin/sqlcmd}
DUCK=${DUCK:-$HOME/.local/bin/duckdb}
WORK=${WORK:-$(mktemp -d)}
ROWS=${ROWS:-2000000}
KILL_AFTER=${KILL_AFTER:-3}   # the 2 M-row switch load finishes in ~6 s on the dev box
DB=d2m_large; SRC=d2m_large_src
PART=$((ROWS / 25))          # rows per market / partition
pass=0; fail=0

sc() { "$SQLCMD" -S "$HOST,$PORT" -U "$USER" -P "$MSSQL_PASSWORD" -C -I "$@"; }
# qd <db> <sql>: scalar/rows query, trimmed, no headers
qd() { sc -d "$1" -h -1 -W -Q "SET NOCOUNT ON; $2" | sed '/rows affected/d;/^$/d'; }
q() { qd master "$1"; }
# ddl <db> <sql>: run a batch, abort the script on error
ddl() { local o; o=$(sc -b -d "$1" -Q "$2" 2>&1) || { echo "DDL failed on $1: $o"; exit 1; }; }
rows() { qd "$DB" "SELECT COUNT_BIG(*) FROM $1"; }
duck() { "$DUCK" "$1" -c "$2" >/dev/null; }
duckq() { "$DUCK" -readonly "$1" -noheader -list -c "$2"; }

check() { if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; pass=$((pass+1)); else echo "  FAIL $1: got '$2' want '$3'"; fail=$((fail+1)); fi; }
check_pos() { if [ "${2:-0}" -gt 0 ] 2>/dev/null; then echo "  PASS $1 ($2)"; pass=$((pass+1)); else echo "  FAIL $1: got '$2' want > 0"; fail=$((fail+1)); fi; }
# cnt <regex> <file>: number of matching lines (0 when none or file missing), safe under pipefail
cnt() { { grep -c -i -E -- "$1" "$2" 2>/dev/null || true; } | awk '{print $1+0}'; }
cntf() { { grep -c -F -- "$1" "$2" 2>/dev/null || true; } | awk '{print $1+0}'; }
# run <outfile> cmd...: capture stdout+stderr, set RC
RC=0
run() { local out=$1; shift; RC=0; "$@" >"$out" 2>&1 || RC=$?; }
# xp <outfile> <db> <duckdb> [export flags]: export --to server
xp() { local out=$1 db=$2 dk=$3; shift 3
       run "$out" "$BIN" export "$dk" --to server --host "$HOST" --port "$PORT" --user "$USER" --database "$db" "$@"; }
conn=(--host "$HOST" --port "$PORT" --user "$USER")

cleanup() {
  jobs -p | xargs -r kill 2>/dev/null || true
  [ "${KEEP:-0}" = 1 ] && return 0
  for d in "$DB" "$SRC" d2m_nonexistent_db; do
    sc -Q "IF DB_ID('$d') IS NOT NULL BEGIN ALTER DATABASE [$d] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$d]; END" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

echo "== 0. setup (work dir $WORK) =="
for d in "$DB" "$SRC" d2m_nonexistent_db; do
  sc -b -Q "IF DB_ID('$d') IS NOT NULL BEGIN ALTER DATABASE [$d] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$d]; END" >/dev/null
done
sc -b -Q "CREATE DATABASE [$DB]; CREATE DATABASE [$SRC];" >/dev/null
# non-default compat level / recovery model so that "unchanged" is a meaningful assertion
sc -b -Q "ALTER DATABASE [$DB] SET COMPATIBILITY_LEVEL = 140; ALTER DATABASE [$DB] SET RECOVERY FULL;" >/dev/null
duck "$WORK/small.duckdb" "CREATE TABLE small_cm AS SELECT i AS id, 'row' || i AS name FROM range(3) t(i)"

# ---------------------------------------------------------------------------------------------------------------
echo "== 1. guard (R1) =="
xp "$WORK/g1.out" Sales "$WORK/small.duckdb" --drop-existing
check "sales drop exit"    "$RC" "3"
check "sales drop message" "$(cntf "refusing DROP DATABASE on 'Sales': not a disposable database (pattern ^d2m_)" "$WORK/g1.out")" "1"

xp "$WORK/g2.out" d2m_nonexistent_db "$WORK/small.duckdb"
check "missing db w/o --create-database exit" "$RC" "3"
check "missing db not created" "$(q "SELECT COUNT(*) FROM sys.databases WHERE name = 'd2m_nonexistent_db'")" "0"

# another session connected to d2m_large: --drop-existing must refuse, list it, and not disconnect it
sc -b -d "$DB" -Q "WAITFOR DELAY '00:00:15'" >"$WORK/holder.out" 2>&1 & hpid=$!
sleep 3
xp "$WORK/g3.out" "$DB" "$WORK/small.duckdb" --drop-existing
check "drop with live session exit" "$RC" "3"
check_pos "drop output names a session / sqlcmd" "$(cnt 'session|sqlcmd' "$WORK/g3.out")"
HRC=0; wait $hpid || HRC=$?
check "other session finished normally" "$HRC" "0"
check "no error in other session" "$(cnt 'terminated|error|Msg ' "$WORK/holder.out")" "0"

before=$(q "SELECT CAST(compatibility_level AS varchar(5)) + '/' + recovery_model_desc FROM sys.databases WHERE name = '$DB'")
xp "$WORK/g4.out" "$DB" "$WORK/small.duckdb"
after=$(q "SELECT CAST(compatibility_level AS varchar(5)) + '/' + recovery_model_desc FROM sys.databases WHERE name = '$DB'")
check "load into existing db exit" "$RC" "0"
check "compat level + recovery model unchanged" "$after" "$before"
check "compat/recovery value is what we set" "$before" "140/FULL"
check "small_cm loaded" "$(rows dbo.small_cm)" "3"

# ---------------------------------------------------------------------------------------------------------------
echo "== 2. into-existing pre-flight (R2/R3) =="
ddl "$DB" "
CREATE TABLE dbo.pf_len   (id int NOT NULL, code varchar(3) NULL);
CREATE TABLE dbo.pf_nn    (id int NOT NULL, name varchar(10) NOT NULL);
CREATE TABLE dbo.pf_rng   (id int NOT NULL, t tinyint NULL);
CREATE TABLE dbo.pf_scale (id int NOT NULL, amt decimal(10,2) NULL);
CREATE TABLE dbo.pf_extra (id int NOT NULL);
CREATE TABLE dbo.pf_ok    (id int NOT NULL PRIMARY KEY, s varchar(10) NULL);"
duck "$WORK/pf.duckdb" "
CREATE TABLE pf_len   AS SELECT 1 AS id, 'ABCD' AS code;
CREATE TABLE pf_nn    AS SELECT 1 AS id, NULL::VARCHAR AS name;
CREATE TABLE pf_rng   AS SELECT 1 AS id, 256 AS t;
CREATE TABLE pf_scale AS SELECT 1 AS id, 1.2345::DECIMAL(10,4) AS amt;
CREATE TABLE pf_extra AS SELECT 1 AS id, 'x' AS extra;
CREATE TABLE pf_ok    AS SELECT i AS id, 'v' || i AS s FROM range(100) t(i);"

xp "$WORK/p1.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_len
check "length: exit 1" "$RC" "1"
check_pos "length: names column + LENGTH" "$(cnt 'LENGTH.*code|code.*LENGTH' "$WORK/p1.out")"
check_pos "length: max length 4 > 3" "$(cnt 'max length 4 > 3' "$WORK/p1.out")"
check "length: no rows moved" "$(rows dbo.pf_len)" "0"

xp "$WORK/p2.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_nn
check "not null: exit 1" "$RC" "1"
check_pos "not null: NOT_NULL + column" "$(cnt 'NOT_NULL.*name|name.*NOT_NULL' "$WORK/p2.out")"
check "not null: no rows moved" "$(rows dbo.pf_nn)" "0"

xp "$WORK/p3.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_rng
check "tinyint 256: exit 1" "$RC" "1"
check_pos "tinyint 256: RANGE + column" "$(cnt 'RANGE.*t\b|\bt\b.*RANGE' "$WORK/p3.out")"
check "tinyint 256: no rows moved" "$(rows dbo.pf_rng)" "0"

xp "$WORK/p4.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_scale
check "scale: exit 1" "$RC" "1"
check_pos "scale: SCALE + column" "$(cnt 'SCALE.*amt|amt.*SCALE' "$WORK/p4.out")"
xp "$WORK/p4b.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_scale --allow-rescale
check "scale: --allow-rescale exit 0" "$RC" "0"
check "scale: --allow-rescale loaded" "$(rows dbo.pf_scale)" "1"
check "scale: rescaled value (rounded or truncated to 2dp)" "$(qd "$DB" "SELECT CASE WHEN amt IN (1.23, 1.24) THEN 'ok' ELSE CAST(amt AS varchar(20)) END FROM dbo.pf_scale")" "ok"

xp "$WORK/p5.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_extra
check "extra column: exit 1" "$RC" "1"
check_pos "extra column: names it" "$(cnt 'extra' "$WORK/p5.out")"
xp "$WORK/p5b.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_extra --ignore-extra-columns
check "extra column: --ignore-extra-columns exit 0" "$RC" "0"
check "extra column: loaded" "$(rows dbo.pf_extra)" "1"

mod0=$(qd "$DB" "SELECT CONVERT(varchar(30), modify_date, 121) FROM sys.objects WHERE object_id = OBJECT_ID('dbo.pf_ok')")
xp "$WORK/p6.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_ok --check-only
check "check-only: exit 0" "$RC" "0"
check "check-only: no rows moved" "$(rows dbo.pf_ok)" "0"
sleep 1
xp "$WORK/p7.out" "$DB" "$WORK/pf.duckdb" --into-existing --tables pf_ok --check-pk full
check "pf_ok load: exit 0" "$RC" "0"
check "pf_ok load: rows" "$(rows dbo.pf_ok)" "100"
mod1=$(qd "$DB" "SELECT CONVERT(varchar(30), modify_date, 121) FROM sys.objects WHERE object_id = OBJECT_ID('dbo.pf_ok')")
check "no DDL on target (modify_date unchanged)" "$mod1" "$mod0"

# ---------------------------------------------------------------------------------------------------------------
echo "== 3. partitioned load with SWITCH (R4), $ROWS rows =="
codes=$(awk 'BEGIN{for(i=0;i<25;i++) printf "%sAA%c", (i?",":""), 65+i}' | sed "s/\([A-Z][A-Z][A-Z]\)/'\1'/g")
bounds=$(echo "$codes" | cut -d, -f2-)       # RANGE RIGHT: 24 boundaries -> 25 partitions, AAA lives in partition 1
cols=""; for k in 1 2 3 4 5 6 7 8; do cols="$cols c$k varchar(16) COLLATE Latin1_General_100_CI_AS NOT NULL,"; done
ddl "$DB" "
CREATE PARTITION FUNCTION pf_mkt (char(3)) AS RANGE RIGHT FOR VALUES ($bounds);
CREATE PARTITION SCHEME ps_mkt AS PARTITION pf_mkt ALL TO ([PRIMARY]);
CREATE TABLE dbo.big (mkt char(3) NOT NULL, $cols id int NOT NULL, v decimal(18,2) NULL,
  CONSTRAINT pk_big PRIMARY KEY CLUSTERED (mkt, c1, c2, c3, c4, c5, c6, c7, c8, id) WITH (DATA_COMPRESSION = PAGE) ON ps_mkt(mkt)) ON ps_mkt(mkt);
CREATE NONCLUSTERED INDEX ix_big_c1 ON dbo.big (c1) WHERE v IS NOT NULL WITH (DATA_COMPRESSION = ROW) ON ps_mkt(mkt);"
check "big: 25 partitions" "$(qd "$DB" "SELECT COUNT(*) FROM sys.partitions WHERE object_id = OBJECT_ID('dbo.big') AND index_id = 1")" "25"
ixbefore=$(qd "$DB" "SELECT name COLLATE DATABASE_DEFAULT + ':' + type_desc COLLATE DATABASE_DEFAULT + ':' + ISNULL(filter_definition,'') COLLATE DATABASE_DEFAULT FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.big') ORDER BY name")

dcols=""; for k in 1 2 3 4 5 6 7 8; do dcols="$dcols 'c${k}_' || (hash(i + $k) % 9973) AS c$k,"; done
duck "$WORK/big.duckdb" "CREATE TABLE big AS SELECT 'AA' || chr(65 + CAST(i % 25 AS INTEGER)) AS mkt, $dcols
  CAST(i AS INTEGER) AS id, CASE WHEN i % 10 = 0 THEN NULL ELSE CAST((i % 1000) / 7.0 AS DECIMAL(18,2)) END AS v FROM range($ROWS) t(i)"
check "duckdb big rows" "$(duckq "$WORK/big.duckdb" 'select count(*) from big')" "$ROWS"

# NOLOCK reader running during the load: staging is invisible until each SWITCH, so counts are multiples of one partition
: > "$WORK/nolock.txt"; touch "$WORK/reader.run"
( while [ -f "$WORK/reader.run" ]; do qd "$DB" "SELECT COUNT_BIG(*) FROM dbo.big WITH (NOLOCK)" >> "$WORK/nolock.txt" || true; done ) &
rpid=$!
t0=$(date +%s)
xp "$WORK/l1.out" "$DB" "$WORK/big.duckdb" --into-existing --tables big --split big=column:mkt --strategy big=switch -j 8 \
   --state-dir "$WORK/state" --verify checksum --log "$WORK/load.log" --summary "$WORK/load.json"
echo "  load took $(( $(date +%s) - t0 ))s"
rm -f "$WORK/reader.run"; wait $rpid || true
tail -n 15 "$WORK/l1.out" | sed 's/^/    | /'
check "switch load exit 0" "$RC" "0"
check "target rows" "$(rows dbo.big)" "$ROWS"
check "25 non-empty partitions" "$(qd "$DB" "SELECT COUNT(*) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('dbo.big') AND index_id = 1 AND row_count > 0")" "25"
check "each partition = $PART rows" "$(qd "$DB" "SELECT COUNT(*) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('dbo.big') AND index_id = 1 AND row_count = $PART")" "25"
check "no __stg_ tables left" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE '%[_][_]stg[_]%'")" "0"
check "indexes unchanged by load" "$(qd "$DB" "SELECT name COLLATE DATABASE_DEFAULT + ':' + type_desc COLLATE DATABASE_DEFAULT + ':' + ISNULL(filter_definition,'') COLLATE DATABASE_DEFAULT FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.big') ORDER BY name")" "$ixbefore"
check "summary json exists" "$([ -s "$WORK/load.json" ] && echo yes)" "yes"
check_pos "summary has \"ok\": true" "$(cnt '"ok": *true' "$WORK/load.json")"
check_pos "log has STMT lines" "$(cnt 'STMT' "$WORK/load.log")"
obs=$(grep -E '^[0-9]+$' "$WORK/nolock.txt" | sort -un | tr '\n' ' ' || true)
echo "  NOLOCK counts observed during load: ${obs:-<none>}"
check "NOLOCK counts all multiples of $PART" "$(grep -E '^[0-9]+$' "$WORK/nolock.txt" | awk -v p="$PART" '$1 % p != 0 {b++} END {print b+0}')" "0"
check "NOLOCK reader saw at least one count" "$([ -n "$obs" ] && echo yes)" "yes"

echo "-- kill and rerun (kill after ${KILL_AFTER}s) --"
ddl "$DB" "TRUNCATE TABLE dbo.big"
xp_kill() { RC=0; timeout -s KILL "${KILL_AFTER}s" "$BIN" export "$WORK/big.duckdb" --to server --host "$HOST" --port "$PORT" --user "$USER" --database "$DB" \
   --into-existing --tables big --split big=column:mkt --strategy big=switch -j 8 --state-dir "$WORK/state2" --log "$WORK/kill.log" \
   >"$WORK/kill1.out" 2>&1 || RC=$?; }
xp_kill
echo "  first run exit=$RC (137 = killed), rows visible so far: $(rows dbo.big), staging tables left: $(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE '%[_][_]stg[_]%'")"
[ "$RC" = 137 ] || echo "  NOTE: the load finished before the kill; lower KILL_AFTER (or raise ROWS) for a real mid-way kill"
check "first run was killed mid-way" "$RC" "137"
sleep 3   # let the server notice the dead connections and roll back their open bulk sessions
xp "$WORK/l2.out" "$DB" "$WORK/big.duckdb" --into-existing --tables big --split big=column:mkt --strategy big=switch -j 8 \
   --state-dir "$WORK/state2" --log "$WORK/rerun.log" --summary "$WORK/rerun.json"
if [ "$RC" = 1 ]; then
  echo "  NOTE: rerun refused (kill between SWITCH and marker?); retrying with --replace-partitions --drop-old"
  xp "$WORK/l2.out" "$DB" "$WORK/big.duckdb" --into-existing --tables big --split big=column:mkt --strategy big=switch -j 8 \
     --state-dir "$WORK/state2" --log "$WORK/rerun.log" --summary "$WORK/rerun.json" --replace-partitions --drop-old
fi
tail -n 10 "$WORK/l2.out" | sed 's/^/    | /'
check "rerun exit 0" "$RC" "0"
check "rerun rows = $ROWS" "$(rows dbo.big)" "$ROWS"
check "no duplicates" "$(qd "$DB" "SELECT CASE WHEN COUNT_BIG(*) = (SELECT COUNT_BIG(*) FROM (SELECT DISTINCT mkt, c1, c2, c3, c4, c5, c6, c7, c8, id FROM dbo.big) d) THEN 'unique' ELSE 'DUPLICATES' END FROM dbo.big")" "unique"
check "no __stg_ tables after rerun" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE '%[_][_]stg[_]%'")" "0"
skipped=$(cat "$WORK/l2.out" "$WORK/rerun.log" 2>/dev/null | awk '/skipped as already done/ { for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+$/) { print $i; exit } }')
check_pos "rerun skipped done jobs ('skipped as already done')" "${skipped:-0}"

# ---------------------------------------------------------------------------------------------------------------
echo "== 4. heap-then-index + serial (R4.2) =="
ddl "$DB" "
CREATE TABLE dbo.t2 (id int NOT NULL, a varchar(20) NULL, b varchar(20) NULL, n int NULL, CONSTRAINT pk_t2 PRIMARY KEY CLUSTERED (id));
CREATE NONCLUSTERED INDEX ix_t2_a ON dbo.t2 (a);
CREATE NONCLUSTERED INDEX ix_t2_b ON dbo.t2 (b) INCLUDE (n) WHERE b IS NOT NULL;
CREATE TABLE dbo.t3 (id int NOT NULL PRIMARY KEY, s varchar(20) NULL);"
duck "$WORK/t2.duckdb" "
CREATE TABLE t2 AS SELECT CAST(i AS INTEGER) AS id, 'a' || (i % 500) AS a, CASE WHEN i % 7 = 0 THEN NULL ELSE 'b' || (i % 900) END AS b, CAST(i % 1000 AS INTEGER) AS n FROM range(200000) t(i);
CREATE TABLE t3 AS SELECT CAST(i AS INTEGER) AS id, 's' || i AS s FROM range(5000) t(i);"
idxsnap() { qd "$DB" "SELECT name COLLATE DATABASE_DEFAULT + ':' + type_desc COLLATE DATABASE_DEFAULT + ':' + CAST(is_primary_key AS varchar(1)) + ':' + CAST(is_unique AS varchar(1)) + ':' + ISNULL(filter_definition,'') COLLATE DATABASE_DEFAULT FROM sys.indexes WHERE object_id = OBJECT_ID('$1') ORDER BY name"; }
snap_before=$(idxsnap dbo.t2)
xp "$WORK/h1.out" "$DB" "$WORK/t2.duckdb" --into-existing --tables t2 --split t2=hash:id:4 --strategy t2=heap-then-index -j 4 --verify checksum --log "$WORK/h1.log"
check "heap-then-index exit 0" "$RC" "0"
check "t2 rows" "$(rows dbo.t2)" "200000"
check "t2 index names/types/filters identical after load" "$(idxsnap dbo.t2)" "$snap_before"
check "t2 has 3 indexes" "$(qd "$DB" "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.t2') AND index_id > 0")" "3"
check "t2 indexes enabled" "$(qd "$DB" "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.t2') AND is_disabled = 1")" "0"
check "t2 PK usable (duplicate insert rejected)" "$(qd "$DB" "BEGIN TRY INSERT INTO dbo.t2 (id) VALUES (1); SELECT 'dup accepted'; END TRY BEGIN CATCH SELECT 'rejected'; END CATCH")" "rejected"
snap3=$(idxsnap dbo.t3)
xp "$WORK/h2.out" "$DB" "$WORK/t2.duckdb" --into-existing --tables t3 --strategy '*'=serial -j 2
check "serial exit 0" "$RC" "0"
check "t3 rows" "$(rows dbo.t3)" "5000"
check "t3 indexes unchanged" "$(idxsnap dbo.t3)" "$snap3"

# ---------------------------------------------------------------------------------------------------------------
echo "== 5. verification (R9) =="
# big was already verified with --verify checksum in section 3 (exit 0 above).
# NOT covered here: corrupt-a-row-and-detect. --verify runs only after a load that appends, so detecting a server-side
# change needs a standalone verify entry point; add it once the CLI has one (handoff-G cases 2-4).
ddl "$DB" "CREATE TABLE dbo.v1 (id int NOT NULL PRIMARY KEY, s varchar(20) NULL, d decimal(10,2) NULL, ts datetime2(3) NULL);"
duck "$WORK/v1.duckdb" "CREATE TABLE v1 AS SELECT CAST(i AS INTEGER) AS id, 's' || i AS s, CAST(i / 4.0 AS DECIMAL(10,2)) AS d, TIMESTAMP '2024-01-01 00:00:00' + to_seconds(i) AS ts FROM range(1000) t(i)"
xp "$WORK/v1.out" "$DB" "$WORK/v1.duckdb" --into-existing --tables v1 --verify checksum
check "verify checksum exit 0" "$RC" "0"
check_pos "verify checksum reported" "$(cnt 'checksum' "$WORK/v1.out")"
ddl "$DB" "TRUNCATE TABLE dbo.v1"
xp "$WORK/v2.out" "$DB" "$WORK/v1.duckdb" --into-existing --tables v1 --verify except --sample 3
check "verify except exit 0" "$RC" "0"
ddl "$DB" "TRUNCATE TABLE dbo.v1"
xp "$WORK/v3.out" "$DB" "$WORK/v1.duckdb" --into-existing --tables v1 --verify counts
check "verify counts exit 0" "$RC" "0"
check "v1 rows" "$(rows dbo.v1)" "1000"

# ---------------------------------------------------------------------------------------------------------------
echo "== 6. safe reads (R6) on $SRC =="
ddl "$SRC" "
CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, v nvarchar(50));
INSERT INTO dbo.t (v) SELECT TOP 200000 'v' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS varchar(10)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
CREATE TABLE dbo.small (k int PRIMARY KEY, s varchar(10));
INSERT INTO dbo.small VALUES (1, 'a'), (2, 'b');
CREATE TABLE dbo.empty_t (x int);
CREATE TABLE dbo.flag (done bit); INSERT INTO dbo.flag VALUES (0);"
imp() { local out=$1 f=$2; shift 2; run "$out" "$BIN" import "$f" "${conn[@]}" --database "$SRC" --overwrite --split-threshold 50000 -j 4 "$@"; }
hold() { sc -b -d "$SRC" -Q "BEGIN TRAN; UPDATE dbo.small SET s='z' WHERE k=1; WAITFOR DELAY '00:00:14'; COMMIT" >"$WORK/hold.out" 2>&1 & hpid=$!; sleep 2; }

echo "-- case 1: lock holder -> exit 1 after --lock-wait"
hold
( imp "$WORK/s1.out" "$WORK/s1.duckdb" --lock-wait 6; echo "$RC" > "$WORK/s1.rc" ) & ipid=$!
sleep 4
blocked=$(qd "$SRC" "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0")
wait $ipid || true; wait $hpid || true
check "lock holder: import exit 1" "$(cat "$WORK/s1.rc")" "1"
check_pos "lock holder: message names dbo.small" "$(cnt 'dbo\.small is locked by other sessions after waiting 6s' "$WORK/s1.out")"
check "holder never blocked by the import" "$blocked" "0"

echo "-- case 2: --on-lock skip + manifest"
hold
imp "$WORK/s2.out" "$WORK/s2.duckdb" --on-lock skip --lock-wait 3 --manifest "$WORK/m.csv"
wait $hpid || true
check "on-lock skip: exit 0" "$RC" "0"
check_pos "on-lock skip: warning printed" "$(cnt 'dbo\.small: skipped, still locked after 3s' "$WORK/s2.out")"
check "on-lock skip: small empty in duckdb" "$(duckq "$WORK/s2.duckdb" 'select count(*) from small')" "0"
check_pos "manifest row -1 for dbo.small" "$(cnt 'small.*,-1,|,-1,.*small' "$WORK/m.csv")"

echo "-- case 5: snapshot advice and --isolation snapshot"
sc -b -Q "ALTER DATABASE [$SRC] SET ALLOW_SNAPSHOT_ISOLATION ON" >/dev/null
imp "$WORK/s5.out" "$WORK/s5.duckdb"
check "snapshot advice: exit 0" "$RC" "0"
check_pos "snapshot advice warning" "$(cnt 'ALLOW_SNAPSHOT_ISOLATION on' "$WORK/s5.out")"
sc -b -d "$SRC" -Q "UPDATE dbo.small SET s = 'a' WHERE k = 1" >/dev/null   # case 1's holder committed 'z'
hold
imp "$WORK/s5b.out" "$WORK/s5b.duckdb" --isolation snapshot
SRC_RC=$RC
wait $hpid || true
check "snapshot with holder: exit 0 (no wait)" "$SRC_RC" "0"
check "snapshot sees committed value, not the holder's" "$(duckq "$WORK/s5b.duckdb" 'select s from small where k = 1')" "a"
sc -b -Q "ALTER DATABASE [$SRC] SET ALLOW_SNAPSHOT_ISOLATION OFF" >/dev/null
imp "$WORK/s5c.out" "$WORK/s5c.duckdb" --isolation snapshot
check "snapshot off: exit 1" "$RC" "1"
check_pos "snapshot off: message" "$(cnt 'neither ALLOW_SNAPSHOT_ISOLATION nor READ_COMMITTED_SNAPSHOT' "$WORK/s5c.out")"

echo "-- case 6: gate"
imp "$WORK/s6.out" "$WORK/s6.duckdb" --wait-for "SELECT done FROM dbo.flag" --wait-timeout 3
check "gate timeout: exit 1" "$RC" "1"
check_pos "gate timeout: message" "$(cnt 'gate query did not return 1 within 3s' "$WORK/s6.out")"
imp "$WORK/s6b.out" "$WORK/s6b.duckdb" --wait-for "DELETE FROM dbo.flag" --wait-timeout 3
check "gate DELETE: exit 3 (read-only)" "$RC" "3"
check "gate DELETE: flag row still there" "$(qd "$SRC" "SELECT COUNT(*) FROM dbo.flag")" "1"
# (the 'flag flipped after 4s -> starts on the next 10s poll' case is skipped to keep the run short)

echo "-- case 7: manifest csv + json"
imp "$WORK/s7.out" "$WORK/s7.duckdb" --manifest "$WORK/in.manifest.csv"
check "manifest: exit 0" "$RC" "0"
check "manifest csv written" "$([ -s "$WORK/in.manifest.csv" ] && echo yes)" "yes"
check "manifest json written" "$([ -s "$WORK/in.manifest.json" ] && echo yes)" "yes"
check_pos "manifest has the empty table" "$(cnt 'empty_t' "$WORK/in.manifest.csv")"
check "import t rows" "$(duckq "$WORK/s7.duckdb" 'select count(*) from t')" "200000"

# ---------------------------------------------------------------------------------------------------------------
echo "== 7. translator (R7) =="
printf 'SELECT ROUND(x, 2, 1) AS r FROM dbo.t;\n' > "$WORK/round.sql"
run "$WORK/t1.out" "$BIN" transpile "$WORK/round.sql" --strict
check "transpile --strict: exit 1" "$RC" "1"
check_pos "transpile --strict: W-ROUND-TRUNC" "$(cnt 'W-ROUND-TRUNC' "$WORK/t1.out")"
run "$WORK/t2.out" "$BIN" transpile "$WORK/round.sql"
check "transpile: exit 0" "$RC" "0"
check_pos "transpile: warning printed" "$(cnt 'W-ROUND-TRUNC' "$WORK/t2.out")"

duck "$WORK/math.duckdb" "CREATE TABLE dummy AS SELECT 1 AS a"
printf 'SELECT 7 * 1.0 / 3 AS r INTO dbo.mres;\n' > "$WORK/math.sql"
run "$WORK/t3.out" "$BIN" run "$WORK/math.duckdb" "$WORK/math.sql" --mssql-math
check "run --mssql-math: exit 0" "$RC" "0"
check "7 * 1.0 / 3 = 2.333333" "$(duckq "$WORK/math.duckdb" 'select cast(r as varchar) from mres' 2>&1 | tr -d ' ')" "2.333333"

duck "$WORK/ci.duckdb" "CREATE TABLE words AS SELECT * FROM (VALUES ('Abc'), ('abc'), ('xyz')) t(w)"
run "$WORK/t4.out" "$BIN" audit-ci "$WORK/ci.duckdb"
check "audit-ci: exit 1" "$RC" "1"
check_pos "audit-ci: lists the column" "$(cnt 'words' "$WORK/t4.out")"
duck "$WORK/ci2.duckdb" "CREATE TABLE words AS SELECT * FROM (VALUES ('abc'), ('xyz')) t(w)"
run "$WORK/t4b.out" "$BIN" audit-ci "$WORK/ci2.duckdb"
check "audit-ci clean data: exit 0" "$RC" "0"

run "$WORK/t5.out" "$BIN" verify-math "${conn[@]}" --cases 200
check "verify-math --cases 200: exit 0" "$RC" "0"

# ---------------------------------------------------------------------------------------------------------------
echo "== 8. swap (R10) =="
ddl "$DB" "
CREATE TABLE dbo.kpi_new (id int, v varchar(10)); INSERT INTO dbo.kpi_new VALUES (1,'n1'),(2,'n2'),(3,'n3');
CREATE TABLE dbo.kpi     (id int, v varchar(10)); INSERT INTO dbo.kpi VALUES (9,'old');"
run "$WORK/w1.out" "$BIN" swap "${conn[@]}" --database "$DB" --pairs kpi_new=kpi
check "swap exit 0" "$RC" "0"
check "kpi has the new rows" "$(rows dbo.kpi)" "3"
check "kpi new content" "$(qd "$DB" "SELECT MIN(v) FROM dbo.kpi")" "n1"
check "kpi__old_* exists" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'kpi[_][_]old[_]%'")" "1"
check "old table holds the old row" "$(qd "$DB" "SELECT SUM(id) FROM dbo.kpi__old_$(qd "$DB" "SELECT RIGHT(name, 12) FROM sys.tables WHERE name LIKE 'kpi[_][_]old[_]%'")")" "9"
check "kpi_new is gone" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name = 'kpi_new'")" "0"
run "$WORK/w2.out" "$BIN" drop-old "${conn[@]}" --database "$DB" --before "$(date -u -d tomorrow +%F)"
check "drop-old exit 0" "$RC" "0"
check "kpi__old_* dropped" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'kpi[_][_]old[_]%'")" "0"
check "kpi itself kept" "$(rows dbo.kpi)" "3"

# ---------------------------------------------------------------------------------------------------------------
echo "== 9. plan (R8) and exit codes =="
ddl "$DB" "CREATE TABLE dbo.plan_t (id int NOT NULL PRIMARY KEY, s varchar(20) NULL, d decimal(10,2) NULL, ts datetime2(3) NULL);"
duck "$WORK/plan.duckdb" "CREATE TABLE plan_t AS SELECT * FROM (SELECT * FROM range(10) t(id)) x, (SELECT 'a' AS s, 1.5::DECIMAL(10,2) AS d, TIMESTAMP '2024-01-01' AS ts)"
xp "$WORK/pl2.out" "$DB" "$WORK/plan.duckdb" --into-existing --tables plan_t --plan
check "plan exit 0" "$RC" "0"
check_pos "plan prints BULK INSERT lines" "$(cnt 'BULK INSERT' "$WORK/pl2.out")"
check "plan sent nothing (plan_t empty)" "$(rows dbo.plan_t)" "0"
big_before=$(rows dbo.big)
xp "$WORK/pl3.out" "$DB" "$WORK/big.duckdb" --into-existing --tables big --split big=column:mkt --strategy big=switch -j 8 --state-dir "$WORK/state_plan" --plan
check "switch plan exit 0" "$RC" "0"
check_pos "switch plan prints SWITCH statements" "$(cnt 'SWITCH' "$WORK/pl3.out")"
check "switch plan: big unchanged" "$(rows dbo.big)" "$big_before"
check "switch plan: no staging tables created" "$(qd "$DB" "SELECT COUNT(*) FROM sys.tables WHERE name LIKE '%[_][_]stg[_]%'")" "0"

run "$WORK/ec.out" "$BIN" export "$WORK/v1.duckdb" --to server --host "$HOST" --port 1 --user "$USER" --database "$DB" --into-existing --tables v1
check "unreachable server: exit 2" "$RC" "2"

echo
echo "Exit codes observed: 0 ok (loads, plan, check-only)  1 check failed (pre-flight, lock, gate, strict, audit-ci)"
echo "                     2 could not run (unreachable server)  3 guard refusal (non-disposable db, live session, missing db, read-only gate)"
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
