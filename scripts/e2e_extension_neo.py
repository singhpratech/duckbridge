#!/usr/bin/env python3
"""End-to-end test of the DuckDB 2.0 (duckdb-neo) build of the extension, on the `duckdb` Python package
(DuckDB 2.0 alpha builds ship no CLI binary, so scripts/e2e_extension.sh cannot run them).

    cd extension && make release-neo && cd ..
    <venv with duckdb 2.0>/bin/python -I scripts/e2e_extension_neo.py

Env: EXT (default extension/build/neo/duckbridge.duckdb_extension), MSSQL_HOST/PORT/USER/PASSWORD,
SQLCMD, KEEP=1 (keep the disposable databases). The interface under test is docs/plan-2.0-extension.md:
bridge_tables, bridge_scan, PRAGMA bridge_import, PRAGMA bridge_run, COPY ... TO 'mssql://...'
(FORMAT duckbridge), bridge_transpile / bridge_prelude, and the old bridge_import/bridge_export/
bridge_run table functions failing with a pointer to the replacement.

It creates two disposable SQL Server databases (d2m_neo_src: the fixture; d2m_neo_out: the COPY
target, since COPY never creates a database) and drops both at the end unless KEEP=1. Values are
compared through a canonical text per column (SQL Server CONVERT on one side, DuckDB cast on the
other). Every check reports PASS or FAIL and the run continues; exit status 0 when nothing failed.
"""
import decimal
import os
import re
import subprocess
import sys
import traceback
import urllib.parse

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXT = os.path.abspath(os.environ.get("EXT", os.path.join(ROOT, "extension/build/neo/duckbridge.duckdb_extension")))
HOST = os.environ.get("MSSQL_HOST", "localhost")
PORT = os.environ.get("MSSQL_PORT", "14319")
USER = os.environ.get("MSSQL_USER", "sa")
PASSWORD = os.environ.get("MSSQL_PASSWORD", "D2m!Strong#2019")
SQLCMD = os.environ.get("SQLCMD", "/opt/mssql-tools18/bin/sqlcmd")
KEEP = os.environ.get("KEEP") == "1"
DSN = f"mssql://{USER}:{urllib.parse.quote(PASSWORD, safe='')}@{HOST}:{PORT}"
SRC_DB = "d2m_neo_src"
OUT_DB = "d2m_neo_out"
SRC = f"{DSN}/{SRC_DB}"
OUT = f"{DSN}/{OUT_DB}"

passed = failed = 0


def check(name, got, want):
    global passed, failed
    if got == want:
        print(f"  PASS {name} ({str(got)[:100]})")
        passed += 1
    else:
        print(f"  FAIL {name}: got {got!r} want {want!r}")
        failed += 1


def fail(name, why):
    global failed
    print(f"  FAIL {name}: {why}")
    failed += 1


def guarded(fn):
    """Run a check group; an unexpected exception is one FAIL, the run goes on."""
    def run(*a, **k):
        try:
            return fn(*a, **k)
        except Exception as e:  # noqa: BLE001
            fail(fn.__name__, f"{type(e).__name__}: {str(e)[:300]}")
            if os.environ.get("TRACE"):
                traceback.print_exc()
    return run


def _sqlcmd(db, sql, extra):
    cmd = [SQLCMD, "-S", f"{HOST},{PORT}", "-U", USER, "-P", PASSWORD, "-C", "-b", *extra, "-Q", sql]
    if db:
        cmd[1:1] = ["-d", db]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"sqlcmd failed: {(r.stdout + r.stderr).strip()[:400]}")
    return r.stdout


def q(sql, db=None):
    """Trimmed single-column output, one value per line (as in e2e_extension.sh)."""
    out = _sqlcmd(db, sql, ["-h", "-1", "-W"])
    return "\n".join(line for line in out.splitlines() if line and "rows affected" not in line)


def qrows(sql, db=None):
    """Rows as lists of stripped fields, separated by '|', unlimited width for the (max) columns."""
    out = _sqlcmd(db, "SET NOCOUNT ON; " + sql, ["-s", "|", "-y", "0"])  # -y 0 prints no header and cannot combine with -h -1
    lines = [line for line in out.splitlines() if line.strip()]
    return [[f.strip() for f in line.split("|")] for line in lines]


# ---------------------------------------------------------------------------------------------
# fixture

# (column, SQL type, kind, rows 1..5: typical, low edge, high edge, NULL, empty/zero)
NL = "NULL"
TYPES = [
    ("c_tinyint", "tinyint", "int", ["7", "0", "255", NL, "1"]),
    ("c_smallint", "smallint", "int", ["-12", "-32768", "32767", NL, "0"]),
    ("c_int", "int", "int", ["123456", "-2147483648", "2147483647", NL, "0"]),
    ("c_bigint", "bigint", "int", ["9000000000", "-9223372036854775808", "9223372036854775807", NL, "0"]),
    ("c_bit", "bit", "bit", ["1", "0", "1", NL, "0"]),
    ("c_decimal", "decimal(18,4)", "dec", ["1234.5678", "-99999999999999.9999", "99999999999999.9999", NL, "0"]),
    ("c_numeric", "numeric(38,10)", "dec", ["12345678901234567890123456.7890123456", "-9999999999999999999999999999.9999999999", "9999999999999999999999999999.9999999999", NL, "0"]),
    ("c_money", "money", "money", ["19.99", "-922337203685477.5808", "922337203685477.5807", NL, "0"]),
    ("c_smallmoney", "smallmoney", "money", ["5.25", "-214748.3648", "214748.3647", NL, "0"]),
    ("c_float", "float", "float", ["1.5", "-1.7976931348623157E+308", "2.5E-300", NL, "0"]),
    ("c_real", "real", "float", ["0.25", "-3.4E+38", "1.18E-38", NL, "0"]),
    ("c_char", "char(5)", "chr", ["'ab'", "'zzzzz'", "'A'", NL, "''"]),
    ("c_varchar", "varchar(50)", "str", ["'hello world'", "'tab' + CHAR(9) + 'q''uote'", "REPLICATE('x', 50)", NL, "''"]),
    ("c_varchar_max", "varchar(max)", "str", ["'plain'", "REPLICATE(CAST('ab' AS varchar(max)), 3000)", "'end'", NL, "''"]),
    ("c_nchar", "nchar(5)", "nchr", ["N'ab'", "NCHAR(26085) + NCHAR(26412)", "N'12345'", NL, "N''"]),
    ("c_nvarchar", "nvarchar(50)", "nstr", ["N'caf' + NCHAR(233) + N' ' + NCHAR(26085) + NCHAR(26412) + NCHAR(35486)", "NCHAR(55357) + NCHAR(56832)", "N'a''b'", NL, "N''"]),
    ("c_nvarchar_max", "nvarchar(max)", "nstr", ["N'unicode ' + NCHAR(1103)", "REPLICATE(CAST(NCHAR(955) AS nvarchar(max)), 3000)", "N'end'", NL, "N''"]),
    ("c_uniqueidentifier", "uniqueidentifier", "uuid", ["'6F9619FF-8B86-D011-B42D-00C04FC964FF'", "'00000000-0000-0000-0000-000000000000'", "'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'", NL, "'12345678-1234-1234-1234-123456789ABC'"]),
    ("c_date", "date", "date", ["'2024-03-15'", "'0001-01-01'", "'9999-12-31'", NL, "'1970-01-01'"]),
    ("c_time", "time(7)", "time", ["'10:30:15.1234560'", "'00:00:00'", "'23:59:59.9999990'", NL, "'12:00:00'"]),
    ("c_datetime", "datetime", "dt", ["'2024-03-15 10:30:15.123'", "'1753-01-01 00:00:00'", "'9999-12-31 23:59:59.997'", NL, "'1970-01-01'"]),
    ("c_smalldatetime", "smalldatetime", "dt", ["'2024-03-15 10:30'", "'1900-01-01 00:00'", "'2079-06-06 23:59'", NL, "'1970-01-01'"]),
    ("c_datetime2", "datetime2(7)", "dt", ["'2024-03-15 10:30:15.1234560'", "'0001-01-01 00:00:00'", "'9999-12-31 23:59:59.9999990'", NL, "'1970-01-01'"]),
    ("c_datetimeoffset", "datetimeoffset(7)", "dto", ["'2024-03-15 10:30:15.1234560 +05:30'", "'0001-01-02 00:00:00 +00:00'", "'9999-12-31 23:59:59.9999990 +00:00'", NL, "'1970-01-01 00:00:00 -08:00'"]),
    ("c_binary", "binary(4)", "bin", ["0xDEADBEEF", "0x00000000", "0xFFFFFFFF", NL, "0x01020304"]),
    ("c_varbinary_max", "varbinary(max)", "bin", ["0x00FF10", "0x", "CAST(REPLICATE(CAST(0xAB AS varbinary(max)), 5000) AS varbinary(max))", NL, "0x00"]),
]
NTYPES_ROWS = 5


def types_ddl():
    cols = ",\n  ".join(f"[{n}] {t} NULL" for n, t, _, _ in TYPES)
    return f"CREATE TABLE dbo.types (id int NOT NULL PRIMARY KEY,\n  {cols});"


def types_inserts():
    out = []
    for i in range(NTYPES_ROWS):
        vals = ", ".join(v[i] for _, _, _, v in TYPES)
        names = ", ".join(f"[{n}]" for n, _, _, _ in TYPES)
        out.append(f"INSERT INTO dbo.types (id, {names}) VALUES ({i + 1}, {vals});")
    return "\n".join(out)


def drop_db(name):
    assert re.match(r"^d2m_", name), name
    q(f"IF DB_ID('{name}') IS NOT NULL BEGIN ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]; END")


def setup():
    drop_db(SRC_DB)
    drop_db(OUT_DB)
    q(f"CREATE DATABASE [{SRC_DB}]")
    q(f"CREATE DATABASE [{OUT_DB}]")
    q(types_ddl(), SRC_DB)
    q(types_inserts(), SRC_DB)
    q("CREATE TABLE dbo.orders (id int IDENTITY(1,1) NOT NULL PRIMARY KEY, customer_id int NOT NULL, "
      "amount decimal(12,2) NOT NULL, status varchar(10) NOT NULL, placed_at datetime2(0) NOT NULL);", SRC_DB)
    q("INSERT INTO dbo.orders (customer_id, amount, status, placed_at) "
      "SELECT n % 97 + 1, CAST((n * 37 % 10000) / 100.0 AS decimal(12,2)), CASE n % 3 WHEN 0 THEN 'new' WHEN 1 THEN 'paid' ELSE 'shipped' END, "
      "DATEADD(minute, n, '2024-01-01') FROM (SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
      "FROM sys.all_objects a CROSS JOIN sys.all_objects b) g;", SRC_DB)
    q("CREATE VIEW dbo.v_orders AS SELECT id, customer_id, amount FROM dbo.orders WHERE amount > 50;", SRC_DB)
    q("CREATE SCHEMA sales;", SRC_DB)
    q("CREATE TABLE sales.customers (customer_id int NOT NULL PRIMARY KEY, name nvarchar(50) NOT NULL);"
      "INSERT INTO sales.customers VALUES (1, N'Ada'), (2, N'Grace'), (3, N'Linus');", SRC_DB)


def teardown():
    if KEEP:
        print(f"== KEEP=1: leaving {SRC_DB} and {OUT_DB} ==")
        return
    drop_db(SRC_DB)
    drop_db(OUT_DB)


# ---------------------------------------------------------------------------------------------
# canonical text per column

def sql_expr(c, kind):
    e = {
        "int": f"CONVERT(varchar(40), {c})",
        "bit": f"CONVERT(varchar(1), {c})",
        "dec": f"CONVERT(varchar(60), {c})",
        "money": f"CONVERT(varchar(40), {c}, 2)",
        "float": f"CONVERT(varchar(40), CAST({c} AS float), 3)",
        "str": f"CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2)",
        "chr": f"CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2)",
        "nstr": f"CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2)",
        "nchr": f"CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2)",
        "uuid": f"CONVERT(varchar(36), {c})",
        "date": f"CONVERT(varchar(10), {c}, 23)",
        "time": f"CONVERT(varchar(20), {c}, 121)",
        "dt": f"CONVERT(varchar(30), {c}, 121)",
        "dto": f"CONVERT(varchar(40), SWITCHOFFSET({c}, '+00:00'), 121)",
        "bin": f"CONVERT(varchar(max), {c}, 2)",
    }[kind]
    return f"CASE WHEN {c} IS NULL THEN 'NULL' ELSE 'V:' + {e} END"


def duck_expr(c, kind):
    return {
        "str": c, "chr": c, "nstr": c, "nchr": c,
        "float": f"{c}::DOUBLE",
        "bit": f"{c}::INTEGER::VARCHAR",
        "bin": f"hex({c})",
    }.get(kind, f"{c}::VARCHAR")


def norm_ts(s):
    s = s.strip().replace("T", " ")
    s = re.sub(r"\s*\+00(:00)?$", "", s)
    if "." in s:
        base, frac = s.split(".", 1)
        frac = frac.rstrip("0")
        s = base + ("." + frac if frac else "")
    return s


def _norm(kind, t):
    """t: a text (SQL Server side) or a Python value (DuckDB side) -> comparable canonical string."""
    if t is None:
        return "NULL"
    if kind in ("int", "bit", "date"):
        return str(t)
    if kind in ("dec", "money"):
        return format(decimal.Decimal(str(t)), "f")
    if kind == "float":
        return repr(float(t))
    if kind == "uuid":
        return str(t).lower()
    if kind in ("time", "dt", "dto"):
        return norm_ts(str(t))
    if kind == "bin":
        return str(t).upper()
    if kind in ("chr", "nchr"):
        return str(t).rstrip(" ")
    return str(t)


def sql_side(col, kind, table="dbo.types", db=SRC_DB):
    out = {}
    for row in qrows(f"SELECT id, {sql_expr(col, kind)} FROM {table} ORDER BY id", db):
        rid, t = int(row[0]), row[1]
        if t == "NULL":
            out[rid] = "NULL"
        else:
            t = t[2:]
            if kind in ("str", "chr"):
                t = bytes.fromhex(t).decode("utf-8")
            elif kind in ("nstr", "nchr"):
                t = bytes.fromhex(t).decode("utf-16-le")
            out[rid] = _norm(kind, t)
    return out


def duck_side(con, col, kind, rel):
    return {r[0]: _norm(kind, r[1]) for r in con.sql(f"SELECT id, {duck_expr(col, kind)} FROM {rel} ORDER BY id").fetchall()}


def diff(a, b):
    for k in sorted(set(a) | set(b)):
        if a.get(k) != b.get(k):
            return f"id {k}: {str(a.get(k))[:60]!r} vs {str(b.get(k))[:60]!r}"
    return None


def one(con, sql):
    return con.sql(sql).fetchall()[0][0]


def expect_error(name, con, sql, *needles):
    try:
        rows = con.sql(sql).fetchall()
    except duckdb.Error as e:
        msg = str(e)
        if all(n in msg for n in needles):
            print(f"  PASS {name}: {msg.splitlines()[0][:140]}")
            global passed
            passed += 1
        else:
            fail(name, f"error without {needles}: {msg[:300]}")
        return
    fail(name, f"expected an error, got rows {rows[:3]!r}")


def new_con():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXT}'")
    con.execute("SET TimeZone = 'UTC'")
    return con


# ---------------------------------------------------------------------------------------------
# check groups

@guarded
def t_footer_load_transpile():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    version = con.sql("PRAGMA version").fetchall()[0][0]
    print(f"== engine {version}, extension {EXT}")
    print("== 0. footer ==")
    with open(EXT, "rb") as f:
        f.seek(-512, os.SEEK_END)  # 8 fields of 32 bytes: 3 empty, abi, ext version, duckdb version, platform, "4"
        fields = [f.read(32).rstrip(b"\0").decode() for _ in range(8)]
    check("footer abi", fields[3], "C_STRUCT_UNSTABLE")
    check("footer duckdb version = engine", fields[5], version)
    check("footer platform", fields[6], con.sql("PRAGMA platform").fetchall()[0][0])

    print("== 1. load + transpile + prelude ==")
    con.execute(f"LOAD '{EXT}'")
    check("transpile tsql->duck", one(con, "SELECT bridge_transpile('tsql','duckdb','SELECT TOP 2 ISNULL(a,0) FROM dbo.t')"), "SELECT COALESCE(a, 0) FROM main.t LIMIT 2;")
    check("transpile duck->tsql 2022", one(con, "SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2022)"), "SELECT greatest(a, b) FROM t;")
    check("transpile duck->tsql 2019", one(con, "SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2019)"), "SELECT (SELECT MAX(v) FROM (VALUES (a), (b)) AS g(v)) FROM t;")
    check("transpile NULL in", one(con, "SELECT bridge_transpile('tsql','duckdb',NULL::VARCHAR)"), None)
    check("transpile per row", con.sql("SELECT bridge_transpile('tsql','duckdb', s) FROM (VALUES ('SELECT GETDATE()'), (NULL), ('SELECT 1')) t(s)").fetchall(),
          [("SELECT current_localtimestamp();",), (None,), ("SELECT 1;",)])
    check("prelude lines", one(con, "SELECT length(string_split(bridge_prelude(true), chr(10)))"), 3)
    check("prelude nocase off", one(con, "SELECT bridge_prelude(false) NOT LIKE '%collation%'"), True)
    expect_error("bad dialect errors", con, "SELECT bridge_transpile('oracle','duckdb','x')", "unknown source dialect")


@guarded
def t_functions(con):
    print("== 2. registered functions ==")
    names = {r[0] for r in con.sql("SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'bridge\\_%' ESCAPE '\\'").fetchall()}
    for n in ("bridge_scan", "bridge_tables", "bridge_transpile", "bridge_prelude", "bridge_import", "bridge_run"):
        check(f"duckdb_functions has {n}", n in names, True)
    check("bridge_* function set", sorted(names), sorted(["bridge_scan", "bridge_tables", "bridge_transpile", "bridge_prelude", "bridge_import", "bridge_export", "bridge_run"]))
    pragmas = {r[0] for r in con.sql("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'bridge\\_%' ESCAPE '\\' AND function_type = 'pragma'").fetchall()}
    check("pragma functions", sorted(pragmas), ["bridge_import", "bridge_run"])
    try:
        copies = {r[0] for r in con.sql("SELECT copy_function_name FROM duckdb_copy_functions()").fetchall()}
        check("copy function duckbridge", "duckbridge" in copies, True)
    except duckdb.Error as e:
        print(f"  INFO duckdb_copy_functions() unavailable ({str(e)[:80]}); COPY is exercised below")


@guarded
def t_tables(con):
    print("== 3. bridge_tables ==")
    rows = con.sql(f"SELECT schema, name, kind, rows, columns, pk, import_sql FROM bridge_tables('{SRC}') ORDER BY schema, name").fetchall()
    check("bridge_tables names", [(r[0], r[1], r[2]) for r in rows],
          [("dbo", "orders", "table"), ("dbo", "types", "table"), ("dbo", "v_orders", "view"), ("sales", "customers", "table")])
    by = {(r[0], r[1]): r for r in rows}
    check("rows orders", by[("dbo", "orders")][3], 1000)
    check("rows types", by[("dbo", "types")][3], NTYPES_ROWS)
    check("rows sales.customers", by[("sales", "customers")][3], 3)
    check("columns orders", by[("dbo", "orders")][4], 5)
    check("columns types", by[("dbo", "types")][4], 1 + len(TYPES))
    check("pk orders", by[("dbo", "orders")][5], "id")
    check("pk types", by[("dbo", "types")][5], "id")
    check("pk sales.customers", by[("sales", "customers")][5], "customer_id")
    check("import_sql: CTAS from bridge_scan for tables, empty for views", [bool(r[6]) and "bridge_scan" in r[6] for r in rows], [True, True, False, True])
    check("rows of a view are not counted", by[("dbo", "v_orders")][3], 0)
    only = con.sql(f"SELECT name FROM bridge_tables('{SRC}', tables := 'sales.*')").fetchall()
    check("tables := 'sales.*'", only, [("customers",)])
    ex = con.sql(f"SELECT count(*) FROM bridge_tables('{SRC}', tables := 'dbo.*', exclude := 'dbo.v_*')").fetchall()[0][0]
    check("tables dbo.* exclude dbo.v_*", ex, 2)


@guarded
def t_scan(con):
    print("== 4. bridge_scan ==")
    check("scan types count", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.types')"), NTYPES_ROWS)
    check("scan orders count", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.orders')"), 1000)
    check("scan view count", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.v_orders')"), int(q("SELECT COUNT(*) FROM dbo.v_orders", SRC_DB)))
    check("scan schema sales", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'sales.customers')"), 3)
    check("scan bracketed name", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', '[dbo].[orders]')"), 1000)
    check("scan columns", [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM bridge_scan('{SRC}', 'dbo.orders')").fetchall()],
          ["id", "customer_id", "amount", "status", "placed_at"])
    want = int(q("SELECT COUNT(*) FROM dbo.orders WHERE amount > 50", SRC_DB))
    check("scan query mode count", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'SELECT id, amount FROM dbo.orders WHERE amount > 50')"), want)
    check("scan query mode columns", [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM bridge_scan('{SRC}', 'SELECT id, amount FROM dbo.orders WHERE amount > 50')").fetchall()], ["id", "amount"])
    check("scan nolock := true", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.orders', nolock := true)"), 1000)
    check("scan small batch_rows", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.orders', batch_rows := 77)"), 1000)
    check("scan isolation + readonly", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.orders', isolation := 'read committed', readonly := true)"), 1000)
    check("scan sum(amount) parity", str(one(con, f"SELECT sum(amount) FROM bridge_scan('{SRC}', 'dbo.orders')")), q("SELECT CONVERT(varchar(40), SUM(amount)) FROM dbo.orders", SRC_DB))
    check("scan max(placed_at) parity", norm_ts(str(one(con, f"SELECT max(placed_at)::VARCHAR FROM bridge_scan('{SRC}', 'dbo.orders')"))),
          norm_ts(q("SELECT CONVERT(varchar(30), MAX(placed_at), 121) FROM dbo.orders", SRC_DB)))
    check("scan filter pushdown-free", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.orders') WHERE status = 'paid'"), int(q("SELECT COUNT(*) FROM dbo.orders WHERE status = 'paid'", SRC_DB)))
    check("scan null count", one(con, f"SELECT count(*) FROM bridge_scan('{SRC}', 'dbo.types') WHERE c_int IS NULL"), 1)
    con.execute(f"CREATE TABLE orders_local AS FROM bridge_scan('{SRC}', 'dbo.orders', nolock := true)")
    check("CTAS from bridge_scan", one(con, "SELECT count(*) FROM orders_local"), 1000)
    con.execute(f"CREATE TABLE types_scan AS FROM bridge_scan('{SRC}', 'dbo.types')")
    check("CTAS types", one(con, "SELECT count(*) FROM types_scan"), NTYPES_ROWS)


@guarded
def t_types(con):
    print("== 5. per-column values vs sqlcmd ==")
    for col, sqltype, kind, _ in TYPES:
        try:
            a = sql_side(col, kind)
            b = duck_side(con, col, kind, "types_scan")
            d = diff(a, b)
            if d is None and len(a) != NTYPES_ROWS:
                d = f"row count {len(a)}"
            check(f"types {col} ({sqltype})", d, None)
        except Exception as e:  # noqa: BLE001
            fail(f"types {col} ({sqltype})", f"{type(e).__name__}: {str(e)[:200]}")
    nulls = con.sql("SELECT " + ", ".join(f"count(*) FILTER (WHERE {c} IS NULL)" for c, *_ in TYPES) + " FROM types_scan").fetchall()[0]
    check("NULLs preserved (one per column)", list(nulls), [1] * len(TYPES))


@guarded
def t_import():
    print("== 6. PRAGMA bridge_import ==")
    c = new_con()
    c.execute(f"PRAGMA bridge_import('{SRC}', tables = 'dbo.*', exclude = 'dbo.v_*', schema_map = 'dbo=main', keys = true)")
    names = sorted(r[0] for r in c.sql("SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema IN ('main', 'sales') ORDER BY 1").fetchall())
    check("import dbo.* tables", names, ["main.orders", "main.types"])
    check("import orders count", one(c, "SELECT count(*) FROM orders"), 1000)
    check("import types count", one(c, "SELECT count(*) FROM types"), NTYPES_ROWS)
    check("import sum(amount)", str(one(c, "SELECT sum(amount) FROM orders")), q("SELECT CONVERT(varchar(40), SUM(amount)) FROM dbo.orders", SRC_DB))
    pk = c.sql("SELECT table_name, constraint_column_names FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY' ORDER BY table_name").fetchall()
    check("import primary keys (keys := true)", [(t, list(cols)) for t, cols in pk], [("orders", ["id"]), ("types", ["id"])])
    c2 = new_con()
    c2.execute(f"PRAGMA bridge_import('{SRC}', schema_map = 'dbo=main,sales=sales', keys = false)")
    check("import all, sales schema", one(c2, "SELECT count(*) FROM sales.customers"), 3)
    check("import all, main.orders", one(c2, "SELECT count(*) FROM main.orders"), 1000)
    check("import keys := false has no PK", one(c2, "SELECT count(*) FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY'"), 0)
    c3 = new_con()
    c3.execute(f"PRAGMA bridge_import('{SRC}', tables = 'sales.*', schema_map = 'sales=sales', nolock = true)")
    check("import sales.* only", one(c3, "SELECT count(*) FROM sales.customers"), 3)


@guarded
def t_run(con):
    print("== 7. PRAGMA bridge_run ==")
    con.execute("PRAGMA bridge_run('SELECT TOP 3 id, ISNULL(amount, 0) AS amount INTO dbo.neo_t FROM orders_local ORDER BY id')")
    check("bridge_run SELECT INTO", con.sql("SELECT id FROM main.neo_t ORDER BY id").fetchall(), [(1,), (2,), (3,)])
    con.execute("PRAGMA bridge_run('UPDATE dbo.neo_t SET amount = 1 WHERE id = 2', nocase_mode = 'off')")
    check("bridge_run UPDATE", one(con, "SELECT amount FROM main.neo_t WHERE id = 2"), decimal.Decimal("1.00"))
    expect_error("bridge_run strict raises on a warning", con, "PRAGMA bridge_run('SELECT ROUND(amount, 2, 1) AS r FROM orders_local', strict = true)", "strict")
    try:
        con.execute("PRAGMA bridge_run('SELECT ROUND(amount, 2, 1) AS r FROM orders_local')")
        check("bridge_run non-strict accepts the same statement", True, True)
    except duckdb.Error as e:
        fail("bridge_run non-strict accepts the same statement", str(e)[:200])


@guarded
def t_copy(con):
    print("== 8. COPY TO mssql:// ==")
    tgt = f"{OUT}/dbo.orders_copy"
    con.execute(f"COPY orders_local TO '{tgt}' (FORMAT duckbridge, CREATE true)")
    check("COPY CREATE: count", q("SELECT COUNT(*) FROM dbo.orders_copy", OUT_DB), "1000")
    check("COPY CREATE: SUM(amount) parity", q("SELECT CONVERT(varchar(40), SUM(amount)) FROM dbo.orders_copy", OUT_DB), q("SELECT CONVERT(varchar(40), SUM(amount)) FROM dbo.orders", SRC_DB))
    check("COPY CREATE: CHECKSUM_AGG(id, customer_id) parity", q("SELECT CHECKSUM_AGG(CHECKSUM(id, customer_id)) FROM dbo.orders_copy", OUT_DB), q("SELECT CHECKSUM_AGG(CHECKSUM(id, customer_id)) FROM dbo.orders", SRC_DB))
    con.execute(f"CREATE TABLE orders_back AS FROM bridge_scan('{OUT}', 'dbo.orders_copy')")
    check("COPY CREATE: rows missing after copy", one(con, "SELECT count(*) FROM (SELECT * FROM orders_local EXCEPT SELECT * FROM orders_back)"), 0)
    check("COPY CREATE: rows extra after copy", one(con, "SELECT count(*) FROM (SELECT * FROM orders_back EXCEPT SELECT * FROM orders_local)"), 0)
    check("COPY CREATE: per-column text digest", one(con, "SELECT md5(string_agg(concat_ws('|', id, customer_id, amount, status, placed_at), ',' ORDER BY id)) FROM orders_back"),
          one(con, "SELECT md5(string_agg(concat_ws('|', id, customer_id, amount, status, placed_at), ',' ORDER BY id)) FROM orders_local"))
    con.execute(f"COPY orders_local TO '{tgt}' (FORMAT duckbridge)")
    check("COPY into existing table appends", q("SELECT COUNT(*) FROM dbo.orders_copy", OUT_DB), "2000")
    con.execute(f"COPY (SELECT id, amount FROM orders_local WHERE id <= 10) TO '{OUT}/dbo.orders_small' (FORMAT duckbridge, CREATE true, BATCH_ROWS 4, TABLE_LOCK false)")
    check("COPY query + options", q("SELECT COUNT(*) FROM dbo.orders_small", OUT_DB), "10")
    con.execute(f"COPY (SELECT id, amount FROM orders_local WHERE id <= 10) TO '{OUT}/dbo.orders_small' (FORMAT duckbridge, CREATE true, DROP_EXISTING true)")
    check("COPY DROP_EXISTING replaces", q("SELECT COUNT(*) FROM dbo.orders_small", OUT_DB), "10")
    con.execute(f"COPY (SELECT id, amount FROM orders_local WHERE id <= 5) TO '{OUT}/main' (FORMAT duckbridge, CREATE true, TABLE 'dbo.orders_opt')")
    check("COPY TABLE option overrides path", q("SELECT COUNT(*) FROM dbo.orders_opt", OUT_DB), "5")

    # types round trip: scan -> COPY CREATE -> scan the copy -> compare canonical text per column
    con.execute(f"COPY types_scan TO '{OUT}/dbo.types_copy' (FORMAT duckbridge, CREATE true)")
    check("COPY types: count", q("SELECT COUNT(*) FROM dbo.types_copy", OUT_DB), str(NTYPES_ROWS))
    con.execute(f"CREATE TABLE types_back AS FROM bridge_scan('{OUT}', 'dbo.types_copy')")
    for col, sqltype, kind, _ in TYPES:
        try:
            check(f"types round trip {col}", diff(duck_side(con, col, kind, "types_scan"), duck_side(con, col, kind, "types_back")), None)
        except Exception as e:  # noqa: BLE001
            fail(f"types round trip {col}", f"{type(e).__name__}: {str(e)[:200]}")


@guarded
def t_errors(con):
    print("== 9. error cases ==")
    expect_error("bridge_scan bad DSN", con, "SELECT * FROM bridge_scan('mssql://sa:nope@localhost:1/x', 'dbo.t')")
    expect_error("bridge_scan malformed DSN", con, "SELECT * FROM bridge_scan('not-a-dsn', 'dbo.t')")
    expect_error("bridge_scan DSN without database", con, f"SELECT * FROM bridge_scan('{DSN}', 'dbo.t')")
    expect_error("bridge_scan unknown table", con, f"SELECT * FROM bridge_scan('{SRC}', 'dbo.no_such_table')")
    expect_error("bridge_tables bad DSN", con, "SELECT * FROM bridge_tables('mssql://sa:nope@localhost:1/x')")
    expect_error("COPY to a missing table without CREATE", con, f"COPY orders_local TO '{OUT}/dbo.no_such_table' (FORMAT duckbridge)")
    check("failed COPY created nothing", q("SELECT COUNT(*) FROM sys.tables WHERE name = 'no_such_table'", OUT_DB), "0")
    expect_error("COPY bad DSN", con, "COPY orders_local TO 'mssql://sa:nope@localhost:1/x/dbo.t' (FORMAT duckbridge, CREATE true)")
    expect_error("COPY to a missing database never creates it", con, f"COPY orders_local TO '{DSN}/d2m_neo_nodb/dbo.t' (FORMAT duckbridge, CREATE true)")
    check("COPY did not create the database", q("SELECT COUNT(*) FROM sys.databases WHERE name = 'd2m_neo_nodb'"), "0")
    expect_error("old bridge_import() table function", con, f"SELECT * FROM bridge_import('{SRC}')", "PRAGMA bridge_import")
    expect_error("old bridge_export() table function", con, f"SELECT * FROM bridge_export('{OUT}/x')", "COPY")
    expect_error("old bridge_run() table function", con, "SELECT * FROM bridge_run('SELECT 1')", "PRAGMA bridge_run")


def main():
    t_footer_load_transpile()
    try:
        setup()
    except Exception as e:  # noqa: BLE001
        fail("fixture setup", str(e)[:400])
        print(f"\npassed: {passed}  failed: {failed}")
        return 1
    try:
        con = new_con()
        t_functions(con)
        t_tables(con)
        t_scan(con)
        t_types(con)
        t_import()
        t_run(con)
        t_copy(con)
        t_errors(con)
    except Exception as e:  # noqa: BLE001
        fail("harness", f"{type(e).__name__}: {str(e)[:300]}")
    finally:
        try:
            teardown()
        except Exception as e:  # noqa: BLE001
            fail("teardown", str(e)[:300])
    print(f"\npassed: {passed}  failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
