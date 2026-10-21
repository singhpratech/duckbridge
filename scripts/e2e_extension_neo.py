#!/usr/bin/env python3
"""Smoke test of the DuckDB 2.0 (duckdb-neo) build of the extension, on the `duckdb` Python package
(DuckDB 2.0 alpha builds ship no CLI binary, so scripts/e2e_extension.sh cannot run them).

    cd extension && make release-neo && cd ..
    <venv with duckdb 2.0>/bin/python -I scripts/e2e_extension_neo.py

Env: EXT (default extension/build/neo/duckbridge.duckdb_extension), MSSQL_HOST/PORT/USER/PASSWORD,
SQLCMD. It covers the checks of e2e_extension.sh that DuckDB 2.0 allows today:

* PASS checks: the footer, LOAD, bridge_transpile (both overloads, NULLs, errors), bridge_prelude,
  the five functions in duckdb_functions().
* BLOCKED checks: bridge_run creating a table, bridge_import from SQL Server and
  bridge_export(create_database := true) need a connection to the host database, which the v2 C
  extension API does not give an extension (docs/handoff-neo.md). They must fail with the
  extension's explanation, and must not touch SQL Server; `_neo_out` is dropped with sqlcmd if
  it exists anyway.

Exit status 0 when every PASS check passes and every BLOCKED call fails as documented.
"""
import os
import subprocess
import sys
import urllib.parse

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXT = os.path.abspath(os.environ.get("EXT", os.path.join(ROOT, "extension/build/neo/duckbridge.duckdb_extension")))
HOST = os.environ.get("MSSQL_HOST", "localhost")
PORT = os.environ.get("MSSQL_PORT", "14319")
USER = os.environ.get("MSSQL_USER", "sa")
PASSWORD = os.environ.get("MSSQL_PASSWORD", "D2m!Strong#2019")
SQLCMD = os.environ.get("SQLCMD", "/opt/mssql-tools18/bin/sqlcmd")
DSN = f"mssql://{USER}:{urllib.parse.quote(PASSWORD, safe='')}@{HOST}:{PORT}"
NO_CONNECTION = "is not available on DuckDB 2.0 yet"

passed = failed = blocked = 0


def check(name, got, want):
    global passed, failed
    if got == want:
        print(f"  PASS {name} ({got})")
        passed += 1
    else:
        print(f"  FAIL {name}: got {got!r} want {want!r}")
        failed += 1


def expect_blocked(name, con, sql):
    """`sql` must fail with the extension's no-connection explanation."""
    global blocked, failed
    try:
        rows = con.sql(sql).fetchall()
    except duckdb.Error as e:
        if NO_CONNECTION in str(e):
            print(f"  BLOCKED {name}: {str(e).splitlines()[0][:160]}...")
            blocked += 1
        else:
            print(f"  FAIL {name}: unexpected error {e}")
            failed += 1
        return
    print(f"  FAIL {name}: expected the no-connection error, got rows {rows!r}")
    failed += 1


def q(sql):
    out = subprocess.run([SQLCMD, "-S", f"{HOST},{PORT}", "-U", USER, "-P", PASSWORD, "-C", "-h", "-1", "-W", "-Q", sql],
                         capture_output=True, text=True, check=True).stdout
    return "\n".join(line for line in out.splitlines() if line and "rows affected" not in line)


def main():
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

    print("== 1. load + transpile ==")
    con.execute(f"LOAD '{EXT}'")
    one = lambda sql: con.sql(sql).fetchall()[0][0]
    check("transpile tsql->duck", one("SELECT bridge_transpile('tsql','duckdb','SELECT TOP 2 ISNULL(a,0) FROM dbo.t')"), "SELECT COALESCE(a, 0) FROM main.t LIMIT 2;")
    check("transpile duck->tsql 2022", one("SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2022)"), "SELECT greatest(a, b) FROM t;")
    check("transpile duck->tsql 2019", one("SELECT bridge_transpile('duckdb','tsql','SELECT greatest(a,b) FROM t', 2019)"), "SELECT (SELECT MAX(v) FROM (VALUES (a), (b)) AS g(v)) FROM t;")
    check("transpile NULL in", one("SELECT bridge_transpile('tsql','duckdb',NULL::VARCHAR)"), None)
    check("transpile per row", con.sql("SELECT bridge_transpile('tsql','duckdb', s) FROM (VALUES ('SELECT GETDATE()'), (NULL), ('SELECT 1')) t(s)").fetchall(),
          [("SELECT current_localtimestamp();",), (None,), ("SELECT 1;",)])
    check("prelude lines", one("SELECT length(string_split(bridge_prelude(true), chr(10)))"), 3)
    check("prelude nocase off", one("SELECT bridge_prelude(false) NOT LIKE '%collation%'"), True)
    try:
        con.sql("SELECT bridge_transpile('oracle','duckdb','x')").fetchall()
        check("bad dialect errors", "no error", "unknown source dialect")
    except duckdb.Error as e:
        check("bad dialect errors", "unknown source dialect" in str(e), True)
    check("functions registered", con.sql("SELECT count(DISTINCT function_name) FROM duckdb_functions() WHERE function_name LIKE 'bridge\\_%' ESCAPE '\\'").fetchall()[0][0], 5)

    print("== 2. bridge_run / bridge_import / bridge_export (need a host connection) ==")
    expect_blocked("bridge_run creates a table", con, "SELECT * FROM bridge_run('SELECT 1 AS x INTO dbo.neo_t;')")
    expect_blocked("bridge_import from SQL Server", con, f"SELECT * FROM bridge_import('{DSN}/d2m_extsrc', parallel := 4)")
    expect_blocked("bridge_export create_database", con,
                   f"SELECT * FROM bridge_export('{DSN}/_neo_out', create_database := true, database := '_neo_out', tables := 'main.neo_t')")
    check("_neo_out not created", q("SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '_neo_out'"), "0")
    q("IF DB_ID('_neo_out') IS NOT NULL BEGIN ALTER DATABASE [_neo_out] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [_neo_out]; END")

    print(f"\npassed: {passed}  failed: {failed}  blocked: {blocked}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
