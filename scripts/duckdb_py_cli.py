#!/usr/bin/env python3
"""Stand-in for the subset of the `duckdb` CLI the e2e scripts use, on top of the `duckdb` Python
package. DuckDB 2.0 alpha builds ship no CLI binary, so the duckdb-neo runs point DUCK at this
(see docs/handoff-neo.md):

    DUCK="python3 -I scripts/duckdb_py_cli.py"   # with the 2.0 package on PYTHONPATH / in a venv
    duckdb_py_cli.py [-readonly] [-unsigned] [-noheader] [-list] [DBFILE] [-c SQL]   (SQL from stdin without -c)

Output is list mode without a header: one line per row, values '|'-separated, NULL as an empty
string, every value rendered by DuckDB itself (CAST(... AS VARCHAR)), as the CLI renders them.
"""
import sys

import duckdb


def main(argv):
    readonly = unsigned = False
    db = ":memory:"
    sql = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "-readonly":
            readonly = True
        elif a == "-unsigned":
            unsigned = True
        elif a in ("-noheader", "-list", "-batch"):
            pass
        elif a == "-c":
            i += 1
            sql = argv[i]
        elif a.startswith("-"):
            sys.exit(f"duckdb_py_cli: unsupported option {a}")
        else:
            db = a
        i += 1
    if sql is None:
        sql = sys.stdin.read()
    config = {"allow_unsigned_extensions": "true"} if unsigned else {}
    con = duckdb.connect(db, read_only=readonly, config=config)
    try:
        for st in duckdb.extract_statements(sql):
            if st.type == duckdb.StatementType.SELECT:
                q = st.query.strip().rstrip(";")
                rows = con.sql(f"SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ({q})").fetchall()
            else:
                con.execute(st.query)
                rows = []
                # like the CLI: no changed-row counts for DML / DDL, results for PRAGMA / CALL / EXPLAIN
                if st.type in (duckdb.StatementType.PRAGMA, duckdb.StatementType.CALL, duckdb.StatementType.EXPLAIN):
                    try:
                        rows = [tuple(None if v is None else str(v) for v in r) for r in con.fetchall()]
                    except duckdb.Error:
                        rows = []
            for r in rows:
                print("|".join("" if v is None else v for v in r))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
