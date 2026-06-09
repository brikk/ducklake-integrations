"""W2 (INSERT) smoke helper: a DuckDB+DuckLake side for the Doris-write E2E.

Two modes (MODE env var):
  create  → drop+recreate an empty `tpch.doris_w (id INTEGER, name VARCHAR)`
            for Doris to INSERT into (Doris can't CREATE TABLE yet — W1).
  verify  → print the rows DuckDB+DuckLake sees in `tpch.doris_w`. This is the
            cross-engine read-back: if Doris wrote DuckLake-compatible Parquet
            (field_id == column_id, right footer), DuckDB sees the same rows.

Connection details come from env vars (same pattern as step7-delete.py).
"""

import os

import duckdb


def main() -> None:
    mode = os.environ.get("MODE", "create")

    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    con.execute(
        f"""
        CREATE SECRET lake_s3 (
            TYPE S3,
            KEY_ID    '{os.environ["S3_KEY_ID"]}',
            SECRET    '{os.environ["S3_SECRET"]}',
            ENDPOINT  '{os.environ["S3_ENDPOINT"]}',
            URL_STYLE 'path',
            USE_SSL   false
        )
        """
    )

    attach_uri = (
        f"ducklake:postgres:dbname={os.environ['PG_DB']} host={os.environ['PG_HOST']} "
        f"user={os.environ['PG_USER']} password={os.environ['PG_PASSWORD']}"
    )
    con.execute(f"ATTACH '{attach_uri}' AS lake (DATA_PATH '{os.environ['DATA_PATH']}')")

    if mode == "create":
        con.execute("DROP TABLE IF EXISTS lake.tpch.doris_w")
        con.execute("CREATE TABLE lake.tpch.doris_w (id INTEGER, name VARCHAR)")
        print("created empty lake.tpch.doris_w (id INTEGER, name VARCHAR)", flush=True)
    else:
        rows = con.execute(
            "SELECT id, name FROM lake.tpch.doris_w ORDER BY id"
        ).fetchall()
        print(f"DuckDB+DuckLake sees {len(rows)} rows in lake.tpch.doris_w: {rows}", flush=True)

    con.execute("DETACH lake")


if __name__ == "__main__":
    main()
