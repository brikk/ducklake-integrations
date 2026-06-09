"""W2 / W2c (INSERT) smoke helper: a DuckDB+DuckLake side for the Doris-write E2E.

Two modes (MODE env var):
  create  → drop+recreate an empty `tpch.<TABLE> (id INTEGER, name VARCHAR)` for Doris
            to INSERT into (Doris can't CREATE TABLE yet — W1). If PARTITION_BY is set,
            the table is partitioned by that expression (e.g. `bucket(4, name)`) — this
            is how W2c stands up a bucketed target.
  verify  → print the rows DuckDB+DuckLake sees in `tpch.<TABLE>`. This is the
            cross-engine read-back: if Doris wrote DuckLake-compatible Parquet
            (field_id == column_id, right footer), DuckDB sees the same rows.

Env vars: TABLE (default doris_w), PARTITION_BY (default none); connection details as
in step7-delete.py.
"""

import os

import duckdb


def main() -> None:
    mode = os.environ.get("MODE", "create")
    table = os.environ.get("TABLE", "doris_w")
    partition_by = os.environ.get("PARTITION_BY", "").strip()

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
        con.execute(f"DROP TABLE IF EXISTS lake.tpch.{table}")
        con.execute(f"CREATE TABLE lake.tpch.{table} (id INTEGER, name VARCHAR)")
        msg = f"created empty lake.tpch.{table} (id INTEGER, name VARCHAR)"
        if partition_by:
            # SET PARTITIONED BY on the empty table so Doris's subsequent INSERT writes
            # partitioned files (the bootstrap uses the same pattern for by_name_bucket).
            con.execute(f"ALTER TABLE lake.tpch.{table} SET PARTITIONED BY ({partition_by})")
            msg += f" PARTITIONED BY ({partition_by})"
        print(msg, flush=True)
    else:
        rows = con.execute(
            f"SELECT id, name FROM lake.tpch.{table} ORDER BY id"
        ).fetchall()
        print(f"DuckDB+DuckLake sees {len(rows)} rows in lake.tpch.{table}: {rows}", flush=True)

    con.execute("DETACH lake")


if __name__ == "__main__":
    main()
