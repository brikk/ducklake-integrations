"""W2 / W2c (INSERT) smoke helper: a DuckDB+DuckLake side for the Doris-write E2E.

Two modes (MODE env var):
  create  → drop+recreate an empty `<SCHEMA>.<TABLE> (id INTEGER, name VARCHAR)` for Doris
            to INSERT into. If PARTITION_BY is set, the table is partitioned by that
            expression (e.g. `bucket(4, name)`). NOTE: as of W1b(b) the smoke stands its
            INSERT targets up via Doris `CREATE TABLE` (the live DDL route); this mode is
            kept only as a fallback for when the live DDL route is unavailable.
  verify  → print the rows DuckDB+DuckLake sees in `<SCHEMA>.<TABLE>`. This is the
            cross-engine read-back: if Doris wrote DuckLake-compatible Parquet
            (field_id == column_id, right footer), DuckDB sees the same rows.

Env vars: SCHEMA (default tpch), TABLE (default doris_w), PARTITION_BY (default none);
connection details as in step7-delete.py.
"""

import os

import duckdb


def main() -> None:
    mode = os.environ.get("MODE", "create")
    schema = os.environ.get("SCHEMA", "tpch")
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
        con.execute(f"DROP TABLE IF EXISTS lake.{schema}.{table}")
        con.execute(f"CREATE TABLE lake.{schema}.{table} (id INTEGER, name VARCHAR)")
        msg = f"created empty lake.{schema}.{table} (id INTEGER, name VARCHAR)"
        if partition_by:
            # SET PARTITIONED BY on the empty table so Doris's subsequent INSERT writes
            # partitioned files (the bootstrap uses the same pattern for by_name_bucket).
            con.execute(f"ALTER TABLE lake.{schema}.{table} SET PARTITIONED BY ({partition_by})")
            msg += f" PARTITIONED BY ({partition_by})"
        print(msg, flush=True)
    else:
        rows = con.execute(
            f"SELECT id, name FROM lake.{schema}.{table} ORDER BY id"
        ).fetchall()
        print(f"DuckDB+DuckLake sees {len(rows)} rows in lake.{schema}.{table}: {rows}", flush=True)

    con.execute("DETACH lake")


if __name__ == "__main__":
    main()
