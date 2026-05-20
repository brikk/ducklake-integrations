"""Step 7 smoke helper: issue a DELETE through DuckDB+DuckLake against the
trino-ducklake substrate. Reads connection details from environment variables
so the smoke script can pass them in without baking secrets into the image.

Creates (or recreates) a dedicated `tpch.step7_orders` table seeded from
`tpch.orders`, then DELETEs DELETE_COUNT rows from it. Using a dedicated
table keeps the Step 5 smoke against `tpch.orders` clean and the Step 7
exercise idempotent across runs.

DuckLake materialises the DELETE as a new position-delete parquet file
plus a ducklake_delete_file row; the next snapshot inlines that delete
file onto the affected data file (JdbcDucklakeCatalog#getDataFiles
LEFT JOIN), which is what the Doris side reads at scan time.
"""

import os

import duckdb


def main() -> None:
    pg_db = os.environ["PG_DB"]
    pg_host = os.environ["PG_HOST"]
    pg_user = os.environ["PG_USER"]
    pg_password = os.environ["PG_PASSWORD"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_key_id = os.environ["S3_KEY_ID"]
    s3_secret = os.environ["S3_SECRET"]
    data_path = os.environ["DATA_PATH"]
    delete_count = int(os.environ["DELETE_COUNT"])

    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    # Parameter binding doesn't work for CREATE SECRET; the substrate's
    # init_ducklake.py uses ?-bind but only for the secret values. For the
    # smoke we can just inline — the values come from controlled env vars.
    con.execute(
        f"""
        CREATE SECRET lake_s3 (
            TYPE S3,
            KEY_ID    '{s3_key_id}',
            SECRET    '{s3_secret}',
            ENDPOINT  '{s3_endpoint}',
            URL_STYLE 'path',
            USE_SSL   false
        )
        """
    )

    attach_uri = (
        f"ducklake:postgres:dbname={pg_db} host={pg_host} "
        f"user={pg_user} password={pg_password}"
    )
    con.execute(f"ATTACH '{attach_uri}' AS lake (DATA_PATH '{data_path}')")

    # DuckLake defaults DATA_INLINING_ROW_LIMIT high enough that small
    # DELETEs land in ducklake_inlined_delete_<table_id> rows in the
    # catalog DB instead of materialising as ducklake_delete_file +
    # position-delete parquet. The Doris BE iceberg reader only consumes
    # file-based deletes, so we force the file path. Idempotent — once set,
    # the row persists in ducklake_metadata.
    con.execute("CALL lake.set_option('data_inlining_row_limit', '0')")

    # Recreate the step7-only table on every run so the exercise is
    # idempotent and we never see leftover state from a previous run.
    con.execute("DROP TABLE IF EXISTS lake.tpch.step7_orders")
    con.execute(
        """
        CREATE TABLE lake.tpch.step7_orders AS
        SELECT * FROM lake.tpch.orders
        ORDER BY o_orderkey LIMIT 100
        """
    )

    before = con.execute(
        "SELECT COUNT(*) FROM lake.tpch.step7_orders"
    ).fetchone()[0]
    print(f"DuckDB sees {before} rows in lake.tpch.step7_orders before DELETE",
          flush=True)

    con.execute(
        f"""
        DELETE FROM lake.tpch.step7_orders
        WHERE o_orderkey IN (
            SELECT o_orderkey FROM lake.tpch.step7_orders
            ORDER BY o_orderkey LIMIT {delete_count}
        )
        """
    )

    after = con.execute(
        "SELECT COUNT(*) FROM lake.tpch.step7_orders"
    ).fetchone()[0]
    print(
        f"DuckDB sees {after} rows in lake.tpch.step7_orders after DELETE "
        f"(deleted {before - after}, expected {delete_count})",
        flush=True,
    )

    con.execute("DETACH lake")


if __name__ == "__main__":
    main()
