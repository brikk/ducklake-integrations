"""Schema-evolution smoke helper: exercise the column-DEFAULT *backfill* read
path (the issue_1135 semantics) through DuckDB+DuckLake against the substrate.

Creates (or recreates) `tpch.default_probe`, inserts rows that predate a new
column, then `ALTER TABLE … ADD COLUMN b … DEFAULT 42`, and finally inserts one
row that carries an explicit `b`. The pre-existing rows' data files physically
lack column `b`; DuckLake records `initial_default` in `ducklake_column`, so a
correct reader must backfill `42` for those old rows while returning the
explicit `99` for the new one.

`data_inlining_row_limit=0` forces the old rows into a committed Parquet data
file (not an inlined catalog row) so the "old file is missing the column"
backfill path is the one under test. Connection details come from env vars so
the smoke script can pass them in without baking secrets into the image.
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

    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

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

    # Force the pre-ADD rows into a real Parquet data file (not inlined) so the
    # backfill-from-missing-column path is what the Doris read exercises.
    con.execute("CALL lake.set_option('data_inlining_row_limit', '0')")

    con.execute("DROP TABLE IF EXISTS lake.tpch.default_probe")
    con.execute("CREATE TABLE lake.tpch.default_probe (a INTEGER)")
    con.execute("INSERT INTO lake.tpch.default_probe VALUES (1), (2), (3)")

    # The evolution under test: old rows predate b and must read back the DEFAULT.
    con.execute("ALTER TABLE lake.tpch.default_probe ADD COLUMN b INTEGER DEFAULT 42")

    # One post-ADD row that carries an explicit b — must read back its own value.
    con.execute("INSERT INTO lake.tpch.default_probe (a, b) VALUES (4, 99)")

    rows = con.execute(
        "SELECT a, b FROM lake.tpch.default_probe ORDER BY a"
    ).fetchall()
    print(f"DuckDB readback of tpch.default_probe: {rows}", flush=True)

    con.execute("DETACH lake")


if __name__ == "__main__":
    main()
