"""Bootstrap a DuckLake catalog into PostgreSQL via DuckDB's ducklake extension.

Idempotent: re-running against an already-initialized catalog is a no-op (DuckDB's
ATTACH detects existing __ducklake_metadata_* tables and reuses them).
"""

import os
import sys
import time

import duckdb

PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ["PG_DB"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_KEY_ID = os.environ["S3_KEY_ID"]
S3_SECRET = os.environ["S3_SECRET"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_DATA_PREFIX = os.environ.get("S3_DATA_PREFIX", "data/")

TPCH_SCALE_FACTOR = float(os.environ.get("TPCH_SCALE_FACTOR", "0") or "0")
TPCH_SCHEMA = os.environ.get("TPCH_SCHEMA", "tpch")

TPCH_TABLES = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)

DATA_PATH = f"s3://{S3_BUCKET}/{S3_DATA_PREFIX}"
ATTACH_URI = (
    f"ducklake:postgres:dbname={PG_DB} host={PG_HOST} port={PG_PORT} "
    f"user={PG_USER} password={PG_PASSWORD}"
)


def main() -> int:
    print(f"DuckDB version: {duckdb.__version__}", flush=True)
    print(f"Postgres: {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}", flush=True)
    print(f"S3: endpoint={S3_ENDPOINT} data_path={DATA_PATH}", flush=True)

    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    con.execute(
        """
        CREATE SECRET minio_s3 (
            TYPE S3,
            KEY_ID ?,
            SECRET ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL false
        )
        """,
        [S3_KEY_ID, S3_SECRET, S3_ENDPOINT],
    )

    last_err: Exception | None = None
    for attempt in range(1, 31):
        try:
            con.execute(f"ATTACH '{ATTACH_URI}' AS lake (DATA_PATH '{DATA_PATH}')")
            break
        except Exception as exc:
            last_err = exc
            print(f"ATTACH attempt {attempt} failed: {exc}", flush=True)
            time.sleep(2)
    else:
        print(f"ATTACH never succeeded: {last_err}", file=sys.stderr, flush=True)
        return 1

    rows = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'"
    ).fetchone()
    print(f"DuckLake catalog ready ({rows[0]} catalog tables visible).", flush=True)

    if TPCH_SCALE_FACTOR > 0:
        seed_tpch(con)
    else:
        print("TPC-H seeding skipped (TPCH_SCALE_FACTOR=0).", flush=True)

    con.execute("DETACH lake")
    return 0


def seed_tpch(con: duckdb.DuckDBPyConnection) -> None:
    existing = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND schema_name = ?",
            [TPCH_SCHEMA],
        ).fetchall()
    }
    missing = [t for t in TPCH_TABLES if t not in existing]
    if not missing:
        print(
            f"TPC-H schema lake.{TPCH_SCHEMA} already populated "
            f"({len(existing)} tables); skipping dbgen.",
            flush=True,
        )
        return

    print(
        f"Generating TPC-H at sf={TPCH_SCALE_FACTOR} into lake.{TPCH_SCHEMA} "
        f"(missing: {', '.join(missing)})",
        flush=True,
    )
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf = {TPCH_SCALE_FACTOR})")

    con.execute(f'CREATE SCHEMA IF NOT EXISTS lake."{TPCH_SCHEMA}"')

    for table in missing:
        print(f"  -> lake.{TPCH_SCHEMA}.{table}", flush=True)
        con.execute(
            f'CREATE TABLE lake."{TPCH_SCHEMA}"."{table}" AS '
            f'SELECT * FROM memory.main."{table}"'
        )

    counts = con.execute(
        f"""
        SELECT table_name, estimated_size
        FROM duckdb_tables()
        WHERE database_name = 'lake' AND schema_name = ?
        ORDER BY table_name
        """,
        [TPCH_SCHEMA],
    ).fetchall()
    print("TPC-H seed complete:", flush=True)
    for name, size in counts:
        print(f"  lake.{TPCH_SCHEMA}.{name}: ~{size} rows", flush=True)


if __name__ == "__main__":
    sys.exit(main())
