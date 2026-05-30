# DuckLake position-delete parquet files declare `file_path` / `pos` as OPTIONAL; Iceberg-compatible readers expect REQUIRED

**Affects**: DuckDB 1.5.2 (`8a5851971f`) + ducklake extension. Reproduced
via both `duckdb` (Python pip 1.5.2) and `org.duckdb:duckdb_jdbc:1.5.2.0`.

**Severity**: blocks downstream Iceberg-compatible engines from honouring
DuckLake position deletes through the standard delete-file dispatch.

## Summary

DuckLake's on-disk position-delete file format borrows the Iceberg
position-delete shape — same column names (`file_path`, `pos`), same
types (`string`, `long`), same row semantics (one row per deleted
position in the referenced data file). The Iceberg spec
([Position-delete files](https://iceberg.apache.org/spec/#position-delete-files))
requires both columns to be `required` (non-null). DuckLake writes them
as `OPTIONAL` instead, with definition-levels of 1 for every row
(every value is present, no actual nulls).

For any downstream engine that selects its column reader based on the
declared Iceberg schema, this looks like a parquet/spec inconsistency
and raises before reading any data.

## Repro

```sql
-- 1.5.2 default would inline this DELETE; force the file-based path
-- so we get an on-disk position-delete parquet.
CALL lake.set_option('data_inlining_row_limit', '0');

CREATE TABLE lake.demo.orders (id INTEGER);
INSERT INTO lake.demo.orders SELECT range FROM range(100);
DELETE FROM lake.demo.orders WHERE id = 42;

-- The ducklake_delete_file row points at a parquet file under DATA_PATH.
SELECT path FROM ducklake_delete_file
WHERE end_snapshot IS NULL;
-- → s3://.../demo/orders/ducklake-019e...-delete.parquet
```

Inspect the file's parquet schema:

```sql
SELECT name, type, repetition_type, converted_type
FROM parquet_schema('s3://.../demo/orders/ducklake-019e...-delete.parquet');
```

Output:

```
('duckdb_schema', NULL,         'REQUIRED', NULL)
('file_path',     'BYTE_ARRAY', 'OPTIONAL', 'UTF8')
('pos',           'INT64',      'OPTIONAL', 'INT_64')
```

The actual row data has no nulls (verified by reading the file: every
row has a valid `(file_path, pos)` pair). The discrepancy is purely in
the parquet schema metadata.

## Why this matters

The Iceberg position-delete spec is the de-facto delete-file format for
the broader lakehouse ecosystem. Many engines (Trino, Doris, Spark with
the Iceberg connector, etc.) already implement an Iceberg-shaped delete
dispatch and reuse it for compatible formats. DuckLake's column shape
matches that spec by intent — clearly enabling these engines to consume
DuckLake delete files via their existing iceberg reader path.

The nullability deviation breaks that compatibility silently. A
consumer that takes the not-nullable fast path on an Iceberg schema
(which says `file_path` and `pos` are `required`) sees the parquet
schema disagree and raises. Concrete example from Doris:

```
[INTERNAL_ERROR]Read parquet file …-delete.parquet failed,
reason = [CORRUPTION]Not nullable column has null values in parquet file
```

(There are no actual null values; the error is about the OPTIONAL
declaration on a column the reader treats as REQUIRED.)

## Suggested fix

In the ducklake extension's position-delete parquet writer, mark the
`file_path` and `pos` columns as `REQUIRED` (not nullable) rather than
`OPTIONAL`. The columns are never null by construction — every row in
a position-delete file has a valid path and position — so the change
has no impact on what gets written, only on what the schema declares.

Expected scope: one-line change at the parquet writer that emits these
columns.

## Notes for context

- DuckLake's data-file parquet uses field-ids that align with what
  Iceberg-shape readers expect (sanity-check between Doris and DuckLake
  confirms data files are read transparently via the Iceberg reader
  with no changes). The delete-file nullability is the only schema-level
  deviation we've hit.
- Workaround on the consumer side requires per-query rewriting of the
  delete parquet to flip the repetition_type, which is cost-prohibitive
  for any non-trivial workload.
- A complementary fix on the Doris side is being tracked in parallel
  (see `REPORT-doris-iceberg-reader-strict-on-delete-file-nullability.md`
  in this repo) — but the DuckLake-side fix is the most spec-aligned
  resolution and helps any downstream engine that ever takes the
  not-nullable fast path.

## Probe commands

For reference, the smallest end-to-end probe that exhibits the issue:

```bash
docker run --rm --network <substrate-net> \
  -e PG_HOST=… -e PG_DB=… -e PG_USER=… -e PG_PASSWORD=… \
  -e S3_ENDPOINT=… -e S3_KEY_ID=… -e S3_SECRET=… \
  python:3.12-slim sh -c '
    pip install --quiet duckdb==1.5.2
    python -c "
import os, duckdb
con = duckdb.connect()
con.execute(\"INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs\")
# … secret + ATTACH as in init_ducklake.py …
con.execute(\"CALL lake.set_option('data_inlining_row_limit', '0')\")
con.execute(\"DELETE FROM lake.tpch.orders WHERE o_orderkey = 1\")
path = con.execute(\"SELECT path FROM lake.ducklake_delete_file WHERE end_snapshot IS NULL\").fetchone()[0]
print(con.execute(f\"SELECT name, type, repetition_type FROM parquet_schema('{path}')\").fetchall())
"
'
```

Expected output: `repetition_type = 'OPTIONAL'` on both `file_path`
and `pos`.

## Contact

Filed by the DuckLake-on-Doris connector team at
`jvm/doris-ducklake/` in the
[ducklake-integrations](https://github.com/jminard-brikk/ducklake-integrations)
repo (private). Happy to help repro, test a fix, or co-author a PR.
