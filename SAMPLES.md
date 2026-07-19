# Trino DuckLake Connector — SQL Samples

Examples specific to using [DuckLake](https://ducklake.select) through Trino.
All examples assume a catalog named `ducklake`.

## Catalog Configuration

`etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake

# PostgreSQL catalog metadata
ducklake.catalog.database-url=jdbc:postgresql://postgresql:5432/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=<password>

# Data file location (local path or object storage)
ducklake.data-path=s3://my-bucket/ducklake-data/

# Optional tuning
ducklake.catalog.max-connections=10

# S3 access (if using object storage)
fs.native-s3.enabled=true
s3.region=us-east-1
```

S3 settings are per-catalog in Trino. Even if `hive.properties` or `iceberg.properties`
already defines `s3.*`, `ducklake.properties` needs its own S3 settings.

## Schema and Table DDL

```sql
-- Create a schema
CREATE SCHEMA ducklake.analytics;

-- Simple table
CREATE TABLE ducklake.analytics.events (
    event_id BIGINT,
    user_id INTEGER,
    event_type VARCHAR,
    payload VARCHAR,
    created_at TIMESTAMP(6)
);

-- Table with nested types
CREATE TABLE ducklake.analytics.user_profiles (
    user_id INTEGER,
    name VARCHAR,
    tags ARRAY(VARCHAR),
    address ROW(street VARCHAR, city VARCHAR, zip VARCHAR),
    preferences MAP(VARCHAR, VARCHAR)
);

-- Partitioned by column value (identity transform)
CREATE TABLE ducklake.analytics.orders (
    order_id BIGINT,
    region VARCHAR,
    amount DECIMAL(12,2),
    order_date DATE
)
WITH (partitioned_by = ARRAY['region']);

-- Partitioned by temporal transform
CREATE TABLE ducklake.analytics.logs (
    log_id BIGINT,
    message VARCHAR,
    event_date DATE
)
WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)']);

-- Views
CREATE VIEW ducklake.analytics.recent_orders AS
    SELECT * FROM ducklake.analytics.orders
    WHERE order_date > CURRENT_DATE - INTERVAL '30' DAY;
```

## Data Writes

```sql
-- INSERT
INSERT INTO ducklake.analytics.events
VALUES (1, 42, 'click', '{"page": "/home"}', TIMESTAMP '2025-06-01 10:30:00');

-- Bulk insert from another table
INSERT INTO ducklake.analytics.orders
SELECT order_id, region, amount, order_date
FROM hive.staging.raw_orders;

-- CREATE TABLE AS SELECT
CREATE TABLE ducklake.analytics.us_orders AS
    SELECT * FROM ducklake.analytics.orders
    WHERE region = 'US';

-- CTAS with partitioning
CREATE TABLE ducklake.analytics.orders_by_region
WITH (partitioned_by = ARRAY['region']) AS
    SELECT order_id, amount, order_date, region
    FROM ducklake.analytics.orders;
```

## Row-Level Mutations

```sql
-- DELETE
DELETE FROM ducklake.analytics.orders
WHERE region = 'TEST';

-- UPDATE (atomic delete + insert in one snapshot)
UPDATE ducklake.analytics.orders
SET amount = amount * 1.1
WHERE region = 'US';

-- MERGE
MERGE INTO ducklake.analytics.orders target
USING ducklake.staging.order_updates source
ON target.order_id = source.order_id
WHEN MATCHED AND source.is_cancelled THEN DELETE
WHEN MATCHED THEN UPDATE SET amount = source.amount
WHEN NOT MATCHED THEN INSERT (order_id, region, amount, order_date)
    VALUES (source.order_id, source.region, source.amount, source.order_date);
```

## Schema Evolution

```sql
-- Add a column (existing data files return NULL for the new column)
ALTER TABLE ducklake.analytics.orders ADD COLUMN priority VARCHAR;

-- Drop a column (existing data files retain the column data, ignored on read)
ALTER TABLE ducklake.analytics.orders DROP COLUMN priority;

-- Rename a column (field_id-based matching; existing Parquet files read correctly)
ALTER TABLE ducklake.analytics.events RENAME COLUMN payload TO event_payload;
```

## Time Travel

```sql
-- Read a specific snapshot by ID
SELECT * FROM ducklake.analytics.orders FOR VERSION AS OF 3;

-- Read as of a point in time
SELECT *
FROM ducklake.analytics.orders
FOR TIMESTAMP AS OF TIMESTAMP '2025-06-01 00:00:00 UTC';

-- Compare current data with a previous snapshot
SELECT 'current' AS source, count(*) FROM ducklake.analytics.orders
UNION ALL
SELECT 'snapshot_3', count(*) FROM ducklake.analytics.orders FOR VERSION AS OF 3;
```

## Snapshot Pinning

Pin all reads in a session to a specific snapshot:

```sql
-- Pin by snapshot ID
SET SESSION ducklake.read_snapshot_id = 5;
SELECT * FROM ducklake.analytics.orders;
SELECT * FROM ducklake.analytics.events;
RESET SESSION ducklake.read_snapshot_id;

-- Pin by timestamp
SET SESSION ducklake.read_snapshot_timestamp = '2025-06-01T12:00:00Z';
SELECT * FROM ducklake.analytics.orders;
RESET SESSION ducklake.read_snapshot_timestamp;
```

`read_snapshot_id` and `read_snapshot_timestamp` are mutually exclusive.

Catalog-level pinning is also available in `ducklake.properties`:

```properties
# Pin all reads for all sessions (set at most one)
ducklake.default-snapshot-id=5
ducklake.default-snapshot-timestamp=2025-06-01T12:00:00Z
```

Precedence: query clause > session property > catalog config > current snapshot.

## Metadata Tables

DuckLake metadata is exposed via `$`-suffixed virtual tables (must be quoted in Trino):

```sql
-- List data files for a table
SELECT data_file_id, path, file_format, record_count, file_size_bytes
FROM ducklake.analytics."orders$files";

-- List all snapshots
SELECT snapshot_id, snapshot_time, schema_version
FROM ducklake.analytics."orders$snapshots"
ORDER BY snapshot_id DESC;

-- Current snapshot
SELECT * FROM ducklake.analytics."orders$current_snapshot";

-- Snapshot audit trail (what changed in each snapshot)
SELECT snapshot_id, changes_made, author, commit_message
FROM ducklake.analytics."orders$snapshot_changes"
ORDER BY snapshot_id DESC;
```

## Cross-Engine Compatibility with DuckDB

The connector writes Parquet files with `field_id` annotations matching DuckLake `column_id`
values, so DuckDB can correctly read Trino-written files and vice versa. Both engines can
operate on the same PostgreSQL catalog concurrently.

In DuckDB, attach the same catalog:

```sql
-- DuckDB
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:postgresql://postgresql:5432/ducklake?user=ducklake&password=<password>'
    AS lake (DATA_PATH 's3://my-bucket/ducklake-data/');

-- Read data written by Trino
SELECT * FROM lake.analytics.orders;

-- Write data that Trino can then read
INSERT INTO lake.analytics.orders VALUES (100, 'EU', 42.00, '2025-07-01');
```

## Smoke Tests

After installation, verify the connector is working:

```sql
-- Catalog is visible
SHOW CATALOGS LIKE 'ducklake';

-- Schemas and tables are accessible
SHOW SCHEMAS FROM ducklake;
SHOW TABLES FROM ducklake.analytics;

-- Data reads work
SELECT count(*) FROM ducklake.analytics.orders;
SELECT * FROM ducklake.analytics."orders$current_snapshot";

-- DDL round-trip
CREATE SCHEMA ducklake.tmp_smoke_test;
CREATE TABLE ducklake.tmp_smoke_test.t1 (id INTEGER, name VARCHAR);
INSERT INTO ducklake.tmp_smoke_test.t1 VALUES (1, 'test');
SELECT * FROM ducklake.tmp_smoke_test.t1;
DROP TABLE ducklake.tmp_smoke_test.t1;
DROP SCHEMA ducklake.tmp_smoke_test;
```
