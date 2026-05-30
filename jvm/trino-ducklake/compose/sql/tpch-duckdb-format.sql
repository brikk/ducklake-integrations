-- TPC-H side-by-side: parquet vs duckdb format.
--
-- Run via:
--   docker exec -it trino-ducklake-trino trino --catalog ducklake
-- or pipe with --file=/path/to/this.sql. Each block is a logical step.
SELECT * FROM ducklake.tpch.region;
SELECT * FROM ducklake.tpch_duckdb.region;

CREATE SCHEMA IF NOT EXISTS ducklake.tpch_duckdb;

CREATE TABLE ducklake.tpch_duckdb.region   WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.region;
CREATE TABLE ducklake.tpch_duckdb.nation   WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.nation;
CREATE TABLE ducklake.tpch_duckdb.supplier WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.supplier;
CREATE TABLE ducklake.tpch_duckdb.customer WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.customer;
CREATE TABLE ducklake.tpch_duckdb.part     WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.part;
CREATE TABLE ducklake.tpch_duckdb.partsupp WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.partsupp;
CREATE TABLE ducklake.tpch_duckdb.orders   WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.orders;
CREATE TABLE ducklake.tpch_duckdb.lineitem WITH (data_file_format = 'duckdb') AS SELECT * FROM ducklake.tpch.lineitem;

-- Catalog row should report file_format='duckdb' for the new tables.
SELECT file_format, count(*) AS files
FROM ducklake.tpch_duckdb."lineitem$files"
GROUP BY file_format;

-- TPC-H Q1: full-scan aggregation with a date predicate. Predicate pushes into
-- DuckDB SQL on the duckdb path; Trino's parquet reader uses row-group skipping
-- with the same domain. Run each, compare elapsed time printed by the CLI.
SELECT l_returnflag, l_linestatus,
       sum(l_quantity)                          AS sum_qty,
       sum(l_extendedprice)                     AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount))  AS sum_disc_price,
       avg(l_quantity)                          AS avg_qty,
       count(*)                                 AS count_order
FROM ducklake.tpch.lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

SELECT l_returnflag, l_linestatus,
       sum(l_quantity)                          AS sum_qty,
       sum(l_extendedprice)                     AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount))  AS sum_disc_price,
       avg(l_quantity)                          AS avg_qty,
       count(*)                                 AS count_order
FROM ducklake.tpch_duckdb.lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Selective point lookup. The duckdb path pushes the equality into DuckDB, which
-- skips blocks via its native zone maps. Parquet path relies on row-group stats.
SELECT count(*) FROM ducklake.tpch.lineitem        WHERE l_orderkey > 12345;
SELECT count(*) FROM ducklake.tpch_duckdb.lineitem WHERE l_orderkey > 12345;

-- Mixed-format join: parquet table joined to duckdb tables in one query, both
-- readers active.
SELECT n.n_name, count(*) AS orders
FROM ducklake.tpch.nation n                                            -- parquet
JOIN ducklake.tpch_duckdb.customer c ON c.c_nationkey = n.n_nationkey  -- duckdb
JOIN ducklake.tpch_duckdb.orders   o ON o.o_custkey   = c.c_custkey
GROUP BY n.n_name ORDER BY orders DESC LIMIT 5;

-- =============================================================================
-- BENCHMARK BLOCK — generate a larger TPC-H from Trino's built-in tpch connector,
-- materialize parallel parquet + duckdb copies in the lake, then run a timing
-- query across both. Designed for shutdown/restart cycles to compare cold vs
-- warm-cache behavior.
--
-- PRE-REQS
--   * Trino's built-in `tpch` catalog must be registered. If not present:
--     create `compose/trino/etc/catalog/tpch.properties` with one line —
--     `connector.name=tpch` — and restart the trino service.
--   * MinIO/S3 must have headroom for both copies. sf1 lineitem is ~600 MB
--     parquet + similar for duckdb.
-- =============================================================================

-- USER: create whatever S3 secret / credential / Trino session config your
-- environment needs here. The compose stack already wires MinIO into
-- `ducklake.properties`, so locally there's nothing to do — this block is
-- a placeholder for production S3 setups.
--
-- (placeholder: CREATE SECRET / SET SESSION ... etc.)
show catalogs;
SHOW SCHEMAS FROM ducklake_ovh;

DROP TABLE ducklake_ovh.bench_parquet.region;
DROP TABLE ducklake_ovh.bench_parquet.nation;
DROP TABLE ducklake_ovh.bench_parquet.supplier;
DROP TABLE ducklake_ovh.bench_parquet.part;
DROP TABLE ducklake_ovh.bench_parquet.partsupp;
DROP TABLE ducklake_ovh.bench_parquet.customer;
DROP TABLE ducklake_ovh.bench_parquet.orders;
DROP TABLE ducklake_ovh.bench_parquet.lineitem;
DROP SCHEMA ducklake_ovh.bench_parquet CASCADE;

CREATE SCHEMA IF NOT EXISTS ducklake_ovh.bench_parquet;

CREATE TABLE ducklake_ovh.bench_parquet.region   WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.region;
CREATE TABLE ducklake_ovh.bench_parquet.nation   WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.nation;
CREATE TABLE ducklake_ovh.bench_parquet.supplier WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.supplier;
CREATE TABLE ducklake_ovh.bench_parquet.part     WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.part;
CREATE TABLE ducklake_ovh.bench_parquet.partsupp WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.partsupp;
CREATE TABLE ducklake_ovh.bench_parquet.customer WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.customer;
CREATE TABLE ducklake_ovh.bench_parquet.orders   WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.orders   WHERE mod(orderkey, 2) = 0;
CREATE TABLE ducklake_ovh.bench_parquet.lineitem WITH (data_file_format = 'parquet') AS SELECT * FROM tpch.sf1.lineitem WHERE mod(orderkey, 2) = 0;

-- Same payload, duckdb format. Per-CREATE WITH (data_file_format = 'duckdb')
-- avoids the previous session-property dance.
DROP TABLE ducklake_ovh.bench_duckdb.region;
DROP TABLE ducklake_ovh.bench_duckdb.nation;
DROP TABLE ducklake_ovh.bench_duckdb.supplier;
DROP TABLE ducklake_ovh.bench_duckdb.part;
DROP TABLE ducklake_ovh.bench_duckdb.partsupp;
DROP TABLE ducklake_ovh.bench_duckdb.customer;
DROP TABLE ducklake_ovh.bench_duckdb.orders;
DROP TABLE ducklake_ovh.bench_duckdb.lineitem;
DROP SCHEMA ducklake_ovh.bench_duckdb CASCADE;

CREATE SCHEMA IF NOT EXISTS ducklake_ovh.bench_duckdb;
CREATE TABLE ducklake_ovh.bench_duckdb.region   WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.region;
CREATE TABLE ducklake_ovh.bench_duckdb.nation   WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.nation;
CREATE TABLE ducklake_ovh.bench_duckdb.supplier WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.supplier;
CREATE TABLE ducklake_ovh.bench_duckdb.part     WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.part;
CREATE TABLE ducklake_ovh.bench_duckdb.partsupp WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.partsupp;
CREATE TABLE ducklake_ovh.bench_duckdb.customer WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.customer;
CREATE TABLE ducklake_ovh.bench_duckdb.orders   WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.orders   WHERE mod(orderkey, 2) = 0;
CREATE TABLE ducklake_ovh.bench_duckdb.lineitem WITH (data_file_format = 'duckdb') AS SELECT * FROM tpch.sf1.lineitem WHERE mod(orderkey, 2) = 0;

-- =============================================================================
-- PERF QUERY — TPC-H Q1 against each format. The CLI prints elapsed time at the
-- end of each result. First run after `docker compose down/up` is cold (no local
-- materialization for duckdb, no S3 page cache for parquet); subsequent runs
-- exercise warm paths. Run several times in each state for a noise floor.
--
-- What this exercises:
--   * parquet path: full scan of ~6M lineitem rows from S3, row-group filter on
--     l_shipdate, GROUP BY + 5 aggregates pushed into Trino's executor.
--   * duckdb path: same scan via DuckDB's native columnar reader, predicate +
--     projection pushed into DuckDB SQL, results streamed back as Arrow batches.
--
-- The first-run delta on duckdb includes one-time .db materialization to local
-- tmp; warm runs skip that step. Restart the trino container (`docker compose
-- restart trino`) to clear the materialized cache and re-measure cold.
--
-- Use `EXPLAIN ANALYZE <query>` for a per-stage breakdown if a delta is
-- surprising.
-- =============================================================================

describe ducklake_ovh.bench_parquet.lineitem ;
SELECT returnflag, linestatus,
       sum(quantity)                          AS sum_qty,
       sum(extendedprice)                     AS sum_base_price,
       sum(extendedprice * (1 - discount))  AS sum_disc_price,
       sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
       avg(quantity)                          AS avg_qty,
       avg(extendedprice)                     AS avg_price,
       avg(discount)                          AS avg_disc,
       count(*)                                 AS count_order
FROM ducklake_ovh.bench_parquet.lineitem
WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;

SET SESSION ducklake_ovh.duckdb_read_mode = 'httpfs';
SET SESSION ducklake_ovh.duckdb_read_mode = 'materialize';

SELECT returnflag, linestatus,
       sum(quantity)                          AS sum_qty,
       sum(extendedprice)                     AS sum_base_price,
       sum(extendedprice * (1 - discount))  AS sum_disc_price,
       sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
       avg(quantity)                          AS avg_qty,
       avg(extendedprice)                     AS avg_price,
       avg(discount)                          AS avg_disc,
       count(*)                                 AS count_order
FROM ducklake_ovh.bench_duckdb.lineitem
WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;

----

SELECT l.returnflag, l.linestatus,
       sum(l.quantity)                                       AS sum_qty,
       sum(l.extendedprice)                                  AS sum_base_price,
       sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
       sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
       avg(l.quantity)                                       AS avg_qty,
       avg(l.extendedprice)                                  AS avg_price,
       avg(l.discount)                                       AS avg_disc,
       count(*)                                              AS count_order,
       count(DISTINCT o.custkey)                             AS distinct_customers,
       any_value(o.custkey)                                  AS sample_custkey,
       sum(o.totalprice)                                     AS sum_order_total
FROM ducklake_ovh.bench_parquet.lineitem l
JOIN ducklake_ovh.bench_parquet.orders   o ON l.orderkey = o.orderkey
WHERE l.shipdate <= DATE '1998-09-02'
GROUP BY l.returnflag, l.linestatus
ORDER BY l.returnflag, l.linestatus;

SET SESSION ducklake_ovh.duckdb_read_mode = 'materialize';
SET SESSION ducklake_ovh.duckdb_read_mode = 'httpfs';

SELECT count(DISTINCT custkey) FROM ducklake_ovh.bench_duckdb.orders;
SELECT custkey FROM ducklake_ovh.bench_duckdb.orders ORDER BY custkey LIMIT 10;
SELECT custkey FROM ducklake_ovh.bench_duckdb.orders LIMIT 10;
SELECT min(custkey), max(custkey), count(*) FROM ducklake_ovh.bench_duckdb.orders;
SELECT count(*), count(DISTINCT custkey), min(custkey), max(custkey)
FROM ducklake_ovh.bench_duckdb.orders;


-- cross parquet / duckdb

SELECT l.returnflag, l.linestatus,
       sum(l.quantity)                                       AS sum_qty,
       sum(l.extendedprice)                                  AS sum_base_price,
       sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
       sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
       avg(l.quantity)                                       AS avg_qty,
       avg(l.extendedprice)                                  AS avg_price,
       avg(l.discount)                                       AS avg_disc,
       count(*)                                              AS count_order,
       count(DISTINCT o.custkey)                             AS distinct_customers,
       any_value(o.custkey)                                  AS sample_custkey,
       sum(o.totalprice)                                     AS sum_order_total
FROM ducklake_ovh.bench_duckdb.lineitem l
JOIN ducklake_ovh.bench_parquet.orders   o ON l.orderkey = o.orderkey
WHERE l.shipdate <= DATE '1998-09-02'
GROUP BY l.returnflag, l.linestatus
ORDER BY l.returnflag, l.linestatus;


SELECT l.returnflag, l.linestatus,
       sum(l.quantity)                                       AS sum_qty,
       sum(l.extendedprice)                                  AS sum_base_price,
       sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
       sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
       avg(l.quantity)                                       AS avg_qty,
       avg(l.extendedprice)                                  AS avg_price,
       avg(l.discount)                                       AS avg_disc,
       count(*)                                              AS count_order,
       count(DISTINCT o.custkey)                             AS distinct_customers,
       any_value(o.custkey)                                  AS sample_custkey,
       sum(o.totalprice)                                     AS sum_order_total
FROM ducklake_ovh.bench_parquet.lineitem l
JOIN ducklake_ovh.bench_duckdb.orders   o ON l.orderkey = o.orderkey
WHERE l.shipdate <= DATE '1998-09-02'
GROUP BY l.returnflag, l.linestatus
ORDER BY l.returnflag, l.linestatus;




