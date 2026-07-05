# doris-ducklake

⚠️ **Pre-alpha. Do not use.** This plugin targets the unreleased
`fe-connector` catalog SPI from Apache Doris's `branch-catalog-spi`
(the P-series migration tracked in [apache/doris#62767](https://github.com/apache/doris/pull/62767)
and successors), which changes shape on every baseline bump. It also
depends on a two-line FE patch ([`fe-patches/`](./fe-patches/)) that is
not in any Doris release, so a stock Doris build cannot load it.

State (2026-07-05, P6 baseline `8b391c7`): reads with snapshot pinning,
filter/projection pushdown, live DDL (CREATE/DROP DATABASE + TABLE,
bucket-partitioned included), INSERT and CTAS all pass the live FE+BE
smoke ([`compose/smoke.sh`](./compose/smoke.sh)); merge-on-read delete
*reads* are blocked on a known BE parquet-nullability gap, and
DELETE/UPDATE/MERGE writes are not built yet.

Plan and working docs live in [`dev-docs/`](./dev-docs/) — start with
[`dev-docs/PLAN.md`](./dev-docs/PLAN.md) (phases: read-side Parquet
fully → write-side Parquet → other formats only if they still make
sense for Doris).

The plugin is unstable (regularly broken by upstream SPI churn) and
unsupported. If you are looking for a working DuckLake adapter for an
OLAP engine today, use [`trino-ducklake`](../trino-ducklake/) instead.
This directory exists so the integration is ready the day the upstream
SPI lands; treat anything you find here as a worktree, not a release.
