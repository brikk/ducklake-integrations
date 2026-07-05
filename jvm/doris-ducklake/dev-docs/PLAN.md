# DuckLake-on-Doris — The Plan

> Created 2026-07-05 on the P6 baseline (`branch-catalog-spi` @ `8b391c7`, the
> iceberg SPI cutover — see [`REPORT-doris-p6-iceberg-spi-cutover.md`](./REPORT-doris-p6-iceberg-spi-cutover.md)).
> This is the **big-step plan**: phases, ordering rule, and exit criteria.
> Micro items live in [`TODO-read.md`](./TODO-read.md) /
> [`TODO-write.md`](./TODO-write.md) / [`TODO-research.md`](./TODO-research.md);
> collect new items there as we go, not here.
>
> **The ordering rule: read-side Parquet fully → write-side Parquet →
> other file formats last, only if they still make sense for Doris.**

## Where we are (2026-07-05, validated live)

The full compose smoke is GREEN on the P6 FE: reads with snapshot pinning,
CREATE/DROP DATABASE + TABLE (unpartitioned + bucket-partitioned), INSERT,
bucketed INSERT (murmur3 parity), CTAS. Step-7 delete-file reads reach the one
known BE blocker (parquet nullability). The connector compiles against the P6
SPI, tests + detekt green, plugin zip + FE overlay image current.

So: read is *good* but not *full*; write is *started* (INSERT/DDL) but not
*full*. That maps exactly onto phases R and W below.

---

## Phase R — Read side, Parquet, FULLY  *(current phase)*

Everything a Doris user can ask of a Parquet-backed DuckLake table via SELECT
must work and be smoke-provable. The P6 bridge removed every fe-core ceiling
that used to be iceberg-only; the remaining work is ours plus one BE blocker.

Big steps, roughly in order:

1. **Merge-on-read deletes actually filter rows.** FE plumbing is done
   (delete files ride `iceberg_params.delete_files`); blocked on the BE
   OPTIONAL-vs-REQUIRED parquet nullability gap
   ([`REPORT-doris-iceberg-reader-strict-on-delete-file-nullability.md`](./REPORT-doris-iceberg-reader-strict-on-delete-file-nullability.md),
   [`REPORT-ducklake-position-delete-parquet-nullability.md`](./REPORT-ducklake-position-delete-parquet-nullability.md),
   friction 2026-05-19). Path A: upstream/patch the BE reader to take the
   nullable path for delete columns. Path B: DuckLake writes REQUIRED delete
   columns. Includes inline-delete synthesis (DuckLake inlines small DELETEs
   into catalog rows — surface them to the BE or synthesize a delete file).
2. **Pushdown completeness.** Filter/projection pushdown exist; finish the
   ladder: limit pushdown, **count pushdown** (P6 threads `countPushdown`
   through `planScan` — free COUNT(*) from catalog stats), partition pruning
   wired through `applyFilter`, file pruning from DuckLake column stats.
3. **Partition correctness hardening.** Adopt `isPartitionBearing()` + return
   metadata partition values on scan ranges so the BE never hive-path-parses
   our non-`key=value` layout (P6 seam, iceberg does this).
4. **Statistics.** `getTableStatistics` from `ducklake_table_stats` /
   `ducklake_file_column_stats` so the planner sees row counts and join orders
   stop being blind.
5. **Time travel completeness.** Snapshot pin + `FOR VERSION/TIME AS OF` work;
   finish `VERSION_REF` (named refs, new P6 `Kind.VERSION_REF`) or reject
   cleanly, and version-aware pins for mixed-version statements.
6. **Read-parity audit vs trino-ducklake.** Sweep the trino read suite for
   semantics (types, temporal TZ, unicode, nested types) and pin what applies;
   the two structural non-goals stay out (function pushdown, DuckDB-native
   execution — Doris BE evaluates, see archived roadmap §1).

**Exit:** any SELECT a trino-ducklake user can run against Parquet-backed
DuckLake works on Doris with correct rows — including after DELETEs — and the
smoke proves it.

Out of R by design (execution-model mismatch, not backlog): function/expression
pushdown, DuckDB-native `.db` reads.

## Phase W — Write side, Parquet

INSERT/DDL/CTAS are live; the P6 write-framework unification opened every
remaining door (see the P6 report §1 — this is where their iceberg gold pays
off, since we ride the same generic seams). Big steps:

1. **Row-level DML: DELETE / UPDATE / MERGE.** Declare the ops, supply
   `getSyntheticWriteColumns` (row-id STRUCT), dispatch `planWrite` on
   `WriteOperation` (`TIcebergTableSink` vs delete/merge sinks), map commit
   data to DuckLake delete files (`commitDelete`/`commitMerge` primitives
   already exist in `ducklake-catalog`). The P6 MoR machinery admits us purely
   by capability — no FE patch.
2. **INSERT OVERWRITE + static-partition INSERT.** Add `OVERWRITE` to
   `supportedOperations()`; declare `requiresMaterializeStaticPartitionValues()`
   + `validateStaticPartitionColumns()` so `PARTITION (col=val)` literals
   materialize Iceberg-style.
3. **Sorted writes.** `getWriteSortColumns()` → engine-built `TSortInfo`
   stamped on the sink (unparks the trino-side "sorted writes PARKED" item for
   Doris).
4. **Maintenance procedures.** `getProcedureOps()`: `expire_snapshots`,
   `rewrite_data_files` (fe-core supplies the distributed rewrite driver),
   riding the same GC/recompaction semantics `ducklake-catalog` already
   implements for trino.
5. **Write hardening.** `invalidateAll()` on REFRESH CATALOG, narrow-int BE
   writer crash avoidance (friction 2026-06-10), OCC write-constraint checks
   (`applyWriteConstraint`), schema evolution DDL (ALTER column ops — the P6
   `ConnectorTableOps` surface) as demanded.

**Exit:** cross-engine write parity for Parquet — anything written by Doris is
readable by DuckDB/trino-ducklake and vice versa, DML included, smoke-proven.

## Phase F — Other file formats  *(last; only if still sensible)*

DuckLake can also carry DuckDB-native `.db` data files (and the wider lake
world has Lance/Vortex). For Doris these are **unlikely to make sense**: the BE
reads Parquet (and ORC) natively in C++ and has no DuckDB/Lance/Vortex reader;
there is no in-JVM execution bridge like trino's. Decide *at the end of W*,
with fresh facts:

- Re-check whether upstream Doris grew relevant BE readers or a JNI scanner
  seam (`be-java-extensions` — e.g. the paimon/hudi JNI scanners — is the only
  plausible route today).
- If still no: record as permanently out of scope and keep the connector
  honestly Parquet-only (reject non-Parquet data files with a clear error at
  plan time rather than a BE crash).

## Continuous track — baseline & upkeep (not a phase)

- **Track `branch-catalog-spi`.** Each baseline bump: reset worktree → re-apply
  `fe-patches/ducklake-fe.patch` → rebuild FE → `mvn install -P flatten` the
  SPI jars → connector tests → smoke. Procedure in
  [`../fe-patches/FE-PATCHES.md`](../fe-patches/FE-PATCHES.md).
- **Patch upstreaming.** Keep pushing the two asks: `SPI_READY_TYPES`
  registration seam and connector-declared engine mapping (P6's own javadoc
  acknowledges the burden).
- **Vendor migration (optional).** `vendor/doris` exists in-tree; if the
  external `~/DEV/OSS/doris-catalog-spi` worktree becomes a liability, fetch
  `branch-catalog-spi` into the vendored clone and repoint docs/build.
- **FE/BE skew watch.** The 4.1.0 BE needs the `enable_local_shuffle_planner`
  shim (friction 2026-07-05); drop it when a matching BE image exists.

## Doc map

| Doc | Role |
|---|---|
| [`PLAN.md`](./PLAN.md) | this — big steps + ordering rule |
| [`TODO-read.md`](./TODO-read.md) | read-path items (phase R detail) |
| [`TODO-write.md`](./TODO-write.md) | write-path items (phase W detail) |
| [`TODO-research.md`](./TODO-research.md) | unscheduled research / ideas |
| [`REPORT-doris-p6-iceberg-spi-cutover.md`](./REPORT-doris-p6-iceberg-spi-cutover.md) | P6 baseline analysis — what the SPI now offers + adoption roadmap |
| [`ducklake-doris-friction.md`](./ducklake-doris-friction.md) | running log of SPI/FE/BE surprises (upstream-pickable fixes) |
| [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md) | SPI mechanics, build wiring, test harness (canonical reference) |
| [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md) | one-shot architectural review |
| REPORT-\*delete\*-nullability.md | the phase-R BE blocker, both sides |
| [`archive/`](./archive/) | superseded: pre-P6 roadmap, W1b handoff, fallback non-SPI plan |
