# DuckLake × Doris Connector — Development Roadmap

> **Created 2026-06-08.** Companion to `ducklake-doris-integration-spi-plan.md`
> (SPI mechanics + build) and `ducklake-doris-todo.md` (step-by-step state).
> This doc is the **"get us moving" plan**: where we are, how far the Doris SPI
> lets us go versus the mature `trino-ducklake` plugin, and the phased work to
> get there. Synthesized from a three-way investigation (2026-06-08) of the
> Doris `branch-catalog-spi` SPI, our existing `doris-ducklake` module, and the
> `trino-ducklake` read path.

---

## 0. TL;DR

- **`SELECT *` over Parquet already works.** Our `doris-ducklake` module (provider
  → connector → metadata → scan-plan → Iceberg-shaped thrift → S3 creds) is
  implemented and unit-tested; a live FE+BE smoke returned rows on 2026-05-19
  (against the M-series `1.2-SNAPSHOT`). The job is **not** "build SELECT *" — it
  is **retarget to the P-series SPI, then climb the read-path ladder.**
- **The Doris SPI read-path ceiling is generous** and can reach ~trino-ducklake
  *read* parity: filter/projection/limit pushdown, statistics, partition listing,
  time-travel, MVCC pin — all present in `fe-connector-api`.
- **Two trino-ducklake features are out of scope by design, not backlog:**
  function/expression pushdown (the ~80 `trino_parity` DuckDB macros) and
  DuckDB-native file reads. Both ride trino-ducklake's in-JVM DuckDB execution
  bridge. **Doris BE reads Parquet natively**, so there is no DuckDB to push to —
  a different execution model, not missing code.
- **Testing got easier**: `branch-catalog-spi` ships an in-process connector test
  harness (`FakeConnectorPlugin` + the trino/hudi plugin tests) that needs no
  cluster. Our tests already match that style.

---

## 1. Execution model — why Doris ≠ Trino here (read this first)

| | `trino-ducklake` | `doris-ducklake` |
|---|---|---|
| Where scans execute | **In-JVM**: Trino's `ParquetReader`, or DuckDB (`InProcess`/`Quack`) for the DuckDB-native path | **In Doris BE** (C++), via native Parquet reader |
| Connector's job | Metadata **and** data: plans splits *and* produces `Page`s | **FE-side only**: metadata + scan-*plan*; emits Iceberg-shaped thrift ranges, BE does the reading |
| Predicate eval | Trino operators, or pushed into DuckDB (`trino_parity` macros) | **Doris BE** evaluates during its native scan; `applyFilter` just hands BE a filter + prunes files |
| `trino_parity` extension | Central to the DuckDB path (function parity) | **Not in the read path at all** |
| DuckDB-native `.db` files | Read via DuckDB executor + Arrow bridge | **Unreadable by BE** → Parquet-only |

**Consequence:** the Doris connector is *simpler* on execution (no executor/Arrow
bridge) but is **bounded by what Doris BE's Parquet reader accepts** — which is
exactly where the one known blocker lives (§5, OPTIONAL-column deletes).

---

## 2. Capability matrix — trino-ducklake vs. "Doris SPI allows" vs. "ours today" vs. "v1 target"

| Read-path feature | trino-ducklake | Doris SPI allows? | doris-ducklake today | v1 target |
|---|:--:|---|---|:--:|
| Schema / table listing | ✅ | ✅ `listDatabaseNames` / `listTableNames` | ✅ done | ✅ |
| Table handle + schema + columns | ✅ | ✅ `getTableHandle/Schema/ColumnHandles` | ✅ done | ✅ |
| Type mapping | ✅ full | ✅ connector returns `ConnectorType` | ✅ done (full parser) | ✅ |
| Full scan `SELECT *` (Parquet) | ✅ | ✅ `planScan` + `FILE_SCAN` | ✅ done (FE) + live smoke | ✅ (retarget) |
| Predicate / filter pushdown | ✅ | ✅ `applyFilter(ConnectorFilterConstraint)` | ❌ "Step 6" | ✅ **Phase 1** |
| Projection pushdown | ✅ | ✅ `applyProjection` | ❌ "Step 6" | ✅ **Phase 1** |
| Limit pushdown | ❌ (Trino enforces above) | ✅ `applyLimit` | ❌ | ⚠️ cheap add |
| Time travel (`FOR VERSION/TIME`) | ✅ | ✅ `beginQuerySnapshot/getSnapshotAt/ById` + `SUPPORTS_TIME_TRAVEL` | ❌ "Step 8" (MVCC codec already wired) | ✅ **Phase 2** |
| Column statistics | ✅ | ✅ `getTableStatistics` (rowCount + dataSize) | ⚠️ capability declared, no impl | ✅ **Phase 3** |
| Partition pruning | ✅ | ✅ `listPartitions` + `applyFilter` | ⚠️ capability declared, no impl | ✅ **Phase 3** |
| COUNT(*) shortcut | partial | ⚠️ no `applyAggregation`; via `getTableStatistics` rowCount | ❌ | ⚠️ via stats |
| Position deletes (MOR) | ✅ | ✅ delete files on scan range | ✅ FE plumbing; ❌ **BE-blocked** | ✅ **Phase 4** |
| Inline deletes | ✅ | ✅ | ❌ "Step 7.5" (`@Disabled` test) | ✅ **Phase 4** |
| Equality deletes | ❌ | — | ❌ | ❌ (DuckLake doesn't emit) |
| Function / expression pushdown | ✅ (~80 via `trino_parity`) | ⚠️ **N/A to Doris model** (BE evaluates) | ❌ | ❌ **out of scope** |
| DuckDB-native `.db` reads | ✅ (DuckDB executor) | ❌ BE has no DuckDB reader | ❌ | ❌ **Parquet-only v1** |
| Write path (INSERT/CTAS/DELETE/UPDATE/MERGE/DDL) | ✅ full | ✅ `ConnectorWriteOps` | ❌ | ❌ post-v1 |

**Reading the matrix:** everything marked ✅ in "v1 target" is reachable on the
current SPI and brings us to **read parity with trino-ducklake for Parquet-backed
DuckLake tables**. The two ❌-out-of-scope rows are the only real read gaps, and
they are execution-model mismatches (§1), not unfinished work.

---

## 3. Phased roadmap

Phase labels map to the existing `ducklake-doris-todo.md` "Step N" markers.

### Phase 0 — Retarget to the P-series + re-green  *(small, do first)*
The module currently compiles against M-series `1.2-SNAPSHOT`; the P-series uses
the **same coordinate** but the SPI is now `ConnectorMetadata` composed of six
sub-interface ops (`Schema/Table/Pushdown/Statistics/Write/IdentifierOps`).
1. Build + publish the SPI to `~/.m2` from `branch-catalog-spi` (see spi-plan §1.2:
   `mvn install … -pl '!fe-core,!hive-udf,!be-java-extensions'`).
2. **Diff the SPI shapes** the module imports (esp. `ConnectorMetadata`,
   `ConnectorScanPlanProvider.planScan`, `ConnectorScanRange`, the thrift
   `TIcebergFileDesc/TTableFormatFileDesc`) against the freshly-built jars; fix
   any signature drift. *This is the main unknown — quantify it early.*
3. Re-enable `include(":doris-ducklake")` in `jvm/settings.gradle.kts`.
4. `./gradlew :doris-ducklake:test` green (in-process, no cluster).
5. Add `"ducklake"` to `SPI_READY_TYPES` (`CatalogFactory.java:52`).
6. Re-confirm `SELECT *` via `compose/smoke.sh` (live FE+BE).
- **Exit:** module builds, unit tests green, `SELECT *` returns rows on P-series.

### Phase 1 — Predicate + projection pushdown  *("Step 6")*
1. Add `DuckLakePredicateConverter`: `ConnectorExpression` → (a) a filter Doris BE
   applies during its Parquet scan, and (b) file-level pruning via
   `catalog.findDataFileIdsInRange` / column stats.
2. Implement `applyFilter` / `applyProjection` on `DuckLakeConnectorMetadata`;
   return `FilterApplicationResult` (fully/partially enforced) and honor
   `notPushedConjunctIndices`.
3. Declare `SUPPORTS_FILTER_PUSHDOWN` / `SUPPORTS_PROJECTION_PUSHDOWN`.
4. (Optional, cheap) `applyLimit`.
- **Tests:** `DuckLakePredicateConverterTest` (mirror `TrinoPredicateConverterTest`),
  in-process. **Exit:** `EXPLAIN` shows pushed filters; file pruning observable.

### Phase 2 — Time travel  *("Step 8")*
1. `getTableHandle` overload taking `ConnectorTableVersion` → resolve via
   `catalog.getSnapshotAtOrBefore` / by id (MVCC codec is already wired).
2. Implement `beginQuerySnapshot` / `getSnapshotAt` / `getSnapshotById`; declare
   `SUPPORTS_TIME_TRAVEL`.
- **Tests:** snapshot resolution in-process. **Exit:** `FOR VERSION/TIME AS OF`
  returns the correct historical snapshot.

### Phase 3 — Statistics + partition pruning
1. `getTableStatistics` → `catalog.getTableStats` (rowCount) + `getColumnStats`
   (min/max/nulls). Gives the COUNT(*) shortcut for free.
2. Partition path: `listPartitions` + `catalog.getPartitionSpecs` /
   `getFilePartitionValues` + file pruning; wire into the Phase-1 `applyFilter`.
- **Tests:** in-process stats + partition pruning. **Exit:** planner sees row
  counts; partitioned tables prune files.

### Phase 4 — Deletes / merge-on-read
1. Inline-delete synthesis ("Step 7.5"): surface `ducklake_inlined_delete_<tableId>`
   to BE (synthetic delete file or inline path); un-`@Disable` the test.
2. **Resolve the BE blocker** (§5) so file-based position deletes actually filter
   rows. FE plumbing is already done.
- **Exit:** DELETE/UPDATE-then-read returns correct rows.

### Out of v1 scope (record, don't schedule)
- **Function/expression pushdown** — model mismatch (§1); Doris BE evaluates.
- **DuckDB-native format reads** — BE has no DuckDB reader; Parquet-only.
- **Write path** — `ConnectorWriteOps` exists; defer to a v2 once read is solid.

---

## 4. Testing strategy  *(harness is better now)*

`branch-catalog-spi` introduced a real in-process connector test surface — use it.

- **In-process unit (no cluster), per phase** — the default. Pattern: copy
  `FakeConnectorPlugin` (`fe-core/src/test/.../connector/fake/`) for
  `ConnectorSession`/`ConnectorContext` doubles; mirror the trino/hudi plugin
  tests (`ProviderTest`, `TypeMappingTest`, `PredicateConverterTest`,
  `ScanRangeTest`). **Our existing `doris-ducklake` tests already do this**
  (Testcontainer Postgres for a real DuckLake catalog, full FE-side
  metadata + scan-plan + thrift-parity coverage, no live cluster).
- **Live end-to-end** — `compose/smoke.sh` (real FE+BE) for `SELECT *` and each
  phase's exit criterion.
- **CI regression (later)** — add a groovy suite under
  `regression-test/suites/external_table_p0/ducklake_connector/` mirroring
  `…/trino_connector/`, gated by a `regression-conf.groovy` flag, so DuckLake
  rides the same CI path as trino/hudi.

---

## 5. Known blockers & risks

- **BE OPTIONAL-column position deletes** *(Phase 4 blocker, BE-side).* DuckLake
  writes position-delete Parquet with `OPTIONAL` columns; Doris BE's
  `ScalarColumnReader` fast-path requires `REQUIRED` and throws
  `[CORRUPTION] Not nullable column has null values`. FE threads the paths
  correctly. **Fix:** either DuckDB writes `REQUIRED`, or Doris BE falls through
  to the nullable path for Iceberg-spec delete columns. Needs a BE-side change or
  upstream coordination.
- **SPI drift risk** *(Phase 0).* "Same `1.2-SNAPSHOT` coordinate" ≠ guaranteed
  same shapes. The Phase-0 diff is the gate; budget for small signature fixes
  (`ConnectorMetadata` sub-interface split is the most likely touch point).
- **`SPI_READY_TYPES` is a manual fe-core edit** until P8 removes the gate.
- **Option-A thrift encoding** (claiming `iceberg` table-format) is a workaround;
  watch for a first-class DuckLake/plugin file-desc as the SPI matures.

---

## 6. Immediate next actions

1. **Phase 0, steps 1–2**: publish P-series SPI to `~/.m2`, diff against the
   module's imports, fix drift. (Highest-information, do today.)
2. Re-enable the module + green `:doris-ducklake:test`.
3. Add `"ducklake"` to `SPI_READY_TYPES`; re-run `compose/smoke.sh`.
4. Open Phase 1 (`DuckLakePredicateConverter`) — the first real capability climb.

*Cross-refs:* `ducklake-doris-integration-spi-plan.md` (SPI surface, build,
wiring), `ducklake-doris-todo.md` (Step N state), `ducklake-doris-sanity-check.md`
(BE delete workaround, JDK-17 ABI).
