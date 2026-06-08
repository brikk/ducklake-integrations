# DuckLake Connector — Doris fe-connector SPI Implementation Plan

> ## ✅ This is the canonical plan
>
> **Status as of 2026-05-19**: this is the plan we are executing. The legacy
> hardcoded-dispatch plan at `ducklake-doris-integration-plan.md` is marked
> IGNORE FOR NOW — fallback only.
>
> Working order of operations + current state live in
> [`ducklake-doris-todo.md`](./ducklake-doris-todo.md). Dependency-level
> review (including the BE workaround for position deletes and the JDK 17
> ABI rules) lives in
> [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md).

This plan covers **Doris-side SPI mechanics**: which interfaces to
implement, how to wire the plugin into the build, and how to test it at
unit / metadata-integration / live-FE levels. DuckLake semantics
(catalog, types, partitions, deletes, stats, concurrency) live in the
legacy main plan — read those sections for background, but ignore its
architecture recommendations.

This plan now targets the **P-series connector-SPI migration** on branch
**`branch-catalog-spi`** (morningman/doris fork) — the incremental, mergeable
restart of the connector SPI. It **supersedes** the original target, PR
[#62767](https://github.com/apache/doris/pull/62767) ("M0: Lock the plugin SPI
surface" — the *M-series*), which is now kept as a design reference only. See
the dated update directly below; §1 has been rewritten to match.

---

## Targeting & Build Update — 2026-06-08

> Supersedes the original PR #62767 (M-series) targeting. The §1.4 `~/.m2`
> artifact coordinates (still `1.2-SNAPSHOT`) remain valid and unchanged.

### Which SPI to target — lineage resolved

| Thing | What it is | Status |
|---|---|---|
| **PR #62183** | "fe-connector SPI framework + migrate JDBC/ES" | ✅ **MERGED** 2026-04-18 — the shared foundation both efforts descend from. |
| **PR #62767** (branch `spi-mega`) | "M0: lock plugin SPI surface" — the **M-series** | ⚠️ **OPEN, never merged.** Went too deep (all connectors, read+write). **Mine it, don't target it.** Local: `~/DEV/OSS/db/doris-spi-mega` @ `32f1c22`. |
| **`branch-catalog-spi`** | The **P-series**: incremental, risk-ordered, mergeable strangler-fig migration | ✅ **LIVE TARGET.** Local: `~/DEV/OSS/db/doris` @ `5c240dc`. |

The P-series is a re-staged reboot of the same idea — **not** built on #62767's commits.

**Phase roadmap:** P0/P1 infra ✅ · P2 trino ✅ (#64096) · **P3 hudi ✅ (#64143 — frontier, merged 2026-06-06)** · P4 maxcompute → P5 paimon → P6 iceberg ⏳ (each ~2–5 wks; **P6 is months out — don't block on it**).

**Templates:** copy the **P2 trino connector plugin** (the SPI-consumption skeleton — `ConnectorProvider`/`Connector`/`ConnectorMetadata`/`ScanPlanProvider`/`Properties`/`PredicateConverter`/`TypeMapping`). **Hudi (P3)** is the closest lakehouse analog. The **M-series** (`doris-spi-mega`) stays the richest iceberg/write-path reference — treat as provisional, ratify against P6 when it lands.

### Does the new SPI remove the old enum/whitelist workarounds? — MOSTLY YES

**Eliminated** — generic `PLUGIN` mechanisms replace the per-type enums we edited last time: `InitCatalogLog.Type.PLUGIN` (was `…DUCKLAKE`), `TableType.PLUGIN_EXTERNAL_TABLE` (was `DUCKLAKE_EXTERNAL_TABLE`), no `CatalogFactory` `switch` case, no `instanceof` in `PhysicalPlanTranslator`, no `TableFormatType.DUCKLAKE`. The catalog type is now a **free-form property string** via `PluginDrivenExternalCatalog.getType()`.

**Residual — two intentional points** (detailed in the rewritten §1.5): add `"ducklake"` to `SPI_READY_TYPES`, and drop the plugin into `connector_plugin_root`. The whitelist disappears in P8.

## 1. Reference repos & local FE build

**Live target (P-series):** `~/DEV/OSS/db/doris` on branch `branch-catalog-spi`.
**M-series reference:** `~/DEV/OSS/db/doris-spi-mega` on branch `spi-mega`.
Doris is on the **4.1 series**. No worktree/PR-checkout needed — both are
already cloned locally.

### 1.1 Prerequisites — macOS/Intel (verified 2026-06-08)

The FE is pure Java (JDK 17), but Doris's `build.sh` is Linux-shaped and trips
on three macOS gaps — all fixed with Homebrew. `env.sh` auto-generates
`custom_env_mac.sh` that prepends the brew tool bins to `PATH`, so once
installed they're picked up automatically.

| Need | Why it's required | Install |
|---|---|---|
| **JDK 17** | FE pins `maven.compiler=17`; the default JDK (25) is rejected | `JAVA_HOME=~/.sdkman/candidates/java/17.0.2-open` |
| **Maven** | `build.sh` invokes `mvn` | `brew install maven` |
| **gnu-getopt** | macOS BSD `getopt` can't parse `--fe` → the build **silently no-ops** (`BUILD_FE=0`, fake "Successfully build Doris", no `output/fe`) | `brew install gnu-getopt` |
| **coreutils** | `build.sh:276` calls Linux-only `nproc` | `brew install coreutils` |
| **Prebuilt thirdparty** | `gensrc` needs `thrift 0.16.0` + `protoc`; the prebuilt bundle avoids the multi-hour source build | see below |
| **node 18 + pnpm 9** | *only* if building the web UI (off by default here — see §1.2) | asdf `nodejs 18.20.8`; pnpm 9 (10/11 need Node 20+) |

**Prebuilt thirdparty (darwin-x86_64):**

```bash
cd ~/DEV/OSS/db/doris/thirdparty
./download-prebuild-thirdparty.sh master   # 4.1 trunk → 'automation' tag, ~292 MB, download-only
tar -xJf doris-thirdparty-prebuilt-darwin-x86_64.tar.xz   # extracts installed/
```

The arg (`master`/`4.0`/`3.1`/…) selects the Doris branch line, not the thrift
version. The sentinel `installed/lib/libbrotlienc.a` satisfies the
`build.sh:458` check so it won't try to rebuild thirdparty.

### 1.2 Build the FE

```bash
cd ~/DEV/OSS/db/doris
export JAVA_HOME="$HOME/.sdkman/candidates/java/17.0.2-open"
export PATH="/usr/local/opt/gnu-getopt/bin:/usr/local/opt/coreutils/libexec/gnubin:$JAVA_HOME/bin:/usr/local/bin:$PATH"
export DISABLE_BUILD_UI=ON          # else the FE builds the React/webpack-4 console (needs npm)
./build.sh --fe                     # → output/fe/ : doris-fe.jar, start_fe.sh, fe-connector-api/spi jars
```

- **FE-only skips BE submodules** (clucene/orc/faiss are gated behind `--be`).
  Do **not** run `git submodule update` — those are BE and hit access errors.
  (Full build only: `git submodule update --init --recursive`.)
- This produces a runnable FE in `output/fe/` but does **not** publish to `~/.m2`.

**For the plugin's compile classpath**, publish the SPI jars to `~/.m2`
(restrict the reactor to skip fe-core's long compile):

```bash
cd fe
DORIS_THIRDPARTY="$HOME/DEV/OSS/db/doris/thirdparty" \
mvn install -DskipTests -Dskip.doc=true -pl '!fe-core,!hive-udf,!be-java-extensions' -T 1C
```

### 1.3 Dep-trip fallback

The 2026-06-08 build did **not** hit these, but if a later refresh fails on
fastutil/jakarta deps, cherry-pick the two unpushed `catalog-spi-04` fixes:

```bash
git -C ~/DEV/OSS/db/doris fetch morningman catalog-spi-04
git -C ~/DEV/OSS/db/doris cherry-pick 0f9b6645b00   # hive-catalog-shade → 3.1.2-SNAPSHOT (fastutil)
git -C ~/DEV/OSS/db/doris cherry-pick f46bb7eae11   # pin jakarta.servlet-api 6.1.0 (Jetty 12.0.34)
```

Any diff in `fe-connector-api/` or `fe-connector-spi/` is a potential
break-the-plugin change (see §6).

### 1.4 Published artifacts (`~/.m2/repository/org/apache/doris/`)

Current version: **`1.2-SNAPSHOT`** (on `branch-catalog-spi`; same coordinate
the old M-series used). Will rev when the P-series merges and the Doris release
line advances.

For your plugin's compile classpath:

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    compileOnly("org.apache.doris:fe-connector-api:1.2-SNAPSHOT")
    compileOnly("org.apache.doris:fe-connector-spi:1.2-SNAPSHOT")
}
repositories {
    mavenLocal()
    mavenCentral()
}
```

```xml
<!-- Maven -->
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>fe-connector-api</artifactId>
    <version>1.2-SNAPSHOT</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>fe-connector-spi</artifactId>
    <version>1.2-SNAPSHOT</version>
    <scope>provided</scope>
</dependency>
```

Both are `provided`/`compileOnly` because fe-core's parent classloader supplies
them at runtime (per `fe-connector-iceberg`'s `plugin-zip.xml` excludes — see
§2).

Other artifacts in `~/.m2` you may want:

| Coordinate | Use |
|------------|-----|
| `org.apache.doris:fe-extension-spi:1.2-SNAPSHOT` | Cache / extension hooks the SPI calls into. Sometimes needed transitively. |
| `org.apache.doris:fe-connector-trino-connector:1.2-SNAPSHOT` | The P2 template plugin — structural skeleton to copy. Pull the `-sources.jar` to read alongside your own code. *(exact artifactId pending agent verification)* |
| `org.apache.doris:fe-core:1.2-SNAPSHOT` | The whole FE (19 MB). Needed only if you run a live FE locally for §5.3 smoke tests. |

### 1.5 fe-core wiring — two points (on `branch-catalog-spi`)

**(1) `SPI_READY_TYPES` whitelist.** `CatalogFactory.java` gates SPI dispatch
behind a hardcoded set (~line 52 on this branch):

```java
private static final Set<String> SPI_READY_TYPES = ImmutableSet.of("jdbc", "es", "trino-connector");
```

A `ConnectorProvider` whose type is not in this set is **silently ignored** —
the factory falls through to the legacy `switch` and throws `Unknown catalog
type: ducklake`. Shipping DuckLake needs a one-line edit adding `"ducklake"`.
The gate intentionally lets connectors land dormant and is **removed entirely
in P8**, leaving pure ServiceLoader discovery.

**(2) `connector_plugin_root`.** Install the `fe-connector-ducklake` plugin
(shipping `META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`)
under `${DORIS_HOME}/plugins/connector` (`Config.connector_plugin_root`), where
`ConnectorPluginManager` discovers it via `ServiceLoader`. Without it,
`CatalogFactory` throws *"No connector plugin loaded for catalog type
'ducklake'"*.

> ⚠️ Exact `SPI_READY_TYPES` contents and line number are pending confirmation
> against `branch-catalog-spi` HEAD (the research note records `"trino-connector"`
> replacing the old `"iceberg"`; a mapping pass is verifying file:line refs).

## 2. Plugin module layout

```
fe/fe-connector/fe-connector-ducklake/
├── pom.xml
├── src/main/assembly/plugin-zip.xml          # mirror fe-connector-iceberg
├── src/main/resources/META-INF/services/
│   └── org.apache.doris.connector.spi.ConnectorProvider   # one line: FQCN
└── src/main/java/org/apache/doris/connector/ducklake/
    ├── DuckLakeConnectorProvider.java
    ├── DuckLakeConnector.java
    ├── DuckLakeConnectorMetadata.java
    ├── DuckLakeConnectorProperties.java
    ├── DuckLakeScanPlanProvider.java
    ├── DuckLakeTableHandle.java
    ├── DuckLakeColumnHandle.java
    ├── DuckLakeInsertHandle.java
    ├── DuckLakePredicateConverter.java
    ├── DuckLakeTransactionContext.java
    ├── DuckLakeTypeMapping.java
    ├── DuckLakeRefOps.java                    # D5 time travel
    ├── DuckLakeConnectorMvccSnapshot.java     # snapshot codec for FE↔BE
    └── cache/                                 # D3 cache bindings (optional v1)
```

The closest reference is `fe/fe-connector/fe-connector-iceberg/` in the PR.
Iceberg is also a file-based lakehouse format with snapshots, partition
specs, and positional deletes — virtually every type listed above has a
direct Iceberg analogue (`IcebergConnectorProvider`, `IcebergConnector`,
`IcebergConnectorMetadata`, etc.). Copy the Iceberg module's structure as
the starting skeleton and replace Iceberg-specific bits with calls into the
shared `ducklake-catalog` library.

Backend pluggability (Postgres / SQLite / DuckDB metadata DBs) can mirror
the Iceberg backend pattern (`fe-connector-iceberg-backend-{rest,glue,…}`)
as a follow-on, but is **not required for v1** — start with a single
Postgres-backed module.

Add the new module to `fe/fe-connector/pom.xml`'s aggregator `<modules>`.

## 3. SPI surface — what to implement

Per the handbook (`fe/fe-connector/README.md` §7), the minimum implementation
for a working read+write connector is:

| Class                              | What it does                                                  |
|------------------------------------|---------------------------------------------------------------|
| `DuckLakeConnectorProvider`        | `ServiceLoader` entry. `getType() = "ducklake"`. Returns supported backends. Constructs `DuckLakeConnector`. |
| `DuckLakeConnector`                | Holds `ConnectorContext`, exposes `getMetadata`, `getCapabilities`, `getScanPlanProvider`, `defaultTestConnection`. |
| `DuckLakeConnectorMetadata`        | Reads (`getTableHandle`, `getTableSchema`, `applyFilter` / `applyProjection` / `applyLimit`) and writes (`begin*` / `finish*` / `abort*`). Wraps `DucklakeCatalog` from the shared library. |
| `DuckLakeScanPlanProvider`         | Asks the library for data files at the active snapshot, emits `ConnectorScanRange`s with positional-delete file references. |
| `DuckLakeTableHandle` / `ColumnHandle` / `InsertHandle` | Opaque handles passed FE↔BE. Mirror Iceberg's shape. |
| `DuckLakePredicateConverter`       | Doris `ConnectorExpression` tree → library filter format (range/in/null/etc.). |
| `DuckLakeTransactionContext`       | Wraps the library's `DucklakeWriteTransaction`. Drives optimistic-retry on `TransactionConflictException`. |
| `DuckLakeTypeMapping`              | DuckLake type strings ↔ Doris `Type`. Direct port of the Trino plugin's `DucklakeTypeConverter`. |
| `DuckLakeRefOps` (optional v1)     | D5 time travel — `BySnapshot(id)`, `ByTimestamp(ts)`. The library already exposes both. |
| `DuckLakeConnectorMvccSnapshot`    | Sealed type + `Codec` (binary frame) so the snapshot ID flows through FE→BE plumbing. Mirror `IcebergConnectorMvccSnapshot`. |

`ConnectorCapability` declarations (handbook §5) gate which features the
planner uses. Start with the **smallest set that matches v1 scope** (see the
main plan's "Initial Scope" — read, predicate pushdown, identity+temporal
partition pruning, position deletes, FOR VERSION/TIMESTAMP AS OF, INSERT,
DELETE, UPDATE, MERGE, DDL). Do **not** declare a capability without the
matching implementation; the planner will gate features and crash loudly
when they're missing.

Do not implement (v1 deferred, per main plan):
- Bucket partition pruning / writes
- Puffin deletion vectors
- Sorted writes
- RENAME TABLE / RENAME SCHEMA / COMMENT ON TABLE / COMMENT ON COLUMN
- ALTER TABLE SET TYPE, struct field ALTERs
- ANALYZE

## 4. Library wiring (Kotlin shared lib → Java plugin)

The shared library (`dev.brikk.ducklake.catalog`) is moving to Kotlin with
ABI target Java 17. The plugin module stays pure Java to match the rest of
`fe-connector-*`. Kotlin → Java interop is fine; the boundary just needs to
expose Java-friendly types (no `kotlin.Result`, `kotlin.Pair`, default-arg
overloads on the public API). Library author owns this constraint.

Plugin's `pom.xml` declares (in addition to `fe-connector-api` and
`fe-connector-spi`):

- `dev.brikk:ducklake-catalog:<version>` — the shared library
- `org.postgresql:postgresql` — JDBC driver for the metadata DB (provided
  scope; user supplies at deploy time via `DRIVER_URL` per the existing
  `JdbcExternalCatalog` pattern, *or* bundled if community prefers)
- `com.zaxxer:HikariCP:6.0.0` — already in fe-core; library transitively
  depends on it

The library's `JdbcDucklakeCatalog` takes a `DataSource` (or url+user+pass
config); the plugin constructs it from `CREATE CATALOG` properties.

## 5. Testing strategy

The PR ships **no shared TCK / verifier suite** for plugin authors. What
exists, in order of cost:

### 5.1 Unit tests (cheapest, no FE boot)

JUnit 5 + AssertJ. Mirror the Iceberg connector's test layout:

```
fe/fe-connector/fe-connector-ducklake/src/test/java/.../connector/ducklake/
├── DuckLakeConnectorMetadataTest.java
├── DuckLakeConnectorMetadataDeleteFilesTest.java
├── DuckLakeConnectorMetadataWriteOpsTest.java
├── DuckLakeConnectorProviderAccessControllerDefaultTest.java
├── DuckLakePredicateConverterTest.java
├── DuckLakeRefOpsTest.java
├── DuckLakeTimeTravelTest.java
├── DuckLakeTransactionContextTest.java
├── DuckLakeTableHandleTest.java
├── DuckLakeTypeMappingTest.java
└── cache/
    ├── FakeConnectorContext.java         # copy from Iceberg, ~3.5 KB
    ├── InMemoryMetaCacheHandle.java      # copy from Iceberg, ~4 KB
    └── DuckLakeCacheBindingsTest.java
```

The Iceberg module's `cache/FakeConnectorContext.java` (~3.5 KB) and
`cache/InMemoryMetaCacheHandle.java` (~4 KB) are the closest thing to a
reusable harness. Copy them into the DuckLake test sources, rename to
`FakeDuckLakeConnectorContext` if desired, and trim to the surface DuckLake
actually consumes. They give you:

- A `ConnectorContext` instance that returns no-op or in-memory
  implementations of every engine service
- Lets you instantiate `DuckLakeConnectorMetadata` with no fe-core
  dependency and exercise full metadata flows (planning, predicate
  conversion, type mapping, snapshot resolution) in-process

For the DuckLake side specifically, **mock the `DucklakeCatalog` interface
with Mockito** at first; later add tests that wire a real
`JdbcDucklakeCatalog` against a Testcontainers Postgres (the shared
library already does this in its own tests; the plugin can mirror or
import the same testcontainers fixtures).

Run from repo root:

```bash
cd fe
mvn -q -pl fe-connector/fe-connector-ducklake -am \
    -Dmaven.build.cache.enabled=false test
```

The `-Dmaven.build.cache.enabled=false` flag is **required** — the build
cache extension caches Surefire results and silently skips re-runs after
source changes (handbook §9).

### 5.2 In-process metadata-integration tests (medium cost)

Same harness as 5.1 but with a real `JdbcDucklakeCatalog` backed by a
Testcontainers Postgres instance, so the metadata SQL paths are exercised
end-to-end. The shared library already uses this pattern via
`TestingDucklakePostgreSqlCatalogServer` (a `testFixturesApi` export of
the catalog module). Pull it in as a `<scope>test</scope>` dependency:

```xml
<dependency>
  <groupId>dev.brikk</groupId>
  <artifactId>ducklake-catalog</artifactId>
  <classifier>test-fixtures</classifier>
  <scope>test</scope>
</dependency>
```

What these tests prove:

- Plugin correctly translates Doris `ConnectorExpression` → library filter
  → SQL against `ducklake_*` tables
- Snapshot resolution against real `ducklake_snapshot` rows
- Type mapping round-trips for every DuckLake primitive and nested type
- Transaction commit / rollback semantics on `TransactionConflictException`
- Predicate pushdown actually prunes via `ducklake_file_column_stats`

What they don't prove: whether the plugin loads correctly via
`ServiceLoader` into a running FE, or whether scan-range plumbing reaches
the BE Parquet reader. Those need 5.3 / 5.4.

### 5.3 Live-FE smoke test (high cost, gives end-to-end signal)

Drop the plugin jar into a running Doris FE and exercise it with real SQL.
There's no in-process FE-loaded harness in the PR. The flow is:

1. `sh build.sh --fe` from the PR worktree builds the FE with all
   `fe-connector-*` modules.
2. Build the plugin jar: `mvn -q -pl fe-connector/fe-connector-ducklake
   -am package`. The `plugin-zip.xml` assembly produces the deployable jar.
3. Boot a single-node FE + BE (the Doris dev compose stack, or a minimal
   `fe.conf` + `start_fe.sh`). FE `META-INF/services` discovery picks up
   the plugin from the build output.
4. Stand up a Postgres + S3 DuckLake source. Easiest: use the
   `trino-ducklake/compose/` stack from `~/DEV/brikk/repos/ducklake-integrations`
   — it ships a Postgres + MinIO + seed-data stack.
5. From `mysql -h <fe-host> -P 9030`:
   ```sql
   CREATE CATALOG dl PROPERTIES (
       'type' = 'ducklake',
       'metadata.url' = 'jdbc:postgresql://localhost:5432/ducklake_meta',
       'metadata.user' = 'ducklake',
       'metadata.password' = '...',
       'storage.warehouse' = 's3://warehouse/'
   );
   USE dl.public;
   SHOW TABLES;
   SELECT COUNT(*) FROM <table>;
   SELECT * FROM <table> LIMIT 10;
   ```

Smoke-test checklist (the boot-up validation):

- [ ] FE log shows `Loaded ConnectorProvider: org.apache.doris.connector.ducklake.DuckLakeConnectorProvider, type=ducklake`
- [ ] `CREATE CATALOG dl ...` returns success (no `IllegalArgumentException` from `validateProperties`)
- [ ] `SHOW CATALOGS` lists `dl`
- [ ] `SHOW DATABASES IN dl` enumerates schemas from `ducklake_schema`
- [ ] `SHOW TABLES IN dl.<db>` enumerates tables from `ducklake_table` at the current snapshot
- [ ] `DESC dl.<db>.<table>` matches the column list in `ducklake_column`
- [ ] `SELECT COUNT(*) FROM dl.<db>.<table>` returns `ducklake_table_stats.record_count`
- [ ] `SELECT * FROM dl.<db>.<table> LIMIT 10` returns rows (BE successfully reads Parquet at the file paths the plugin emitted)
- [ ] `EXPLAIN VERBOSE SELECT * FROM ... WHERE col > 100` shows file-level pruning happened (fewer files than total)
- [ ] DuckDB writes via `ducklake-integrations/compose/` are visible to Doris reads (cross-engine round trip)

### 5.4 Regression tests (Doris's standard mechanism)

Doris has Groovy-DSL regression suites under `regression-test/`. The
existing pattern is:

- `regression-test/suites/external_table_p0/iceberg/` — read-side suites
- `regression-test/suites/external_table_p2/iceberg/` — write-side suites

For DuckLake, mirror with `external_table_p0/ducklake/` and
`external_table_p2/ducklake/`. The `doris-docker-regression` skill
(available in this checkout) runs these against a clean Doris package; that
is the canonical "is the integration regression-clean" gate.

What v1 should cover:

- Read: SELECT, WHERE pushdown, LIMIT, ORDER BY, COUNT(*), aggregates
- Partition pruning: identity + each temporal transform
- Time travel: `FOR VERSION AS OF <id>`, `FOR TIME AS OF <ts>`
- Position deletes: read a table that has DELETE history applied
- Schema evolution: read a table after ADD COLUMN / DROP COLUMN / RENAME
- Write: INSERT, CTAS, DELETE, UPDATE, MERGE
- DDL: CREATE / DROP SCHEMA / TABLE, ALTER ADD/DROP/RENAME COLUMN
- Cross-engine: DuckDB-written → Doris-read, Doris-written → DuckDB-read

Fixture data can be generated by the trino-ducklake compose stack, or by a
small Java/Kotlin seed script that uses the shared library directly.

## 6. Tracking SPI churn

The P-series SPI is still evolving phase-by-phase, so the API surface can shift
under us. Track changes by following `branch-catalog-spi`:

```bash
# in ~/DEV/OSS/db/doris:
git fetch morningman branch-catalog-spi
git diff HEAD..morningman/branch-catalog-spi -- fe/fe-connector
```

Any diff in `fe-connector-api/` or `fe-connector-spi/` is a potential
break-the-plugin change. Diffs in the in-tree connectors (trino, hudi, …) are
informative — they show how the SPI is meant to be consumed — but don't affect
a downstream plugin's compile. Watch the P4→P6 PRs as they land; the **P6
iceberg PR** will be the golden lakehouse sample.

## 7. Hand-off checklist

For the agent picking up implementation:

- [ ] FE built from `branch-catalog-spi`; SPI artifacts published to `~/.m2`
- [ ] Plugin module skeleton (`fe-connector-ducklake/`) created by copying the
      **P2 trino connector plugin** (closest current template) and renaming
- [ ] `META-INF/services/...ConnectorProvider` registered
- [ ] Module added to `fe/fe-connector/pom.xml` aggregator
- [ ] Library dependency wired (Kotlin lib, Java 17 ABI)
- [ ] First smoke test: `CREATE CATALOG dl PROPERTIES('type'='ducklake', ...)` succeeds against a Postgres+MinIO DuckLake source
- [ ] First read: `SELECT COUNT(*) FROM dl.<db>.<table>` returns the right number
- [ ] First scan: `SELECT * FROM dl.<db>.<table> LIMIT 10` returns rows from BE's native Parquet reader
- [ ] Predicate pushdown observable in `EXPLAIN VERBOSE` (file-level pruning)
- [ ] Time-travel `FOR VERSION AS OF` returns the correct historical snapshot
- [ ] First write: `INSERT INTO dl.<db>.<table> VALUES (...)` commits and is visible to a DuckDB reader against the same metadata DB

After those gates, the work is incremental — each remaining feature
(MERGE, schema evolution, partition pruning per transform, MVCC pinning)
unlocks one regression-test file at a time.

## 8. Cross-references

- Main plan: `docs/ducklake-doris-integration-plan.md` (DuckLake semantics,
  library architecture, scope, open questions)
- **Live target:** branch `branch-catalog-spi` (morningman/doris) at
  `~/DEV/OSS/db/doris` — see the Targeting Update at the top of this doc
- **Template plugin:** the P2 trino connector (#64096) on `branch-catalog-spi`;
  hudi (P3, #64143) is the closest lakehouse analogue
- **M-series reference (design only):** PR
  <https://github.com/apache/doris/pull/62767>, branch `spi-mega` at
  `~/DEV/OSS/db/doris-spi-mega` — richest iceberg/write-path shapes
- SPI handbook: `fe/fe-connector/README.md` on `branch-catalog-spi`
  *(exact path pending agent verification)*
- Shared library: `~/DEV/brikk/repos/ducklake-integrations/jvm/ducklake-catalog/`
- Trino reference adapter:
  `~/DEV/brikk/repos/ducklake-integrations/jvm/trino-ducklake/`
