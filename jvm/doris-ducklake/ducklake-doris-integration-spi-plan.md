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

This plan targets **Apache Doris PR
[#62767](https://github.com/apache/doris/pull/62767)** ("M0: Lock the plugin
SPI surface"), which establishes the `fe-connector` plugin SPI. DuckLake
ships as a sibling connector module under that PR's structure.

## 1. Reference branch & local build

The PR head is in morningman's fork. Pull it as a worktree to keep `master`
clean:

```bash
# from /Users/jminard/DEV/OSS/db/doris (existing checkout):
git worktree add ../doris-pr-62767
cd ../doris-pr-62767
gh pr checkout 62767
```

### 1.1 Prerequisites (one-time)

- **JDK 17.** Doris pins to 17; the build script rejects anything else. Set
  `JAVA_HOME` (and `JDK_17` if your `java` default isn't 17).
- **Thirdparty toolchain.** `fe-thrift` requires `thirdparty/installed/bin/thrift`
  (Apache Thrift 0.16.0) and `gensrc` needs `protoc`. Brew thrift is the wrong
  version; use the prebuilt bundle:

  ```bash
  cd thirdparty
  sh download-prebuild-thirdparty.sh master   # ~490 MB on darwin-arm64
  tar -xJf doris-thirdparty-prebuilt-darwin-arm64.tar.xz
  ```

  The argument (`master`/`4.0`/`3.1`/`3.0`/`2.1`) selects the Doris branch, not
  the thrift version — the bundle pins all native build deps.

### 1.2 Build

```bash
# from repo root
JAVA_HOME=/path/to/jdk17 JDK_17=/path/to/jdk17 sh generated-source.sh

# full FE install (publishes everything to ~/.m2):
cd fe
JAVA_HOME=/path/to/jdk17 \
DORIS_THIRDPARTY=/path/to/doris-pr-62767/thirdparty \
mvn install -DskipTests -Dskip.doc=true -T 1C
```

If you only need the API/SPI jars (skips fe-core's ~7-min compile), restrict
the reactor to the connector aggregator and its prereqs:

```bash
mvn install -DskipTests -Dskip.doc=true \
    -pl '!fe-core,!hive-udf,!be-java-extensions' -T 1C
```

### 1.3 Refreshing against new PR commits

```bash
cd doris-pr-62767
git fetch origin pull/62767/head:pr-62767-fresh && git reset --hard pr-62767-fresh
cd fe
mvn install -DskipTests -Dskip.doc=true -T 1C
```

Any diff in `fe-connector-api/` or `fe-connector-spi/` is a potential
break-the-plugin change (see §6).

### 1.4 Published artifacts (`~/.m2/repository/org/apache/doris/`)

Current version: **`1.2-SNAPSHOT`** (PR #62767 head). Will change when the PR
merges and the Doris release line revs.

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
| `org.apache.doris:fe-connector-iceberg:1.2-SNAPSHOT` | Closest reference plugin. Pull the `-sources.jar` to read alongside your own code. |
| `org.apache.doris:fe-core:1.2-SNAPSHOT` | The whole FE (19 MB). Needed only if you run a live FE locally for §5.3 smoke tests. |

### 1.5 fe-core whitelist (required, today)

`CatalogFactory.java` gates SPI dispatch behind a hardcoded whitelist:

```java
private static final Set<String> SPI_READY_TYPES = ImmutableSet.of("jdbc", "es", "iceberg");
```

A `ConnectorProvider` whose `getType()` is not in this set is **silently
ignored** — the factory falls through to the legacy `switch` and throws
`Unknown catalog type: ducklake`. So shipping the DuckLake plugin requires a
one-line edit to add `"ducklake"` to that set. This worktree already carries
the edit; rebase against PR head before each refresh and re-apply if it gets
clobbered.

This whitelist is a transition mechanism, not the final SPI design. Push back
to the Doris team for "any registered `ConnectorProvider` wins" once the SPI
stabilizes.

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

## 6. Tracking PR churn

The PR description claims the API surface is locked at M0, but reviewer
pushback may force tweaks before merge. Track changes by:

```bash
# in the worktree:
git fetch origin pull/62767/head:pr-62767-fresh
git diff pr-62767-fresh -- fe/fe-connector/fe-connector-api fe/fe-connector/fe-connector-spi
```

Any diff in `fe-connector-api/` or `fe-connector-spi/` is a potential
break-the-plugin change. Diffs elsewhere (tests, in-tree connector
implementations) are informative but don't affect a downstream plugin's
compile.

`gh pr view 62767 --comments` shows reviewer feedback; flagged comments on
`*Provider`, `Connector*`, `*Capability`, or `*Ops` types are the highest-
risk areas for plugin authors.

## 7. Hand-off checklist

For the agent picking up implementation:

- [ ] Worktree pulled at PR #62767 head; SPI artifacts in `~/.m2`
- [ ] Plugin module skeleton (`fe-connector-ducklake/`) created by copying
      `fe-connector-iceberg/` and renaming
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
- Reference PR: <https://github.com/apache/doris/pull/62767>
- Reference plugin (Iceberg, the closest analogue):
  `fe/fe-connector/fe-connector-iceberg/` at PR #62767's head
- SPI handbook: `fe/fe-connector/README.md` at PR #62767's head (1094 lines)
- Shared library: `~/DEV/brikk/repos/ducklake-integrations/jvm/ducklake-catalog/`
- Trino reference adapter:
  `~/DEV/brikk/repos/ducklake-integrations/jvm/trino-ducklake/`
