# DuckLake-on-Doris — Dependency-Level Sanity Check

A read of [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md)
(canonical) and `ducklake-doris-integration-plan.md` (**IGNORE FOR NOW** —
fallback only) against what the published `fe-connector-api` 1.2-SNAPSHOT
and `fe-connector-spi` 1.2-SNAPSHOT jars actually contain, plus what the
`fe-connector-iceberg` reference plugin actually does. Scope is narrow:
dependencies + reuse opportunities. Anything about DuckLake semantics,
library API, partitioning model, etc. is owned by the SPI plan and the
legacy main plan.

Working order of operations + current state live in
[`TODO-read.md`](./TODO-read.md). The "Roadmap to
`SELECT *`" section at the top of the TODO is the ordered execution
sequence; this document is the load-bearing review.

[`ducklake-doris-friction.md`](./ducklake-doris-friction.md) is the
sister doc: a running log of SPI / FE / BE surprises hit during
implementation, sized for the Doris team to scan for small upstream
fixes. The sanity check is a one-shot architectural review; the
friction log accumulates.

## 1. Things in the plan that should be deleted

### 1.1 Plan B (legacy `CatalogFactory` switch) is dead — drop it entirely

The main plan still hedges between Plan A (new SPI module) and Plan B
(`CatalogFactory.java` / `PhysicalPlanTranslator.java` / `TableFormatType.java`
edits). The SPI artifacts are now in `~/.m2`, the iceberg connector is the
reference, the directory and Gradle wiring have been set up against the
SPI. **Plan B no longer pays for the open-question paragraphs it costs in
both documents** — keeping it around just confuses readers about the actual
direction. Strike it and the "Plan A vs Plan B" entry in the open questions.

### 1.2 The Phase 0 "Java 17 vs bump-Doris-to-21" debate is settled

Plan §"Phase 0" presents two options for getting the library onto Doris's
classpath. We picked Option A and enforced it: `ducklake-catalog` now
compiles with `sourceCompatibility/targetCompatibility = 17` and Kotlin
`jvmTarget = JVM_17`, and the three Java 21/22 source-level usages in
`JdbcDucklakeCatalog.java`, `ConflictMatrix.java`, `LogicalConflictCheck.java`
have been down-leveled. The bytecode major in the published catalog jar is
`0x3d` (= 61, Java 17). Strike the option-B paragraph.

What needs to land **in addition** to closing this out: a CI check that
fails the catalog build if Java 18+ syntax (pattern switch, unnamed `_`,
`String.formatted()`-style API gates) creeps in. Simplest form is
`-Xlint:options -source 17 -target 17` with `-Werror` on the catalog
module's `JavaCompile` tasks — javac already raises the right errors, we
just need to keep them honest. We don't need `animal-sniffer`; the
`--release` flag would be stricter but disagrees with the rest of the build's
toolchain story.

### 1.3 The per-file class list in the SPI plan is over-enumerated

`ducklake-doris-integration-spi-plan.md` §2 lists 13 separate Java files
under `fe/fe-connector/fe-connector-ducklake/.../`. Cross-checking against
what the Iceberg reference actually compiles to, the real surface is:

| Class                              | Status            | Reason |
|------------------------------------|-------------------|--------|
| `DuckLakeConnectorProvider`        | required          | `ServiceLoader` entry, must be a class |
| `DuckLakeConnector`                | required          | Owns `ConnectorContext` + cache bindings |
| `DuckLakeConnectorMetadata`        | required          | The big one — implements 7 `*Ops` interfaces |
| `DuckLakeScanPlanProvider`         | required          | Implements `ConnectorScanPlanProvider` |
| `DuckLakeScanRange`                | required          | Concrete impl of `ConnectorScanRange` |
| `DuckLakeTableHandle`              | required          | Tiny record, `implements ConnectorTableHandle` |
| `DuckLakeColumnHandle`             | required          | Tiny record, `implements ConnectorColumnHandle` |
| `DuckLakeConnectorMvccSnapshot`    | required          | ~30 LOC, mirror `IcebergConnectorMvccSnapshot` |
| `DuckLakePredicateConverter`       | required (static) | Free function, no class hierarchy |
| `DuckLakeTypeMapping`              | required (static) | Free function |
| `DuckLakeConnectorProperties`      | **delete**        | Returned inline from `Connector.getCatalogProperties()` — no need for a class |
| `DuckLakeTransactionContext`       | **delete**        | Implement `ConnectorTransactionContext` directly on the library's `DucklakeWriteTransaction`, no wrapper |
| `DuckLakeInsertHandle`             | **delete**        | Use `ConnectorInsertHandle` (a marker interface) — a record with `(snapshotId, tableId)` is fine |
| `DuckLakeRefOps`                   | **delete (v1)**   | DuckLake has no branches/tags; leave `refOps()` returning `Optional.empty()` |
| `cache/*` package                  | **defer**         | See §3.4 |

That's **9 classes + 2 static utilities**, not 13. The plan's count
suggests more work than there is, and more importantly invites
over-abstraction: e.g. `*ConnectorProperties` becomes a place to silt up
property-validation logic that should live next to `validateProperties()`
on the provider.

## 2. The big reuse wins the plans hint at but don't make concrete

### 2.1 Position deletes — emit Iceberg's wire format (resolved 2026-05-18)

This is the single largest reuse opportunity. The worktree agent
confirmed the BE shape concretely.

**The BE position-delete dispatch is double-keyed** — both on a string
AND on a typed sub-struct:

1. `be/src/exec/scan/file_scanner.cpp:1252` (Parquet) and `:1343` (ORC) —
   `if (table_format_params.table_format_type == "iceberg") {
   IcebergParquetReader::create_unique(...) }`. There's no default
   branch; an unknown value (`"ducklake"`) falls off the end and
   `_cur_reader` stays unset.
2. `be/src/format/table/iceberg_reader_mixin.h:126` — the reader reads
   exclusively from the typed `iceberg_params` field on
   `TTableFormatFileDesc` (alongside parallel `paimon_params`,
   `hudi_params`, etc.). Position-delete entries live in
   `iceberg_params.delete_files` as `TIcebergDeleteFileDesc`.

**The bytes work in our favor**: `TIcebergDeleteFileDesc` carries
`path`, `position_lower_bound`, `position_upper_bound`, `field_ids`,
`content`, optional `file_format` — every field DuckLake's positional-
delete model needs. DuckLake also uses field-id-based Parquet column
resolution, so the reader's existing field-id path Just Works for us.
No new descriptor type required.

**Resolution for v1 (Option A)**: the DuckLake FE plugin emits
`table_format_type = "iceberg"` and packs delete files into
`iceberg_params.delete_files`. Zero BE change. The reader is a pure
consumer of the descriptor fields — it doesn't validate "is this really
Iceberg" — so this works today.

**Tradeoff**: EXPLAIN VERBOSE, BE profile/stats output, and any
operator-facing error message says "iceberg" for DuckLake tables.
Operationally confusing for debuggers who don't know the workaround.
Worth documenting in the smoke-test recipe.

**Upstream ask (Option B)**: a 5-line BE change makes the dispatch
recognise `"ducklake"` and route to the same reader:

```cpp
} else if (range.__isset.table_format_params &&
           (range.table_format_params.table_format_type == "iceberg" ||
            range.table_format_params.table_format_type == "ducklake")) {
    // existing IcebergParquetReader path
}
```

Frame it to the Doris team alongside the `SPI_READY_TYPES` whitelist
ask — same shape of transitional hardcoded list. Until Option B merges,
Option A is the deploy path; the thrift-parity unit test (§8.2) locks
in our wire format and will keep passing under Option B without
changes.

A heavier Option C — parameterize the dispatch into a registry — is
the right *eventual* answer but not realistic for v1.

### 2.2 `ConnectorScanRange` is an interface, not a base class

Iceberg's `IcebergScanRange` is a `final class implements ConnectorScanRange`.
The interface has defaults for everything except `getRangeType()` and
`getProperties()`. The DuckLake plugin needs **one** concrete scan-range
record with maybe a half-dozen fields — there is no inheritance hierarchy
to fit into. The plan's mention of "scan-range plumbing" is solved by
implementing 4 methods on a record.

### 2.3 `ConnectorDeleteFile` is already concrete

`org.apache.doris.connector.api.scan.ConnectorDeleteFile` is a final class
with constructor `(path, fileFormat, recordCount, properties)`. The plugin
shouldn't define its own delete-file value type for what gets returned
from `getDeleteFiles()` on the scan range — just construct
`ConnectorDeleteFile` directly. The Iceberg-specific bits
(position bounds, content type, partition spec id) are written into the
thrift descriptor in `populateRangeParams`, not carried on
`ConnectorDeleteFile` itself.

### 2.4 `ConnectorMetadata` is open-by-default — implement only what we ship

`ConnectorMetadata` extends 7 `*Ops` interfaces (`Schema`, `Table`,
`Pushdown`, `Statistics`, `Write`, `Identifier`, `SystemTable`) and
exposes 6 more as optional methods returning `Optional<...Ops>`
(`refOps`, `mtmvOps`, `policyOps`, `eventSourceOps`, `actionOps`,
`auditOps`). **Every method on every one of these has a default
implementation** that returns "not supported" / empty / no-op. So we get
to write methods for the operations we actually ship and inherit
sensible defaults for the rest.

This collapses the plan's §3 table dramatically. The work isn't
"implement 9 interfaces" — it's "override these specific methods on the
one `DuckLakeConnectorMetadata` class":

- `listDatabaseNames`, `databaseExists`, `getDatabase`, `createDatabase`,
  `dropDatabase` (5)
- `listTableNames`, `getTableHandle` (×2 overloads), `getTableSchema`,
  `getColumnHandles`, `createTable`, `dropTable`,
  `buildTableDescriptor` (8)
- `applyFilter`, `applyProjection`, `applyLimit` (3)
- `getTableStatistics` (1)
- `supportsInsert/Delete/Merge`, `getWriteConfig`, `beginTransaction`,
  `beginInsert`, `finishInsert`, `abortInsert`, similar for
  delete/merge, `txnCapabilities` (~12)

That's ~30 methods. Each individually small. The Iceberg `Metadata` class
is ~1.5k LOC, and most of that is Iceberg-API plumbing we *don't* have to
do (no `Catalog`/`Table`/`Transaction` lifecycle to manage — our library
owns those).

### 2.5 `Connector.getCatalogProperties()` replaces `CREATE CATALOG` boilerplate

The plan says we need property validation, name conventions, etc. The SPI
already has a typed property descriptor (`ConnectorPropertyMetadata<T>`
with `Builder`, validators, scopes, deprecation markers). Implementing
`Connector.getCatalogProperties()` returning a static list of
`ConnectorPropertyMetadata<?>` is how this is supposed to work. Doris's
DDL layer reads these for `SHOW CREATE CATALOG`, completion, validation
hints, etc. — for free. No custom property class.

### 2.6 `FilterApplicationResult` allows partial pushdown — useful escape hatch

`applyFilter` returns `Optional<FilterApplicationResult<TableHandle>>` with
fields `(handle, remainingFilter, precalculateStatistics)`. Doris will
execute the `remainingFilter` itself. So the predicate converter doesn't
have to support every `ConnectorExpression` shape on day one — return
"yes, I took these clauses, please run the rest yourself". This lets us
ship a smaller predicate converter for v1 and grow it.

### 2.7 `ConnectorTableVersion` already has the 5 variants DuckLake needs

`org.apache.doris.connector.api.timetravel.ConnectorTableVersion` is sealed
with `BySnapshotId`, `ByTimestamp`, `ByRef`, `ByRefAtTimestamp`,
`ByOpaque`. The library already exposes the first two natively. `ByRef`
returns "not supported" (DuckLake has no refs in v1). `ByOpaque` decodes
through our `MvccSnapshot.Codec`. No custom enum, no plumbing changes.

### 2.8 Snapshot codec is ~30 lines

`IcebergConnectorMvccSnapshot` is essentially:

```java
final class IcebergConnectorMvccSnapshot implements ConnectorMvccSnapshot {
    static final int MAGIC = 0x...;
    static final int FORMAT_VERSION = 1;
    static final int FRAME_BYTES = 16;
    private final long snapshotId;
    private final Instant commitTime;
    // ... constructor, accessors, toOpaqueToken (base64 of the 16-byte frame)
}
```

…and a sibling `Codec` that does the inverse (parse base64, validate magic,
read fields). The plan's "snapshot codec for FE→BE plumbing" suggests a
larger piece of work than it actually is. Port the Iceberg shape verbatim,
substituting our snapshot ID.

## 3. Dependencies — what's actually needed at compile and runtime

### 3.1 Compile classpath

What the build currently resolves for `:doris-ducklake:compileClasspath`
(via `gradle :doris-ducklake:dependencies`):

- `org.apache.doris:fe-connector-api:1.2-SNAPSHOT` (compileOnly)
- `org.apache.doris:fe-connector-spi:1.2-SNAPSHOT` (compileOnly)
- transitively from those:
  - `org.apache.doris:fe-extension-spi:1.2-SNAPSHOT` (4 classes: `Plugin`,
    `PluginContext`, `PluginException`, `PluginFactory`)
  - SLF4J 2.0.17, log4j 2.25.3 (provided by FE at runtime)
  - `awaitility:4.2.2` (test-utility leakage in the pom; we don't use it)
- `project(":ducklake-catalog")` (implementation)

Notably **absent**, and that's correct:

- `org.apache.iceberg:iceberg-core` — we don't depend on Iceberg
- `org.apache.hadoop:hadoop-common` — we don't depend on Hadoop
- `org.apache.doris:fe-thrift` — provided at runtime by FE's parent
  classloader; only needed compile-time if we directly construct thrift
  objects in our plugin (we will, for `populateRangeParams`). **Add
  this as `compileOnly` when we start writing scan-range code.**

### 3.2 Plugin-zip runtime classpath

What we'll need to ship inside the plugin jar/zip (FE supplies the rest):

- `ducklake-catalog` (and its transitive deps)
- jOOQ 3.20.8 — used by the catalog library internally. ~5 MB.
- `org.postgresql:postgresql:42.7.8` — JDBC driver for the metadata DB
- `com.zaxxer:HikariCP:7.0.2` — already supplied by FE (`fe-core` pins
  `hikaricp.version=6.0.0` per the api pom). **Risk: version skew**
  (we're on 7.x; FE is on 6.x). Audit whether HikariCP 6→7 has wire
  incompatibilities; if not, exclude it from our jar and accept FE's. If
  yes, shade ours.

The plan's open question "is jOOQ acceptable as a new FE dep?" goes away
under Plan A — jOOQ lives in our plugin's classloader, FE never sees it,
no upstream sign-off needed.

### 3.3 mavenLocal scoping — already done, briefly worth re-affirming

The Doris snapshots are in `~/.m2` and won't land in central. Settings
`exclusiveContent { forRepository { mavenLocal() } filter {
includeGroup("org.apache.doris") } }` in
`doris-ducklake/build.gradle.kts` is the right shape: only Doris
coordinates are resolved through mavenLocal, every other module of the
build is untouched. When the SPI is published to Maven Central (i.e.
after PR #62767 merges and Doris cuts a release), this block disappears.

### 3.4 Cache bindings — defer for v1

`fe-connector-iceberg` registers three cache bindings (catalog, table,
snapshots) via `ConnectorMetaCacheBinding<K, V>`. This is a serious piece
of code in the Iceberg plugin (manifest cache, weight functions,
invalidation listeners, ~hundreds of LOC).

**For DuckLake v1 we should return an empty list from
`Connector.getMetaCacheBindings()`.** Reason: the metadata DB *is* the
cache substrate. HikariCP holds open Postgres connections, jOOQ queries
are sub-millisecond on a warm Postgres, and prepared-statement caching
in the JDBC driver covers the hot path. Adding an FE-side cache layer
on top of that creates an invalidation problem (concurrent writers from
other engines update Postgres without telling Doris) for a perf win that
may be zero. Defer until profiling shows a hot path that needs it.

The plan's `cache/` package becomes a v1.5 item, not a v1 item.

### 3.5 The `SPI_READY_TYPES` whitelist is an upstream-coordination dependency

`CatalogFactory.java` in fe-core has a hardcoded
`SPI_READY_TYPES = {"jdbc", "es", "iceberg"}` set, and connectors not in
the set are silently ignored by the factory. The SPI plan flags this and
notes the worktree already carries the one-line edit (`+ "ducklake"`).

What this means concretely for our team:

- **Doesn't block development** — we publish locally from a fork with
  the patch, and our worktree-built FE picks up the plugin
- **Does block production deployment** — community Doris won't load our
  plugin until either (a) the whitelist is replaced by "any registered
  provider wins" upstream, or (b) we maintain a one-line patch on top of
  every Doris release we deploy

This is a real ongoing engineering cost (rebase + redeploy on every
upstream release) until upstream removes the whitelist. Raise this with
the Doris team early; the plan to push back is right but should be
called out as **a release blocker, not a polish item**.

## 4. Concrete capability set for `Connector.getCapabilities()`

The `ConnectorCapability` enum exposes 30+ feature flags. Doris's planner
gates behavior on these. **Declaring a capability without the matching
implementation crashes the planner loudly** (per the SPI plan, §3 — that
warning is correct). Here's the EnumSet for DuckLake v1, cross-referenced
against the main plan's "Initial Scope":

```java
EnumSet.of(
    SUPPORTS_FILTER_PUSHDOWN,       // applyFilter wired
    SUPPORTS_PROJECTION_PUSHDOWN,   // applyProjection wired
    SUPPORTS_LIMIT_PUSHDOWN,        // applyLimit wired
    SUPPORTS_PARTITION_PRUNING,     // identity + temporal transforms
    SUPPORTS_POSITION_DELETE,       // §2.1 above
    SUPPORTS_STATISTICS,            // file/column stats from catalog
    SUPPORTS_TIME_TRAVEL,           // ByVersion / ByTimestamp
    SUPPORTS_MVCC_SNAPSHOT,         // §2.8 codec
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,
    SUPPORTS_CREATE_TABLE,
    SUPPORTS_ACID_TRANSACTIONS,     // catalog DB owns the txn boundary
    SUPPORTS_PARALLEL_WRITE         // optimistic concurrency in DuckLake
)
```

**Deliberately NOT in v1**, mapping to the main plan's "Not yet" list:

- `SUPPORTS_EQUALITY_DELETE` — DuckLake doesn't have equality deletes
- `SUPPORTS_DELETION_VECTOR` — Puffin v3 deferred
- `SUPPORTS_BRANCH_TAG` — DuckLake has no branches/tags
- `SUPPORTS_TIME_TRAVEL_WRITE` — no write @ snapshot
- `SUPPORTS_INSERT_OVERWRITE` / `SUPPORTS_PARTITION_OVERWRITE` /
  `SUPPORTS_DYNAMIC_PARTITION_INSERT` — possible v1.5; needs a design
  decision (the library doesn't yet expose a TRUNCATE-then-INSERT
  primitive)
- `SUPPORTS_VENDED_CREDENTIALS`, `SUPPORTS_FAILOVER_SAFE_TXN`,
  `SUPPORTS_PASSTHROUGH_QUERY` — deferred
- `SUPPORTS_METASTORE_EVENTS` / `SUPPORTS_PULL_EVENTS` /
  `SUPPORTS_PUSH_EVENTS` — defer; DuckLake doesn't have an event source
- `SUPPORTS_PROCEDURES`, `SUPPORTS_SYSTEM_TABLES`,
  `SUPPORTS_NATIVE_SYS_TABLES`, `SUPPORTS_TVF_SYS_TABLES` — defer
  (`information_schema`-style sys tables are nice-to-have, not v1)
- `SUPPORTS_UPSERT` — same as `MERGE`; pick one

Pin this list in `DuckLakeConnector.getCapabilities()` as the v1 contract.

## 4a. JDK 17 ABI applies to the entire transitive runtime classpath

Setting `targetCompatibility = 17` and `kotlin.jvmTarget = JVM_17` on
`:ducklake-catalog` made our **own** classes JDK 17-compatible — but it
did **not** make the catalog as a whole runnable on JDK 17. The first
attempt to instantiate `JdbcDucklakeCatalog` under a JDK 17 toolchain
crashed with:

```
UnsupportedClassVersionError: org/jooq/Field has been compiled by a
more recent version of the Java Runtime (class file version 65.0)
```

jOOQ **3.20** raised its compile target to JDK 21 (the `Field` class
is bytecode v65). Doris FE is pinned to JDK 17. So even with our
catalog at v61, the link to jOOQ poisoned the deploy.

**Resolution**: pin `jooq = "3.19.22"` in `gradle/libs.versions.toml`
(latest jOOQ on the JDK 17 baseline). Regenerate codegen output
(`./gradlew :ducklake-catalog:jooqCodegenDucklake`) — 3.20-only
methods like `resetTouchedOnNotNull()` go away. The trino-ducklake
side cares not.

**General rule**: when committing to a JDK *N* ABI, audit every
non-leaf jar on the runtime classpath and verify its bytecode and its
own transitive deps respect *N*. Picking a major-version dependency
bump silently breaks the contract.

This sanity-check missed it the first time. Sorry.

## 4b. The "source > target" trap, and why Kotlin is the escape

A reasonable instinct after seeing us down-level Java 21/22 syntax in
the catalog is "can't javac accept `-source 22 -target 17`?" Not
safely:

- **Pattern matching in `switch` (JDK 21)** emits `invokedynamic`
  calls into `java.lang.runtime.SwitchBootstraps.typeSwitch()`, which
  does not exist on JDK 17. Compile is silent; runtime crash on first
  dispatch with `ClassNotFoundException`.
- **Unnamed variables `_` (JDK 22)** desugar to nothing — they're
  pure source-level sugar — so *in isolation* this one is safe at
  source 22 / target 17. But to allow it, we drop `--release 17`, and
  with the safety net off, the next person to type `Stream.mapMulti(...)`
  (JDK 16 API) or `Files.mismatch(...)` (JDK 12) slips through.
  `--release N` is the only knob that strictly enforces source AND
  API ≤ N; it has no "let just one feature through" mode.

So the right move for the Java code is to keep `--release 17` and
accept the syntactic ugliness of `instanceof` chains and `k`-named
lambda params.

**The proper escape is the Kotlin migration that's already on the
catalog roadmap.** The Kotlin compiler can target JDK 17 bytecode
while keeping every modern Kotlin syntax feature (`when (subject) is
X -> …`, smart casts, sealed interfaces, etc.), and it manages the
bytecode-target-vs-stdlib invariant for us (the Kotlin stdlib has its
own version policy and JVM-version splits). Once the three files we
down-leveled (`JdbcDucklakeCatalog.java`, `ConflictMatrix.java`,
`LogicalConflictCheck.java`) move to Kotlin, the JDK-17 constraint
becomes invisible to the catalog author.

Pin to memory: **JDK 17 ABI for a Kotlin-leaning library is the
Kotlin migration's payoff**, not just an inconvenience.

## 5. Library boundary — Java 17 ABI is now a load-bearing rule

A consequence of §1.2 that's worth restating: **every change to
`ducklake-catalog` must keep its bytecode at Java 17**. The catalog is
no longer a free-floating library — it's now consumed by both
`trino-ducklake` (toolchain 25, target 25) and `doris-ducklake`
(toolchain 17, target 17). Trino-side code can continue to use any modern
language feature, but the catalog can't.

Three guardrails are worth adding:

1. **CI**: a job that runs `./gradlew :ducklake-catalog:compileJava` with
   `-Werror -Xlint:options` and fails on any 18+ source-level feature.
2. **A note at the top of `JdbcDucklakeCatalog.java`** (or wherever the
   author convention lives) calling out the constraint, so PR authors
   notice before CI catches them.
3. **A `:ducklake-catalog:checkAbi` task** (optional) that fails if the
   produced class major version is not exactly 61. Cheap to write,
   prevents toolchain drift.

The three downlevel edits we just made (`computeIfAbsent(k -> …)`,
`instanceof` chains replacing pattern switches, `catch (Ex ignored)`
replacing `catch (Ex _)`) are stylistically ugly. If we want them gone,
the right move is to (a) leave them as-is in main code, (b) consider
moving the catalog to Kotlin, which targets JVM 17 cleanly while still
supporting modern syntax. The Kotlin transition is mentioned in the SPI
plan §4 as already on the roadmap; the JDK 17 constraint becomes
invisible after that.

## 6. Test wiring — match the trino-ducklake pattern

The SPI plan §5.1 enumerates 11 unit tests and a `cache/` directory of
test harness classes copied from Iceberg. The shorter version:

- `doris-ducklake/test/src/` mirrors the package layout; use JUnit 5 +
  AssertJ (already on the test classpath via `libs.junit.bom` /
  `libs.assertj.core`)
- `testImplementation(testFixtures(project(":ducklake-catalog")))` to get
  `TestingDucklakePostgreSqlCatalogServer` — same dependency the
  `trino-ducklake` module declares. This is the right reuse handle; no
  need to duplicate Testcontainers setup.
- The Iceberg `FakeConnectorContext` / `InMemoryMetaCacheHandle` test
  doubles are small (~3.5 KB + ~4 KB). Copy verbatim, rename, trim to
  what we use. The SPI plan flags this correctly.
- No need to bring `awaitility` (transitive from the api pom) into our
  test classpath — `Thread.sleep` + assertions are sufficient for the
  kinds of in-process tests at this layer.

## 7. Open questions the plans flag that are now closed

| Plan-of-record question                                 | Resolution                                  |
|---------------------------------------------------------|---------------------------------------------|
| Plan A vs B                                             | Plan A. Strike Plan B.                      |
| Java target for the library build                       | JDK 17 ABI. Enforced in build.gradle.kts.   |
| jOOQ as a new FE dep                                    | Plugin-classloader-only; no FE-wide impact. |
| Catalog backend coverage at launch                      | Postgres only. SQLite/DuckDB v1.5+.         |

## 8. Testing strategy — where to invest

The SPI plan's §5 enumerates four test tiers. Translated against what
actually exists in the PR worktree (`~/DEV/OSS/db/doris-pr-62767/`) and
what running it actually costs:

### 8.1 Unit + integration in `:doris-ducklake:test` (90% of investment)

Pure Java, JUnit 5, runs in seconds. Three layers, all in the same Gradle
test task:

1. **No-fixture logic tests** — `DuckLakeTypeMappingTest`,
   `DuckLakePredicateConverterTest`, `DuckLakeTableHandleTest`,
   `DuckLakeColumnHandleTest`, `DuckLakeConnectorMvccSnapshotTest`. 1:1
   with the iceberg connector's same-named files at
   `fe/fe-connector/fe-connector-iceberg/src/test/java/...`.

2. **In-process metadata tests** — exercise `DuckLakeConnectorMetadata`
   end-to-end without booting an FE. Possible because the iceberg test
   suite ships two ~4 KB harness classes — `FakeConnectorContext` and
   `InMemoryMetaCacheHandle` (under `.../iceberg/cache/`) — that
   implement `ConnectorContext` + `MetaCacheHandle<K,V>` in pure Java
   with no fe-core dependency. **Copy these verbatim** into our
   `test/src/.../cache/`, trim to what we actually consume, and the
   metadata class is directly instantiable in `@Test` methods.

3. **Postgres-backed integration tests** — same as (2) but with
   `JdbcDucklakeCatalog` backed by `TestingDucklakePostgreSqlCatalogServer`
   (already a `testFixturesApi` export of `:ducklake-catalog`). Declare
   `testImplementation(testFixtures(project(":ducklake-catalog")))` —
   identical to the `trino-ducklake` wiring at
   `trino-ducklake/build.gradle.kts:79`. This exercises real SQL through
   jOOQ → Postgres and is where snapshot resolution, predicate-to-SQL
   conversion, and transaction-commit semantics actually get validated.

### 8.2 The thrift-parity test — locks in the §2.1 workaround

The iceberg suite ships `IcebergScanRangeThriftParityTest`: it serializes
the `TFileRangeDesc` produced by `IcebergScanRange.populateRangeParams(...)`
with `TSerializer` and asserts byte-identity against a hand-rolled
"golden" builder copied from `fe-core`'s `IcebergScanNode.setIcebergParams`.

**Mirror this for DuckLake**: assert our `DuckLakeScanRange` produces a
`TFileRangeDesc` that is **byte-identical to what Iceberg produces** for
the same logical input (path + position-delete file). With Option A
from §2.1 (emit `table_format_type = "iceberg"` and populate
`iceberg_params`), the bytes literally are the iceberg-shaped bytes, and
the BE accepts them.

Once Option B merges upstream and we flip to `table_format_type =
"ducklake"`, the test stays sound: the `iceberg_params` sub-struct
bytes remain identical, only the discriminator string differs. Worth
parameterizing the golden output by `table_format_type` in the test so
the flip is a one-line change.

### 8.3 Live-FE smoke test (10% of investment, ~minutes per cycle)

There is **no in-process FE** — the PR explicitly ships nothing that
boots Doris in JUnit. E2E means a real cluster, and the supported
mechanism is `docker/runtime/doris-compose/doris-compose.py` from the PR
worktree.

Pragmatic workflow:

1. Build the FE tar once (the SPI_READY_TYPES whitelist patch already
   lives in the worktree):
   `cd ~/DEV/OSS/db/doris-pr-62767 && sh build.sh --fe`
2. Build our plugin zip: `./gradlew :doris-ducklake:pluginZip` (task
   doesn't exist yet — needs the assembly described in §8.5).
3. Stand up the DuckLake substrate by **reusing `trino-ducklake/compose/`**
   — same Postgres + MinIO + seed-data stack. Cross-engine round trips
   (DuckDB-written → Doris-read) fall out for free.
4. `doris-compose up`, drop the plugin zip into the FE plugin dir.
5. Drive through `mysql -h <fe> -P 9030` with the smoke checklist from
   the SPI plan §5.3 (CREATE CATALOG → SHOW DATABASES → SELECT COUNT →
   SELECT LIMIT → EXPLAIN VERBOSE).

This is **not** the inner dev loop — it's a per-significant-change gate.

### 8.4 Regression suite (production CI only, post-v1)

`regression-test/suites/external_table_p0/iceberg/` is the canonical
gate Doris uses; the equivalent for us lives at
`regression-test/suites/external_table_p0/ducklake/`. Groovy DSL driven,
runs against a clean Doris package. The `doris-docker-regression` skill
in this repo wraps the runner. Worth pursuing **only after** the §8.3
smoke loop is reliable — until then, regression failures are
indistinguishable from "plugin doesn't load".

### 8.5 Missing build artifacts for §8.3 to work

Two files are needed before the plugin can be loaded by an FE; both are
zero-LOC once templated and should land alongside the first real
implementation class:

1. **`src/main/resources/META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`**
   — a one-line file containing the FQCN of our provider
   (`dev.brikk.ducklake.doris.plugin.DuckLakeConnectorProvider`).
   Without this, `ServiceLoader` discovery returns empty and fe-core's
   `CatalogFactory` falls through to "Unknown catalog type" even if our
   jar is on the classpath.

2. **`src/main/assembly/plugin-zip.xml`** + a Gradle `pluginAssemble`
   task. The iceberg reference at
   `fe/fe-connector/fe-connector-iceberg/src/main/assembly/plugin-zip.xml`
   is short (~20 lines of `<dependencySet>`) and tells the assembly which
   parent-classloader artifacts to **exclude** from the plugin zip:
   `fe-connector-api`, `fe-connector-spi`, `fe-extension-spi`,
   `fe-filesystem-api`, all log4j, all slf4j. For DuckLake we should
   additionally exclude `fe-thrift` (provided) and consider whether to
   exclude `HikariCP` to defer to FE's pinned version (see §3.2).

   The Gradle side is a 5-line `Copy` task that bundles `jar` +
   `runtimeClasspath` minus the same exclusions — see
   `trino-ducklake/build.gradle.kts:123-128` for our existing analogue
   on the Trino side. Translate to a Maven-assembly-equivalent and we're
   compatible with how Doris already packages connector plugins.

## 9. Open questions actually worth tracking

1. **BE position-delete reader's table-format gate** — does it route on
   the parent `tableFormatType` string, or just on
   `TIcebergDeleteFileDesc` presence? §2.1 is gated on the answer. A
   5-minute look at `be/src/vec/exec/format/parquet/` should settle it.
2. **HikariCP version skew** — FE pins 6.0.0 (per api pom); our build
   pulls 7.0.2 through the catalog. Either match FE's or confirm 7
   classes load alongside 6's (likely fine because Hikari is a leaf
   library, but verify).
3. **`SPI_READY_TYPES` upstream timeline** — who in upstream Doris owns
   replacing the whitelist with provider discovery? Until that lands,
   our prod deployments carry a fork. **Track in a project memory.**
4. **`SUPPORTS_INSERT_OVERWRITE` design** — the main plan says "Not
   yet", but Doris users typically expect `INSERT OVERWRITE`. Decide
   whether v1.5 ships this and what it means semantically for a
   metadata-DB-backed lakehouse (transactional truncate + insert? or
   delete-by-predicate + insert?).
5. **Property-validation surface** — what catalog-level
   `ConnectorPropertyMetadata` entries should be required vs optional,
   and which should validate at `preCreateValidation` time. Iceberg
   handles this with a fair amount of code; lift the pattern.
6. **Concurrent-writes API drift** — same as the main plan's open
   question 6; relevant here because `DuckLakeConnectorMetadata.beginTransaction`
   has to surface whatever conflict semantics the library exposes.
