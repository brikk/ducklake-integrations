# DuckLake-on-Doris — READ-path Working TODO

Living checklist for the **read path** (SELECT, pushdown, deletes-on-read,
time travel, statistics). Sibling todo files hold the other tracks:
- ✍️ [`ducklake-doris-todo-write.md`](./ducklake-doris-todo-write.md) — write path (INSERT/CTAS/DELETE/UPDATE/MERGE/DDL); post read-v1
- 🔬 [`ducklake-doris-todo-research.md`](./ducklake-doris-todo-research.md) — research / feasibility / "remember this" ideas

Lead with the ordered roadmap to `SELECT *`; reference and historical sections
follow. The phased read-path *extension* plan (capability matrix vs trino-ducklake)
lives in [`ducklake-doris-development-roadmap.md`](./ducklake-doris-development-roadmap.md).

Cross-references:
- ✅ [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md) — canonical plan (SPI mechanics, build, test)
- ⚠️ [`ducklake-doris-integration-plan.md`](./ducklake-doris-integration-plan.md) — **IGNORE FOR NOW**, fallback only (hardcoded-dispatch path)
- 📚 [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md) — dependency review, capability set, BE workaround
- 🪤 [`ducklake-doris-friction.md`](./ducklake-doris-friction.md) — running log of SPI / FE / BE surprises and workarounds (Doris-team-monitorable)
- 🚀 [`compose/smoke.sh`](./compose/smoke.sh) — one-command live-FE smoke loop

## Where we are (2026-05-19)

🎉 **`SELECT * FROM dl.tpch.orders LIMIT 5` returns rows end-to-end through
Doris.** Live FE+BE cluster stands up reproducibly via `compose/smoke.sh`.
38 unit tests green + 1 @Disabled (3 metadata + 10 type-mapping + 8
mvcc-codec + 3 capabilities + 7 scan-plan-provider [+1 @Disabled inline] + 7
thrift-parity).

Step 7 (position deletes) plumbing is wired end-to-end FE-side: the
catalog already inlines the active delete file path on `DucklakeDataFile`
via LEFT JOIN; `DuckLakeScanPlanProvider` resolves it to absolute paths
and threads it through `DuckLakeScanRange`; `populateRangeParams` packs
it into `iceberg_params.delete_files` with
`content = POSITION_DELETE (1)`, `file_format = FORMAT_PARQUET`. Smoke
shows the BE receives the wire bytes correctly. **Blocked on two
interop gaps surfaced by live smoke** — see [friction log](./ducklake-doris-friction.md)
entries dated 2026-05-19:

1. **DuckLake defaults DELETEs to inline (Postgres rows), not files.** Per-table
   `ducklake_inlined_delete_<tableId>` carries `(file_id, row_id,
   begin_snapshot)` for DELETEs that fit under `DATA_INLINING_ROW_LIMIT`.
   Workaround: `CALL lake.set_option('data_inlining_row_limit', '0')` on
   the catalog forces the file path. Long-term: Step 7.5 honours inline
   deletes by synthesising a parquet delete file FE-side
   ([catalog API already there](../ducklake-catalog/src/dev/brikk/ducklake/catalog/DucklakeCatalog.java#L151-L177)).
2. **DuckLake-written delete-file parquet uses OPTIONAL columns; BE
   iceberg reader expects REQUIRED.** Schema is otherwise Iceberg-spec
   compatible (`file_path: VARCHAR`, `pos: BIGINT`, field-ids align).
   Reader's NOT-NULL fast path raises `[CORRUPTION]Not nullable column
   has null values in parquet file` despite zero actual nulls. Pickable
   one-line fix on either side; tracked in friction log.

Plugin code stable: provider (with mvcc snapshot codec), properties,
connector (with v1 capability set + scan-plan provider), metadata
(listing + handle + schema), type mapping, handle records, MVCC
snapshot + codec, scan-plan provider (file-format dispatch +
storage-cred forwarding with AWS_* aliases + position-delete plumbing)
+ scan range (Option-A iceberg-shaped wire bytes with position deletes)
+ path resolver. `fe-thrift` 1.2-SNAPSHOT on compile + test classpath.
Next is Step 6 — applyFilter / applyProjection / applyLimit
for pushdown performance, OR Step 7.5 — inline-delete synthesis.

## 🚀 Roadmap to `SELECT *`

The ordered sequence to get `SELECT * FROM dl.tpch.orders LIMIT 10`
returning rows. Each step is independent enough to stand alone with a
unit test and (where applicable) a live-FE smoke checkpoint.

### Step 1 — Snapshot codec (no UI-visible change yet)
- [x] `DuckLakeConnectorMvccSnapshot` + `Codec` — ~30 LOC mirror of `IcebergConnectorMvccSnapshot`. Carries a long snapshot ID + commitTime, packs into a fixed 24-byte big-endian frame (`magic=0x444C414B "DLAK"`, `formatVer=1`, `snapshotId`, `commitTimeMs`), validates magic + format version. Wired through `DuckLakeConnectorProvider.getMvccSnapshotCodec()`.
- [x] Unit tests in `DuckLakeConnectorMvccSnapshotCodecTest`: round-trip, `asVersion()` returns `BySnapshotId`, opaque-token shape, bad-magic raises, wrong-length raises, unsupported-format-version raises, encode rejects a foreign `ConnectorMvccSnapshot` impl, on-wire layout golden.

### Step 2 — Declare capabilities
- [x] `DuckLakeConnector.getCapabilities()` returns the EnumSet from sanity-check §4. Initial set for the SELECT-* milestone: `SUPPORTS_MVCC_SNAPSHOT`, `SUPPORTS_POSITION_DELETE`, `SUPPORTS_TIME_TRAVEL`, `SUPPORTS_PARTITION_PRUNING`, `SUPPORTS_STATISTICS`. Filter / projection / limit pushdown stay off until Step 6 — declaring without implementing crashes the planner.
- [x] `DuckLakeConnectorCapabilitiesTest` pins the v1 set, asserts pushdown capabilities are absent, and asserts the provider supplies a `DuckLakeConnectorMvccSnapshot.Codec`.

### Step 3 — Scan plan provider, no deletes path
- [x] `DuckLakeScanPlanProvider implements ConnectorScanPlanProvider` with `planScan(ConnectorScanRequest)` returning `List<ConnectorScanRange>`. For each active `DucklakeDataFile` from the catalog at the snapshot pinned in the table handle, emit one `DuckLakeScanRange` carrying (resolved absolute path, start=0, length=fileSize, fileSize, file_format).
- [x] `DuckLakeScanRange implements ConnectorScanRange` — no-deletes skeleton. Uses the SPI's default `populateRangeParams` for now so we stay off fe-thrift; Step 4 swaps in the Iceberg-shaped descriptor.
- [x] `DuckLakePathResolver` ported from the Trino plugin (sans Guice) to thread DuckLake's three-level scoped paths (catalog &rarr; schema &rarr; table &rarr; file).
- [x] Wired `DuckLakeConnector.getScanPlanProvider()` lazy (same double-checked pattern as `catalog()`).
- [ ] `fe-thrift` added as `compileOnly` in `doris-ducklake/build.gradle.kts` — **deferred to Step 4** when `populateRangeParams` is overridden; Step 3 keeps thrift types off our compile classpath via the SPI's default.
- [x] `DuckLakeScanPlanProviderTest` (Postgres-backed): seeded `sales.orders` produces &ge;1 well-formed FILE_SCAN range (parquet, full-file extent, absolute path under the warehouse, no partitions / deletes); empty `sales.customers` produces zero ranges; provider instance is cached on the connector.

### Step 4 — Scan range with Iceberg-shaped wire bytes (Option A)
- [x] `DuckLakeScanRange implements ConnectorScanRange` carries (path, start, length, fileSize, partitionValues=empty, properties=empty, deleteFiles=empty). Step 3 landed the skeleton; Step 4 adds the thrift wire override.
- [x] `getTableFormatType()` returns `"iceberg"` with `TODO(option-B)` comment (sanity-check §2.1).
- [x] `populateRangeParams(TTableFormatFileDesc, TFileRangeDesc)` sets `iceberg_params` with `formatVersion=2`, `originalFilePath=path`, empty `deleteFiles` list. Partition columns_from_path stays unset for unpartitioned reads.
- [x] `fe-thrift` 1.2-SNAPSHOT added as `compileOnly` in `doris-ducklake/build.gradle.kts` + `testImplementation` so the parity test can construct thrift objects directly.
- [x] **`DuckLakeScanRangeThriftParityTest`** — 4 tests: byte-identity against a hand-built iceberg-v2-data-file golden via `TSerializer(TBinaryProtocol)`; per-range descriptor remains untouched for unpartitioned ranges; discriminator string assertion pins the Option-A choice; field-by-field assertions on `iceberg_params` (formatVersion=2, originalFilePath set, deleteFiles=[], partition fields unset). Golden's discriminator is parameterised so the Option-A → Option-B flip is a one-line change.

### Step 5 — Live `SELECT * LIMIT 10` smoke ✅ GREEN
- [x] `DuckLakeScanPlanProvider.getScanNodeProperties()` emits `file_format_type=parquet` so `PluginDrivenScanNode.mapFileFormatType()` dispatches `FORMAT_PARQUET` instead of defaulting to `FORMAT_JNI`. Also emits storage creds under `ducklake.location.*` prefix.
- [x] `populateScanLevelParams()` strips the location prefix AND emits BE-canonical `AWS_*` aliases (`AWS_ENDPOINT`, `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, `AWS_REGION`, `AWS_TOKEN`). The BE's `S3ClientFactory::convert_properties_to_s3_conf` (`be/src/util/s3_util.cpp`) looks up these keys verbatim — the FE-side `S3ObjStorage` normaliser does NOT run on the parquet-reader path, so the alias has to happen on the FE side of the wire.
- [x] `DuckLakeScanPlanProvider.canonicalAwsAlias()` is the single source of truth for the FE→BE key mapping; pinned by `canonicalAwsAliasMapping` test.
- [x] `compose/smoke.sh` extended: CREATE CATALOG carries `s3.endpoint=http://trino-ducklake-minio:9000`, `s3.access_key=minioadmin`, `s3.secret_key=minioadmin`, `s3.region=us-east-1`, `use_path_style=true`. After the DESC step, runs `USE dl.tpch; SELECT * FROM orders LIMIT 5;` and prints 5 TPCH orders rows.
- [x] **Live result** (2026-05-19): `SELECT * FROM dl.tpch.orders LIMIT 5` returns 5 rows through the BE's iceberg+S3 parquet reader. Field-ids in DuckLake parquet are picked up by the existing iceberg reader path; no decoding regression.
- [ ] Run the full smoke checklist (SHOW DATABASES with row counts, EXPLAIN VERBOSE, cross-engine round-trip with a fresh DuckDB write). Track as a v1 polish item.

### Step 6 — pushdown (filters / projection / pruning) — mostly landed
- [x] `DuckLakePredicateConverter` — maps `ConnectorExpression` conjuncts → `ColumnRangePredicate`s (`col <op> literal` comparisons + `BETWEEN`; functions / LIKE / IN / OR / NE skipped — **no function pushdown by design**).
- [x] `applyProjection` (column pruning) + `applyFilter` (stats-based file pruning via `findDataFileIdsInRange`) on the metadata; `SUPPORTS_PROJECTION_PUSHDOWN` + `SUPPORTS_FILTER_PUSHDOWN` on. Pruning is best-effort file elimination — the BE re-evaluates the full predicate.
- [x] **IDENTITY partition pruning** — works for free through `applyFilter`'s stats path: DuckLake records `file_column_stats` for partition columns, so a `region = 'us'` filter prunes non-matching partition files. Verified by `prunesFilesByPartitionEqualityFilter` against the `sales.by_region` fixture.
- [ ] **Temporal / BUCKET partition pruning** — `year/month/day/hour(col)` and `bucket(N, col)` partitions need *transform-aware* matching (apply the transform to the predicate literal, then compare to the partition value). Stats pruning does NOT cover these (stats are on the source column; the partition value is the transformed bucket). Wire `getPartitionSpecs` + `getFilePartitionValues` + the transform.
- [ ] **add_files-without-stats partition pruning** — files registered via `add_files` may lack `file_column_stats`; `findDataFileIdsInRange` inner-joins stats so it silently can't prune them. The `getFilePartitionValues` path would prune by recorded partition value regardless of stats. Edge case; revisit if/when add_files support lands.
- [ ] `applyLimit` — the file-scan model gains nothing from limit pushdown; left off. Revisit only if a concrete use case appears.
- ⛔️ **Function / expression pushdown** — out of scope: it would require the `trino_parity` DuckDB bridge, which doesn't fit Doris's BE-native Parquet read path. Do not attempt.

### Step 7 — Position deletes path (FE-side plumbing landed, BE-side blocked)
- [x] Library: `DucklakeDataFile` already inlines the active delete file path/format via LEFT JOIN in `JdbcDucklakeCatalog#getDataFiles`; catalog enforces at-most-one active delete file per data file per snapshot (`checkDeleteFileOverlap`). No new catalog method needed.
- [x] `DuckLakePositionDelete` record + `DuckLakeScanRange` populates `iceberg_params.delete_files` with `TIcebergDeleteFileDesc` (content=POSITION_DELETE, format=FORMAT_PARQUET). Field-ID semantics compatible (sanity-check §2.1).
- [x] `DuckLakeScanPlanProvider.resolvePositionDeletes` threads the delete path through the path resolver.
- [x] Unit tests: 3 parity tests (golden byte-identity, field-level assertions on populated delete-file descriptor, accessor immutability) + 1 scan-plan-provider test seeding a DELETE on `sales.returns`.
- [x] Live smoke: BE receives the iceberg_params wire bytes correctly when `data_inlining_row_limit=0` forces DuckLake to write file-based deletes.
- [ ] **BLOCKED**: BE iceberg reader rejects DuckLake's delete-file parquet with `[CORRUPTION]Not nullable column has null values`. DuckLake writes `(file_path, pos)` as OPTIONAL; Iceberg spec requires REQUIRED. Friction-log entry has pickable upstream fixes on either side.

### Step 7.5 — Inline-delete handling (newly scoped)

Real users will produce both inline and file-based deletes depending on
DuckLake's `data_inlining_row_limit` and DuckDB driver behaviour — the
connector must handle both. We don't prescribe `data_inlining_row_limit=0`
to users; we only flip it in tests / smoke as fixture setup.

- [ ] Honour `ducklake_inlined_delete_<tableId>` rows (catalog API exists: `hasInlinedDeletes` / `getInlinedDeletes`).
- [ ] Likely path: synthesise a parquet delete file FE-side per scan, drop under a scratch location, emit through `iceberg_params.delete_files`. Per-query parquet write — acceptable as a v1.5 once the Step 7 BE block is unstuck.
- [ ] Fixture work: DuckDB-JDBC 1.5.2 doesn't reliably honour `data_inlining_row_limit` for DELETE inlining the way DuckDB-Python does — the test bootstrap will need to INSERT directly into `ducklake_inlined_delete_<tableId>` over a PG JDBC connection to simulate an inline-delete row. The `DuckLakeScanPlanProviderTest#emitsDeletesForInlineDeletePath` test is @Disabled until both this fixture work AND the connector implementation land.
- [ ] Alternative: get the BE / Doris to accept "exclude row indexes" pushdown. No hook today.

### Step 8 — Time travel
- [ ] `DuckLakeConnectorMetadata.getTableHandle` second overload taking `Optional<ConnectorTableVersion>` + `Optional<ConnectorRefSpec>`. Map `BySnapshotId` and `ByTimestamp` to the library's existing snapshot resolvers; reject `ByRef` / `ByRefAtTimestamp` for v1 (DuckLake has no branches/tags). `ByOpaque` decodes via our MVCC codec from Step 1.
- [ ] Smoke: `SELECT * FROM dl.tpch.orders FOR VERSION AS OF <snapshot_id>` returns the historical snapshot's rows.

## Build hygiene (run before merging code from any step above)

- [ ] **CI gate for JDK 17 ABI** on `:ducklake-catalog`: fail the build if a Java 18+ source feature creeps in OR if a runtime-classpath jar has a class with major version > 61. Cheapest form: `-Werror -Xlint:options` for source drift; a `:ducklake-catalog:checkAbi` task that walks the runtime configuration jars and asserts every `.class` ≤ 61 for transitive drift. Sanity-check §4a. Without this, the next jOOQ-3.21-style bump silently re-breaks deploy.
- [ ] **HikariCP version skew** (FE pins 6.0.0; our plugin zip ships 7.0.2). Verify 6↔7 wire compat or downgrade ours. Sanity-check §3.2.
- [ ] **Plugin-zip exclusion audit** — once a quarter, diff our `pluginZip` task's exclude list against `fe-connector-iceberg/src/main/assembly/plugin-zip.xml` in the worktree. Drift introduces silent runtime conflicts.
- [ ] **Kotlin migration for catalog** (`JdbcDucklakeCatalog.java`, `ConflictMatrix.java`, `LogicalConflictCheck.java` lost pattern switch + unnamed `_` to JDK 17 ABI; Kotlin reclaims modern syntax while emitting 17 bytecode). Separate scope on the catalog roadmap. Sanity-check §4b.

## Upstream coordination (blockers for production, not for dev)

Bundle the conversation — three asks, all the same shape ("transitional
hardcoded list should be open / discoverable"):

- [ ] **`SPI_READY_TYPES` whitelist removal**. `CatalogFactory.java` silently ignores any `ConnectorProvider` whose `getType()` is not in `{"jdbc","es","iceberg"}`. Until upstream parameterizes, we carry a one-line patch (`+ "ducklake"`) on every Doris release we deploy. Sanity-check §3.5.
- [ ] **Option B BE dispatch ask**. 5-line PR to `be/src/exec/scan/file_scanner.cpp:1252` and `:1343` adding `|| table_format_type == "ducklake"` to the iceberg branch. Unblocks honest table-format-string reporting in EXPLAIN/profile. Sanity-check §2.1.
- [ ] **`connector_plugin_root` discoverability** — surface the hardcoded default at `Config.java:3541` as a commented-out line in `conf/fe.conf`. Zero behavior change.
- [ ] **API-surface churn on PR #62767**. Diff `fe-connector-api/` and `fe-connector-spi/` against the head of PR #62767 whenever we re-pull; flag breaking changes.
- [ ] **Avoid binlog/CCR table-create paths** in our smoke loop — PR-branch FE removed `TBinlogFormat` etc. from the FE↔BE thrift; stock 4.1.0 BE talks to PR FE fine for everything we care about, but `CREATE TABLE … PROPERTIES("binlog.enable"="true")` would deadlock. Document in the smoke recipe (done) and keep out of regression tests.
- [ ] **DuckLake delete-file parquet nullability**. Upstream ask to DuckDB/DuckLake: write `file_path` + `pos` as `REQUIRED` in position-delete parquet, matching Iceberg's spec. Or, to the Doris BE: fall through to the nullable column reader path when an Iceberg-spec'd delete-file column reports OPTIONAL. Friction log 2026-05-19 has the full repro.

## Done — shipped milestones

- [x] **Bootstrapping**: Gradle wiring, JDK 17 ABI on catalog, plugin module skeleton, mavenLocal scoped to `org.apache.doris`, `pluginZip` Gradle task.
- [x] **Empty plugin**: `DuckLakeConnectorProvider` + `DuckLakeConnector` stubs, `ServiceLoader` discoverable, zip assembles with the right exclusions.
- [x] **Property metadata + validation**: `DuckLakeConnectorProperties` declares `metadata.url` / `metadata.user` / `metadata.password` / `storage.warehouse`; `validateProperties` enforces required without rejecting Doris-injected unknowns.
- [x] **Driver classloader fix**: `Class.forName("org.postgresql.Driver")` from inside `DuckLakeConnector.buildCatalog` to register the JDBC driver under the plugin classloader.
- [x] **Hello tables**: `DuckLakeConnectorMetadata.listDatabaseNames` / `databaseExists` / `getDatabase` / `listTableNames`. `SHOW DATABASES FROM dl` + `SHOW TABLES FROM tpch` return real seeded data.
- [x] **DESC**: `DuckLakeTypeMapping` (full DuckLake type-string parser → `ConnectorType`), `DuckLakeTableHandle` + `DuckLakeColumnHandle` records, `getTableHandle` + `getTableSchema` + `getColumnHandles`. `DESC dl.tpch.lineitem` returns 16 columns with types.
- [x] **Live-FE smoke loop**: `compose/docker-compose.yml` + `compose/smoke.sh` + `compose/fe.conf` produce a reproducible cluster + plugin-install + driver loop. Six bring-up bugs captured in *Lessons learned* below.
- [x] **jOOQ pinned to 3.19.22**: 3.20 raised baseline to JDK 21 and broke the JDK 17 ABI commitment (sanity-check §4a).
- [x] **BE is stock**: `apache/doris:be-4.1.0` (arm64) works against PR-branch FE; no Doris-side BE changes for our scope. FE/BE wire compat verified via `SHOW BACKENDS` / `SHOW FRONTENDS` / `SELECT 1`.

## Lessons from first live-FE bring-up (2026-05-18)

Six real bugs surfaced and got fixed during the smoke loop. Keep for the
next person re-staging this:

1. **FE 8GB default heap killed the JVM** on Docker Desktop. `start_fe.sh`
   sources `fe.conf` for `JAVA_OPTS_FOR_JDK_17`, so env-var override
   doesn't work — must ship a tuned `fe.conf` as a writable bind mount
   (`init_fe.sh` appends `priority_networks`, so `:ro` deadlocks it).
   `compose/fe.conf` is at `-Xmx2g -Xms2g`.
2. **FE healthcheck deadlock**. `SELECT 1` needs a BE to dispatch the plan,
   but `depends_on: service_healthy` gates BE startup on FE health.
   Healthcheck switched to `SHOW FRONTENDS` (FE-local, no BE needed).
3. **BE env contract is `FE_SERVERS` + `BE_ADDR`**, not the
   `MASTER_FE_IP`/`CURRENT_BE_IP`/`CURRENT_BE_PORT` that init_be.sh
   *derives* internally.
4. **Doris injects `enable.mapping.varbinary` (and friends) into every
   `CREATE CATALOG`.** Our `validateProperties` was strictly rejecting
   unknown keys — removed the unknown check; only required-property
   enforcement remains.
5. **Podman macOS bind mounts don't propagate directory contents** into
   the VM (single files like `fe.conf` work; directory trees with jars
   inside don't). Plugin install uses a named volume populated via
   `docker create + docker cp + docker start` (daemon API, bypasses
   host↔VM file sharing).
6. **`DriverManager.getDriver` doesn't see plugin-classloader drivers**.
   Fix: `Class.forName("org.postgresql.Driver")` from inside the plugin
   forces driver static-init under the plugin CL, registering it via
   `DriverManager.registerDriver(this)`.

## Reference / scratch

- `jvm/trino-ducklake/compose/` already ships Postgres + MinIO + DuckDB
  TPC-H seed. Reuse for the live-FE substrate — don't author a second.
- **FE/BE ports (PR-branch defaults)**: http_port=8030, rpc_port=9020,
  query_port=9030, edit_log_port=9010, arrow_flight_sql_port=8070.
  `init_fe.sh` wires `priority_networks` from env var.
- **FE build gotchas (macOS host)**: `brew install gnu-getopt` for
  `build.sh --fe` to parse long opts; stock
  `docker/runtime/doris-compose/Dockerfile` fails on FE-only output —
  use `docker/runtime/doris-fe-overlay/Dockerfile`.
- Iceberg test reference: `~/DEV/OSS/db/doris-pr-62767/fe/fe-connector/fe-connector-iceberg/src/test/java/`
  — 34 files; most have a 1:1 DuckLake analogue with `Iceberg` → `DuckLake`.
- Doris SPI handbook: `~/DEV/OSS/db/doris-pr-62767/fe/fe-connector/README.md`
  — 1094 lines; §3 (lifecycle), §4 (core types), §5 (capability gating),
  §7 (step-by-step recipe) are the parts to re-read when something's
  ambiguous.
- Plugin install path is **`${DORIS_HOME}/plugins/connector/<name>/`** (singular).
  The sibling `plugins/connectors/` (plural) is the legacy Trino-bridge
  loader (`TrinoConnectorPluginLoader.java:92`) — coexists, don't delete.
