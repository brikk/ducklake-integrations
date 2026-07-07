# DuckLake-on-Doris — READ-path Working TODO

**Phase R of [`PLAN.md`](./PLAN.md)** ("read side Parquet fully") — collect
read-path items here (SELECT, pushdown, deletes-on-read, time travel,
statistics). Sibling todo files hold the other tracks:
- ✍️ [`TODO-write.md`](./TODO-write.md) — write path (INSERT/CTAS/DELETE/UPDATE/MERGE/DDL); phase W
- 🔬 [`TODO-research.md`](./TODO-research.md) — research / feasibility / "remember this" ideas

Lead with the ordered roadmap to `SELECT *`; reference and historical sections
follow. The pre-P6 capability matrix vs trino-ducklake lives in
[`archive/ducklake-doris-development-roadmap.md`](./archive/ducklake-doris-development-roadmap.md)
(superseded by PLAN.md; matrix still useful). New SPI seams available since the
P6 baseline (count pushdown, `isPartitionBearing`, `VERSION_REF`, …) are
catalogued in [`REPORT-doris-p6-iceberg-spi-cutover.md`](./REPORT-doris-p6-iceberg-spi-cutover.md).

Cross-references:
- ✅ [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md) — canonical plan (SPI mechanics, build, test)
- ⚠️ [`ducklake-doris-integration-plan.md`](./archive/ducklake-doris-integration-plan.md.ignore) — **IGNORE FOR NOW**, fallback only (hardcoded-dispatch path)
- 📚 [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md) — dependency review, capability set, BE workaround
- 🪤 [`ducklake-doris-friction.md`](./ducklake-doris-friction.md) — running log of SPI / FE / BE surprises and workarounds (Doris-team-monitorable)
- 🚀 [`compose/smoke.sh`](../compose/smoke.sh) — one-command live-FE smoke loop

## Where we are (2026-07-05, P6 baseline)

Phase-R sweep on the P6 baseline (`8b391c7`) landed four features + a parity
audit (full module suite green: 140+ tests + detekt):

- **COUNT(\*) pushdown** — 7-arg `planScan` collapses a clean scan to one range
  carrying the summed `recordCount` (`getPushDownRowCount()` + thrift
  `table_level_row_count`, iceberg emission shape). Refusal gates (any of
  these → normal ranges, BE counts by reading): remaining filter, pushed
  filter/pruned files on the handle, any delete file, inlined deletes,
  inlined data rows, partial-compacted files (`partialMax > snapshotId`).
- **Partition-bearing scan ranges** — `isPartitionBearing()`=true whenever the
  table has an active spec (stops BE hive-path-parsing our layout);
  identity-transform partition values surfaced lowercase-column-name-keyed
  (iceberg keying); bucket/temporal transforms deliberately NOT surfaced as
  raw values; partition-evolution guard (file spec ≠ active spec → empty map).
- **`VERSION_REF` time travel** (P6 `FOR VERSION AS OF '<name>'`) — explicit
  clean rejection (DuckLake has numeric snapshot ids only, no named refs).
- **`timestamp_ns` clamp** — read surface now `DATETIMEV2(6)` (Doris max
  scale; was invalid 9). Documented-lossy; no longer round-trips through the
  create-table mapper (by design, like the other degraded mappings).
- **Read-parity audit tests ported from trino-ducklake** (5 files, 20 tests):
  temporal types (TIMESTAMPTZ stays zone-aware incl. nested; TIME/INTERVAL →
  STRING pinned as deliberate), unicode identifiers (NFC/NFD-distinct, binary
  exact), snapshot-pinned handle stability across DDL/inserts/drops,
  nested-type reconstruction through the real catalog
  (`resolveColumnType` path the pure-string tests never hit).

**⏸️ PAUSE on new internally-written parity tests:** the shared **upstream
DuckLake corpus replay runner** is BUILT and full-corpus green (7,681/7,681
records through the DuckDB oracle; `jvm/ducklake-corpus-replay` on branch
`ducklake-corpus-test` @ `5be8131`). Our adapter is read-only query mirroring
(`ReplayReadEngine`: `connect`/`accepts`/`executeQuery`) — no golden-text
parsing, live-vs-live row comparison. Blocked on the runner's backend axis
(Postgres metadata catalog + shared/MinIO data path — next milestone after
Trino contact). Design + prep checklist:
[`DESIGN-corpus-replay-adapter.md`](./DESIGN-corpus-replay-adapter.md) — the
normalizer, `accepts()` v1, and the headless compose profile are all
build-able before the gate lifts.

**Corpus first contact (2026-07-06, adapter LIVE):** starter dirs 477/477
mirrored records green. It immediately found a REAL bug — tables with live
DuckLake **inlined data/delete rows** (`ducklake_inlined_*`, the PG-backend
default for small writes) returned **silently wrong rows** (empty/stale); now
a loud documented-gap error at plan time (`failOnLiveInlinedState`).
- [x] **Field-id schema dictionary (`ducklake_name_mapping`)** — DONE
  2026-07-06. `DuckLakeSchemaDictionary` emits `current_schema_id` +
  `history_schema_info` (iceberg-shaped) so the BE matches file↔table columns
  by field id (renamed/reordered columns read correctly instead of NULL),
  with the per-file `name_mapping` fallback for `add_files`/legacy files.
  DuckLake `column_id` IS the field id, and DuckLake Parquet carries
  `field_id` in file metadata (verified), so the iceberg field-id path works
  unchanged. **Scalar columns only** — a nested TField without its subfield
  tree SIGABRTs the BE's `by_parquet_field_id` recursion, so struct/list/map
  columns are omitted (they read by name; a subset dictionary is safe).
  Closed corpus files: `add_files_rename`, `compaction_multiple_rename_column`.
  - [x] **Conflict-aware `name_mapping` union** — DONE 2026-07-06.
    `getNameMaps` already keys by `target_field_id` (the table field id), so
    rename + simple add_files are correct. Added
    `DuckLakeSchemaDictionary.safeAlternateNames`: a source-name that maps to
    more than one field id across files (the DROP+re-ADD reuse case) is
    dropped from the alternates, so we never silently mis-bind.
  - [ ] **BLOCKED (needs upstream) — per-file column mapping.** Two corpus
    files (`add_files.test`, `delete/delete_legacy_missing_mapping_...`)
    remain unsolvable table-level: id-less files reuse a physical name across
    a DROP+re-ADD field-id boundary, and the scan-node-level schema dictionary
    can't express per-file maps. Needs per-range schema info in the SPI (or a
    BE per-file name→field-id hook) — see the friction log entry
    "Schema dictionary is scan-node-level; can't express per-FILE column mapping".
- [ ] **Column DEFAULT values** (corpus `issues/issue_1135`). `ALTER TABLE
  ADD COLUMN b INT DEFAULT 42` must backfill rows written before the column
  existed with the default (`WHERE b = 42` should match old rows). DuckLake
  stores the default in `ducklake_column`, but our `DuckLakeColumn` doesn't
  carry it and `getTableSchema` passes `defaultValue = null`, so old rows read
  NULL. Two parts: (a) catalog surfaces the default (shared module change), and
  (b) the read path applies it for rows in files predating the column — likely
  the iceberg "generated/default column" BE seam (check whether the BE backfills
  a not-in-file column from a default, or if it always NULL-fills). May be
  partly BE-gated; investigate the iceberg default-column path first.
- [x] **Serve inlined data rows — Stage 1 DONE (2026-07-07).** The FE
  synthesizes a temp Parquet from `readInlinedData` (`DuckLakeInlinedParquetWriter`,
  low-level parquet `Types...id()` carrying `field_id == column_id`, written
  via `LocalOutputFile` to sidestep Hadoop UGI on JDK 25) and emits it as a
  normal FILE_SCAN range appended to the file ranges. The catalog already
  snapshot-filters the rows; the temp file sits under the table data dir so the
  BE reads it at the same path. Corpus `data_inlining` 491/0. Value conversion
  mirrors trino's `DucklakeInlinedValueConverter` (blob `\xNN`, non-finite
  floats, decimals). **Gotchas fixed:** (a) filtered reads dropped inlined rows
  when `applyFilter` set `prunedFileIds` — inlined ranges are now ALWAYS added
  (file-prune targets catalog files, not inlined synthesis; BE re-applies the
  filter); (b) writer round-trip unit test assume-skips on the JDK-25
  parquet-format shaded-thrift ABI break (FE runtime is JDK 17; validated
  live).
  - **Stage 1 scope:** scalar columns + local-fs warehouse only. Excluded
    (fail loud → engine-skip): nested (list/struct/map), degraded-to-string
    types (json/variant/interval/time/uuid/uint*/int128/geometry), and S3
    warehouses.
  - [x] **timestamptz read (2026-07-07).** Now readable, inlined AND
    file-based, mapped to naive DATETIMEV2(6). Two fixes: (a) FE type name must
    be `TIMESTAMPTZ` not `TIMESTAMPTZV2` (else Nereids UNSUPPORTED); (b) the
    4.1.0 BE can't read a UTC-micros parquet column into a `TimeStampTz` slot
    ("DateTimeV2 => TimeStampTz"), so we degrade to naive DATETIMEV2 — correct
    UTC values, zone-naive typing. **Restore `TIMESTAMPTZ` when the BE supports
    the conversion** (friction log 2026-07-07).
  - [ ] **Stage 2:** nested inlined columns (DuckDB-text recursive parser),
    inlined DELETEs (of file rows), S3-warehouse temp write + lifecycle/GC,
    mixed inline+file under a file-prune.
**Per-dir corpus sweep (2026-07-06, verified GREEN — run one dir at a time,
~1 min each; NEVER the full corpus unless explicitly asked):**

| dir | records | notes |
|---|---|---|
| catalog | 180 / 0 | multi-schema reads; REFRESH-CATALOG freshness fixed earlier divergences |
| time_travel | 34 / 0 | inline `AT (VERSION => n)` rewrite landed |
| types | 83 / 0 | infinity-literal denial fixed 3 divergences |
| insert | 35 / 0 | |
| delete | 107 / 0 | BE nullability → uniform skip; FILTER(WHERE) denied |
| update | 64 / 0 | |
| merge | 57 / 0 | |
| partitioning | 422 / 0 | partition-bearing + bucket-prune hold across 17 files |
| schema_evolution | 11 / 0 | |
| stats | 416 / 0 | found+fixed time-travel-over-compaction over-read (see below) |
| comments | 0 fail | |
| alter | 545 / 0 | read-after-schema-evolution across 34 files |
| view | (with general) 0 fail | DuckLake views correctly skip (not surfaced yet) |
| general | 0 fail | |
| table_changes | (with metadata) 0 fail | |
| metadata | 0 fail | |
| constraints/default/reserved_names/comments/snapshot_info/list_files/initialize | 0 fail | |
| issues | 0 fail (4 skips) | found: column DEFAULT-values gap (issue_1135); rest = known inlined-delete / view / fault-injection |

**Read-relevant corpus surface swept — no remaining read gaps.** The
remaining unswept dirs are write/maintenance (compaction, rewrite_data_files,
add_files, remove_orphans, migration, checkpoint, sorted_table,
data_inlining, deletion_inlining) — those exercise DuckDB-side operations the
oracle performs; our only involvement is the SELECT after, which the swept
dirs already cover. Full-corpus run remains a CI/later concern.

**REAL BUG found by corpus (stats/count_star_optimization_time_travel):** time
travel AS OF a snapshot OLDER than a compaction that merged newer rows
returned the MERGED row count (300) instead of the live-at-snapshot count
(100) — and the plain read over-returned rows too, not just COUNT(*). Root
cause: a compacted "partial" data file (`partial_max > snapshot`) physically
holds rows from newer snapshots tagged by a hidden
`_ducklake_internal_snapshot_id` column; trino filters them in its page
source, but the Doris BE reader has no hook to apply that per-row snapshot
predicate. Fixed: `failOnUnfilterablePartialFile` fails loudly at plan time
(latest-snapshot reads unaffected). Count-pushdown also now refuses any
non-latest snapshot. Un-gate when the BE can snapshot-filter partial files.

Dialect/adapter learnings folded in: literal inline time-travel rewrite,
WITH-CTE acceptance, infinity-temporal-literal denial (silent-wrong-answer
landmine), `FILTER (WHERE)` denial (Doris parse gap), BE delete-nullability
classified as engine-skip.

- [ ] Grow `DorisCorpusDialect` accepts() deliberately as read features land
  (each widening = more mirrored records). Full-corpus run is a
  later/CI concern, not an interactive gate.

**NEXT read feature — hive-layout `add_files` path partition fill** (teed up
2026-07-06; corpus `add_files/add_files_hive*.test`, skip-listed as
`GAP_HIVE_PARTITION_FILL`). When files are registered from a hive directory
layout (`part_key=1/part_key2=10/f.parquet`), the parquet BODY contains only
the non-partition columns; partition columns live in the path and in
`ducklake_file_partition_value`. Oracle constant-fills them; our scan returns
NULL/omits → divergence. Fix shape:
  1. Detect hive-layout files: partition columns absent from the file schema
     but present as `DucklakeFilePartitionValue`s (distinguish from
     iceberg-style where the body carries them).
  2. Emit `path_partition_keys` + per-range `columns_from_path` values into
     `TFileScanRangeParams`/`TFileRangeDesc` so the BE constant-fills — the
     native hive/iceberg reader path. `DuckLakeScanRange` already carries the
     partition-value map; today it deliberately does NOT emit
     `columns_from_path` (see the class doc) — this feature flips that ONLY
     for hive-layout files.
  3. Re-pin `DuckLakeScanRangeThriftParityTest` for the new range shape.
  Model: iceberg connector's `columns_from_path` handling; trino side
  constant-fills from partition values in `DucklakeSplitManager`.
  Verify: `corpusReplayTest -Dducklake.corpus.dirs=add_files` + un-skip the
  three `add_files_hive*` entries.

Follow-ups from the sweep (not yet done):
- [ ] `add_files`-registered hive-layout files may lack partition columns in
  the parquet body; trino constant-fills from partition values. Doris path
  would need `path_partition_keys` + `columns_from_path` (iceberg-style) —
  coordinate with the pinned thrift shape before attempting.
- [ ] Count pushdown never nets out deletes (conservative permanent fallback
  to BE-side counting when any delete exists) — revisit only if COUNT(*) on
  deleted-from tables shows up hot.
- [ ] DuckDB 1.5.4 CHECKPOINT can consolidate multiple INSERTs into one data
  file — don't write tests that assume "one INSERT = one parquet file" (the
  shared bootstrap comment overpromises; audit fixtures assert
  stability-at-pin instead).

## Where we were (2026-05-19)

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
- [x] `DuckLakePredicateConverter` — maps `ConnectorExpression` conjuncts → `ColumnRangePredicate`s: `col <op> literal` comparisons, `BETWEEN`, **membership** (`col IN (…)`, same-column `col = a OR col = b …`) collapsed to the typed `[min..max]` span (shared recognizer `DuckLakeMembership`), and **prefix `LIKE`** (`col LIKE 'abc%'` → `[abc, abd)`, i.e. `>= 'abc' AND < 'abd'`). Skipped: functions / REGEXP / non-prefix·escaped·`_` LIKE / IS NULL / NE / NOT IN / mixed-column or range `OR` — **no function pushdown by design**. String ranges assume codepoint/binary collation (DuckDB VARCHAR default), the same assumption already under `>`/`<`/`BETWEEN`.
- [x] `applyProjection` (column pruning) + `applyFilter` (stats-based file pruning via `findDataFileIdsInRange`) on the metadata; `SUPPORTS_PROJECTION_PUSHDOWN` + `SUPPORTS_FILTER_PUSHDOWN` on. Pruning is best-effort file elimination — the BE re-evaluates the full predicate.
- [x] **IDENTITY partition pruning** — works for free through `applyFilter`'s stats path: DuckLake records `file_column_stats` for partition columns, so a `region = 'us'` filter prunes non-matching partition files. Verified by `prunesFilesByPartitionEqualityFilter` against the `sales.by_region` fixture.
- [x] **Temporal partition pruning** — already covered by the stats path (no separate code). A `year/month/day/hour(date_col)` partition file still carries `date_col` min/max stats, so a `date_col <op> literal` filter prunes old-partition files via `findDataFileIdsInRange` (typed comparison) — the same mechanism the IDENTITY `region` test verifies, applied to a date column.
- [x] **BUCKET partition pruning** — done (Doris-local). `DuckLakeBucketTransform` ports DuckLake's `(murmur3_32(value) & Int.MAX) % N` (Iceberg-compatible; hand-rolled murmur3, no Guava dep), **pinned against DuckLake's own reference values** (`bucket(4)`: alice→1, bob→2, charlie→3) in `DuckLakeBucketTransformTest`. `applyFilter`'s `bucketPrune` matches a membership constraint (`col = literal`, `col IN (…)`, same-column `col = a OR col = b …` via `DuckLakeMembership`) to the file's stored bucket (`getFilePartitionValues`) — keeping files whose bucket ∈ `{bucket(v) : v ∈ candidates}` — **intersected** with stats pruning, so a wrong hash empties the result and fails the e2e tests (`prunesFilesByBucket{Equality,InList}Filter`, `prunesFilesByOrOfEqualitiesFilter`) instead of silently keeping the wrong file. Candidate sets union across conjuncts (safe over-approximation; stats-path intersection recovers precision when column stats exist). Handles String + int/long/date literals; other types fall through to "keep all". *Optional future cleanup:* de-dup the murmur3 into a shared `ducklake-catalog` helper used by both trino + doris.
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
**SPI note (2026-06-24):** the original plan below assumed the old `getTableHandle`
overload (`ConnectorTableVersion`/`ConnectorRefSpec`). The P-series SPI replaced that
with the MVCC model: `resolveTimeTravel(ConnectorTimeTravelSpec)` resolves the spec to a
`ConnectorMvccSnapshot`, and `applySnapshot(handle, snapshot)` threads the pin onto the
handle. The engine drives both for plugin catalogs in
`PluginDrivenMvccExternalTable.loadSnapshot` (calls `resolveTimeTravel` then
`applySnapshot`, then `getTableSchema(pinnedHandle)` for schema-at-snapshot) — so this is
genuinely wired upstream (unlike plugin DELETE).
- [x] **`resolveTimeTravel`** — `SNAPSHOT_ID` (FOR VERSION AS OF) + `TIMESTAMP` (FOR TIME
  AS OF) → DuckLake's `getSnapshot` / `getSnapshotAtOrBefore`; `TAG`/`BRANCH`/`INCREMENTAL`
  return empty (no DuckLake equivalent). (Shipped with the SPI consolidation fix.)
- [x] **`applySnapshot`** — threads the resolved `snapshotId` onto `DuckLakeTableHandle`
  (the read path resolves schema + data files + pushdown + partitions by
  `(tableId, snapshotId)`, so re-stamping snapshotId pins the whole read AT that snapshot;
  schema-at-snapshot is automatic). Null / negative / same-id pins leave the handle
  unchanged (read latest). Tests: `DuckLakeConnectorMetadataTimeTravelTest` (8) — incl.
  `timeTravelPinResolvesTheHistoricalDataFileSet` (two-snapshot fixture: a historical pin
  resolves the OLD data-file set, current resolves the new one).
- [x] **Smoke:** `SELECT … FOR VERSION AS OF <snapshot_id>` on the live FE+BE returns the
  historical snapshot's rows (native amd64, 2026-06-24).

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
