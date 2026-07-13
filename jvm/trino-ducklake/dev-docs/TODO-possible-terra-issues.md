# Possible Terra Issues — Review Notes

Review notes from 2026-07-12 onward. These are deliberately not fixes or
workarounds. The original entries below remain reproduction candidates; the
newer entries identify control-flow or API-contract defects directly in the
implementation and include a focused reproduction. The priority labels describe
likely user impact, not certainty.

## P1 — An explicit empty `expire_snapshots` list falls through to retention expiry

`snapshot_ids => ARRAY[]` (and `ARRAY[NULL]`) is treated the same as omitting
`snapshot_ids`: the procedure converts it to an empty set and then takes the
retention-threshold branch.  A caller asking to expire no explicit snapshots can
therefore expire every non-latest snapshot old enough for the configured
threshold.

- Evidence: `DucklakeExpireSnapshotsProcedure.kt:76-81`.  The catalog API
  already distinguishes a non-null version set, so preserving an empty set would
  select no snapshots rather than widening the request.
- Why this matters: expiration removes snapshot history immediately and schedules
  data for later cleanup.  In test/development configurations with a zero or
  short minimum retention this is easy to trigger accidentally.
- Suggested proof: create several snapshots with a retention threshold allowing
  expiry, call the procedure with `snapshot_ids => ARRAY[]`, and confirm the
  current code expires them.
- Real fix direction: branch on whether the argument itself is null, not whether
  its filtered set is empty.  Make an empty explicit list a no-op (or reject it)
  and reject null members rather than silently discarding them.

> **VERDICT: CONFIRMED (trino, 2026-07-13).** `DucklakeExpireSnapshotsProcedure.kt:82-90`
> — `explicit` is the *filtered* set (`snapshotIds?.filterNotNull()?.toSet().orEmpty()`);
> `ARRAY[]` and `ARRAY[NULL]` both collapse to empty → `explicit.isNotEmpty()` false →
> the `else` retention branch runs (default threshold `7d`, line 69), expiring every
> non-latest snapshot past the cutoff. Fail-dangerous. **✅ FIXED 2026-07-13** —
> `DucklakeExpireSnapshotsProcedure` now branches on `snapshotIds != null` (empty
> explicit list = no-op; NULL members rejected). Pinned by
> `TestDucklakeExpireSnapshots.emptyExplicitSnapshotIdsIsNoOpNotRetentionExpiry`.

## P1 — Partial-file time travel fails open when its required marker is absent

A data file whose catalog row has `partial_max > requested_snapshot` must carry
`_ducklake_internal_snapshot_id`; that marker is what identifies rows newer than
the historical read.  The page source asks for the marker in this situation, but
the delete reader returns an empty set when it is absent, making the historical
read include every row in the file.

- Evidence: `DucklakeSplitManager.kt:637` sets `snapshotFilterMax` from
  `partial_max`; `DucklakePageSourceProvider.kt:995-1000` invokes the filter;
  `DucklakeDeleteFileReader.kt:245` returns `emptySet()` for a missing marker.
- Why this matters: a mismatched/corrupt externally supplied partial file returns
  rows that did not exist at the requested snapshot instead of failing.  The
  normal DuckDB partial-file writer emits the column, so this is specifically an
  integrity-boundary failure, not a normal-path regression.
- Suggested proof: take the existing partial-file test fixture, remove or rename
  the internal marker while retaining its `partial_max`, then time-travel to the
  earlier snapshot.
- Real fix direction: when `snapshotFilterMax` is present, require a readable
  BIGINT marker and throw a clear `TrinoException` if it is absent or malformed.

> **VERDICT: CONFIRMED (trino, 2026-07-13).** `DucklakeDeleteFileReader.kt:245` —
> `messageColumnIO.getChild(INTERNAL_SNAPSHOT_ID_COLUMN) ?: return emptySet()`. An
> empty set = "no rows are newer than the requested snapshot", so a partial file
> missing its marker returns ALL rows, including those that post-date the snapshot.
> This path only runs when `snapshotFilterMax` is set (`DucklakeSplitManager.kt:637`
> from `partial_max`), i.e. the file is KNOWN to contain filterable rows — so a
> missing marker is an integrity failure, not a normal path. Violates AGENTS.md
> "fail loud over silently wrong". **✅ FIXED 2026-07-13** — `readSnapshotDropPositions`
> now throws a clear `TrinoException` (GENERIC_INTERNAL_ERROR) when the marker is
> absent, and rethrows specific TrinoExceptions unwrapped. Pinned by
> `TestDucklakePartialFileFilter.partialFileMissingSnapshotMarkerFailsLoud`.

## P2 — Opaque-format `add_files` registers unreadable schemas

The Lance, Vortex, and DuckDB-file branches document exact name/type matching,
but validate only row count.  Their empty projection becomes `SELECT 1`, so no
table column is projected or type-checked before an opaque file is committed to
the catalog.

- Evidence: `DucklakeAddFilesProcedure.kt:203-215` dispatches those formats;
  `:539-558` counts rows with an empty `ExecutionRequest`.  The opaque fragment
  builders record no name map, while later reads project current table names.
- Why this matters: `add_files` can successfully commit a dataset with missing,
  renamed, or incompatible columns, then fail only when a user reads it.  This
  violates the procedure's own validation contract and leaves an invalid active
  catalog entry.
- Suggested proof: create a target table with `id`, register a `.db`, Vortex, or
  Lance dataset that lacks `id` (or supplies it with a wrong type), and observe
  that registration succeeds before a subsequent SELECT fails.
- Real fix direction: preflight a zero/one-row typed projection (including
  partition-body requirements) through the same executor and reject a mismatch
  before `commitAddFiles`.

> **VERDICT: CONFIRMED (trino, 2026-07-13).** `DucklakeAddFilesProcedure.kt:539-558`
> — `countRows` builds an `ExecutionRequest` with `emptyList<DucklakeColumnHandle>()`
> (`SELECT 1`), so nothing but the row count is validated for the lance/vortex/duckdb
> branches; the parquet path (`buildFragment`) does name/type matching. A dataset
> missing/renaming/mistyping a column commits an active catalog entry that only fails
> at read. Deferred (P2) — the fix is a typed preflight projection through the same
> executor; larger than the current batch and touches three format builders. Left
> open for a focused follow-up.

## P2 — `DROP SCHEMA ... CASCADE` is ignored

The Trino SPI supplies the `cascade` flag, but `DucklakeMetadata.dropSchema`
does not use it and always delegates to a catalog operation that rejects
non-empty schemas.  Consequently a standard `DROP SCHEMA s CASCADE` fails
instead of dropping the tables and schema.

- Evidence: `DucklakeMetadata.kt:831-834`; `JdbcDucklakeCatalog.kt:2263-2270`
  explicitly rejects schemas that still contain tables.
- Why this matters: it is a concrete SQL DDL semantic mismatch, particularly
  disruptive for deployment teardown and test/database lifecycle tooling.
- Suggested proof: create a schema containing a table and run `DROP SCHEMA ...
  CASCADE` through Trino.
- Real fix direction: decide and implement catalog-consistent cascade semantics.
  This needs an atomicity/transaction decision; do not simply loop over table
  drops without defining failure behavior.

> **VERDICT: CONFIRMED (trino, 2026-07-13).** `DucklakeMetadata.kt:831-834` — the
> `cascade` SPI flag is unused; delegates to `catalog.dropSchema(schemaName)`, which
> rejects non-empty schemas (`JdbcDucklakeCatalog.kt:2263`). So `DROP SCHEMA s CASCADE`
> fails on a non-empty schema. Deferred (P2) — needs an atomicity decision (per-table
> drop ordering + failure/rollback behavior) that shouldn't be rushed into this batch.
> Left open; flagged for a design pass.

## P2 — Identity partition writes accept types the encoder cannot serialize

DuckLake permits identity partitions over the source type, but the connector's
partition encoder handles only a handful of scalars and falls back to
`Type.getSlice()`.  For `TIMESTAMP` and `DECIMAL` that is the base Trino method
and throws `UnsupportedOperationException`; the table is created successfully
and its first INSERT fails.

- Evidence: `DucklakeTableProperties.kt:143-185` and `DucklakeMetadata.kt:852-853`
  accept/pass identity fields without a type gate; `DucklakePartitionComputer.kt:93-109`
  uses the `getSlice()` fallback.  `AbstractType.getSlice()` is the throwing base
  implementation for timestamp/decimal types.
- Why this matters: valid upstream partition definitions fail only on the write
  path.  Other non-slice types have the same risk; raw UUID bytes would also make
  an invalid UTF-8 partition string rather than a canonical value.
- Suggested proof: `CREATE TABLE ... (ts TIMESTAMP, d DECIMAL(10,2)) WITH
  (partitioned_by = ARRAY['ts'])`, followed by an INSERT.
- Real fix direction: implement canonical partition-text encoding and matching
  parsing for every identity-partitionable DuckLake type, or reject unsupported
  types at CREATE TABLE time until that is complete.

> **VERDICT: CONFIRMED (trino, 2026-07-13).** `DucklakePartitionComputer.kt:93-109`
> — `computeIdentityValue` handles a fixed set of scalars then falls to
> `type.getSlice(block, position)`; for TIMESTAMP/DECIMAL (short forms are `long`s,
> not slices) that hits `AbstractType.getSlice`, which throws
> `UnsupportedOperationException`. CREATE TABLE succeeds; the first INSERT crashes
> (not even a clean error). Adjacent to the hive-path encoding just fixed (same
> method). **✅ FIXED 2026-07-13** — `computeIdentityValue` now canonically encodes
> DECIMAL (short/long) and the TIMESTAMP family (mirroring the read-side
> `DucklakePartitionValueParser`, which already parsed them), and throws a clear
> NOT_SUPPORTED for genuinely unencodable types (VARBINARY/UUID/CHAR) instead of
> the opaque `getSlice` crash. Pinned by `TestDucklakePartitionComputer` (encode +
> parser round-trip) and `TestDucklakePartitionedWrite
> .testIdentityPartitionOnTimestampAndDecimal`. NOTE: rejecting the unencodable
> types at CREATE TABLE (vs first INSERT) remains a smaller follow-up.

## P1 — Connector-written floating-point files omit `contains_nan`

Every connector writer records `contains_nan = false` for a REAL/DOUBLE column,
even if the file contains NaN. The catalog persists false as SQL NULL. DuckLake
uses this flag to decide whether a data file can contain a match for `x = NaN`
and to retain NaN-containing files for `x > <non-NaN>`; an upstream DuckDB reader
can therefore prune a connector-written file that holds qualifying rows.

- Evidence: `DucklakeStatsExtractor.kt:70,118-125` hard-codes false for Parquet;
  `DucklakeColumnStatsAccumulator.kt:115-123` does the same for Vortex/Lance;
  `DuckDbFileWriter.kt:404-411` and `DuckDbArrowStreamFileWriter.kt:472` do it
  for `.db` writers. `DuckDbWriterSupport.formatStatValue` deliberately omits NaN
  from min/max (`:102-117`), so min/max cannot compensate for the missing flag.
- Why this matters: upstream `GenerateConstantFilterDouble`
  (`vendor/ducklake/src/storage/ducklake_metadata_manager.cpp:1100-1121`)
  produces `contains_nan` for equality with NaN and adds `OR contains_nan` to
  greater-than filters. SQL NULL/false makes that pruning predicate reject the
  whole file, returning too few rows in cross-engine reads.
- Suggested proof: write a Trino-owned Parquet (and one non-Parquet) file with
  `[1.0, NaN]`, then read the table through DuckDB with `WHERE x = 'NaN'::DOUBLE`
  and `WHERE x > 0`; inspect `ducklake_file_column_stats.contains_nan` alongside
  the result.
- Real fix direction: track a per-column NaN bit while each writer consumes
  pages (the Vortex/Lance accumulator already walks every value), or derive it
  reliably during the `.db` stats query. Persist TRUE when observed; leave the
  value unknown rather than falsely asserting no NaN when it cannot be known.

> **VERDICT: CONFIRMED (trino ↔ DuckDB, 2026-07-13).** This is not merely
> imprecise optimizer metadata: DuckLake's own filter generator relies on
> `contains_nan` for correctness because NaN sorts above ordinary floating-point
> values. All connector write formats currently serialize false/NULL, so a file
> containing NaN can be eliminated before DuckDB reads it. Fail-dangerous;
> open for a focused cross-engine regression test and per-writer stats fix.

> **Triage pass 2026-07-12 (terra).** Each item below carries a `VERDICT`
> block: code was read against the cited lines. CONFIRMED items are copied to
> the durable backlog (`TODO-WRITE-MODE.md` / `TODO-READ-MODE.md`); KNOWN items
> were already tracked/documented; REFUTED (whole or part) items include the
> counter-evidence so they are not re-raised. Nothing here has been *fixed* —
> this pass only classifies.

## P1 — URL-encode hive partition path values on connector writes

`DucklakePageSink.buildRelativePath` writes partition values directly into
`key=value/` directory names. `DucklakePartitionComputer` returns raw VARCHAR
text, so a value such as `a/b`, `%`, or a space becomes a non-canonical and,
for `/`, structurally different path.

- Evidence: `DucklakePageSink.kt:540-553`; raw identity values originate in
  `DucklakePartitionComputer.kt:93-109`.
- Why this matters: the read/add-files path explicitly treats hive partition
  values as URL-encoded and decodes them (`DucklakeSplitManager.kt:765-818`),
  and DuckDB's Hive partition implementation URL-encodes paths. A slash value
  can be parsed as a different directory layout by another engine.
- Suggested proof: insert identity-partition values containing a slash, percent
  sign, whitespace, Unicode, and `__HIVE_DEFAULT_PARTITION__`; verify file
  layout and reads using both Trino and DuckDB.
- Real fix direction: encode *path segments* using the compatible Hive/DuckDB
  escaping rule, while retaining the raw value in catalog partition metadata.

> **VERDICT: CONFIRMED.** `buildRelativePath` (`DucklakePageSink.kt:540-554`)
> appends the raw value with no escaping, and `computeIdentityValue`
> (`DucklakePartitionComputer.kt`) returns VARCHAR as
> `getSlice(...).toStringUtf8()` (raw text). The read side URL-*decodes*
> (`hiveUrlDecode`, `DucklakeSplitManager.kt:797-819`) and treats
> `__HIVE_DEFAULT_PARTITION__` as NULL (`:761`). So Trino write→Trino read is
> NOT round-trip safe: a value with `%` decodes wrong, a `/` splits into extra
> path segments (parsed as a different layout), and a literal
> `__HIVE_DEFAULT_PARTITION__` collides with the NULL sentinel — plus it
> diverges from DuckDB, which URL-encodes. **✅ FIXED 2026-07-13** —
> `DucklakeHivePartitionCodec` (encode+decode, key & value), see
> [TODO-WRITE-MODE.md § Hive Partition Path Encoding](TODO-WRITE-MODE.md#hive-partition-path-encoding-on-writes--done-2026-07-13).

## P1 — Do not turn present-but-unreadable data into NULL

The Parquet reader treats a physical column that cannot be constructed for the
declared Trino type the same as an absent schema-evolution column, emitting a
constant NULL/default. It also catches malformed stored partition/default text
and returns NULL.

- Evidence: `DucklakePageSourceProvider.kt:1209-1215` and `:1768-1789`.
- Why this matters: corrupt/incompatible externally written Parquet data, a bad
  partition value, or malformed `initial_default` can yield successful but
  incorrect query results. The caller cannot distinguish that from genuine SQL
  NULL or an old file predating `ADD COLUMN`.
- Suggested proof: register a Parquet file with a present incompatible physical
  type, and corrupt an identity partition/default value in an isolated catalog.
  Confirm the current query returns NULL rather than failing.
- Real fix direction: keep NULL/default projection only for a demonstrably
  absent column. Throw a clear `TrinoException` when a present mapped field or
  catalog value cannot be decoded.

> **VERDICT: PARTIAL — site 1 mostly REFUTED, site 2 CONFIRMED.**
> *Site 1 (`:1212`, `field.isEmpty`):* `constructField`
> (`DucklakeParquetTypeUtils.kt:45-120`) returns a Field for **every present
> primitive** (line 115) with no type-compat check — a genuinely incompatible
> physical/logical type is NOT silenced here; it surfaces as a loud decode error
> in `ParquetReader`. `field.isEmpty` only fires for structurally-malformed
> *nested* schemas (RowType with no matching child, MapType childrenCount≠2,
> ArrayType childrenCount≠1), which is a narrow edge, and those are arguably
> "genuinely unreadable structure" rather than "present incompatible data". The
> review's headline framing ("a present incompatible physical type → NULL")
> does not hold for the common primitive case.
> *Site 2 (`buildMissingColumnBlock`, `:1780-1789`):* CONFIRMED. An unparseable
> stored partition value or `initial_default` is swallowed to NULL. The comment
> justifies it by analogy to the pruning path — but pruning-tolerance keeps the
> file (safe/conservative), whereas here it emits a *wrong value*. This violates
> AGENTS.md "prefer failing loud over silently wrong". Copied (site 2 only) to
> [TODO-READ-MODE.md § Robustness Follow-ups](TODO-READ-MODE.md#robustness--performance-follow-ups-review-2026-07-12).

## P2 — Sorted writes have unbounded per-writer heap usage

For the sorted-parquet write gate, the page sink retains every incoming page
until `finish()`, then sorts all rows at once. `getMemoryUsage()` exposes the
number but does not impose a limit, spill, or backpressure path.

- Evidence: `DucklakePageSink.kt:102-115`, `:235-241`, and `:370-388`.
- Why this matters: a sufficiently large INSERT into an unpartitioned sorted
  table can OOM a writer. This is especially easy to miss because the current
  sorted-write test uses one small insert.
- Suggested proof: bounded-heap integration run with an increasing sorted CTAS
  or INSERT. Confirm whether the sink can reserve/limit memory through Trino;
  do not assume reporting alone is enforcement.
- Real fix direction: a bounded external/merge sort (or a documented,
  enforced row/byte limit that fails before heap exhaustion). Do not silently
  produce unsorted files.

> **VERDICT: KNOWN — already tracked, not new.** The unbounded buffer is an
> explicitly documented, accepted v1 trade-off in code
> (`DucklakePageSink.kt:102-110`) and in the backlog
> ([TODO-WRITE-MODE.md § Sorted Table Writes](TODO-WRITE-MODE.md#sorted-table-writes---done-2026-07-08-recommended-gated-scope),
> DONE 2026-07-08), whose follow-ups already list *"spill-based sort for large
> unpartitioned inserts"*. One correction to the note: `getMemoryUsage()` is not
> mere reporting — Trino feeds it into query memory accounting and CAN abort the
> query on limit before an OOM, so the failure mode is "query killed", not
> necessarily "worker OOM". No new backlog entry; the spill-sort follow-up
> stands.

## P2 — File-stat pruning repeats full active-file scans per predicate

`DucklakeSplitManager.pruneDataFiles` invokes one range lookup for each
predicate column. The catalog implementation currently fetches every active
file/stats row and filters it in JVM code for each invocation.

- Evidence: `DucklakeSplitManager.kt:261-302`; implementation at
  `jvm/ducklake-catalog/.../JdbcDucklakeCatalog.kt:960-1013`.
- Why this matters: a query with C independently prunable columns and F active
  files does roughly C full catalog scans/materializations, before normal split
  construction. This becomes a planning bottleneck for large tables and wide
  filters.
- Suggested proof: instrument catalog calls/rows for a table with many files
  and a multi-column predicate; compare with a single batched query.
- Real fix direction: combine the predicates in one typed SQL/catalog operation
  or allow the range lookup to consume the current candidate set. Preserve the
  conservative "unknown stats keep the file" semantics.

> **VERDICT: CONFIRMED.** `pruneDataFiles` (`DucklakeSplitManager.kt:261-291`)
> calls `findDataFileIdsInRange` once per predicate column;
> `JdbcDucklakeCatalog.kt:960-1014` runs a column-type lookup + a
> fetch-all-active-files (LEFT JOIN stats) then filters `isWithinBounds` in the
> JVM. So C prunable columns ≈ 2·C queries each materializing F rows. Note the
> JVM-side filter is *deliberate*: MIN/MAX are stored as text and need
> type-aware `parseStatValue` comparison that is hard to push into generic SQL
> across duckdb/postgres/mysql/sqlite backends — so the "one typed SQL op" fix is
> constrained. The cheap, safe win is to fetch stats for all predicate columns in
> a single query and pivot in the JVM (C→1 scans), keeping the "unknown stats →
> keep" LEFT-JOIN semantics. Copied to
> [TODO-READ-MODE.md § Performance Follow-ups](TODO-READ-MODE.md#robustness--performance-follow-ups-review-2026-07-12).

## P2 — Puffin delete vectors expand into several large boxed sets

The Puffin reader loads the entire file into a byte array, decodes compressed
Roaring bitmaps into `HashSet<Long>`, merges them into another mutable set, and
the delete transform copies those sets again.

- Evidence: `DucklakePuffinDeleteReader.kt:119-135`, `:293-318`; merge at
  `DucklakePageSourceProvider.kt:989-1022`; copies at `:1857-1864`.
- Why this matters: a compact dense deletion vector can use far more heap than
  its on-disk form, with boxed-Long allocation churn per split. Concurrent scans
  compound the cost.
- Suggested proof: profile a dense/multi-million-position Puffin deletion vector
  under concurrent scans, capturing peak heap and allocation rate.
- Real fix direction: retain Roaring structures (or another primitive compact
  membership representation) through filtering, avoid duplicate copies, and add
  a conscious resource limit for pathological delete files.

> **VERDICT: CONFIRMED (perf/heap only).** `decodeBitmaps`
> (`DucklakePuffinDeleteReader.kt:293-318`) expands each Roaring bitmap into a
> `HashSet<Long>` (boxed longs, `:306-307`); dense vectors cost far more heap
> than their on-disk form and churn boxed-Long allocations per split. Correctness
> is fine and it is not unbounded — `readAllBytes` guards `length > Int.MAX_VALUE`
> (`:121-122`) and corruption fails the split loudly. This is an optimization
> (retain RoaringBitmap through the membership test) plus a possible explicit
> resource cap, not a correctness bug. Copied to
> [TODO-READ-MODE.md § Puffin Deletion Vector Reads](TODO-READ-MODE.md#puffin-deletion-vector-reads) follow-ups.

## P2 — Change-feed setup is fully eager and row-proportional

Before the first output page, a change-feed scan resolves all files/events and
can read each affected Parquet file's entire lineage column into a `LongArray`.
It also retains delete-position/update-pairing state for the complete snapshot
window.

- Evidence: `DucklakePageSourceProvider.kt:568-617`, `:754-763`, `:814-839`;
  lineage collection at `DucklakeDeleteFileReader.kt:201-216`.
- Why this matters: large history windows have startup latency and heap use
  proportional to every affected file/row rather than to the page being
  returned. It can also re-read input once for lineage and again for output.
- Suggested proof: run a large update/delete history window and measure time to
  first row plus retained heap. Include rewritten files with lineage enabled.
- Real fix direction: process change units lazily or in bounded batches. Pairing
  may require a compact on-disk/keyed state; do not drop update-pair semantics
  just to stream.

> **VERDICT: CONFIRMED (perf/latency only).** `createChangeFeedPageSource`
> (`DucklakePageSourceProvider.kt:560-618`) resolves ALL insert files,
> deletions, per-file lineage (`readInsertLineage` → `LongArray` per file), and
> whole-window `deletedRowidsBySnapshot`/update-pairing before returning the
> first page — startup latency and retained heap scale with the history window,
> not the page. Correct, but eager. Note the update-pairing (delete+reinsert on
> the same rowid) inherently needs cross-window state, so a lazy/batched rewrite
> is non-trivial and must not drop pair semantics. Copied to
> [TODO-READ-MODE.md § R6: Change Feed](TODO-READ-MODE.md#r6-change-feed-and-extended-metadata-parity) follow-ups.

## Lower-priority follow-up — materialized DuckDB read cache

The cache intentionally has no eviction and places its shared `<key>.partial`
name directly under the OS temp directory. Its locks are JVM-local.

- Evidence: `DucklakeMaterializedFileCache.kt:31-49`, `:79-108`.
- Risks: disk growth over worker lifetime; two JVMs sharing the same temp
  directory can race on the same partial file because they do not share the
  lock.
- Suggested proof: run two JVMs against a shared cache directory and materialize
  the same remote object concurrently; separately stress unique-file scans.
- Real fix direction: process-specific cache roots or cross-process locking plus
  bounded eviction. This is not a request for shared-storage scratch machinery.

> **VERDICT: SPLIT — eviction KNOWN/deferred, `.partial` race CONFIRMED.**
> *Eviction:* the no-eviction/disk-growth behavior is an explicitly documented
> Phase-1 deferral (`DucklakeMaterializedFileCache.kt:37-49`: "Capacity caps and
> TTL come in a later step if measurements show they're needed"; OS tmpdir
> cleanup is the interim story). Not new — left as a documented deferral.
> *Cross-process `.partial` race:* CONFIRMED latent gap. The partial name is
> `<key>.partial` where key = hash(remotePath, fileSize) (`:80,:92`) — shared,
> not process-unique — and the lock is JVM-local (`keyLocks`, `:53`). Two
> co-located workers sharing `${java.io.tmpdir}/ducklake-read/` can interleave
> writes to the same `.partial`; the `downloaded != fileSize` guard (`:95-103`)
> catches truncation but not two full-size interleaved writers, so a corrupt
> full-size `.db` could be moved into place and handed to DuckDB `ATTACH`. Cheap
> fix: process-unique partial name (`<key>.<pid>-<uuid>.partial`) before the
> atomic move. **✅ FIXED 2026-07-13** (eviction still a documented deferral), see
> [TODO-READ-MODE.md § Robustness Follow-ups](TODO-READ-MODE.md#robustness--performance-follow-ups-review-2026-07-12).
