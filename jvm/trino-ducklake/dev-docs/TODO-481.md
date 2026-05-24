# Trino 481 upgrade

Source: https://trino.io/docs/current/release/release-481.html, full diff PR #28894 ([release log](https://github.com/trinodb/trino/pull/28894)).

The official release notes omit several SPI changes that touch any non-built-in connector. The big ones we actually hit:

- `MetadataReader.readFooter(ParquetDataSource, DataSize, Optional)` → 4-arg form taking `ParquetReaderOptions`
- `ParquetWriter.getWrittenBytes()` + `getBufferedBytes()` → single `getEstimatedWrittenBytes()`
- `CachingHostAddressProvider` deleted; replaced with `ConnectorSplit#getAffinityKey()` + injected `SplitAffinityProvider` (PR [#29182](https://github.com/trinodb/trino/pull/29182))
- `Type.getObject` was *not* in fact removed in 481 — `javap` against the 481 jar confirms it's still there. Earlier assumption was wrong.

## Completed

- Bumped `libs.versions.toml`, `compose/.env`, `trino-ducklake/build.gradle.kts` to 481 / 481-1-ALPHA.
- `MetadataReader.readFooter` switched to 4-arg form at `DucklakePageSourceProvider.java:416/704`, `DucklakeAddFilesProcedure.java:278`.
- `ParquetWriter.getEstimatedWrittenBytes()` at `ParquetFileWriter.java:78/89`, `DucklakeMergeSink.java:205`.
- Soft affinity restored via new SPI:
  - `DucklakeSplit` carries `Optional<String> affinityKey` (replacing the dead `addresses` list) and overrides `getAffinityKey()`.
  - `DucklakeSplitManager` injects `SplitAffinityProvider` (bound by `FileSystemModule`; `Noop*` by default, `Cache*` when `fs.cache.enabled=true`).
  - Per-file key computed at split creation: `splitAffinityProvider.getKey(dataFilePath, 0L, primary.fileSizeBytes())` — same pattern Iceberg uses.
  - Tests pass a fresh `NoopSplitAffinityProvider()` to the ctor.

## Status: green

`./gradlew :ducklake-catalog:test :trino-ducklake:test` — **789 tests, 0 skipped, 0 failed**.

## Items per release notes, mapped to our exposure

| Note | Affects us? | Action |
|---|---|---|
| `connectorMetrics` on QueryInputMetadata | Future opportunity | Could emit cache hits, format breakdown, executor (in-process vs quack) per split. Track as enhancement. |
| Race on dynamic-catalog drop vs `system.jdbc` queries (#28017) | **No** — our catalogs are static config files; no runtime add/drop | None |
| Legacy object storage removed (Azure / GCS / S3 / S3-compat) | **No** — we already use `fs.native-s3.enabled=true` in all catalog props | None; verify compose files set native flag (currently yes) |
| `variant` type for Iceberg v3 | **Watch** — DuckLake spec doesn't define variant. If upstream DuckLake adopts it (DuckDB has VARIANT in roadmap), we'd need column-type mapping. | Track in research log when DuckLake side moves. |
| Iceberg `timestamp(9)` / `timestamp(9) with time zone` for v3 | **No** — Iceberg-specific. Our connector already supports TIMESTAMP precisions 0/3/6/9 on writes (Arrow stream writer maps to TIMESTAMP_NS for precision 9). | None |
| Iceberg `$files` additions, including `added_snapshot_id` (#29044, #28911) | **Comparison opportunity** — our `$files` (`DucklakeMetadata.java:803`) has: `data_file_id`, `path`, `file_format`, `record_count`, `file_size_bytes`, `row_id_start`, `partition_id`, `delete_file_path`. We don't expose `added_snapshot_id` (we have `begin_snapshot` in the catalog, easy to add). | Add `begin_snapshot` (and optionally `end_snapshot`) to `$files`. Trivial; aligns UX with Iceberg. |
| `ST_Union` geo breaking change | **No** — no geospatial | None |
| `ConnectorAccessControl.checkCanXxx` gains `tableBranch` param | **No** — we don't implement custom access control | None |
| `applyFilter` can push down `COALESCE` (#28984) | **Aligned with our pushdown work** | Add `COALESCE` to the function mapping table when we start implementing pushdown. Note in [`TODO-pushdown-duckdb.md`](TODO-pushdown-duckdb.md). |

## Deferred deprecation cleanup (warnings, not errors)

Four `[removal]` warnings remain, all for SPI overloads superseded in 481 but still callable:

- `ConnectorPageSourceProvider.createPageSource(... List<ColumnHandle>, DynamicFilter)` — new overload takes `Optional<ConnectorTableCredentials>` between `table` and `columns`. (`DucklakePageSourceProvider.java:142`)
- `ConnectorPageSinkProvider.createPageSink(... ConnectorOutputTableHandle, ConnectorPageSinkId)`, `... ConnectorInsertTableHandle, ConnectorPageSinkId)`, `createMergeSink(... ConnectorMergeTableHandle, ConnectorPageSinkId)` — `DucklakePageSinkProvider.java:68/78/88`. Likely also gain `tableCredentials` per the 481 access-control changes.

Implement the new overloads before we hit a release that deletes the deprecated ones. Mechanical for the page source; for the page sinks, may also need to thread credentials through `DucklakePageSink` and `DucklakeMergeSink`.

## Doris connector exposure

`:doris-ducklake` pulls Trino SPI transitively but is not part of the green-test target on this branch. Out of scope here; will lag.
