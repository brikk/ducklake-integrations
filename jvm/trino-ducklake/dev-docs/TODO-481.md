# TODO: Trino 481 upgrade

Source: https://trino.io/docs/current/release/release-481.html

## Must-do for upgrade

1. **Bump versions**: `libs.versions.toml` (`trino = "481"`), `compose/.env` (`TRINO_VERSION=481`, `PLUGIN_VERSION=481-1-ALPHA`).
2. **SPI breaking change — `Type.getObject` / `Type.appendTo` removed.** We use `Type.getObject` in:
   - `DuckDbFileWriter.java:320,332,344`
   - `DucklakeUnsignedRangeChecker.java:278`
   - `DucklakePartitionComputer.java:161,174,261,272`
   - `DuckDbArrowStreamFileWriter.java:908,1002`
   Switch to whatever the 481 replacement is (likely `Type.getObjectValue(...)` or direct block-typed read). Must investigate at upgrade time.
3. **Run full `:trino-ducklake:test`** after the bump; address any other API gaps that surface.

## Items per release-note, mapped to our exposure

| Note | Affects us? | Action |
|---|---|---|
| `connectorMetrics` on QueryInputMetadata | Future opportunity | Could emit cache hits, format breakdown, executor (in-process vs quack) per split. Track as enhancement. |
| Race on dynamic-catalog drop vs `system.jdbc` queries (#28017) | **No** — our catalogs are static config files; no runtime add/drop | None |
| Legacy object storage removed (Azure / GCS / S3 / S3-compat) | **No** — we already use `fs.native-s3.enabled=true` in all catalog props | None; but verify compose files set native flag (currently yes) |
| `variant` type for Iceberg v3 | **Watch** — DuckLake spec doesn't define variant. If upstream DuckLake adopts it (DuckDB has VARIANT in roadmap), we'd need column-type mapping. | Track in research log when DuckLake side moves. |
| Iceberg `timestamp(9)` / `timestamp(9) with time zone` for v3 | **No** — Iceberg-specific. Our connector already supports TIMESTAMP precisions 0/3/6/9 on writes (Arrow stream writer maps to TIMESTAMP_NS for precision 9). | None |
| Iceberg `$files` additions, including `added_snapshot_id` (#29044, #28911) | **Comparison opportunity** — our `$files` (DucklakeMetadata.java:803) has: `data_file_id`, `path`, `file_format`, `record_count`, `file_size_bytes`, `row_id_start`, `partition_id`, `delete_file_path`. We don't expose `added_snapshot_id` (we have `begin_snapshot` in the catalog, easy to add). | Add `begin_snapshot` (and optionally `end_snapshot`) to `$files`. Trivial; aligns the UX with Iceberg. |
| `ST_Union` geo breaking change | **No** — no geospatial | None |
| `ConnectorAccessControl.checkCanXxx` gains `tableBranch` param | **No** — we don't implement custom access control | None |
| `applyFilter` can push down `COALESCE` (#28984) | **Aligned with our pushdown work** | Add `COALESCE` to the function mapping table when we start implementing pushdown. Note in [`TODO-pushdown-duckdb.md`](TODO-pushdown-duckdb.md). |

## Open after upgrade

- Find canonical replacement for `Type.getObject(block, position)`. If the new API is `Type.getObjectValue(...)`, mechanical 8-site rename. If it's a per-type read-method approach, more work.
- Confirm `:doris-ducklake` (separate state, not actively built in this work, but pulls Trino SPI transitively) doesn't break — or note it's out of scope and let it lag.

## Status

- Not started.
