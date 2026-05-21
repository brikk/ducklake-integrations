# DuckLake 1.0 Spec Impact

DuckLake 1.0 was released on 2026-04-13 alongside DuckDB 1.5.2. The spec version
jumped from 0.3 to 1.0 with backward-compatibility guarantees. This document is the
**spec/state reference** for what 1.0 changed and what each change means for our
connector. Actionable work items have moved into
[TODO-READ-MODE.md](TODO-READ-MODE.md) and
[TODO-WRITE-MODE.md](TODO-WRITE-MODE.md); link back here from those when spec
context is useful.

The connector compiles and passes the test suite against DuckDB 1.5.2 with zero
failures. Catalog schema changes are additive; no existing functionality broke.

## DuckDB Version Upgrade

| Item | Before | After |
|------|--------|-------|
| DuckDB JDBC | 1.5.1.0 | 1.5.2.0 |
| DuckLake spec | 0.3 | 1.0 |

Drop-in replacement; no code changes were needed beyond the version bump.

## Catalog Schema Changes

Additive changes to the metadata tables our connector queries. None break existing
queries since they add new columns with nullable/default semantics.

### ducklake_column — 2 new columns

| Column | Type | Purpose |
|--------|------|---------|
| `default_value_type` | VARCHAR | `literal` or `expression` — classifies the default value |
| `default_value_dialect` | VARCHAR | Dialect for expression defaults (e.g., `duckdb`) |

Our connector already writes `default_value_type='literal'` and leaves
`default_value_dialect` SQL NULL for the "no user default" common case. When user-
defined `DEFAULT` expressions ship for Trino-written tables, we'll start writing
`default_value_dialect='trino'` — see TODO-WRITE-MODE for the open work.

### ducklake_schema_versions — 1 new column

| Column | Type | Purpose |
|--------|------|---------|
| `table_id` | BIGINT | Tracks schema changes per table (not just globally) |

We already read and write `table_id` for cross-engine compatibility. 1.0 formalizes
what was already practice.

### ducklake_name_mapping — 1 new column

| Column | Type | Purpose |
|--------|------|---------|
| `is_partition` | BOOLEAN | Marks columns used for hive-partitioning |

We use `ducklake_partition_info` + `ducklake_partition_column` for partition detection
and don't read `ducklake_name_mapping`. No action needed unless we want to write this
flag for completeness.

### ducklake_snapshot — type change

| Column | Before | After |
|--------|--------|-------|
| `snapshot_time` | TIMESTAMP | TIMESTAMPTZ |

PostgreSQL returns timezone-aware timestamps. `getSnapshotTimestamp()` reads via JDBC
which handles both transparently.

### ducklake_delete_file — format expansion + new column

| Change | Detail |
|--------|--------|
| `format` values | Was: only `parquet`. Now: `parquet` or `puffin` (experimental) |
| New column: `partial_max` | BIGINT — max snapshot_id in partial deletion files |

A DuckDB user enabling `write_deletion_vectors=true` produces `puffin` delete files;
those are now read by `DucklakePuffinDeleteReader` (2026-05-20) and applied via the
same `deletedRows` set as parquet positional deletes. `DucklakeSplitManager.validateDeleteFileFormats`
accepts both `parquet` and `puffin`. The `partial_max` column is still not consulted
by either our reader or the Rust reference reader — when a compaction rewrites a delete
file as partial we may mis-attribute deletes to older snapshots. Tracked separately in
[COMPARE-datafusion-ducklake.md § Action items surfaced by this comparison](COMPARE-datafusion-ducklake.md#action-items-surfaced-by-this-comparison).

## New Features in 1.0

### 1. Bucket Partitioning

**Spec**: `bucket(N)` partition transform using `(murmur3_32(v) & INT_MAX) % N`.

```sql
ALTER TABLE events SET PARTITIONED BY (bucket(8, user_name));
```

**Impact**: a DuckDB-created bucket-partitioned table fails to parse on our side
today (unknown transform). Tracked under
[TODO-WRITE-MODE § Bucket Partitioning](TODO-WRITE-MODE.md#bucket-partitioning).

### 2. Sorted Tables

**Spec**: new `ducklake_sort_info` and `ducklake_sort_expression` tables.

```sql
ALTER TABLE events SET SORTED BY (event_time ASC);
```

**Impact**: low for reads — sorted tables read fine; sort metadata is ignored but
harmless. For writes, Trino-written files won't be sorted; DuckDB compaction can
re-sort them later. Tracked under
[TODO-READ-MODE § Sorted-Table Awareness (Read)](TODO-READ-MODE.md#sorted-table-awareness-read)
and [TODO-WRITE-MODE § Sorted Table Writes](TODO-WRITE-MODE.md#sorted-table-writes).

### 3. Variant Type

**Spec**: full variant support with binary encoding, shredded sub-fields, and
statistics.

**Impact**: variant maps to VARCHAR (degraded). Data accessible as a string but
without variant-specific operators, shredded field access, or pushdown. Tracked under
[TODO-READ-MODE § Type-Support Improvements](TODO-READ-MODE.md#type-support-improvements).

### 4. Geometry Type

**Spec**: geometry with bounding-box statistics per file.

**Impact**: geometry types map to VARBINARY (degraded). No spatial pruning. Tracked
under [TODO-READ-MODE § Type-Support Improvements](TODO-READ-MODE.md#type-support-improvements).

### 5. Deletion Vectors (Experimental) — shipped (read path)

**Spec**: Iceberg V3 deletion vectors in Puffin file format (Roaring bitmap),
opt-in via `write_deletion_vectors=true`.

**Impact**: read path landed 2026-05-20 via `DucklakePuffinDeleteReader`. Both
`parquet` and `puffin` delete-file formats are accepted; positions are merged
into the same per-split `deletedRows` set. Cross-engine test
`TestDucklakeCrossEnginePuffinDeleteRoundTrip` proves Trino can read deletes
written by DuckDB with the writer flag enabled.

Write path is *not* changed — Trino-side DELETE/UPDATE/MERGE still emits parquet
positional delete files. That asymmetry is deliberate (no `write_deletion_vectors`
session property added) and noted in the README's "Known Limitations".

Implementation note: DuckLake's `.puffin` file is **not** a real Iceberg Puffin
file — see the "Reuse analysis" section below for the discovered format and why
we did not adopt `iceberg-core`.

### 6. New Integer Types (int128, uint128) — shipped

**Spec**: 128-bit signed and unsigned integers.

Mapping (`DucklakeTypeConverter`):
- `int128` → `DECIMAL(38,0)` (fits in Trino's max-precision decimal except at the
  extremes of int128's ±1.7e38 range).
- `uint128` → `VARCHAR` (DuckLake's `0..3.4e38` range exceeds any Trino decimal
  precision; degraded to text so values round-trip without numeric ops).

Tested in `TestDucklakeTypeConverter` and exercised end-to-end in
`TestDucklakeCrossEngineTypeAudit#testDuckdbHugeintColumnReadsAsDecimalInTrino` /
`testDuckdbUhugeintColumnReadsAsVarcharInTrino`.

## Configuration Changes

### New DuckLake-specific options (set via DuckDB `set_option()`)

| Option | Default | Purpose |
|--------|---------|---------|
| `sort_on_insert` | true | Sort data during INSERT per table sort spec |
| `write_deletion_vectors` | false | Use puffin deletion vectors instead of parquet delete files |

These are DuckDB-side options. They don't directly affect our connector but change
the catalog data we read.

### Catalog backend changes

- MySQL is no longer recommended. PostgreSQL is the sole recommended multi-user
  backend.
- The DuckDB extension name changed from `postgresql` to `postgres`.

No connector impact: we already use PostgreSQL exclusively.

## Spec Clarifications (No Code Impact)

| Area | Change |
|------|--------|
| Type encoding for stats | Formally documented how min/max values are string-encoded for each type |
| Type encoding for inlining | Documented PostgreSQL and SQLite type mappings for inlined data |
| Merge adjacent files | Clarified that only same-schema-version files are merged |
| Geometry type name | `linestring z` (space) renamed to `linestring_z` (underscore) — `DucklakeTypeConverter` accepts both spellings so catalogs written against either spec version read correctly |
| Snapshot changes descriptions | Minor wording fixes (e.g., "ran" vs "run") |
| Documentation links | All changed from `docs/preview/` to `docs/stable/` |

## DuckLake "Puffin" Delete Files: Format Discovery (Implementation Notes)

This section was previously a "reuse analysis" assuming DuckLake's puffin delete
files matched the Iceberg V3 spec. Implementing the reader (2026-05-20) revealed
they don't — capturing the discovered format here for future maintainers.

### What DuckLake actually writes

Per `vendor/ducklake/src/storage/ducklake_delete.cpp` `WriteDeletionVectorFile`,
a DuckLake `.puffin` file contains **only the deletion-vector blob bytes** —
no PFA1 magic, no JSON footer, no blob framing. It's a `.puffin` file in name only.

Blob layout (per `DuckLakeDeletionVectorData::ToBlob` in
`ducklake_deletion_vector.cpp`):

```
[4B BE u32 vector_size]            length of magic+count+bitmaps (excludes vector_size and CRC)
[4B magic 0xD1 0xD3 0x39 0x64]
[8B LE u64 bitmap_count]
for each bitmap:
  [4B LE i32 high_bits]            high 32 bits of the row positions in this group
  [N bytes portable Roaring]       CRoaring portable serialization (LITTLE-endian)
[4B BE u32 CRC32]                  checksum over magic+count+bitmaps
```

Row positions are reconstructed as `(high_bits << 32) | (low & 0xFFFFFFFFL)`.

### Divergence from Iceberg V3

The Iceberg V3 spec says the blob is a single **64-bit** portable Roaring bitmap
(`Roaring64NavigableMap`). DuckLake uses multiple **32-bit** Roarings keyed by
high 32 bits — functionally equivalent but byte-format-incompatible. Iceberg's
puffin lib cannot decode DuckLake puffin files.

### Why we did not adopt `iceberg-core`

Both the divergent blob format and the absent outer puffin container would have
required custom code anyway. The ~150-line `DucklakePuffinDeleteReader` is
cheaper than a transitive `iceberg-core` dependency. Only
`org.roaringbitmap:RoaringBitmap` (1.6.13 transitively from Trino) was added.

### Library gotcha caught

`RoaringBitmap.deserialize(ByteBuffer)` (versions 1.x) internally slices the
input buffer and **does not** advance the original buffer's position. The reader
manually advances by `bitmap.serializedSizeInBytes()` after each call. Without
this, the second bitmap in a multi-group blob fails with `InvalidRoaringFormat`
because the cookie read happens at the wrong offset. Regression-pinned by
`TestDucklakePuffinDeleteReader.testMultipleHighGroupsRoundTrip`.

### Reference points (kept for posterity)

The Iceberg and Delta connectors in Trino both ship RoaringBitmap-based
deletion-vector readers; neither is directly reusable for DuckLake's bespoke
format but their integration patterns informed the page-source dispatch:

- `plugin/trino-iceberg/.../IcebergPageSourceProvider.java` — reads DV blob by
  offset+size from a Puffin file (the actual format, with footer)
- `plugin/trino-delta-lake/.../delete/DeletionVectors.java` — reads Delta-format
  DV blobs (also not Iceberg Puffin)

## DuckLake v1.1 Preview

Two features planned for v1.1 (spec changes required):

1. **Variant Inlining**: today, variant columns prevent table inlining for non-DuckDB
   catalogs. v1.1 will add spec support for variant inlining in PostgreSQL/SQLite
   catalogs.
2. **Multi-Deletion Vector Puffin Files**: store multiple deletion vectors per puffin
   file to preserve time-travel information while reducing small-file proliferation.

## DuckLake v2.0 Preview

Larger features under consideration:

- Git-like branching for DuckLake versions
- Permission-based role access control
- Incremental materialized views
