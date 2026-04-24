# DuckLake 1.0 Impact Assessment

Date: 2026-04-23

## Summary

DuckLake 1.0 was released on 2026-04-13 alongside DuckDB 1.5.2. The spec version jumped from
0.3 to 1.0, signaling a stable baseline with backward-compatibility guarantees. This document
assesses the impact on our Trino DuckLake connector.

**Bottom line**: Our connector compiles and passes all 429 tests against DuckDB 1.5.2 with zero
failures. The catalog schema changes are backward-compatible additions. No existing functionality
is broken. Several new features are available for future implementation.

## DuckDB Version Upgrade

| Item | Before | After |
|------|--------|-------|
| DuckDB JDBC | 1.5.1.0 | 1.5.2.0 |
| DuckLake spec | 0.3 | 1.0 |
| Test results | 429 pass, 0 fail | 429 pass, 0 fail |

The upgrade is a drop-in replacement. No code changes were needed beyond the version bump in
`pom.xml`.

## Catalog Schema Changes

These are additive changes to the metadata tables our connector queries. None break existing
queries since they add new columns with nullable/default semantics.

### ducklake_column — 2 new columns

| Column | Type | Purpose |
|--------|------|---------|
| `default_value_type` | VARCHAR | `literal` or `expression` — classifies the default value |
| `default_value_dialect` | VARCHAR | Dialect for expression defaults (e.g., `duckdb`) |

**Impact**: None immediate. Our connector already writes `default_value='NULL'`,
`default_value_type='literal'`, `default_value_dialect='duckdb'` for cross-engine compatibility.
These columns were already in the schema before 1.0 but are now formally specified.

### ducklake_schema_versions — 1 new column

| Column | Type | Purpose |
|--------|------|---------|
| `table_id` | BIGINT | Tracks schema changes per table (not just globally) |

**Impact**: None. We already read and write `table_id` in `ducklake_schema_versions` for
cross-engine compatibility. This formalizes what was already practice.

### ducklake_name_mapping — 1 new column

| Column | Type | Purpose |
|--------|------|---------|
| `is_partition` | BOOLEAN | Marks columns used for hive-partitioning |

**Impact**: Low. We don't currently read `ducklake_name_mapping` for partition detection; we use
`ducklake_partition_info` + `ducklake_partition_column`. No action needed unless we want to write
this flag for completeness.

### ducklake_snapshot — type change

| Column | Before | After |
|--------|--------|-------|
| `snapshot_time` | TIMESTAMP | TIMESTAMPTZ |

**Impact**: Low. PostgreSQL will return timezone-aware timestamps. Our `getSnapshotTimestamp()`
reads this as a `Timestamp` via JDBC, which handles both transparently. Verify if any time-travel
comparisons need timezone awareness.

### ducklake_delete_file — format expansion + new column

| Change | Detail |
|--------|--------|
| `format` values | Was: only `parquet`. Now: `parquet` or `puffin` (experimental) |
| New column: `partial_max` | BIGINT — max snapshot_id in partial deletion files |

**Impact**: Medium (future). Our connector only writes and reads `parquet` delete files. If a
DuckDB user enables `write_deletion_vectors=true` and creates `puffin` delete files, our
connector will encounter them. We need to either:
- (a) Handle puffin deletion vectors (Roaring bitmap format), or
- (b) Gracefully reject/skip puffin delete files with a clear error message

**Recommendation**: Add a guard in `DucklakeSplitManager` or `DucklakePageSourceProvider` that
checks delete file format and throws `TrinoException` with a clear message if `puffin` is
encountered, rather than silently producing wrong results.

## New Features — Not Yet Implemented

### 1. Bucket Partitioning

**Spec addition**: `bucket(N)` partition transform using `(murmur3_32(v) & INT_MAX) % N`.

```sql
ALTER TABLE events SET PARTITIONED BY (bucket(8, user_name));
```

**Current state**: `DucklakePartitionTransform` enum has: IDENTITY, YEAR, MONTH, DAY, HOUR.
No BUCKET transform.

**Impact**: If DuckDB creates a bucket-partitioned table, our connector will fail to parse the
partition spec (unknown transform). This would surface as a catalog error when listing the table.

**Work needed**:
- Add `BUCKET` to `DucklakePartitionTransform` with arity parameter
- Implement Murmur3 hash computation in `DucklakePartitionComputer`
- Update `DucklakeTableProperties` to accept `bucket(N, col)` syntax
- Parse the `bucket(N)` transform string from `ducklake_partition_column.transform`

**Effort**: Medium. Murmur3 is available in Guava (`Hashing.murmur3_32_fixed()`). The partition
pruning path needs to apply the hash to filter values.

### 2. Sorted Tables

**Spec addition**: New `ducklake_sort_info` and `ducklake_sort_expression` tables.

```sql
ALTER TABLE events SET SORTED BY (event_time ASC);
```

**Current state**: No support in the connector. No `sorted_by` table property.

**Impact**: Low for reads. Sorted tables are still readable — sorting is a write-time optimization
that produces better-ordered Parquet files. Trino reads them normally. The sort metadata is
ignored but harmless.

**Impact for writes**: Trino-written files won't be sorted according to the table's sort spec.
DuckDB compaction can re-sort them later.

**Work needed** (optional, optimization):
- Read `ducklake_sort_info` / `ducklake_sort_expression` to expose sort order
- Apply sort order during Parquet writes in `DucklakePageSink`
- Expose `sorted_by` table property for DDL

**Effort**: Medium-high. Requires integrating with Trino's Parquet writer sort support.

### 3. Variant Type

**Spec status**: Full variant support with binary encoding, shredded sub-fields, and statistics.

**Current state**: `variant` maps to VARCHAR (degraded). Data is accessible as a string but
without type-specific operations, shredding, or pushdown.

**Impact**: No breakage. Variant columns remain readable as VARCHAR. DuckDB's variant binary
encoding in Parquet files is decoded by reading the raw bytes. However:
- No variant-specific query pushdown
- No shredded field access (e.g., `payload.user`)
- Statistics for shredded variant fields are ignored

**Work needed**: Major effort. Trino doesn't have a native Variant type. Options:
- Keep VARCHAR degradation (current, works)
- Implement JSON-like accessor functions over VARCHAR representation
- Wait for Trino Variant type support (if planned)

### 4. Geometry Type

**Spec status**: Geometry with bounding-box statistics per file.

**Current state**: All geometry types (geometry, point, linestring, polygon, etc.) map to
VARBINARY (degraded).

**Impact**: No breakage. Geometry data is accessible as raw bytes. Bounding-box file statistics
are ignored (no spatial pruning).

**Work needed**: Integration with a spatial library for Trino. Low priority unless spatial queries
are a requirement.

### 5. Deletion Vectors (Experimental)

**Spec status**: Iceberg V3 deletion vectors in Puffin file format (Roaring bitmap). Experimental,
opt-in via `write_deletion_vectors=true`.

**Current state**: Only Parquet positional delete files are supported.

**Impact**: See "ducklake_delete_file" section above. Tables with puffin deletion vectors will
produce incorrect read results if we don't detect and reject them.

**Work needed**:
- Short term: Add format guard to reject puffin delete files with a clear error
- Long term: Implement Roaring bitmap reader for puffin deletion vectors

### 6. New Integer Types (int128, uint128)

**Spec addition**: 128-bit signed and unsigned integers.

**Current state**: Not in `DucklakeTypeConverter`.

**Impact**: Tables with `int128`/`uint128` columns will fail type conversion. These types are
uncommon but should be mapped.

**Work needed**:
- `int128` -> Trino `DECIMAL(38,0)` (fits in 128-bit two's complement, Trino decimal max is 38 digits)
- `uint128` -> Trino `DECIMAL(39,0)` — exceeds Trino's `DECIMAL(38,0)` max. Options: VARCHAR
  degradation, or truncated DECIMAL with range validation.

**Effort**: Low for int128, needs design decision for uint128.

## Configuration Changes

### New DuckLake-specific options (set via DuckDB `set_option()`)

| Option | Default | Purpose |
|--------|---------|---------|
| `sort_on_insert` | true | Sort data during INSERT per table sort spec |
| `write_deletion_vectors` | false | Use puffin deletion vectors instead of parquet delete files |

These are DuckDB-side options. They don't directly affect our connector but change the catalog
data we read.

### Catalog backend changes

- MySQL is no longer recommended. PostgreSQL is the sole recommended multi-user backend.
- The DuckDB extension name changed from `postgresql` to `postgres`.

**Impact**: None for us. We already use PostgreSQL exclusively.

## Spec Clarifications (No Code Impact)

| Area | Change |
|------|--------|
| Type encoding for stats | Formally documented how min/max values are string-encoded for each type |
| Type encoding for inlining | Documented PostgreSQL and SQLite type mappings for inlined data |
| Merge adjacent files | Clarified that only same-schema-version files are merged |
| Geometry type name | `linestring z` (space) renamed to `linestring_z` (underscore) |
| Snapshot changes descriptions | Minor wording fixes (e.g., "ran" vs "run") |
| Documentation links | All changed from `docs/preview/` to `docs/stable/` |

The `linestring z` -> `linestring_z` rename may affect our type converter if we handle that
specific geometry subtype. Check `DucklakeTypeConverter` for the old spelling.

## TODO — Prioritized Work Items

### Phase 1 — Safety guards + easy wins

- [ ] **Bucket partitioning (full implementation)**: Add BUCKET transform to enum, partition
  computer (Murmur3 via Guava `Hashing.murmur3_32_fixed()`), table properties parser
  (`bucket(N, col)` syntax), and partition pruning. Formula: `(murmur3_32(v) & INT_MAX) % N`.
  Iceberg-compatible semantics. ~100 lines across 4-5 files.

- [ ] **int128/uint128 type mapping**: Both are 39 digits — overflow Trino's DECIMAL(38,0).
  Options: VARCHAR degradation (safe, loses numeric ops) or DECIMAL(38,0) with overflow risk.
  Decision pending. Spec notes these types don't have min/max stats.

- [ ] **Fix `linestring z` type name**: DuckLakeTypeConverter needs to accept `linestring_z`
  (underscore) alongside `linestring z` (space). One-line fix.

### Deferred — track for DuckLake v1.1

- [ ] **Puffin deletion vector support**: Currently experimental and opt-in
  (`write_deletion_vectors=true`). Nobody will hit this by accident. Revisit when DuckLake 1.1
  moves DVs out of experimental and adds multi-DV puffin files. At that point: add a format
  guard (reject puffin with clear error) as a quick safety net, then implement full Roaring
  bitmap read support. See "Existing Puffin Support in Trino" section for reuse analysis.

### Phase 2 — Feature parity

- [ ] **Sorted table awareness (read-only)**: Read `ducklake_sort_info` /
  `ducklake_sort_expression` tables. Expose sort order metadata. Not required for correctness
  (sorted tables read fine without it), but enables future optimizations.

- [ ] **Sorted table writes**: Apply table sort spec during Parquet writes in DucklakePageSink.
  Medium-high effort. Trino-written files would then be pre-sorted for DuckDB compaction.

### Phase 3 — Enhanced type support

- [ ] **Variant type improvements**: Evaluate Trino's roadmap for a native Variant type. Consider
  JSON accessor functions as an intermediate step.

- [ ] **Geometry type improvements**: Integrate spatial library for bounding-box statistics and
  spatial pushdown.

## Existing Puffin Support in Trino (Reuse Analysis)

Both the Iceberg and Delta Lake connectors already have deletion vector / Roaring bitmap
support. Key findings:

### Iceberg connector — full Puffin stack

| File | Purpose | Reusable? |
|------|---------|-----------|
| `plugin/trino-iceberg/.../delete/DeletionVector.java` | In-memory DV with RoaringBitmap array | Reference only — coupled to Iceberg internals |
| `plugin/trino-iceberg/.../delete/DefaultDeletionVectorWriter.java` | Writes DVs to Puffin via `org.apache.iceberg.puffin.PuffinWriter` | Reference only |
| `plugin/trino-iceberg/.../IcebergPageSourceProvider.java` (line ~512) | Reads DV blob from Puffin file by offset+size | **Pattern reusable** |
| `plugin/trino-iceberg/.../delete/DeleteFile.java` | `isDeletionVector()`, content offset/size | Reference only |

### Delta Lake connector — separate implementation

| File | Purpose | Reusable? |
|------|---------|-----------|
| `plugin/trino-delta-lake/.../delete/RoaringBitmapArray.java` | Roaring bitmap wrapper | Reference only |
| `plugin/trino-delta-lake/.../delete/DeletionVectors.java` | Reads/writes DV blobs (NOT Puffin format) | Reference only |

### Key takeaways

- **Both connectors use `org.roaringbitmap:RoaringBitmap`** — we can add the same dependency.
- **Iceberg uses Apache Iceberg's Puffin library** (`org.apache.iceberg.puffin.*`) for
  reading/writing Puffin files. DuckLake's Puffin format should be compatible since both
  follow the Iceberg Puffin spec.
- **DeletionVector serialization format**: Iceberg's `DeletionVector.java` handles the binary
  format (magic number `0x64_39_d3_d1`, RoaringBitmap array, CRC32 checksums). DuckLake's
  puffin deletion vectors likely use the same format since they claim Iceberg V3 compatibility.
- **None of the code is directly reusable** (too tightly coupled to each connector), but the
  patterns and libraries are proven. Our implementation would:
  1. Add `org.roaringbitmap:RoaringBitmap` dependency
  2. Add `org.apache.iceberg:iceberg-core` dependency (or just the puffin subset)
  3. Read puffin blob by offset+size from `TrinoFileSystem`
  4. Deserialize RoaringBitmap using the same format as Iceberg's DeletionVector
  5. Apply bitmap filter in page source (same pattern as our existing parquet delete files)
- **Estimated effort**: Medium. Most of the hard work (Puffin format, bitmap format) is solved
  by existing libraries. The integration into our page source pipeline is the real work.

## DuckLake v1.1 Preview

Two features planned for v1.1 (spec changes required):

1. **Variant Inlining**: Currently, variant columns prevent table inlining for non-DuckDB
   catalogs. v1.1 will add spec support for variant inlining in PostgreSQL/SQLite catalogs.

2. **Multi-Deletion Vector Puffin Files**: Store multiple deletion vectors per puffin file to
   preserve time-travel information while reducing small file proliferation.

## DuckLake v2.0 Preview

Larger features under consideration:

- Git-like branching for DuckLake versions
- Permission-based role access control
- Incremental materialized views
