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

If a DuckDB user enables `write_deletion_vectors=true`, our connector will encounter
`puffin` delete files and silently produce wrong results. Tracked under
[TODO-READ-MODE § Puffin Deletion Vector Reads](TODO-READ-MODE.md#puffin-deletion-vector-reads).

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

### 5. Deletion Vectors (Experimental)

**Spec**: Iceberg V3 deletion vectors in Puffin file format (Roaring bitmap),
opt-in via `write_deletion_vectors=true`.

**Impact**: only Parquet positional delete files are supported today. See the
`ducklake_delete_file` section above and the Puffin reuse analysis below for what
implementation will look like; the open work is under
[TODO-READ-MODE § Puffin Deletion Vector Reads](TODO-READ-MODE.md#puffin-deletion-vector-reads).

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

## Existing Puffin Support in Trino (Reuse Analysis)

Reference for the Puffin DV implementation work tracked in
[TODO-READ-MODE](TODO-READ-MODE.md). Both the Iceberg and Delta Lake connectors
already have deletion vector / Roaring bitmap support.

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

- **Both connectors use `org.roaringbitmap:RoaringBitmap`** — we can add the same
  dependency.
- **Iceberg uses Apache Iceberg's Puffin library** (`org.apache.iceberg.puffin.*`)
  for reading/writing Puffin files. DuckLake's Puffin format should be compatible
  since both follow the Iceberg Puffin spec.
- **DeletionVector serialization format**: Iceberg's `DeletionVector.java` handles
  the binary format (magic number `0x64_39_d3_d1`, RoaringBitmap array, CRC32
  checksums). DuckLake's puffin deletion vectors likely use the same format since
  they claim Iceberg V3 compatibility.
- **None of the code is directly reusable** (too tightly coupled to each connector),
  but the patterns and libraries are proven. Our implementation would:
  1. Add `org.roaringbitmap:RoaringBitmap` dependency
  2. Add `org.apache.iceberg:iceberg-core` dependency (or just the puffin subset)
  3. Read puffin blob by offset+size from `TrinoFileSystem`
  4. Deserialize RoaringBitmap using the same format as Iceberg's DeletionVector
  5. Apply bitmap filter in page source (same pattern as our existing parquet
     delete files)
- **Estimated effort**: Medium. Most of the hard work (Puffin format, bitmap format)
  is solved by existing libraries. The integration into our page source pipeline is
  the real work.

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
