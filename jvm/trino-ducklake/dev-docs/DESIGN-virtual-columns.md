# DESIGN: Virtual Columns (`$path`, `$row_id`, `$snapshot_id`, `$file_row_number`)

**Status:** Design reconciled with the ported Kotlin code; approach decided, no code yet.
**Scope:** trino-ducklake connector. Read path only.
**Companion to:** [TODO-READ-MODE.md § Virtual Columns](TODO-READ-MODE.md#virtual-columns).

> **Reconciliation note (post-port).** This doc was sketched against a
> Java-flavored design. Verified against the current Kotlin module, three
> things changed (details inline): (a) handles use the existing
> sentinel-`columnId` pattern on `DucklakeColumnHandle`, **not** a separate
> handle type — see § 3.4; (b) `$snapshot_id`'s source `DucklakeDataFile`
> doesn't exist — the value is added as a `beginSnapshot` field on
> `DucklakeSplit` at split creation — see § 3.5 / § 4.2; (c) the row-varying
> injection reuses the existing `RowIdInjectingPageSource` (generalized to
> `VirtualColumnInjectingPageSource`), which already computes
> `rowIdStart + position` — see § 4.2.

## 1. Goal

Expose DuckLake's row-lineage metadata as hidden columns Trino users can
reference by name in `SELECT`, `WHERE`, and similar clauses — without
polluting `SELECT *` or `DESCRIBE`. Matches the pattern Trino's Iceberg
and Delta connectors use (`$path`, `$pos`, `$file_modified_time`).

DuckDB exposes the same information through unprefixed names
(`rowid`, `snapshot_id`, `filename`, `file_row_number`, `file_index`).
We expose the high-value subset under `$`-prefixed names; aliases
deferred.

## 2. v1 scope — four columns

| Column | Type | Source | Per-split or per-row? |
|---|---|---|---|
| `$path` | `VARCHAR` | `DucklakeDataFile.path()` via `DucklakePathResolver` | constant per split |
| `$snapshot_id` | `BIGINT` | `DucklakeDataFile.beginSnapshot()` | constant per split |
| `$file_row_number` | `BIGINT` | Parquet's positional metadata column | per row, 0-based |
| `$row_id` | `BIGINT` | `DucklakeSplit.rowIdStart() + $file_row_number` | per row, globally unique within the table |

**Deferred to v2:** `$file_index`, `$file_size_bytes`, `$filename`
(unprefixed alias). The first two largely duplicate the `$files`
metadata table; the third is a naming aesthetic.

## 3. Key design decisions

### 3.1 Naming: `$`-prefix, not DuckDB-style unprefixed

Match Trino's lakehouse convention. Avoids collisions with user columns
named `filename`/`rowid` and signals "this is a connector synthetic
column" to BI tools that already special-case `$`-prefixed columns from
Iceberg/Delta. We can add a DuckDB-name alias mode later if anyone
actually asks for it; reversing the default would be the painful
direction.

### 3.2 `$row_id` is a scalar `BIGINT`, not Iceberg-style ROW

DuckLake's internal row-id contract is already
`rowIdStart + file_row_number` — that's how the parquet-delete merge
path matches rows today. So:

- Encoding matches upstream's semantic contract
- Cheap to compute (one add)
- Globally unique within the table
- Plain `BIGINT` is friendlier in `WHERE` clauses than a ROW

Iceberg uses `ROW(file_path, pos)` because their row ids aren't
globally meaningful across files. Ours are.

### 3.3 SPI mechanism: hidden columns

```java
ColumnMetadata.builder()
    .setName("$path")
    .setType(VARCHAR)
    .setHidden(true)
    .build();
```

Trino's hidden-column contract: not in `SELECT *`, not in `DESCRIBE`,
queryable by explicit name. Iceberg's `$path` is the template.

### 3.4 Handle representation: reserved sentinel `columnId` on `DucklakeColumnHandle`

**Decision (overrides the original sketch's separate-handle proposal).**
Virtual columns reuse the existing `DucklakeColumnHandle` record,
distinguished by a reserved **negative `columnId`** — the same pattern the
MERGE `$row_id` handle already uses (`ROW_ID_COLUMN_ID = -100`). Real
catalog columns always have non-negative ids, so the negative range is
collision-free. A `VirtualKind` enum is the single source of truth for each
virtual's reserved id, name, type, and value-source; a factory builds the
handle from the kind.

```kotlin
enum class VirtualKind(val columnId: Long, val columnName: String, val columnType: Type) {
    PATH(-101, "\$path", VARCHAR),
    SNAPSHOT_ID(-102, "\$snapshot_id", BIGINT),
    FILE_ROW_NUMBER(-103, "\$file_row_number", BIGINT),
    ROW_ID(-104, "\$row_id", BIGINT),  // queryable; distinct id from the MERGE channel's -100
}
```

**Why this over a separate `DucklakeVirtualColumnHandle` type:**

- Smaller blast radius — no `@JsonTypeInfo`/`@JsonSubTypes` polymorphism on
  `DucklakeColumnHandle`, which is Jackson-serialized throughout the split path.
- Matches the `$row_id` sentinel convention already in the code.

**Accepted downside + required mitigation.** Virtual-ness is a magic-number
convention, not a distinct type, so it is **not compiler-enforced**: a
consumer that treats a handle as a real catalog column (catalog lookup,
Parquet field id) will silently mishandle a virtual one if it forgets to
check, and the write-path rejection (§ 4.4) fails *open* rather than at
compile time. Mitigation is mandatory, not optional — centralize everything
behind the enum plus helpers on `DucklakeColumnHandle`:

- `fun virtualKind(): VirtualKind?` — non-null only for virtual handles
- `fun isVirtual(): Boolean`

and route the single write-path guard through `isVirtual()`. Keep the magic
numbers only inside `VirtualKind`. **Escape hatch:** if this convention
causes real bugs later, promote virtuals to a separate
`DucklakeVirtualColumnHandle` type (the original proposal) — a localized
refactor we can take then rather than pay for now.

The queryable `$row_id` (id `-104`, via `getColumnHandles`) and the MERGE
channel's `$row_id` (id `-100`, via `getMergeRowIdColumnHandle`) share the
name and the `rowIdStart + position` encoding but flow through different SPI
hooks; distinct ids keep them separate handles, honoring § 3.6's
"don't conflate."

### 3.5 Inlined-data behavior: NULL for file-bound virtuals

Inlined rows have no parquet file. Options considered:

| Option | Verdict |
|---|---|
| Reject queries that reference virtuals on inlined splits | Too surprising — users can't know in advance which path serves a row |
| Synthesize sentinel values | Lies |
| **Return NULL for file-bound virtuals** | **Pick.** SQL has NULL precisely for "value not applicable here" |

So on inlined splits: `$path` = NULL, `$file_row_number` = NULL,
`$row_id` = NULL; `$snapshot_id` = the inlined data's begin_snapshot
(still meaningful).

### 3.6 Don't conflate with MERGE row identity

The MERGE rewrite already builds an internal `RowIdHandle` at planner
time for matching. Exposing `$row_id` as a queryable hidden column does
NOT replace that — they're separate code paths and we should keep them
that way in v1. Unifying them could happen later as a cleanup.

## 4. Plumbing sketch

### 4.1 Surface (`DucklakeMetadata`)

- `getColumnHandles(session, tableHandle)` — append the four virtual
  handles to the existing column-handle map
- `getColumnMetadata(session, tableHandle, columnHandle)` — when handle
  is `DucklakeVirtualColumnHandle`, return `ColumnMetadata` with
  `setHidden(true)` and the kind's name + type
- `getTableMetadata` — must NOT include virtuals in the visible column
  list; they only appear via `getColumnHandles` lookup

### 4.2 Scan (`DucklakePageSourceProvider`)

- Inspect the requested column list. Partition into
  `(parquetSourceColumns, virtualColumns)`
- Build the inner parquet page source with only `parquetSourceColumns`
- If `$file_row_number` or `$row_id` is requested, ALSO request
  Parquet's positional-metadata column from the inner reader
- Wrap with a `VirtualColumnInjectingPageSource` that, per page:
  - For `$path`, `$snapshot_id` → `RunLengthEncodedBlock(constant, positionCount)`
  - For `$file_row_number` → forward the Parquet positional block
  - For `$row_id` → add `split.rowIdStart()` to each position in the
    positional block

### 4.3 Inlined path

`DucklakeInlinedSplitPageSource` (or wherever inlined rows are served)
emits NULL blocks for `$path` / `$file_row_number` / `$row_id` and the
snapshot constant for `$snapshot_id`. Keep this in one place — don't
sprinkle the NULL logic across pages.

### 4.4 Write path

`DucklakeMetadata.beginInsert` / `beginCreateTable` / `applyMerge`
must reject virtual column handles in the input list with a clear
`NOT_SUPPORTED` ("virtual columns cannot be written"). Add the check
at the entry points, not deep in the page sink.

## 5. Test plan

**Unit:**

- `getColumnHandles` returns the 4 virtuals as hidden, plus all user
  columns as visible
- `DucklakeVirtualColumnHandle` round-trips through Jackson (Trino
  serializes handles for split distribution)
- Write-path entry points reject virtual handles with `NOT_SUPPORTED`

**Cross-engine:**

- Insert a known set of rows; assert `$path` is the resolved file path
  and ends with `.parquet`
- Assert `$file_row_number` runs `0..N-1` per file
- Assert `$row_id` is unique and dense across the table; values match
  `rowIdStart + file_row_number` per split
- Assert `$snapshot_id` equals the snapshot that ran the INSERT
- DELETE half the rows → query `$row_id`; assert the remaining set is
  the original minus the deleted ones (no stale rows leak through)
- Inlined-data table → query `$path`/`$row_id`/`$file_row_number`,
  assert NULL; query `$snapshot_id`, assert non-null
- Confirm `SELECT *` from the table does NOT include virtuals
- Confirm `DESCRIBE` does NOT include virtuals
- Confirm `SELECT $path, COUNT(*) FROM t GROUP BY $path` works (i.e.
  the virtuals plug into the rest of the planner cleanly)

## 6. Estimate

~2 focused days.

- **Day 1**: Surface hidden columns; implement `$path` + `$snapshot_id`
  (constant-per-split) end-to-end with tests.
- **Day 2**: `$file_row_number` + `$row_id` (row-varying, needs the
  Parquet positional-metadata hookup); inlined-data NULL behavior;
  README + IMPACT-doc updates.

## 7. Risks / unknowns

The only non-trivial piece is wiring `$file_row_number` from Trino's
Parquet reader. Iceberg does this and its pattern is the reference.
If that pattern doesn't map cleanly onto `DucklakePageSourceProvider`'s
structure, the degraded fallback is: count positions in the
injecting page source (one add per page). Slower for huge files but
correct, and the failure is local to one component.

## 8. Out of scope (for v1)

- `$file_index`, `$file_size_bytes`, `$filename` — see § 2
- DuckDB-name aliases (`rowid`, `filename`, etc.) — see § 3.1
- Using `$row_id` as the merge row-id source — see § 3.6
- Virtual columns on metadata tables (`$files`, `$snapshots`) — they
  already expose lineage via their own column set
- Predicate pushdown on virtual columns (e.g. `WHERE $path = '...'`
  pruning splits before scan) — a real follow-up, but the
  user-visible feature works without it; pruning is a perf-only layer
  on top
