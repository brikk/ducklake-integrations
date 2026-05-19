# Upstream-Tracking Research TODO

Open research questions and proposed action items surfaced by upstream
surveys. Each entry has a stable anchor so the log can link to it.

When the user escalates an item to a real working TODO, move it into
`jvm/trino-ducklake/dev-docs/TODO-WRITE-MODE.md` or `TODO-READ-MODE.md`
and strike it through here with a back-reference.

See [`RESEARCH-HOWTO.md`](RESEARCH-HOWTO.md) for the workflow and
[`RESEARCH-LOG.md`](RESEARCH-LOG.md) for the surveys that produced
these items.

---

## Open

### rename-table-change-chain

**Source:** `ducklake/` PR #1154 ("Keep change-chain when renaming
tables/views"), merged to `v1.5-variegata` 2026-05-13.
**Kind:** research → likely gap
**Impact:** write path; specifically `JdbcDucklakeCatalog.renameTable`.
**Our state:** We already do this correctly for **views** —
`renameView` emits `altered_view:<viewId>` (the spec change-type), and
the per-table `ducklake_schema_versions` row is bumped via
`incrementSchemaVersion()`. See `COMPARE-pg_ducklake.md` C1/C2 closed
items. Whether `renameTable` emits the analogous `altered_table:<id>`
change-record and preserves the `snapshot_changes` chain across the
rename is unverified.
**Proposed next step:** spike (30 min): read `renameTable` in
`JdbcDucklakeCatalog.java`, confirm it writes `altered_table:<id>` to
`ducklake_snapshot.snapshot_changes`, and that `ducklake_table.table_id`
is preserved (not re-allocated). If yes — record as parity in the log
and remove this entry. If no — escalate to working TODO with a test:
DuckDB renames a Trino-created table and reads `table_changes()` on it
without `InvalidInputException` (mirroring the existing view test
`testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges`).

### internal-retry-strategy

**Source:** `ducklake/` PR #1163 ("Fix retrial conflicts") merged
2026-05-15, plus #1150 ("Fix off-by-one error in retry attempts").
**Kind:** research / design
**Impact:** write path concurrency; `JdbcDucklakeCatalog` commit path.
**Our state:** No internal retry loop. On lineage conflict we throw
`DUCKLAKE_TRANSACTION_CONFLICT` and Trino's engine retries the whole
query. Upstream maintains a bounded retry inside `FlushChanges` and
just shipped two bug-fixes against it. The hazards their fixes
address (catalog-ID reuse across attempts, off-by-one on the retry
counter) are real, and would apply to us *if* we ever added internal
retry.
**Proposed next step:** decision spike. Two options:

1. Stay query-level retry — write a short doc note in
   `dev-docs/REPORT_CROSS_ENGINE_WRITE.md` (or similar) explaining
   why we deliberately don't have an internal retry loop, and what
   the implications are (each query retry re-pays planning, but
   avoids the upstream class of bugs).
2. Add bounded internal retry — would shrink wasted planning work on
   conflict-heavy workloads, but requires correctly re-allocating
   catalog-IDs per attempt (the upstream pitfall) and re-checking
   conflict on fresh state. Non-trivial.

User decides which posture is the right one before any code lands.

### uint-type-promotion-audit

**Source:** `ducklake/` PR #1128 ("Fix type promotion for UINTEGER"),
merged 2026-05-07.
**Kind:** research → potential read-side gap
**Impact:** read path; `DucklakeTypeConverter`.
**Our state:** We already map `uint8` → SMALLINT, `uint16` → INTEGER,
`uint32` → BIGINT, `uint64` → DECIMAL(20, 0) — wider-on-read, with the
new `DucklakeUnsignedRangeChecker` validating writes. Upstream just
fixed a type-promotion bug specifically for UINTEGER. Is the bug
something *they* could expose in catalog output we read (e.g. a column
that gets promoted *in the catalog representation* under schema
evolution, which we then mis-decode)? Or is it purely an in-engine
DuckDB execution issue?
**Proposed next step:** read the PR #1128 description + the test
(`vendor/ducklake/test/sql/alter/...`) for ~10 min. If the fix is purely
in DuckDB's expression execution (not in the catalog schema), record
parity. If the fix is in how `ducklake_column.column_type` is written
or how `ducklake_data_file` schema annotations resolve, we need a
corresponding fix on our read side.

### schema-evolution-missing-column

**Source:** `ducklake/` PR #1142 ("Fix missing column in schema
evolution"), merged 2026-05-11.
**Kind:** research → potential read-side gap
**Impact:** read path; how we project newly-added columns over old
data files.
**Our state:** We resolve columns by ID via `ducklake_column` and
project against the active snapshot's schema. A column added *after*
a data file was written should read as NULL (or the configured
default) for that file. Whether we currently do this correctly for
all the cases #1142 covers is unverified.
**Proposed next step:** spike (1h). Read PR #1142 + its test case.
Build the same scenario in our cross-engine test
(`TestDucklakeCrossEngineCompatibility`): DuckDB inserts, `ALTER
TABLE ADD COLUMN`, DuckDB inserts more, Trino reads — verify the
old-file rows project the new column as NULL/default and the
new-file rows project the inserted value.

### disallow-drop-sorted-column

**Source:** `ducklake/` PR #1112 ("Disallow dropping sorted columns"),
merged 2026-05-05.
**Kind:** gap, pending — only relevant after sorted-table writes land
**Impact:** write path; DDL.
**Our state:** We don't support sorted-table writes yet (it's in the
TODO-WRITE-MODE.md priority list). When we do, the DDL path needs to
reject `DROP COLUMN` for any column that's part of the sort key.
**Proposed next step:** track. When sorted-table writes are picked up,
add a checklist item to that section: "reject DROP COLUMN if column
is in `ducklake_table_column_sort` (or whichever sort-key catalog
table the spec defines)".

### delete-file-filter-pushdown

**Source:** `ducklake/` commits `df1f8dee` ("Add expression comparison
filter in delete") + `5b2b7f52` ("If an expression is a
BOUND_COMPARISON with a constant or a BOUND_OPERATOR with a compare_in
we push it down").
**Kind:** research → possible optimization
**Impact:** read path; specifically delete-file scan.
**Our state:** We read delete files as `(file_path, pos)` tuples,
build a per-data-file position set, and filter in the Parquet reader.
We don't push predicates *into* the delete-file scan itself.
**Proposed next step:** scoping spike. Look at Trino's
`ConnectorPageSource` filter-pushdown surface to see if we can hand a
`TupleDomain` to the delete-file Parquet reader the same way we do for
data files. If yes, the win is reading less of the delete-file when
the query has a narrow filter (e.g. a single `pos` value in a MERGE
plan). If no, record as "not actionable on Trino's API surface" and
keep an eye on Doris's analogous API.

### quack-hardening-watch

**Source:** `ducklake/` PR #1151 ("Experimental"), plus #1159
("Fix ducklake option segfault" on the quack branch), 2026-05-12 →
2026-05-15.
**Kind:** watch
**Impact:** catalog backend; cross-cutting.
**Our state:** Quack listed as priority 1 in `TODO-WRITE-MODE.md`.
Upstream label is still "Experimental" and there is at least one
post-merge bug fix on the quack branch.
**Proposed next step:** check on each refresh: (1) is the
"Experimental" label dropped from upstream docs / PRs? (2) is there a
DuckDB version cut where Quack is documented as stable on
`ducklake-web/docs/stable/`? (3) does the DuckDB JDBC driver
documentation describe how to point a local DuckDB at a Quack server?
Only commit to a hard cross-engine CI dependency once these three are
green; before then, keep the work in a development branch.

## Closed (escalated to working TODO)

*(none yet)*
