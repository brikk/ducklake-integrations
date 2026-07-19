# Strategic / cross-cutting items (slim successor to the driving list)

The original **driving list** (Jayson's 2026-06-12 completeness sweep — the F/T/H groups) has
served its purpose: essentially everything on it shipped. The full, verbatim record — every group
with its done-notes — is archived at
[`archive/TODO-jayson-special-list-COMPLETED-2026-07.md`](archive/TODO-jayson-special-list-COMPLETED-2026-07.md).

This file is the slim successor: only the items that DON'T reduce to a read/write checkbox —
strategic north-stars, cross-cutting threads, and standing tech-debt. Everyday feature work lives
in [TODO-READ-MODE.md](TODO-READ-MODE.md) / [TODO-WRITE-MODE.md](TODO-WRITE-MODE.md); upstream
risk-watches in [TODO-uhoh.md](TODO-uhoh.md).

## Still live — strategic / cross-cutting

- [ ] **Index lifecycle — index DEFINITION on the table, generic across non-parquet formats (F3).**
  The design north-star: "this table always wants that index for new data," with auto-index /
  index-on-write / maintenance semantics, planned so duckdb (ART), lance, and future formats plug
  into ONE surface — not a lance one-off. Catalog representation, DDL/procedure surface, and *when*
  indexing runs are the open design questions. **Blocked** on (a) a direction decision — 4 options
  framed in [RESEARCH-lance-index-lifecycle.md](RESEARCH-lance-index-lifecycle.md) — and (b) upstream
  lance, which today has NO index-creation function (indexes must be built externally and arrive via
  `add_files`; only `__lance_optimize_index` / compaction ops are callable, and those mutate the
  dataset in place → snapshot-safety question). Re-probe on extension bumps (`TestLanceExtensionCanary`
  is the trip-wire). No connector action possible until (a) or (b) moves.

- [ ] **Decompose the monoliths (H3).** `DucklakeMetadata` and `DucklakePageSourceProvider` are each
  1,000+ lines of multi-concern code (and grew again in 2026-07 with SET TYPE / setFieldType /
  promoted-column reshaping). Plan-first refactor: extract per-format page-source construction,
  metadata-table handling, stats extraction, and upload/cleanup behind the existing SPI entry points
  via delegation. Match the genre where the SPI forces the shape; delegate where it doesn't.

- [ ] **Retire the in-process DuckDB engine → Quack-only is the destination.** Standing direction
  (spans read + write + CI): once the Quack (out-of-process DuckDB sidecar) backend is safe/mature,
  drop the in-process executor. Ties into the two CI follow-ups below and the server-side-commit
  trajectory watch in [TODO-uhoh.md](TODO-uhoh.md).

- [ ] **Quack backend maturation** — the active cross-cutting migration thread; remaining boxes are
  mostly verification/CI rather than construction. Read/write specifics live in the respective
  trackers; the CI enablement is tracked in [TODO-WRITE-MODE.md § CI Follow-ups](TODO-WRITE-MODE.md#ci-follow-ups-github-actions)
  (glibc-portable parity-extension build for the Quack container + the one CI-unstable orphan-dir test).

## Standing hygiene (low-risk, opportunistic)

- [ ] **Kotlinization candidate list (H2).** Convert the remaining Java-accent shapes where SPI/Jackson
  is NOT forcing them: accessor-method classes (`ExecutionRequest` → data class w/ default args is the
  poster child), stray `@get:JvmName`/`@JvmRecord` on never-serialized internal types, telescoping
  constructors → default args, manual equals/hashCode → data classes. Excluded by rule: Trino SPI
  signatures, the `@JvmRecord` Jackson wire DTOs (load-bearing for the module-less mapper), airlift
  `DucklakeConfig`.

## Where the rest of the driving list went (collapsed 2026-07-08)

Done and closed (see the archived record for details): **F1** DDL gaps (RENAME/COMMENT/TRUNCATE/
ANALYZE/nested ADD·DROP FIELD, and **SET TYPE** + nested SET TYPE 2026-07-08), **F2** vortex + hive
`add_files`, **F4** non-parquet row-level CRUD (gate kept, documented), **F5** partitioned non-parquet
writes, **F6** all maintenance ops, **F7** puffin-delete writes + **sorted writes** (2026-07-08),
**F9** change feed, **H1** archive sweep, **H4** LikePattern (caged). Collapsed into trackers:

| Driving-list item | Now tracked in |
|---|---|
| F2 `.db` (duckdb-format) `add_files` registration | [TODO-WRITE-MODE.md § `add_files` Follow-Ups](TODO-WRITE-MODE.md#add_files-follow-ups) |
| F8 degraded types (json / interval / uint128) + F10 Variant | [TODO-READ-MODE.md § Type-Support Improvements](TODO-READ-MODE.md#type-support-improvements) |
| F11 lance/vortex extension canaries (ROW/MAP/FTS gates, `record_count` scan cost) | [TODO-uhoh.md § Upstream lance/vortex extension canaries](TODO-uhoh.md) |
| F11 pushdown Step 5 (DuckDB-exclusive fns), arithmetic/concat/position translation | [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md) |
| F11 catalog backends (MySQL landed; SQLite/Turso deferred), vortex scalar+ type audit | [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md) / TODO-vortex |
| F11 cross-dialect view transpilation, `rowid`/`snapshot_id` virtual aliases | [TODO-READ-MODE.md](TODO-READ-MODE.md) |
