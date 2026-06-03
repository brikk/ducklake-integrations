# AFTER backlog — slice triage

> Companion to `RECONCILE-SWEEP.md`. The reconciliation sweep confirmed that 50 in-tree
> `TODO(review:after id=…)` markers form the AFTER backlog. Source of truth for marker
> bodies / Java + Kotlin anchors / severity: `after-markers.json` (`markers[]`, keyed by
> the `id` field that also appears in each in-tree comment).
>
> Original adversarial finding text: `adversarial-review-result.json` (`confirmed[]`,
> indexed by `finding_index` in `after-markers.json`).
>
> This document slices the 50 markers into 8 PR-sized branches with effort estimates and
> file-overlap callouts so AFTER work can ship slice-by-slice without merge-cascade risk.

## Phase 1 — Correctness-adjacent (do first)

### Slice 1 — SPI contract gaps  *(5 markers · 5 fixes · 4 files · S/M)*
Playbook's recommended first slice. Each fix is a small override; testing/assertion
work is the bulk.

- `spi-listtables-omits-views` [medium]
- `spi-pagesink-no-memory-usage` [medium]
- `spi-pagesink-no-completed-bytes` [low]
- `spi-rowidinjecting-no-metrics-isblocked` [low]
- `lowtail-completedbytes-output-page-size` [low]

Files: `DucklakeMetadata.kt`, **`DucklakePageSink.kt`**, `DucklakePageSourceProvider.kt`,
**`DuckDbFilePageSource.kt`**

### Slice 2 — HIGH-severity correctness  *(5 markers · 5 fixes · 3 files · M)*
All four HIGH markers, plus the medium ride-along that lives in the same file as the
cache-lock fix. The concurrency fix is the long pole — needs a race test, not just a
diff.

- `correctness-cache-lock-removed-inside-sync` [HIGH]
- `correctness-cache-no-download-size-verify` [medium] *(ride-along: same file)*
- `correctness-delete-footersize-zero` [HIGH]
- `correctness-snapshot-time-npe-message` [HIGH]
- `correctness-timestamptz-precision-truncation` [HIGH]

Files: `DucklakeMaterializedFileCache.kt`, **`JdbcDucklakeCatalog.kt`**, `DuckDbFileWriter.kt`

## Phase 2 — Correctness tail

### Slice 3 — Type/precision write-path  *(3 markers · 3 fixes · 2 files · S)*
Precision regressions on write. Pairs naturally with Slice 2's timestamptz fix; if you
want a single "type-correctness" PR, merge into Slice 2.

- `lowtail-timestamp-precision-silent-coerce` [low]
- `lowtail-time-precision-dropped` [low]
- `correctness-valuecount-includes-nulls` [medium]

Files: `DucklakeTypeConverter.kt`, `DucklakeStatsExtractor.kt`

### Slice 4 — Sinks (PageSink + MergeSink)  *(7 markers · ~5 fixes · 2 files · M)*
Two MergeSink marker pairs each describe one fix site (`resolveDataFileId` site, and
`insert-fragment-roundtrip` site). PageSink portion is the non-SPI half — overlaps
Slice 1's file, do Slice 1 first.

- PageSink: `correctness-rollover-empty-writer` [medium], `eff-appendpartitioned-boxing` [low], `eff-rollover-partition-recompute` [low]
- MergeSink: `eff-resolvedatafileid-{linear-scan,treemap-floor}` (one fix), `eff-insert-fragment-roundtrip{,-finish}` (one fix)

Files: **`DucklakePageSink.kt`**, `DucklakeMergeSink.kt`

### Slice 5 — Parquet / AddFiles / footer-prefetch hardening  *(6 markers · 6 fixes · 4 files · M)*
Parquet-format reasoning is the long pole. Bounds checks + a 2-level LIST path branch +
prefetch tail cap.

- `correctness-parquet-2level-list-cast` [medium]
- `lowtail-mapmap-unguarded-asgrouptype` [medium]
- `lowtail-mapmap-cce-classification` [low]
- `eff-addfiles-footer-double-read` [medium]
- `eff-paritext-prefetch-no-max-cap` [medium]
- `lowtail-paritext-hint-overflow` [low]

Files: `DucklakeParquetTypeUtils.kt`, `DucklakeAddFilesNameMapper.kt`,
`DucklakeAddFilesProcedure.kt`, `FooterPrefetchingParquetDataSource.kt`

### Slice 6 — Pushdown / partition / predicate  *(6 markers · 6 fixes · 5 files · M)*
Coherent theme: every fix is in the pushdown/partition surface. Needs broad parity test
coverage. Overlaps Slice 1 file (`DucklakePageSourceProvider.kt`) — sequence after Slice 1.

- `correctness-identity-partition-spec-mismatch` [medium]
- `correctness-discrete-values-ignores-inclusive` [low]
- `lowtail-parseboolean-1-0-collision` [medium]
- `correctness-transform-pattern-case-sensitive` [medium]
- `lowtail-transform-pattern-greedy-dot-plus` [low]
- `eff-effectivepredicate-filter-list-contains` [low]

Files: **`DucklakeSplitManager.kt`**, `DucklakeBucketPartitionMatcher.kt`,
`DucklakePartitionValueParser.kt`, `DucklakeTableProperties.kt`,
**`DucklakePageSourceProvider.kt`**

### Slice 7 — Inlined data + literal rendering + error classification  *(9 markers · 9 fixes · 8 files · M)*
Loose grab-bag, but each item is small (< 30 LOC) and they touch independent files so
the slice parallelizes well.

- Inlined: `lowtail-inlinedlist-null-token-collision` [low], `lowtail-decimal-halfup-silent-rescale` [low], `eff-fallback-recordcount-materialize` [medium], `eff-inlined-rows-n-plus-1` [low]
- Literal rendering: `lowtail-date-constant-out-of-range` [low], `lowtail-date-timestamp-literal-out-of-range` [low]
- Error classification: `correctness-readpath-not-supported-misclass` [medium], `lowtail-puffin-unchecked-escape` [low], `lowtail-paritext-warn-drops-stack` [low]

Files: `DucklakeInlinedValueConverter.kt`, `DucklakeMetadata.kt`,
**`DucklakeSplitManager.kt`**, `DuckDbExpressionTranslator.kt`,
`DuckDbWhereClauseTranslator.kt`, **`DuckDbFilePageSource.kt`**,
`DucklakePuffinDeleteReader.kt`, **`TrinoParityExtensionResolver.kt`**

## Phase 3 — Efficiency / hardening tail

### Slice 8 — Input safety + efficiency tail  *(8 markers · 8 fixes · 7 files · S/M)*
Mostly local + mechanical. The exception is `eff-snapshot-at-or-before-full-scan` —
it's a real index/query lift and lives in Slice 2's file, so consider riding it along
with Slice 2 instead.

- Quack input safety: `lowtail-quack-host-unescaped` [low], `lowtail-quack-metadatacatalog-unquoted-use` [low], `lowtail-quacktoken-minlen-unenforced` [low]
- Atomic copy: `correctness-paritext-non-atomic-copy` [low]
- Cheap perf wins: `eff-inprocess-detach-overhead` [low], `eff-parquet-footer-double-serialize` [low], `eff-snapshot-timestamp-double-parse` [low]
- `lowtail-sortkey-contiguity-unchecked` [low]
- `eff-snapshot-at-or-before-full-scan` [medium]  *(consider folding into Slice 2)*

Files: `QuackBackedDuckDbCatalogUrl.kt`, `DucklakeConfig.kt`,
**`TrinoParityExtensionResolver.kt`**, `InProcessDuckDbExecutor.kt`,
`ParquetFileWriter.kt`, `DucklakeSessionProperties.kt`,
`DucklakeSortPropertyMapper.kt`, **`JdbcDucklakeCatalog.kt`**

---

## File overlap (sequencing constraints)

| File | Slices | Action |
|------|--------|--------|
| `DucklakePageSink.kt` | 1 + 4 | Slice 1 first |
| `DucklakeSplitManager.kt` | 6 + 7 | Slice 6 first |
| `DuckDbFilePageSource.kt` | 1 + 7 | Slice 1 first |
| `JdbcDucklakeCatalog.kt` | 2 + 8 | Slice 2 first; consider folding `eff-snapshot-at-or-before-full-scan` into Slice 2 |
| `DucklakePageSourceProvider.kt` | 1 + 6 | Trivial; Slice 1 first |
| `TrinoParityExtensionResolver.kt` | 7 + 8 | Slice 7 first |

## Effort tally

- **S** ≈ half day · **M** ≈ 1–2 days · **L** ≈ 3+ days
- Distribution: 1×S/M (Slice 1), 4×M (Slices 2, 5, 6, 7), 1×S (Slice 3), 1×M (Slice 4), 1×S/M (Slice 8)
- Rough total: **8–14 working days** across all slices, assuming standard test coverage per fix.

## Recommended order

1. **Slice 1** — SPI contract gaps  (playbook-suggested first)
2. **Slice 2** — HIGH-severity correctness  (the four `[high]` markers)
3. **Slice 3** — Type/precision write-path  (pairs with Slice 2 if you want them merged)

That stack (3–5 days) lands the "correctness foundation." After that the remaining
five slices can be parallelized — overlaps are minor and most fixes are co-located
within one slice already.

## Conventions for shipping a slice

- One branch per slice off `main`. Branch name suggestion: `after/<slice-number>-<theme>`
  (e.g. `after/1-spi-contracts`).
- Each marker resolved: delete the `TODO(review:after id=…)` line; commit message
  references the `id` in the trailer (e.g. `Resolves: spi-listtables-omits-views`).
- Update `after-markers.json` `markers[]` (remove the entry) and bump `planted` so the
  reconciliation invariant in `RECONCILE-SWEEP.md` continues to hold:
  `planted + skipped_before_fixed + skipped_other == input_findings`.
- Test baseline to preserve (parse on-disk `TEST-*.xml`, not stdout):
  catalog 33 suites / 108 tests / 0-0-0; trino 70 suites / 826 tests / 0-0-0.
  Never `gradle clean`.
