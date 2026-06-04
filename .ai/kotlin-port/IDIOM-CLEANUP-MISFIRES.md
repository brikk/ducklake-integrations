# Kotlin Idiom-Cleanup — Misfire Report

Companion to [`IDIOM-CLEANUP-PLAN.md`](./IDIOM-CLEANUP-PLAN.md). Records the **6 of 53 apply
units** that did not land cleanly, with **what the unit intended vs. what actually happened**,
so the failures can be re-issued correctly in a follow-up pass.

**Final state is green** — every misfire was either backed out or skipped; no baseline damage.

| Module | Suites | Tests | f / e / s |
|--------|--------|-------|-----------|
| ducklake-catalog | 33 | 110 | 0 / 0 / 0 |
| trino-ducklake | 77 | 862 | 0 / 0 / 0 |

## ✅ Resolution (2026-06-04) — all 6 re-issued and landed

All six misfires are now fixed; baseline unchanged (catalog 33/110, trino 77/862, 0/0/0).

- **Already corrected in the `more idiomatic updates` commit** (the data-class recipe was
  re-applied with `val` on the ctor param, exactly as the systemic lesson below prescribes):
  `DucklakePagePartitioner` (fold + `.map{}` + templates), `DucklakeAddFilesProcedure` (L83 fold —
  all five params now `private val`), `DucklakeMergeSink` (L66 fold — all eight params now
  `private val`).
- **Fixed in this follow-up pass:**
  - `SortTypes.kt` (ex-`DucklakeSortDirection`) — dropped the two surviving `@JvmStatic` on the
    enum `fromCatalog` companions (verified no Java caller: doris doesn't reference the sort enums,
    the only callers are Kotlin companion access in `JdbcDucklakeCatalog.kt:605-606`) and applied
    the elvis null-check idiom.
  - `DucklakePathResolver.kt` — folded `catalog`/`configuredDataPath` into the primary ctor as
    `private val`; string templates in `joinPaths`. The malformed L43 `...`-placeholder finding was
    **discarded** as recommended.
  - `DucklakePartitionComputer.kt` — the re-issue: removed the four redundant smart-cast aliases
    (`timestampType`/`tzType` in `extractLocalDateTime` and `murmur3Hash`) and used the smart-cast
    `columnType`/`type` directly; added `import io.trino.spi.type.LongTimestamp` +
    `LongTimestampWithTimeZone` and switched the inline FQNs to the short names. (Two pre-existing
    `.toInt()` "redundant conversion" warnings on the long-timestamp branch are unrelated and left
    untouched — behavior-sensitive, out of scope.)

The detailed per-unit diagnoses below are kept as the historical record of what went wrong.

## At a glance

| Unit | Outcome | Category at fault | Net repo change |
|------|---------|-------------------|-----------------|
| `DucklakePartitionComputer.kt` | **Fully reverted** | (synthesis gap: dangling refs + missing imports) | none — back at HEAD |
| `DucklakePagePartitioner.kt` | **Fully reverted** | `data-class` | none — back at HEAD |
| `DucklakeAddFilesProcedure.kt` | **Partial** — safe edits kept, 1 backed out | `data-class` | 9 edits applied |
| `DucklakeMergeSink.kt` | **Partial** — safe edits kept, 1 backed out | `data-class` | 2 edits applied |
| `DucklakePathResolver.kt` | **Skip-only** — nothing applied | (file drift) | none |
| `DucklakeSortDirection.kt` | **Skip-only** — nothing applied | (file no longer exists) | none |

### The systemic theme
Four of the six failures are the **`data-class` category** — "fold pass-through constructor
params into primary-ctor `private val`." The review agents proposed *removing* the `private val`
backing declarations but did **not** also add `val` to the matching constructor parameters. In
Kotlin a bare (non-`val`) constructor param is in scope only in `init`/property initializers, **not
in member functions** — so every method that referenced those fields stopped resolving. The
verify-or-revert gate caught all four. **Lesson:** the data-class transform is only safe when the
ctor param gains `val` in the same edit; "delete the backing property" alone is never safe.

The other two are not idiom bugs at all — one is **plan-synthesis incompleteness** (an edit set
that needs companion edits outside the SAFE set) and two are **file-drift / file-renamed** no-ops.

---

## 1. `DucklakePartitionComputer.kt` — fully reverted

**Intended:** 13 safe edits. All 13 snippets matched and were applied exactly as written.

**What happened:** the applied set **failed `:trino-ducklake:compileKotlin` with 14 errors**, so
the whole unit was `git checkout --` reverted. The reverted file compiles clean. This was *not*
file drift — every snippet matched; the **pre-approved set is internally inconsistent**:

1. **Dangling references the set didn't rewrite.**
   - The L144 edit deletes `val timestampType: TimestampType = columnType`, but a later
     `timestampType.getLong(block, position)` (orig L146) is left untouched → `Unresolved
     reference 'timestampType'`.
   - Same defect at L159: deletes `val tzType` but leaves `tzType.getLong(...)` (orig L161) →
     `Unresolved reference 'tzType'`.
2. **Short-name switch with no import.** The L152/L165/L246/L257 edits change fully-qualified
   `io.trino.spi.type.LongTimestamp` / `LongTimestampWithTimeZone` to the bare names, but the file
   has **no import** for either and no edit adds one → `Unresolved reference` at lines 149/161/245/254.

**To re-issue correctly**, add to the unit (these are outside the original SAFE set):
- rewrite the dangling `timestampType.getLong` → `columnType.getLong` and `tzType.getLong` → `columnType.getLong`;
- add `import io.trino.spi.type.LongTimestamp` and `import io.trino.spi.type.LongTimestampWithTimeZone`.

---

## 2. `DucklakePagePartitioner.kt` — fully reverted

**Intended:** 4 edits — 1 `data-class` (L36), 1 `collections` (`.map { }`), 2 string templates.
All 4 snippets matched.

**What happened:** **compile failed** on `:trino-ducklake:compileKotlin`; whole unit reverted to
HEAD (original 4-field form).

**Root cause — the `data-class` edit (L36):** it removed the `partitionSpec` and `allColumns`
backing fields, but both are still used in the class body:
- `this.allColumns` at L48/L49/L51 (the `init` column-lookup loop) → `Unresolved reference 'allColumns'`
- `partitionSpec.partitionId` at L120 (`getPartitionId()`) → `Unresolved reference 'partitionSpec'`

The other 3 edits were individually safe but were applied **as a set** with the unsafe one, so
verify-or-revert rolled back all four.

**To re-issue:** split this unit — apply the 3 safe edits standalone; for the data-class fold,
add `val` to the `partitionSpec` / `allColumns` constructor params (or just drop the data-class
edit for this file).

---

## 3. `DucklakeAddFilesProcedure.kt` — partial (ended green)

**Intended:** 9 string/`other`/null-check edits + 1 `data-class` (L83).

**Applied (9, kept):**
- L129/L133/L213/L218/L265/L309/L463 — string templates
- L243 `other` — `Math.min(dataSource.getEstimatedSize(), …)` → `minOf(dataSource.estimatedSize, …)`
- L338 null-check — `byFieldId.get(...)`/`out.put(...)` → indexed `byFieldId[...]` / `out[...] =`

**Skipped (1) — the `data-class` edit (L83):** would remove the `private val` declarations for
`catalog`, `fileSystemFactory`, `typeConverter`, `pathResolver`, `fileFormatDataSourceStats`
(keeping only `parquetReaderOptions`), but the ctor params are plain (not `val`) and the fields are
referenced in method bodies — `catalog` (L121,122,126,137,138,140,184), `fileSystemFactory` (157),
`typeConverter` (255), `fileFormatDataSourceStats` (229). The agent reverted its partial
application of that one edit and kept the rest. **Compile + tests green (77/862, 0/0/0).**

---

## 4. `DucklakeMergeSink.kt` — partial (ended green)

**Intended:** 2 safe edits + 1 `data-class` (L66).

**Applied (2, kept):**
- L109 `collections` — `computeIfAbsent(dataFileId) { _ -> ArrayList() }` → `getOrPut(dataFileId) { mutableListOf() }`
- L143 `strings` — concat → template `"...data file $dataFileId"`

**Skipped (1) — the `data-class` edit (L66):** would delete the property declarations for
`mergeHandle`, `fileSystem`, `deleteFragmentCodec`, `writerOptions`, `parquetReaderOptions`,
`fileFormatDataSourceStats`, `trinoVersion`, `insertSink` (keeping only an inline `dataColumnCount`
val). Those are bare ctor params, yet they're referenced in `storeMergedRows` / `finish` /
`writeDeleteFile` / `createParquetWriter` / `abort` (L88,131,144,168,171,172,186,189,202,204,252,261).
The edit also left the `init` block reassigning the now-inline `dataColumnCount` and referencing
the dropped `mergeHandle`. Applied then reverted in place; the 2 safe edits stayed. **Green.**

---

## 5. `DucklakePathResolver.kt` — skip-only (file drift)

**Intended:** 3 edits. **Applied: none** — file is byte-identical to HEAD. All 3 snippets failed
exact-match against the current file and were **not forced** (correct behavior):
- **L25 `other`** (fold props into ctor header): the class already declares its header on L23 as
  `open class DucklakePathResolver(catalog: DucklakeCatalog, configuredDataPath: String?)`;
  replacing the property lines would create a duplicate/broken declaration.
- **L71 `strings`** (`return parent + child`): indentation mismatch — snippet uses 12-space indent,
  file (L69) uses 16-space. Exact-match fails.
- **L43 `other`** (`resolveTableDataPath(...): String` → expression body): the signature is at L32
  not L43, **and** the replacement value was a literal `...` placeholder, not compilable code.

**Note:** the L43 finding looks malformed at the source (a `...` placeholder as the proposed
snippet) — worth discarding rather than re-targeting.

---

## 6. `DucklakeSortDirection.kt` — skip-only (file no longer exists)

**Intended:** 2 edits (L28 interop-annotation, L29 null-check). **Applied: none.**

**What happened:** the standalone `DucklakeSortDirection.kt` **no longer exists** — it was collapsed
into `SortTypes.kt` earlier in the same run (collapse group 2). Both edits targeted the deleted
file, so they were skipped (only a stale `DucklakeSortDirection.class` remained under `build/`).

**Why it slipped through:** the collapse and the per-file edits came from the *same* cached review
pass, so the apply queue still listed `DucklakeSortDirection.kt` as a target even though an earlier
unit had deleted it. The collapse agent also **preserved annotations verbatim**, so the intended
`@JvmStatic` drops on the sort enums never happened anywhere.

**Residue / to re-issue:** re-target the `@JvmStatic` / null-check drops for
`DucklakeSortDirection` (and `DucklakeNullOrder`) at their new home in `SortTypes.kt` and re-approve
— the surrounding context (companion object, annotation, null-check block) may differ there.

---

## Suggested follow-up pass (all small, all re-approvable)

1. **Fix the data-class transform recipe** so it adds `val` to the ctor param instead of only
   deleting the backing property, then re-run for `DucklakePagePartitioner`, `DucklakeAddFilesProcedure`,
   `DucklakeMergeSink` (L66), and any other file where this category was proposed.
2. **`DucklakePartitionComputer`** — re-issue with the 2 dangling-ref rewrites + 2 imports folded in.
3. **`SortTypes.kt`** — apply the sort-enum `@JvmStatic` / null-check drops at the post-collapse location.
4. **Discard** the malformed `DucklakePathResolver` L43 finding (`...` placeholder).
5. The held **control-flow** category (deliberately skipped this pass) is still pending in the plan.
