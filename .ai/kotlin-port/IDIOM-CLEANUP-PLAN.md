# Kotlin Idiom-Cleanup Plan (QUALITY ONLY — no behavior change)

This plan modernizes the ported Kotlin to idiomatic form and collapses tiny sibling
files. **No behavior changes.** All public/SPI/Jackson surfaces are preserved exactly.
Edits are grouped **by file** so each file is touched once, and ordered so that
file-collapse moves and in-file idiom edits to the same file do not conflict.

Apply is **serial, per-unit, gradle-verified** against the green baseline in
[Verification](#5-verification).

---

## 1. Summary

### Files reviewed (with findings)
- `ducklake-catalog/.../DucklakeCatalog.kt`
- `ducklake-catalog/.../InterveningChanges.kt`
- `ducklake-catalog/.../JdbcDucklakeCatalog.kt`
- `trino-ducklake/.../DuckDbArrowStreamFileWriter.kt`
- `trino-ducklake/.../DuckDbExpressionTranslator.kt`
- `trino-ducklake/.../DuckDbFileWriter.kt`
- `trino-ducklake/.../DuckDbWhereClauseTranslator.kt`
- `trino-ducklake/.../DucklakeAddFilesNameMapper.kt`
- `trino-ducklake/.../DucklakeAddFilesProcedure.kt`

### Findings by severity
| Severity | Meaning | Count |
|----------|---------|-------|
| **safe** | Mechanical, behavior-identical — auto-apply | ~95 |
| **review** | Behavior-adjacent, cross-file, or structural — human confirm | ~28 |
| **skip** | MUST NOT change (SPI / Jackson / interop / test seam) | ~9 |

### Interop-annotation drops (separate INTEROP sweep)
- **35 files** flagged `safeToDropAll: true` — `@JvmStatic` / `@JvmField` / `@JvmRecord`
  removals where no Java caller exists. The whole `trino-ducklake` module has **no Java
  callers** (its tests are Kotlin; doris depends only on `:ducklake-catalog`). In
  `ducklake-catalog`, only `JdbcDucklakeCatalog` is touched by doris Java, and only via
  its public constructor + unannotated instance methods — never the annotated members.

### Collapse groups
- **4 groups** (3 `safe`, 1 `review`), all in `ducklake-catalog`, all same-package moves
  (FQNs + binary signatures unchanged). 11 source files → 4 new files; 11 deletes.
- ~22 small catalog/trino value types **kept separate** (see [§4](#4-explicitly-skipped)).

---

## 2. Apply order

Work units are ordered so collapse moves (which delete/relocate whole files) run on
files that have **no** in-file idiom edits queued, and vice versa. None of the 4 collapse
groups overlaps with the 9 idiom-edited files, so there is **no file-collapse vs in-file
conflict** in this plan. Collapse groups go first (coarse, structural), then per-file
safe idiom units, then the Needs-review section.

> Note: each unit lists its edits with `[severity]` tags. Apply only `safe` items in the
> automated pass; carry `review` items into [§2.B](#2b-needs-review-apply-after-human-sign-off).

### 2.A Safe units (auto-apply, one gradle verify per unit)

**Unit 1 — Collapse group `CatalogExceptions.kt`** `[safe]`
See [§3](#3-collapse-groups). Pure same-package move of 3 exception classes; verify catalog.

**Unit 2 — Collapse group `SortTypes.kt`** `[safe]`
See [§3]. Move `DucklakeSortKey` + `DucklakeSortDirection` + `DucklakeNullOrder`. Note the
`@JvmStatic fromCatalog`/`fromString` companion factories move intact; per INTEROP they are
also `safeToDropAll` for `DucklakeSortDirection.kt` / `DucklakeNullOrder.kt` — **drop those
`@JvmStatic` in the same unit** while the files are open. Verify catalog.

**Unit 3 — Collapse group `PartitionSpecTypes.kt`** `[safe]`
See [§3]. Move 3 partition data classes; Jackson constructor shapes unchanged. Verify catalog.

**Unit 4 — `InterveningChanges.kt`** `[safe]`
- `[safe interop]` Drop all 16 `@JvmField` (lines 48,51,54,57,60,63,66,69,72,75,78,81,84,87,90) — Kotlin-only callers use property syntax. (INTEROP: `safeToDropAll: true`.)
- `[safe interop]` Drop `@JvmStatic` on `parse` (147) and `parseAll` (184) — companion access from Kotlin is unaffected.
- `[safe strings]` Concatenation → string templates at lines 157, 169, 227, 269, 290, 302, 308, 318, 329, 335 (cause `e` preserved at 335).
- `[skip]` Line 122 `createdNamesInSchema` — keep `LinkedHashSet(nestedMap.keys)` (insertion-ordered defensive snapshot; test at 252 relies on it). Do NOT change.
- `[skip]` Line 41 class visibility — leave; type leaks into `ConflictMatrix.check` public signature.
- Verify catalog.

**Unit 5 — `JdbcDucklakeCatalog.kt`** `[safe]` (the doris-Java-facing catalog impl — keep all SPI signatures)
- `[safe interop]` Drop `@JvmStatic` on companion `of` (963) and `newCatalogUuid` (2187), and `@JvmField` on the private/companion config holder (1032 — but SEE skip below) and the nested `ColumnStatsAccumulator` vars (2076–2079). **Per INTEROP `safeToDropAll: true`: no Java caller references any annotated member.** (Reconciles the REVIEW flags in REVIEW_FINDINGS for 963/2187 — the dedicated INTEROP sweep cleared them.)
  - `[skip]` **EXCEPTION:** line 1032 `@Volatile @JvmField var beforeWriteTransactionAction` is a documented test seam — REVIEW_FINDINGS marks it `skip` (Java tests may assign the field; `@Volatile` gives cross-thread visibility). **Keep both annotations.** (INTEROP's blanket note is overridden here by the explicit per-field skip.)
- `[safe strings]` `String.format("ducklake_inlined_data_%d_%d", …)` → template (965); concatenation → template (1219).
- `[safe null-check]` `java.lang.Boolean.TRUE == x` → `x == true` at 381, 1674, 2409.
- `[safe collections]` `referencedColumnIds` → `flatMap{}.mapTo(HashSet())` (2222); `referencedDataFileIds` → `mapTo(HashSet())` (2232); `touchedDataFileIds` loop → `mapTo(HashSet())` (2022).
- `[safe control-flow]` `OptionalLong` if/else → `?.let{}?:` (944) — keep `OptionalLong` return type.
- `[skip]` Line 193 `getSnapshot` (and all sibling overrides) — keep `Optional<T>` / `java.util.Set` signatures; these match the `DucklakeCatalog` SPI consumed by doris Java.
- Verify catalog.

**Unit 6 — `DuckDbExpressionTranslator.kt`** `[safe]` (trino — no Java callers)
- `[safe interop]` Drop `@JvmField` on `PUSHABLE_FUNCTIONS` (73). (REVIEW_FINDINGS flagged review for an out-of-tree reflective consumer, but INTEROP confirms `safeToDropAll: true` and trino has no Java callers — cleared to safe.)
- `[safe java-factory/collections]` `ArrayList()` → `mutableListOf()` (326, 349); remove `import java.util.ArrayList` (38); `java.util.HashMap()` → `mutableMapOf()` (220).
- `[safe scope-function]` `ifPresent { out.add(it) }` → `ifPresent(out::add)` (335).
- `[safe strings]` `java.lang.Long.toString(x)` → `x.toString()` (419); `"\"" + escaped + "\""` → template (402); `"(" + left + …)` → template (481, 490).
- `[safe other]` `java.lang.Double.isNaN/isInfinite` → `d.isNaN()/d.isInfinite()` (423); `java.lang.Boolean.TRUE == x` → `x == true` (343).
- `[safe null-check]` Redundant parens around cast (412).
- `[safe interop-annotation]` Drop redundant `@org.jetbrains.annotations.Nullable` on already-nullable `session: ConnectorSession?` (325, 371).
- `[safe control-flow]` `if (…) return … return null` chains → `when (type)` / `when (name)` (650 `duckdbTypeName`, 662 `comparisonOperator`); elvis-return bailouts (464 + IS NULL 468–469 + NEGATE 528–529).
- Verify trino (touched suites).

**Unit 7 — `DuckDbFileWriter.kt`** `[safe]` (trino — no Java callers)
- `[safe collections]` `.stream().map(…).toList()` → `.map(…)` (104); `0 until size` → `.indices` (147,168,353,378,390); `.get(i)` → `[i]` (151,169,354,391); `java.util.ArrayList()` → `mutableListOf()` (352); `java.util.ArrayList(n)` → `ArrayList(n)` keep capacity (389); `listOf()` → `emptyList()` (348).
- `[safe strings]` Concatenation → templates (115,139,181,281,291,302,311,321,373); drop no-op `+ ""` (539).
- `[safe other]` `Math.max(0, …)` → `maxOf(0L, …)` (393).
- `[safe control-flow]` Expression-body `getApproximateWrittenBytes` (284).
- `[skip]` Line 423 `log.warn(e, "… %s", localTempFile)` — airlift printf varargs, NOT a template. Leave.
- Verify trino (touched suites).

**Unit 8 — `DucklakeAddFilesNameMapper.kt`** `[safe]` (trino — no Java callers)
- `[safe java-factory]` Remove `import java.util.HashMap`/`LinkedHashMap` (50) once sites converted.
- `[safe collections]` `HashMap()` → `hashMapOf()` (127); `topByName` put-loop → `associateBy { … }` (136); `0 until size`→`.indices` + `.get(i)`→`[i]` (148); `.get`→`[]` map gets (153,202,291,377); `.put`→`[]=` (180,211); `getOrDefault(k, mapOf())` → `[k] ?: emptyMap()` (287,335,376); `.iterator().next()` → `.first()` (341).
- `[safe strings]` `String.format` all-`%s` → templates (161 + the listed siblings 205,215,245,275,298,307,325,337,356,366,372,380,462); concatenation → template (500).
- `[safe control-flow]` statement-`when`+trailing-throw → expression-`when` with `else -> throw` (451); `findRowFieldType` → `firstOrNull{…}?.type` (485); `isNestedType` expression body (494).
- `[skip]` Line 399 `LogicalTypeAnnotationVisitor<Type>` overrides — keep `Optional<Type>` (parquet-lib override contract).
- Verify trino (touched suites).

**Unit 9 — `DucklakeAddFilesProcedure.kt`** `[safe]` (trino — no Java callers)
- `[safe data-class]` Fold pass-through params (`catalog`, `fileSystemFactory`, `typeConverter`, `pathResolver`, `fileFormatDataSourceStats`) into primary-ctor `private val`; delete redundant field re-assignments (83). Keep `parquetReaderOptions` body val (real initializer); keep `pathResolver` even though unused (no API change).
- `[safe strings]` Concatenation → templates (129,133,213,218,265,309).
- `[safe other]` `Math.min(…)` → `minOf(…)` + `getEstimatedSize()` → `estimatedSize` (243).
- `[safe control-flow]` `finally { if (ds!=null) … }` → `ds?.close()` (311); single-expr if/else formatting (146).
- Verify trino (touched suites).

> Notes for `DuckDbArrowStreamFileWriter.kt` and `DuckDbWhereClauseTranslator.kt`: their
> `safe` string/collection edits are listed under their review-bearing units (10, 13) and
> may be applied in the same pass — there is no ordering hazard, but a human reviews the
> file as a whole because each carries `review` items.

### 2.B Needs review (apply after human sign-off)

**Unit 10 — `DuckDbArrowStreamFileWriter.kt`** (mixed)
- `[safe strings]` Concatenation/`String.format` all-`%s` → templates (162,163,196,203,172,209,282,291,303,312,317,333,373,607,967); drop no-op `+ ""` (684).
- `[safe collections]` `.stream().map{}.toList()` → `.map{}` (143); `0 until size` → `.indices` (527); `.get(i)`→`[i]` (232 + siblings 356,391,380,528,541); `ArrayList()` → `mutableListOf()` (354); drop redundant type on `ArrayList(n)` (389); `listOf()` → `emptyList()` (351); expression bodies + `emptyMap()` (504,507,517).
- `[safe control-flow]` Remove rethrow-only try/catch (497).
- `[safe other]` `Math.max(0,…)` → `maxOf(0L,…)` (393).
- `[review strings]` Line 587 `format(Locale.ROOT, "DECIMAL(%d,%d)", …)` → template — removes the only `Locale.ROOT` use; **confirm `java.util.Locale`/`java.lang.String.format` imports are then unused or still needed** (still used? line 172/209 already templated here). Behavior identical for `Int`.
- `[review collections]` Line 228 column-DDL index loop → `joinTo(sql, ", ") { … }` — structural; `buildString`-per-element allocation differs; **verify a test covers `buildCreateTableSql` output**.
- `[review strings]` Line 353 `StringBuilder` in `extractColumnStats` — side-effecting loop mutates `minMaxColumns`; **do not** convert to `buildString` unless untangled. Keep as-is.
- `[review control-flow]` Lines 558/615/743 type-dispatch if-ladders → `when` — large rewrite; **preserve exact branch order + `==`-vs-`is` mix**; same for De Morgan at 359.
- `[review other]` Lines 999/1004 `nanosFloor`/`microsToLocalDateTime` — private `@Suppress("unused")` dead code; **deletion is content change**, confirm with human (also drops `Instant`/`ZoneOffset`/`floorMod` imports).
- `[skip]` Line 676 `formatStatValue` — keep `Optional<String>` (flows into Jackson `DucklakeFileColumnStats.minValue/maxValue`).
- `[skip]` Line 133 `partitionValues: Map<Int, String?>` — already idiomatic; flows into serialized `DucklakeWriteFragment`. Leave.
- `[review interop]` Line 548 companion `log` — reviewed, **no `@Jvm*` present, nothing to drop**.
- Verify trino (touched suites).

**Unit 11 — `DuckDbFileWriter.kt` (review tail)** (the safe items are Unit 7)
- `[review control-flow]` Lines 186/460 `appendValue` / `toDuckDbSqlType` if-ladders → `when` — preserve branch order + `equals`-vs-`is`; trailing `+ type` → `$type`.
- `[review control-flow]` Line 531 `formatStatValue` — **keep `Optional<String>` return** (Jackson `DucklakeFileColumnStats`); only modernize internal control flow; NaN early-empty paths are load-bearing.
- `[review other]` Lines 543/547 `java.lang.Float/Double.isNaN` + `String.valueOf` → `.isNaN()` + `.toString()` — **confirm identical text for persisted stats**.
- Verify trino (touched suites).

**Unit 12 — `DucklakeCatalog.kt`** (SPI interface — coordinate with impl)
- `[skip]` Line 32 + all sibling `Optional<T>` returns and `Optional<…>` params (getSnapshot, getSnapshotAtOrBefore, getSchema, getTable, getTableById, getLatestDataFileFormat, getTableStats, getDataPath, getView, createTable partitionSpec/location) — **MUST stay `Optional`**: consumed by doris Java (`DuckLakeConnectorMetadata.java` etc.) via `.isPresent()/.orElseThrow()`.
- `[review collections]` Line 141 `getNameMaps(mappingIds: java.util.Set<Long>)` → `Set<Long>` — source/binary compatible (erasure), but **must change `JdbcDucklakeCatalog.kt:646` override in lockstep** (out of this file's scope).
- `[review collections]` Line 198 `getInlinedDeletes(...): Map<Long, java.util.Set<Long>>` → `Set<Long>` — **must coordinate with impl at `JdbcDucklakeCatalog.kt:804/822/832`** (`java.util.HashSet` build + `as Map<Long, java.util.Set<Long>>` cast). Read-path-adjacent.
- Verify catalog. (If applied, do it as one combined edit across `DucklakeCatalog.kt` + `JdbcDucklakeCatalog.kt`.)

**Unit 13 — `DuckDbWhereClauseTranslator.kt`** (mixed)
- `[safe strings]` `import java.lang.String.format` + single-`%s` `format(...)` → template (36); remove import.
- `[safe collections]` `String.join(sep, list)` → `joinToString(sep)` (85,135,155); conjuncts `ArrayList`+`ifPresent` loop → `mapNotNull { …orElse(null) }` (78).
- `[safe control-flow]` Lines 218 — `val bd: BigDecimal` + branch-assign → `if`-expression initializer; drop redundant `decimalType` alias (smart-cast).
- `[review java-factory]` Line 124 `literals` loop — has early `return Optional.empty()` short-circuit; **leave the loop** (only cosmetic `ArrayList<String>(n)` allowed).
- `[review control-flow]` Line 196 `formatLiteral` if-chain → `when` — preserve smart-casts (`type is TimestampType && type.isShort()`, `type is DecimalType`) and branch order.
- `[review visibility]` Line 58 `public object` → `internal`; line 264 `formatLiteralOrThrow` public → `internal` (no caller found anywhere — possible dead code for a separate pass). **Confirm KDoc cross-refs / test-module visibility.**
- `[review nullability]` Line 67 `toWhereClause(...): Optional<String>` → `String?` — **requires coordinated edit in `DuckDbSelectSqlBuilder.kt:55–65`** (`isPresent`→`!= null`, `.get()`→value). Mechanical only if both files change together.
- Verify trino (touched suites).

**Cross-cutting review notes carried from REVIEW_FINDINGS**
- `InterveningChanges.kt:325` `parseUnsignedLong` → `toLongOrNull()?:` + `require(...)` — **drops the chained `NumberFormatException` cause**; tests only assert `instanceof IllegalArgumentException` so they pass, but the thrown exception's cause changes. Apply only if the cause is not contractually relied on.
- `DuckDbExpressionTranslator.kt:426` `java.lang.Double.toString(d)` → `d.toString()` — identical, but the emitted SQL literal is **pushdown-load-bearing**; human confirm no representation nuance.
- `DuckDbExpressionTranslator.kt:73/768/316/368` — `@JvmField PUSHABLE_FUNCTIONS` (cleared to safe via INTEROP), `@JvmRecord NameArity`/`MethodPair`/`Extracted` (binary-shape change; trino has no Java callers per INTEROP, but record-ness was a deliberate port choice — human confirm), `translateConjuncts` public→internal (test-module visibility), `translate(...)` keep `Optional<String>` (consumed by `DucklakeMetadata.kt`).
- `DucklakeAddFilesNameMapper.kt` review items: `computeIfAbsent { hashMapOf() }` inside `Optional.ifPresent` block (129), `hivePartitionKeyLower` map→`Set` + two `containsKey`→`in` (140), `pathKey` break-loop → `firstOrNull{}?:` (195), `partitionValuesOut` LinkedHashMap copy → `mapValues{}` keeping `as String` cast (223), `@JvmRecord Result`/`TopLevelMatch` drop (104/117 — trino has no Java callers but binary-shape change).
- `DucklakeAddFilesProcedure.kt` review items: `public class` keyword drop (75 — Guice/Trino entry point, cosmetic), `Optional.isEmpty()`→`isEmpty` / inline unused `schema` local (127), `isPresent()`→`isPresent` (151), `String.format`→template on user-facing error (154).

---

## 3. Collapse groups

All groups are **same-package** moves within `ducklake-catalog/src/dev/brikk/ducklake/catalog/`,
so fully-qualified names and binary signatures are unchanged. For each: create the new file
(license header + the moved declarations, imports merged/deduped), then **delete** the
member source files. `@JvmStatic` companion factories move intact (and, where INTEROP marks
the file `safeToDropAll`, may be stripped in the same unit).

### Group 1 → `CatalogExceptions.kt` — risk: **safe** (Unit 1)
Members (from → declaration):
- `DucklakeException.kt` → `open class DucklakeException`
- `TransactionConflictException.kt` → `open class TransactionConflictException`
- `LogicalConflictException.kt` → `open class LogicalConflictException`

Inheritance chain `LogicalConflictException : TransactionConflictException : DucklakeException`;
tiny (22/38/46 LOC). **Deletes:** the 3 member files.

### Group 2 → `SortTypes.kt` — risk: **safe** (Unit 2)
- `DucklakeSortKey.kt` → `data class DucklakeSortKey`
- `DucklakeSortDirection.kt` → `enum class DucklakeSortDirection`
- `DucklakeNullOrder.kt` → `enum class DucklakeNullOrder`

`DucklakeSortKey` composes the other two (`direction`, `nullOrder`). `@JvmStatic fromCatalog`/`fromString`
companions move intact; per INTEROP both enums are `safeToDropAll: true` — drop those `@JvmStatic`
in this unit. **Deletes:** the 3 member files.

### Group 3 → `PartitionSpecTypes.kt` — risk: **safe** (Unit 3)
- `DucklakePartitionField.kt` → `data class DucklakePartitionField`
- `DucklakePartitionSpec.kt` → `data class DucklakePartitionSpec`
- `PartitionFieldSpec.kt` → `data class PartitionFieldSpec`

`DucklakePartitionSpec` holds `List<DucklakePartitionField>`; `PartitionFieldSpec` is the
create-time analogue with identical BUCKET-arity validation. Jackson ctor shapes unchanged.
`DucklakePartitionTransform` is referenced but **not** moved. **Deletes:** the 3 member files.

### Group 4 → `ColumnStatsTypes.kt` — risk: **review** (apply after sign-off)
- `DucklakeColumnStats.kt` → `data class DucklakeColumnStats`
- `DucklakeFileColumnStats.kt` → `data class DucklakeFileColumnStats`

Sibling per-column stats records (table-aggregate vs per-data-file), same `minValue/maxValue`
`Optional` shape + null-parity compact-ctor init. **Review only because `DucklakeFileColumnStats`
is Jackson-serialized** (`@JsonCreator`/`@JsonProperty`) — a relocation does not change its
serialization shape, but worth a glance. **Deletes:** the 2 member files.

---

## 4. Explicitly skipped

So nobody re-proposes these.

### Optional-must-stay (SPI / Jackson)
- **All `Optional<T>` / `OptionalLong` on `DucklakeCatalog` (and its `JdbcDucklakeCatalog` overrides)** — doris Java consumes `.isPresent()/.orElseThrow()`. Methods: getSnapshot, getSnapshotAtOrBefore, getSchema, getTable(ById), getLatestDataFileFormat, getTableStats, getDataPath, getView, createTable partition/location params.
- **`formatStatValue` returns `Optional<String>`** in `DuckDbArrowStreamFileWriter.kt:676` and `DuckDbFileWriter.kt:531` — flow into Jackson `DucklakeFileColumnStats.minValue/maxValue`.
- **`DuckDbExpressionTranslator.translate(...)` → `Optional<String>`** — consumed by `DucklakeMetadata.kt` `.ifPresent`.
- **`LogicalTypeAnnotationVisitor<Type>` visit overrides → `Optional<Type>`** (`DucklakeAddFilesNameMapper.kt:399`) — parquet-lib override contract.
- **`RowField.getName()` Optional usage** in `findRowFieldType` — Trino SPI; `isPresent/get` retained inside predicate.
- **OPTIONAL inventory:** files flagged `anySpiOrJackson: true` (e.g. `DucklakeMetadata.kt` ×44, `DucklakeSplitManager.kt`, `DucklakePageSourceProvider.kt`, `DucklakeColumn.kt`, `DucklakeWriteFragment.kt`, `DucklakeSplit.kt`, `DucklakeWritableTableHandle.kt`, `DucklakeAlwaysOnSplitAffinityProvider.kt`) keep their `Optional` surfaces. Files with `anySpiOrJackson: false` are eligible for a future `Optional→T?` pass **only when the whole call-graph is internal Kotlin** — out of scope here.

### Interop kept for Java callers / test seams
- **`JdbcDucklakeCatalog.kt:1032` `@Volatile @JvmField var beforeWriteTransactionAction`** — documented test seam; `@JvmField` for field assignment, `@Volatile` for cross-thread visibility. Keep both.
- **`java.util.Set` in `getNameMaps`/`getInlinedDeletes`** — only changeable if interface + impl move together (Unit 12, review).

### Idiom non-edits (reviewed, intentionally left)
- `InterveningChanges.kt:122` keep `LinkedHashSet(nestedMap.keys)` (ordered defensive snapshot; test 252).
- `InterveningChanges.kt:41` class visibility — leaks into `ConflictMatrix.check` public signature.
- `DuckDbFileWriter.kt:423` `log.warn(e, "… %s", …)` — airlift printf varargs, not a template.
- `DuckDbArrowStreamFileWriter.kt:353` `StringBuilder` — side-effecting loop, do not `buildString`.
- `DuckDbArrowStreamFileWriter.kt:133` `Map<Int, String?>` — already idiomatic, serialized fragment.

### Files kept separate (NOT collapsed)
22 small value/entity types deliberately not merged (avoid sprawl-by-smallness): trino enums
`DucklakeExecutionEngine`, `DucklakeMetadataTableType`, `DucklakeTemporalPartitionEncoding`,
and value types `DuckDbTuning`, `LeafStatsTarget`, `DucklakeAddFilesException`; catalog
entity rows / standalone types `DucklakeColumn`, `DucklakeDeleteFragment`,
`DucklakeInlinedDataInfo`, `DucklakeNameMap`, `DucklakeFilePartitionValue`,
`ColumnRangePredicate`, `DucklakeSchema`, `DucklakeTable`, `DucklakeView`,
`DucklakeSnapshot`, `DucklakeSnapshotChange`, `DucklakeTableStats`, `TableColumnSpec`,
`TableLocationSpec`. Each is a distinct concept or top-level entity row, not a value-cluster sibling.

---

## 5. Verification

**Green baseline to preserve** (read on-disk `TEST-*.xml` under each module's
`build/test-results/test/` — **never `gradle clean`**; this plan does not delete prior results):

| Module | Suites | Tests | failures / errors / skipped |
|--------|--------|-------|-----------------------------|
| `ducklake-catalog` | **33** | **110** | 0 / 0 / 0 |
| `trino-ducklake` | **77** | **862** | 0 / 0 / 0 |

> On-disk authoritative counts as of this plan (read 2026-06-03). The memory note's
> "108 / ~853" is an earlier floor; the live artifacts show 110 / 862. Treat the table
> above as the must-not-regress target: **0/0/0 always, suite & test counts must not drop**.

**Apply protocol (serial, per-unit, gradle-verified):**
1. Apply exactly one work unit (one file, or one collapse group).
2. Run the owning module's test task incrementally (no `clean`):
   - catalog units → `:ducklake-catalog:test`
   - trino units → `:trino-ducklake:test` (narrow to the touched suites where the slice config allows; full module is fine for catalog).
3. Re-read the on-disk `TEST-*.xml`; confirm the module's counts match the table and failures/errors/skipped stay `0/0/0`. A drop in suite or test count = regression → revert the unit.
4. Only then proceed to the next unit.
5. Collapse units (1–3, and review group 4) additionally require a full module compile to confirm same-package FQN resolution after the file deletes.
6. `review` and `skip` items are **not** auto-applied; `review` items proceed only after explicit human sign-off, then follow the same per-unit verify.
