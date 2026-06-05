# Unified idiom kit — whole-codebase rationale

One-page rationale for human readers, reconciled across all three Kotlin-ported
modules. Companions in this directory:

- `IdiomKit.kt` — the reconciled manifest of every surviving helper (not compiled;
  the live helpers live in their module source sets, listed below).
- `combined-kit-notes.json` — the catalog+trino combine-pass audit (2026-06-02).
- `kit-notes.json` — the doris-test + doris-main candidate audit trail.

## The bar (applied uniformly to every slice)

Extract a helper **only** when a pattern recurs at **>= 3 distinct call sites
spanning >= 2 files**, the helper is **shorter/clearer** than inline, **and it
has real callers** (no speculative interop surface). Otherwise: write the Kotlin
idiom inline.

## Reconciled verdict by module

| Module | Helpers | Where they live |
|---|---|---|
| `ducklake-catalog` | **none** — all inline | `_idioms/kit-notes.json` (audit only) |
| `trino-ducklake` | **none** — all inline | `_idioms/kit-notes.json` (audit only) |
| `doris-ducklake` | **3** (all wired) | `DorisMainIdiomKit.kt`, `DorisTestIdiomKit.kt` |

There is no single shared helper file because the modules do not depend on each
other (catalog cannot see trino, trino cannot see doris), and the port workflow
forbids cross-module duplication. The "unified kit" is therefore this document +
`IdiomKit.kt` (the manifest) pointing at three per-module source files.

## catalog + trino: everything inline

The combine pass surveyed all **239 ported `.kt` files** in these two modules.
Every candidate helper from the per-slice kits failed the bar:

- **`lowercaseRoot()`** — 17 actual sites, every one already writes
  `lowercase(Locale.ROOT)` directly; both per-slice kits called the direct form
  "idiomatic enough"; zero callers ever migrated. DROP.
- **`safeMessage(e, fallback)`** — only 3 null-safe message sites; 2 are
  subsumed by first-line logic, the third uses `?: return false` flow control
  that does not fit the signature. DROP.
- **`firstLine` / `firstLineOf`** — only 2 sites
  (`InProcessDuckDbExecutor` inline, `QuackDuckDbExecutor`'s private
  `firstLineOrFull`). Below the bar; the project-local private fun is fine.
  DROP.

Net: the unified `IdiomKit.kt` for these modules was deleted at combine time;
the verdict is **write Kotlin idioms inline**. Each module keeps its
`_idioms/kit-notes.json` as the historical record only.

## doris-ducklake: 3 helpers survive

Doris is the one module that earned helpers — its SPI surface is **erased**
(`Object` / `ConnectorTableHandle` handles the plugin must re-narrow) and its
test SPI surface repeats a fixed 5-key properties map.

### `Any?.asDuckLakeHandle<T>()` — doris-main (wired, 3 sites)

`DorisMainIdiomKit.kt`. Reified inline extension that narrows an erased SPI
handle to the concrete `DuckLakeTableHandle`, throwing
`IllegalArgumentException` naming the runtime class on mismatch. Collapses two
byte-identical private copies (in `DuckLakeConnectorMetadata` and
`DuckLakeScanPlanProvider`) and erases the `is X` / `javaClass.name` spelling at
all three call sites, which become `req.table.asDuckLakeHandle<DuckLakeTableHandle>()`.
Kotlin-only by construction (a reified inline generic is not callable from Java);
all call sites are Kotlin, nothing on the Java-facing SPI regresses.

### `isolatedProperties(isolated)` — doris-test (wired, 7 sites)

`DorisTestIdiomKit.kt`. The 5-key `type` + metadata url/user/password + storage
warehouse map every SPI-surface test builds from an `IsolatedCatalog`. Returns
`LinkedHashMap` (not `Map`) so the s3-credentials test can `put` extra keys in
place. `@JvmStatic` retained for cheap future Java-FE reach. 7 call sites across
`DuckLakeScanPlanProviderTest` (6) and `DuckLakeConnectorListingTest` (1).

### `Optional<T>.orFail(message)` — doris-test (wired, 7 sites)

`DorisTestIdiomKit.kt`. `getTableHandle(...).orFail("...")` reads as the
assertion it is. `internal` Kotlin-only sugar (extensions are invisible to
Java); tests are Kotlin-only so no Java entry point is needed. 7 call sites
across `DuckLakeScanPlanProviderTest` (5) and `DuckLakeConnectorListingTest` (2).

## What the reconciliation changed

The doris slices left a fourth helper in the live tree: a `@JvmStatic
require(Optional<T>, String)` static that was the Java-facing twin of `orFail`.
The grep shows **zero callers** — no Java sources exist in `doris-ducklake`, and
every caller is Kotlin using `orFail`. Under the same bar that deleted the entire
catalog/trino kit (drop speculative helpers with no migrated callers), `require`
was **removed** from `DorisTestIdiomKit.kt`. The surviving doris helpers all have
real, counted call sites.

## What stayed dropped in doris (audit summary)

- **`Objects.requireNonNull(x, "x")`** — Java-port residue on already-non-null
  Kotlin params; a kotlin-idiom cleanup, not a shared idiom.
- **`Optional.orElseThrow { ... }` on catalog reads** — 3 sites but divergent
  exception types (`IllegalStateException` vs `IllegalArgumentException`) with
  bespoke messages; a generic helper is no shorter and loses the type at a glance.
- **`normalizeFileFormat` / lowercase-with-default** — 1 file; `ScanRange.toThrift`
  lowercases with opposite semantics (throws vs defaults), not the same helper.
- **double-checked-lock lazy init** — 1 file, divergent bodies (`catalog()` vs
  `getScanPlanProvider`); a `by lazy` rewrite is a kotlin-idiom cleanup.
- **`requireString(props, key)`** — already lives in `DuckLakeConnectorProperties`.
- **`connectWithIsolated { }`** — would hide the `.use {}` resource boundary,
  the single most important line a connector test should show explicitly.
- **`populatedFormatDesc`**, **`name`/`precision` TypeMapping accessors**,
  **`FakeConnectorContext("dl", 1L)` literal** — single-file or trivial.

## Java-interop safety (doris is Java-FE-facing)

`doris-ducklake` is consumed by the Java FE, and `ducklake-catalog` is
doris-Java-facing, so the reconciliation kept the Java surface stable:

- `asDuckLakeHandle` is reified-inline → Kotlin-only by construction; it does not
  add or remove anything on the Java SPI (a Java caller writes `instanceof` + cast).
- `isolatedProperties` stays `@JvmStatic` returning JDK + plugin types.
- `orFail` is deliberately `internal` Kotlin-only sugar (never a Java entry point).
- The removed `require` static was the only Java-facing helper, and it had no
  callers — removing it cannot break a Java consumer.
- catalog + trino expose **no** kit helpers at all, so no Java surface changed there.

## Verification

catalog + trino were verified green at combine time
(`ducklake-catalog` 33 suites / 108 tests; `trino-ducklake` 70 suites / 825
tests — BUILD SUCCESSFUL 2026-06-02). The doris reconciliation only removed a
zero-caller helper; re-run `cd jvm && ./gradlew :doris-ducklake:test` to confirm
the doris suites stay green.
