# Doris idiom kit (test + main slices)

One-page rationale for human readers. Companion: `IdiomKit.kt` (the proposed
helpers) and `kit-notes.json` (the candidate audit trail) next to this file.

The **doris-main** slice (production source) appends one helper to the bottom of
`IdiomKit.kt`; see "## doris-main slice" below. The **doris-test** rationale
follows it unchanged.

> NOTE: the prior **combine pass** already deleted a unified `IdiomKit.kt` and
> recorded its verdict in `combined-kit-notes.json` + the catalog/trino sections
> of this directory's history: *default to inline; extract only at >= 3 distinct
> sites where the helper is shorter/clearer.* This doris-test kit is evaluated
> against exactly that bar.

## TL;DR

The doris-test slice (8 ported `.kt` files under `jvm/doris-ducklake/test`) has
**two** ceremonies dense enough to clear the >= 3-site bar with a readability
win. Both survive into `IdiomKit.kt` as a proposal; everything else is
single-file and stays inline.

## Survey of the 8 ported files

| Pattern | Distinct sites | Files | Verdict |
|---|---|---|---|
| 5-key `isolatedProperties` map from `IsolatedCatalog` | 6 | ScanPlanProvider (5), Listing (1) | **KEEP** |
| `Optional.orElseThrow { AssertionError(msg) }` | 6 | ScanPlanProvider (5), Listing (1*) | **KEEP** |
| `DuckLakeConnectorProvider().create(props, FakeConnectorContext("dl", 1L)).use {}` | 6 | ScanPlanProvider (5), Listing (1) | DROP (see below) |
| `populatedFormatDesc(range)` via `TTableFormatFileDesc()`+`TFileRangeDesc()` | 3 | ThriftParity (own helper), ScanPlanProvider (2 inline) | DROP |
| `name(dlType)` / `precision(dlType)` accessors | many | TypeMapping only | DROP (1 file) |
| `FakeConnectorContext("dl", 1L)` literal | 6 | ScanPlan, Listing, Capabilities | borderline — folded into helpers, not its own |

\* Listing also has one `getTableHandle(...).orElseThrow { AssertionError(...) }`
for `analytics.events`, so the orElseThrow pattern is genuinely 2-file.

## The two that survive

### `isolatedProperties(isolated)` -> `LinkedHashMap<String,String>`

The same 6-line `mapOf("type" to "ducklake", METADATA_URL to isolated.jdbcUrl(),
METADATA_USER to ..., METADATA_PASSWORD to ..., STORAGE_WAREHOUSE to
isolated.dataDir().toAbsolutePath().toString())` is copy-pasted at 6 sites. One
of those sites (the s3-credentials test) already uses a mutable
`LinkedHashMap` and then `put`s extra keys — so the helper returns
`LinkedHashMap` (not `Map`) to let that site keep mutating in place. This is the
strongest candidate in the slice: 6 sites, 2 files, 5 lines collapsed to 1.

### `require(optional, msg)` / `Optional<T>.orFail(msg)`

`metadata.getTableHandle(...).orElseThrow { AssertionError("expected X handle") }`
appears 6 times. Reading it as `metadata.getTableHandle(...).orFail("expected X
handle")` states the intent (assert-or-fail) instead of spelling out the
throw-lambda each time. The `@JvmStatic require` is the Java-interop-safe form;
the `orFail` extension is Kotlin sugar (extensions are invisible to Java).

## What was dropped, and why

- **`connectWithIsolated { connector -> }`** (provider.create + use): tempting,
  but the `create(...).use { connector -> ... }` body varies a lot (some tests
  pull `getMetadata`, some `getScanPlanProvider`, some both) and the
  `FakeConnectorContext("dl", 1L)` is trivial. Wrapping it in a higher-order
  helper hides the `.use {}` resource boundary — the single most important
  line for a connector test to show explicitly. Folding the *properties* into
  `isolatedProperties` already removes the bulk of the duplication; the
  remaining `create(props, ctx).use {}` reads fine inline. **DROP.**
- **`populatedFormatDesc`**: `ThriftParity` already has a private file-local
  `populatedFormatDesc`; `ScanPlanProvider` inlines the
  `TTableFormatFileDesc()`+`TFileRangeDesc()`+`populateRangeParams` twice for a
  slightly different purpose (it reads `formatDesc.icebergParams`, not a golden
  diff). 3 sites but split across two intents and one file already owns it.
  Below a clean extraction. **DROP.**
- **`name`/`precision` in TypeMapping**: single file, already private one-liners.
  Promoting to a cross-file kit would add an import for zero other callers.
  **DROP.**

## Java-interop safety

`IdiomKit.kt` carries `@file:JvmName("DorisTestIdiomKit")` and `@JvmStatic` on
both public helpers; both take/return JDK + plugin types (`Optional`,
`LinkedHashMap`, `String`, `IsolatedCatalog`). The `orFail` extension is
deliberately `internal` Kotlin-only sugar and is not part of the Java surface.

## Rollout

Not applied to call sites in this step (kit artifact only — same convention the
slice workflow uses before the combine/apply pass). If wired in, the file moves
to the test source set `jvm/doris-ducklake/test/.../doris/plugin/` (same package
as callers; no import churn, `internal` connector types stay reachable). The
combine pass makes the final keep/drop call across all slices.

---

## doris-main slice

Surveyed the **13 ported `jvm/doris-ducklake/src/.../doris/plugin` files**
against the same bar: extract only at >= 3 distinct call sites spanning >= 2
files where the helper is shorter/clearer. Exactly **one** ceremony clears it.

### Survey of the 13 ported files

| Pattern | Distinct sites | Files | Verdict |
|---|---|---|---|
| `asDuckLakeHandle(...)` cast-or-throw (`is X` / `javaClass.name`) | 3 | ConnectorMetadata (2), ScanPlanProvider (1) | **KEEP** |
| `Objects.requireNonNull(x, "x")` on non-null Kotlin params | many | ScanRange, PositionDelete, MvccSnapshot, ScanPlanProvider | DROP |
| `Optional.orElseThrow { IllegalState/ArgException(...) }` on catalog reads | 3 | ScanPlanProvider (2), ConnectorMetadata (1) | DROP |
| `normalizeFileFormat` / lowercase-with-default | 2 (1 file) | ScanPlanProvider | DROP |
| `requireString(props, key)` | 4 | Connector | already extracted in `DuckLakeConnectorProperties` |
| double-checked-lock lazy field (`catalog()` / `getScanPlanProvider`) | 2 | Connector only | DROP |

### The one that survives: `Any?.asDuckLakeHandle(): T`

Doris hands every per-table SPI call an **erased** handle —
`ConnectorScanRequest.table` is `Object`, the metadata SPI passes
`ConnectorTableHandle`. The plugin re-narrows it to its concrete
`DuckLakeTableHandle`, throwing `IllegalArgumentException` naming the actual
runtime class on mismatch. The port carries **two byte-identical private
`asDuckLakeHandle(...)` copies** (in `DuckLakeConnectorMetadata` and
`DuckLakeScanPlanProvider`) differing only in the static param type
(`ConnectorTableHandle` vs `Any?`).

A single `internal inline fun <reified T : Any> Any?.asDuckLakeHandle(): T`
collapses both copies and erases the `is X` + `javaClass.name` spelling at all
three call sites, which become `req.table.asDuckLakeHandle()` /
`tableHandle.asDuckLakeHandle()` (target type inferred — no `.class`/`KClass`
token). This is the textbook reified-helper win: a `.class`-token-free cast that
de-duplicates across two files. **KEEP.**

### What was dropped, and why

- **`Objects.requireNonNull(x, "x")`**: Java-port residue. The params it guards
  are already non-null in the Kotlin signatures; stripping it is a
  *kotlin-idiom cleanup* (the `kotlin-idiom-review` skill's job), not a
  cross-file shared idiom. Wrapping it in a helper would *preserve* dead
  ceremony. **DROP.**
- **`Optional.orElseThrow { ... }` on catalog reads**: 3 sites across 2 files,
  so it clears the count, but each throws a *different* exception type
  (`IllegalStateException` in ScanPlanProvider, `IllegalArgumentException` in
  Metadata) with a bespoke multi-line interpolated message. A generic
  `orElseThrow` helper would need both the exception factory and the message
  passed in — no shorter than the inline lambda, and it loses the at-a-glance
  exception type. **DROP.**
- **`normalizeFileFormat`**: lives in one file (`ScanPlanProvider`, 2 internal
  uses). `DuckLakeScanRange.toThrift` also lowercases a format string but with
  *opposite* semantics (throws on non-parquet instead of defaulting), so it is
  not the same helper. Single-file + divergent shape. **DROP.**
- **double-checked-lock lazy init** (`Connector.catalog()` /
  `getScanPlanProvider`): two instances, but both in one file, and the bodies
  differ (one builds a catalog, the other a scan-plan provider). Single-file
  control-flow; a generic DCL helper would obscure the `@Volatile` field it
  guards. **DROP** (a `by lazy` rewrite is a kotlin-idiom cleanup, not a kit).

### Java-interop safety

`asDuckLakeHandle` is a **reified inline** extension — Kotlin-only by
construction (reification cannot be expressed in Java bytecode as a callable
generic). All three ported call sites are Kotlin, so this is the natural
surface. A Java caller needing the same narrowing would write its own
`instanceof` + cast; nothing in the Java surface regresses. The pre-existing
test-slice helpers keep their `@JvmStatic` Java entry points.

### Rollout

Not applied to call sites in this step (kit artifact only, same as the test
slice). If wired in, the helper lives in the production source set
`jvm/doris-ducklake/src/.../doris/plugin/` (same package; `internal` handle
types stay reachable, no import churn) and the two private `asDuckLakeHandle`
copies are deleted. The combine pass makes the final keep/drop call.
