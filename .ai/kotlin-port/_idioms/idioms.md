# Doris-test slice idiom kit

One-page rationale for human readers. Companion: `IdiomKit.kt` (the proposed
helpers) and `kit-notes.json` (the candidate audit trail) next to this file.

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
