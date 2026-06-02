# Unified idiom-kit verdict (combine pass)

One-page rationale for human readers. Companion: `combined-kit-notes.json` next
to this file; per-slice kit-notes live in each `_idioms/` subdirectory under the
module sources.

## TL;DR

The combine pass deleted the speculative `IdiomKit.kt` introduced during the
trino-main slice. No helpers survived. Across 239 ported `.kt` files, inline
Kotlin idioms beat every proposed helper on call-site density and readability.

## When to inline vs. write a helper

Write a helper only when **both** are true:

1. The pattern appears at **>= 3 distinct sites** in the ported code.
2. The helper is **shorter or clearer** than the inline form at the call site
   (after accounting for the import line and the helper-package reach).

A single-line one-liner does **not** earn a helper — the inline form is
self-documenting and avoids an import.

## What we evaluated, and why each was dropped

The trino-main slice introduced four helpers and explicitly noted they were
"introduced, not yet rolled out... future PRs can collapse them." The combine
pass is that future PR; the verdict is the opposite — collapse the helpers.

| Helper        | Sites surveyed | Verdict | Reason                                                                    |
|---------------|----------------|---------|---------------------------------------------------------------------------|
| `lowercaseRoot` | 17 inline, 0 calls | DROP | `lowercase(Locale.ROOT)` is the same length, more familiar to Kotlin readers, no extra import. |
| `safeMessage`   | 3 inline, 0 calls  | DROP | One site uses `?: return false`; two are wrapped by first-line logic. No clean (e, fallback) fit. |
| `firstLine`     | 2 inline, 0 calls  | DROP | Below the 3-site bar. `QuackDuckDbExecutor.firstLineOrFull` (private) covers one site; the other is inlined in `InProcessDuckDbExecutor`. |
| `firstLineOf`   | 0 sites            | DROP | Composition convenience for the two above; falls automatically.            |

## What is still in the per-slice kits

Per-slice `kit-notes.json` files record the DURING correctness wins (locale-aware
lowercase, null-safe SQL messages, resource-leak `use{}`/try-rethrow patterns,
retained-size fixes) and the idiom-cleanup batches (`@JvmRecord` data classes,
`requireNonNull` drops, Kotlin collection factories). Those edits are real and
shipped; the unified verdict above is **only about the speculative shared-helper
extraction**, not about any of the in-line correctness/idiom edits.

## Cross-module note

The workflow constraint is explicit: do **not** introduce IdiomKit in
`ducklake-catalog`. Catalog has 1 inline `lowercase(Locale.ROOT)` site and 1
`?: return false` null-safe message site — far below the threshold, and a
cross-module helper would require duplication (catalog cannot depend on
trino-ducklake). The DuckDB executors' first-line logic stays project-local
inside `trino-ducklake`.

## Default for future authors

Default to inline. If three new sites accumulate for the same pattern and the
inline form is starting to drift, that is the moment to extract — at that point
the helper will earn its keep, and this `combined-kit-notes.json` is the entry
to update.
