# Slice 7 handoff — Inlined data + literal rendering + error classification

You're picking up the **AFTER backlog**, Slice 7 of the DuckLake Kotlin port. Before touching
code, read **`.ai/kotlin-port/AFTER-SLICES.md`** (the slice playbook, §"Slice 7") and the memory
**`project_after_backlog_progress.md`** (where the initiative stands). This handoff is the fast
path; those two are the source of truth.

## State of the tree

- **Slices 1–6 are all merged to `main` and pushed.** `main` == `origin/main` == HEAD **`2fcc48f`**
  (slice 6), linear (fast-forward) history.
- `resolved` = **32 / 50** in `.ai/kotlin-port/after-markers.json`; `len(markers)` = **18**.
- **Branch off `main`:** `git checkout -b after/7-inlined-literals main`. The old hard constraint
  (Slice 6 must precede 7 — `DucklakeSplitManager.kt` overlap) is **already resolved on `main`**, so
  you do *not* stack on `after/6-pushdown`. Just branch off `main`. Slice 8 comes after you (last).

## Context you have

- **Marker registry:** `after-markers.json` (`markers[]`, keyed by `id`, carries `severity` +
  `kotlin_anchor` + `finding_index`). Original finding text: `adversarial-review-result.json`
  (`confirmed[]`, indexed by `finding_index`). Read the `detail`, `suggestion`, and
  `verifierReasoning` for each — the verifier often *corrects the severity down* and tells you the
  real (often narrower) consequence. Don't over-fix beyond what the verifier confirms.
- **The `.java.old` oracle:** every ported file has its pre-port Java sibling renamed
  `<File>.java.old` next to it. For each marker, diff the `.kt` against `.java.old` and **state in
  the commit whether the bug is pre-existing or port-introduced.** Slices 1–6 were *all* pre-existing
  (faithful translation) — expect the same, but verify, don't assume.
- **Commit convention:** `git show 2fcc48f` (slice 6) and `git show 4621c17` (slice 5). Note the
  per-marker `Resolves: <id>` trailers, the "two-markers-collapse-to-one-fix" callout pattern, and
  the pre-existing-vs-port-drift note per file.

## The 9 markers (8 files, est. M — "loose grab-bag," each <30 LOC, independent files)

| marker | sev | fi | site |
|---|---|---|---|
| `eff-fallback-recordcount-materialize` | **med** | 72 | `DucklakeMetadata.kt:436` |
| `correctness-readpath-not-supported-misclass` | **med** | 56 | `DuckDbFilePageSource.kt:120` |
| `lowtail-inlinedlist-null-token-collision` | low | 32 | `DucklakeInlinedValueConverter.kt:208` |
| `lowtail-decimal-halfup-silent-rescale` | low | 33 | `DucklakeInlinedValueConverter.kt:433` |
| `eff-inlined-rows-n-plus-1` | low | 27 | `DucklakeSplitManager.kt:103` |
| `lowtail-date-constant-out-of-range` | low | 26 | `DuckDbExpressionTranslator.kt:439` |
| `lowtail-date-timestamp-literal-out-of-range` | low | 45 | `DuckDbWhereClauseTranslator.kt:230` |
| `lowtail-puffin-unchecked-escape` | low | 59 | `DucklakePuffinDeleteReader.kt:82` |
| `lowtail-paritext-warn-drops-stack` | low | 51 | `TrinoParityExtensionResolver.kt:175` |

Themes: **inlined data** (`DucklakeInlinedValueConverter` ×2, `DucklakeSplitManager` N+1,
`DucklakeMetadata` fallback record-count), **literal rendering** (`DuckDbExpressionTranslator`,
`DuckDbWhereClauseTranslator` — date/timestamp out-of-range), **error classification**
(`DuckDbFilePageSource` NOT_SUPPORTED misclass, `DucklakePuffinDeleteReader` unchecked escape,
`TrinoParityExtensionResolver` warn-drops-stack).

- The **two `[medium]` items are the priority** (`eff-fallback-recordcount-materialize`,
  `correctness-readpath-not-supported-misclass`): add a regression test for each. The correctness
  one (NOT_SUPPORTED misclassification in `DuckDbFilePageSource`) especially — confirm the test
  **fails with the fix temporarily reverted** (revert-verify discipline; see how it was done in
  slice 6 / `2fcc48f`).
- `TrinoParityExtensionResolver.kt` also appears in **Slice 8** (per the file-overlap table) — do
  *your* fix here, don't pull Slice 8's concern in.
- 9 markers map to ~9 fixes (no obvious collapse pairs this time, but check the two
  `DucklakeInlinedValueConverter` sites and the two date-literal sites — if either pair shares a
  helper, collapse and call it out).

## Shipping conventions (confirmed working across slices 1–6)

- **Per marker resolved:** delete its `TODO(review:after id=…)` line; add one `Resolves: <id>`
  trailer (all 9, even any that collapse to one fix).
- **Update `after-markers.json`:** remove the 9 entries from `markers[]`, bump `resolved` 32 → **41**.
  Preserve invariants:
  `planted(50) + skipped_before_fixed(14) + skipped_other(9) == input_findings(73)`, and
  `resolved + len(markers) == planted(50)` → **41 + 9 = 50**. (Verify with a small Python snippet
  before committing — see `2fcc48f`.)
- End the commit with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Don't stage** the unrelated pre-existing `.claude/settings.local.json` change.

## Verification (parse on-disk `TEST-*.xml`, never `gradle clean`)

- **Current baseline to preserve** (HEAD `2fcc48f`): **catalog 33 suites / 108 tests / 0-0-0**;
  **trino 76 suites / 853 tests / 0-0-0**. (Slice 6 added 3 suites / 11 tests over the old 73/842 —
  use these numbers, not the playbook's stale 73/842.)
- Run the **full catalog suite** freely. For **trino**, the playbook allows narrowing to the tests
  actually touched — but several of your files are core read-path (`DuckDbFilePageSource`,
  `DucklakeSplitManager`, `DucklakeMetadata`), so the full trino suite was run for slice 6 to be
  safe; recommend the same. Add a regression test per behavior change and **parse the XML for exact
  tests/failures/errors/skipped** — don't trust stdout.
- Compile incrementally from `jvm/`: `./gradlew :trino-ducklake:compileKotlin` then
  `:trino-ducklake:compileTestKotlin`.
- Tip from slice 6: if a fix lives in a `private`/`companion` function with no public/integration
  test path, making it Kotlin `internal` lets a same-module unit test drive it directly (Kotlin
  treats the test source set as a friend module). This was used for `buildIdentityPartitionValues`.
  Use sparingly and only when there's no reasonable public path.

## After Slice 7

Slice 8 (input safety + efficiency tail, 8 markers) is last. It branches off whatever `main` is
then. Note `TrinoParityExtensionResolver.kt` overlaps 7↔8 (playbook: Slice 7 first) and
`JdbcDucklakeCatalog.kt` carries `eff-snapshot-at-or-before-full-scan` which the playbook suggested
folding into Slice 2 — but Slice 2 already shipped, so it rides with Slice 8.

Good luck. The handoff infra is in good shape — `project_after_backlog_progress.md` +
`AFTER-SLICES.md` will catch anything this note glosses.
