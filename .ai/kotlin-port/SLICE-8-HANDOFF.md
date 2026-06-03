# Slice 8 handoff — Input safety + efficiency tail (the LAST slice)

You're closing out the **AFTER backlog**, Slice 8 of the DuckLake Kotlin port — the final
slice. Before touching code, read **`.ai/kotlin-port/AFTER-SLICES.md`** (§"Slice 8") and the
memory **`project_after_backlog_progress.md`** (the running ledger). This handoff is the fast
path; those two are the source of truth.

## State of the tree

- **Slices 1–7 are all merged to `main` and pushed.** `main` == `origin/main` == HEAD is the
  Slice 7 commit (`after(slice 7): inlined data / literal rendering / error classification …`).
  Linear (fast-forward) history.
- `resolved` = **41 / 50** in `.ai/kotlin-port/after-markers.json`; `len(markers)` = **9** —
  and those 9 are exactly your slice. After you land, the backlog is **done** (50/50, markers
  empty).
- **Branch off `main`:** `git checkout -b after/8-input-safety main`. No upstream slice
  dependency remains. The one historical overlap — `TrinoParityExtensionResolver.kt` (7↔8) — is
  already resolved: Slice 7 took only its `warn-drops-stack` fix; the `correctness-paritext-
  non-atomic-copy` TODO that's left in that file is **yours**.

## Context you have

- **Marker registry:** `after-markers.json` (`markers[]`, keyed by `id`, carries `severity` +
  `kotlin_anchor` + `finding_index`). Original finding text: `adversarial-review-result.json`
  (`confirmed[]`, **indexed by array position = `finding_index`**, NOT a field). Read the
  `detail`, `suggestion`, and `verifierReasoning` for each — the verifier often corrects the
  severity down and narrows the real consequence. **Don't over-fix beyond what the verifier
  confirms**, and feel free to *decline* a verifier suggestion when it conflicts with existing
  behavior — just document why in code + commit (Slice 7 declined the NULL/blob half of
  `lowtail-inlinedlist-null-token-collision` because a genuine null in `list<blob>` is the
  correct, test-pinned default; see that commit for the pattern).
- **The `.java.old` oracle:** every ported file has its pre-port Java sibling renamed
  `<File>.java.old` next to it. For each marker, diff the `.kt` against `.java.old` and **state
  in the commit whether the bug is pre-existing or port-introduced.** Slices 1–7 were *all*
  pre-existing — expect the same, but verify.
- **Commit convention:** `git show HEAD` (Slice 7) and the one before it (Slice 6). Note the
  per-marker `Resolves: <id>` trailers (one per marker even when several collapse to one fix),
  the pre-existing-vs-port-drift note per file, and the test-count footer.

## The 9 markers (7 files — `QuackBackedDuckDbCatalogUrl.kt` carries 2)

| marker | sev | fi | site |
|---|---|---|---|
| `eff-snapshot-at-or-before-full-scan` | **med** | 22 | `JdbcDucklakeCatalog.kt:200` |
| `lowtail-quack-host-unescaped` | low | 52 | `QuackBackedDuckDbCatalogUrl.kt:65` |
| `lowtail-quack-metadatacatalog-unquoted-use` | low | 53 | `QuackBackedDuckDbCatalogUrl.kt:66` |
| `correctness-paritext-non-atomic-copy` | low | 50 | `TrinoParityExtensionResolver.kt:170` |
| `lowtail-sortkey-contiguity-unchecked` | low | 58 | `DucklakeSortPropertyMapper.kt:57` |
| `eff-snapshot-timestamp-double-parse` | low | 49 | `DucklakeSessionProperties.kt:132` |
| `eff-parquet-footer-double-serialize` | low | 63 | `ParquetFileWriter.kt:75` |
| `eff-inprocess-detach-overhead` | low | 41 | `InProcessDuckDbExecutor.kt:153` |
| `lowtail-quacktoken-minlen-unenforced` | low | 35 | `DucklakeConfig.kt:250` |

Themes: **Quack input safety** (`QuackBackedDuckDbCatalogUrl` host-escaping + identifier
validation, `DucklakeConfig` token min-length), **atomic copy** (`TrinoParityExtensionResolver`
write-temp-then-rename), **efficiency tail** (`JdbcDucklakeCatalog` snapshot lookup,
`InProcessDuckDbExecutor` redundant DETACH, `ParquetFileWriter` double-serialize,
`DucklakeSessionProperties` double-parse), and **`DucklakeSortPropertyMapper`** sort-key
contiguity validation.

### Notes / sequencing

- **The one `[medium]` is the priority:** `eff-snapshot-at-or-before-full-scan` —
  `getSnapshotAtOrBefore` materializes the whole snapshot table to find one row. This is a real
  index/query lift (not a one-liner): push the ordering + limit into SQL (e.g. `WHERE
  snapshot_time <= ? ORDER BY snapshot_time DESC LIMIT 1` or the catalog's equivalent). Add a
  regression test and confirm it returns the same row the scan-based version did. It lives in
  Slice 2's file (`JdbcDucklakeCatalog.kt`) — the playbook once suggested folding it into Slice
  2, but Slice 2 already shipped, so it rides here.
- **`QuackBackedDuckDbCatalogUrl.kt` has two markers at adjacent anchors** (host-unescaped at
  :65, identifier-validation at :66). Check whether they share a fix site (both are about safely
  interpolating untrusted catalog/connection values into generated SQL) — if so, collapse to one
  fix and emit both `Resolves:` trailers, calling out the collapse (Slice 6 did this pattern).
- Most of the rest are local, mechanical efficiency wins (double-parse, double-serialize,
  redundant per-split DETACH). Keep each fix minimal; the verifier downgraded all of these to
  low. `lowtail-sortkey-contiguity-unchecked` is a validation-add (detect a gap in
  `sortKeyIndex` and decline the sort claim rather than emit a false one).
- Slice-6 tip still applies: if a fix lives in a `private`/`companion` function with no public
  test path, making it Kotlin `internal` lets a same-module unit test drive it directly.

## Shipping conventions (confirmed working across slices 1–7)

- **Per marker resolved:** delete its `TODO(review:after id=…)` line; add one `Resolves: <id>`
  trailer (all 9, even any that collapse to one fix).
- **Update `after-markers.json`:** remove the 9 entries from `markers[]`, bump `resolved`
  41 → **50**. Invariants to keep:
  `planted(50) + skipped_before_fixed(14) + skipped_other(9) == input_findings(73)`, and
  `resolved + len(markers) == planted(50)` → **50 + 0 = 50** (markers[] becomes empty — that's
  the backlog complete). Verify with a small Python snippet before committing.
- End the commit with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Don't stage** any unrelated `.claude/settings.local.json` change.

## Verification (parse on-disk `TEST-*.xml`, never `gradle clean`)

- **Current baseline to preserve:** **catalog 33 suites / 108 tests / 0-0-0**; **trino 77 suites
  / 859 tests / 0-0-0** (Slice 7 added the `TestDuckDbFilePageSource` suite + 5
  `TestDucklakeInlinedValueConverter` cases). Add a regression test per behavior change and parse
  the XML for exact tests/failures/errors/skipped — don't trust stdout.
- Run the **full catalog suite** freely (your medium fix + the Quack markers are catalog-side).
  For **trino**, the playbook allows narrowing to the tests touched, but several files here are
  shared infra — the full trino suite was run for slices 6–7 to be safe; recommend the same.
- Compile incrementally from `jvm/`: `./gradlew :ducklake-catalog:compileKotlin
  :trino-ducklake:compileKotlin :trino-ducklake:compileTestKotlin`.

## After Slice 8 (you are the last)

- The AFTER backlog is **complete**: `resolved` 50/50, `markers[]` empty. Update
  `project_after_backlog_progress.md` to reflect "all 8 slices shipped," and consider a final
  reconciliation note in `RECONCILE-SWEEP.md` / `AFTER-SLICES.md` (the "Status" section).
- ff-merge `after/8-input-safety` into `main`, push, and delete this handoff (as Slice 7 deleted
  its own). No SLICE-9 handoff is needed — there is no Slice 9.

Good luck closing it out. `project_after_backlog_progress.md` + `AFTER-SLICES.md` will catch
anything this note glosses.
