# Upstream Tracking — Playbook + Latest Findings

The repo root has a `vendor/` directory that holds **read-only working
copies** of upstream projects we audit against. Nothing under `vendor/`
is built or shipped, and the sub-directories themselves are git-ignored
(see the root `.gitignore`). This file is both the playbook an agent runs
on each refresh and a short record of where the most recent survey left
the baselines.

To bootstrap a fresh checkout (or refresh an existing one):

```sh
./vendor/clone-related-projects.sh          # clone any missing, fetch the rest
./vendor/clone-related-projects.sh --pull   # additionally fast-forward
```

Paths in this document are written **relative to the repo root**. Sibling
docs in this same directory:

- Working TODOs: `TODO-WRITE-MODE.md`, `TODO-READ-MODE.md`,
  `TODO-pushdown-duckdb.md`, `TODO-duckdb-lake-format.md`. Research-derived
  items get folded in there directly (each has an "Open Research Items"
  pointer section).
- Comparison audits: `COMPARE-datafusion-ducklake.md`,
  `COMPARE-pg_ducklake.md`.
- Archived historical context:
  [`archive/RESEARCH-LOG.md`](archive/RESEARCH-LOG.md) (full append-only
  log of prior refresh runs — what was surveyed, what was found, and the
  SHAs each survey rested on),
  [`archive/RESEARCH-TODO.md`](archive/RESEARCH-TODO.md) (pre-fold-in
  research questions with the longer rationale for each item that's now
  bulleted in the working TODOs).

The point: keep our integration aligned with the reference implementations
without missing spec-level or correctness-impacting changes.

## Tracked repos

Each sub-directory under `vendor/` is its own git repo. Identify each by
`git remote -v`:

| Sub-dir | Upstream | What it is | What to watch |
|---|---|---|---|
| `ducklake/` | `duckdb/ducklake` | The reference C++ DuckDB extension — effectively the DuckLake spec implementation | Active release branches (currently `v1.5-variegata`); `main` is duckdb-main CI. Spec-impacting catalog/metadata changes, new catalog backends, retry/concurrency fixes, type/schema evolution. |
| `ducklake-web/` | `duckdb/ducklake` (web) | The public DuckLake docs site (jekyll) | `docs/stable/`, `docs/0.4/`, blog posts, landing page (`index.html`) catalog list. Surfaces the publicly-supported feature set. |
| `pg_ducklake/` | (Rely Cloud) | Postgres extension that wires `pg_duckdb` + the upstream DuckLake C++ extension into PG | PG-side glue is mostly not portable, but watch `third_party/ducklake/` (vendored DuckLake reference) for upstream bumps that pg_ducklake exposes early. Also watch the `docs/` for upstream feature signal. |
| `datafusion-ducklake/` | `hotdata-dev/datafusion-ducklake` | Rust DataFusion extension | Spec interpretation cross-check; ideas worth stealing (e.g. footer-size hints). |
| `duckdb-web/` | `duckdb/duckdb-web` | DuckDB's main docs site | Indirect — only check if a DuckLake-related DuckDB feature is being documented (e.g. Quack RPC, Parquet variant types). |

## Per-run procedure

For each tracked repo, **in this order**:

### 1. Establish baseline

Open [`archive/RESEARCH-LOG.md`](archive/RESEARCH-LOG.md) (or the "Latest
baselines" section at the bottom of this file), find the most recent
entry for this repo, and read the recorded "last-surveyed SHA" (and
branch) for it. If no prior entry exists, the baseline is the current
local `HEAD` before fetching.

### 2. Fetch — do not pull

```sh
cd vendor/<repo>
git fetch --all --prune
```

Or, to refresh all repos at once without pulling:

```sh
./vendor/clone-related-projects.sh
```

**Do not** `git pull` (and do not pass `--pull` to the bootstrap script)
during a survey. The `vendor/` repos are read-only research mirrors —
leaving them un-merged makes the diff easy to compute, and avoids
touching submodules. The user advances the local baseline when they're
ready, not the survey agent.

### 3. Diff vs baseline

For the project's main branch (and any active release branch — DuckLake's
case is `v1.5-variegata` right now):

```sh
git log --oneline --no-merges <baseline-SHA>..origin/<branch>
git log --merges --pretty="%h %as %s" <baseline-SHA>..origin/<branch>
git diff --stat <baseline-SHA>..origin/<branch>
```

If a relevant submodule pointer might have moved:

```sh
git ls-tree <baseline-SHA> <submodule-path>
git ls-tree origin/<branch> <submodule-path>
```

(`git fetch` does **not** populate submodules — `git submodule update
--init --recursive <path>` does, if you actually need the working tree.
For diff-level survey, the pointer comparison is usually enough.)

### 4. Triage each substantive change

A change is **substantive** if it touches one of:

- catalog/metadata schema or queries
- snapshot / lineage / commit / retry semantics
- type system or schema evolution
- file layout / Parquet schema annotations / stats
- inlined-data lifecycle
- delete-file or position-delete encoding
- catalog backends (DuckDB-local, SQLite, Postgres, MySQL, Quack, …)
- maintenance ops (`flush_inlined_data`, `merge_adjacent_files`,
  `rewrite_data_files`, `expire_snapshots`, `cleanup_*`)
- views, macros, partitioning, sorting

Skip: dependency bumps, CI tweaks, lint-only changes, comments,
formatting, blog posts unrelated to a feature.

For each substantive change, decide:

- **Parity** — we already do this. Note as confirmation in the chat
  summary; no doc change needed.
- **Gap** — we don't do this; could matter. Add a short bullet under
  "Open Research Items" in the appropriate working TODO
  (`TODO-WRITE-MODE.md` or `TODO-READ-MODE.md`) with a one-line summary
  + proposed spike size.
- **Research** — unclear if it matters until we look closer. Same as
  Gap — bullet in the appropriate TODO.
- **Bug-shaped on our side** — if upstream just fixed something we may
  also have wrong, add a high-priority item directly in the relevant
  working TODO section (not under "Open Research Items"; this is a
  known-need).
- **Doc-only** — write/read mode unaffected; just update the relevant
  `dev-docs/COMPARE-*.md` files in place (the agent has authority to
  do this directly — these are *our* docs, not user code).

### 5. Fold findings into working TODOs

For each gap / research / bug-shaped item, add a short bullet under the
target TODO's "Open Research Items" section (or directly in the relevant
feature section if it's bug-shaped and already in scope). Bullets are
terse; they're pointers, not full rationale. Shape:

```markdown
- **<short-anchor>** — what + where (1 sentence) + proposed spike size.
```

Don't write a separate per-item doc unless the item is substantial
enough to need 100+ lines of analysis. In that case create a `REPORT-*`
or `PLAN-*` sibling in `dev-docs/` and link from the bullet.

### 6. Update "Latest baselines"

Edit the "Latest baselines" section at the bottom of this file: bump the
SHAs you just surveyed so the next run has the right diff anchor.

If the survey was large or the user wants a permanent record, append a
dated entry to [`archive/RESEARCH-LOG.md`](archive/RESEARCH-LOG.md)
mirroring the older entries' shape. For routine refreshes, the in-chat
summary plus the "Latest baselines" update is enough — don't bloat the
log with one-line "nothing substantive" runs.

### 7. Summarize to the user

End the run with a concise summary in chat (not a doc):

- Repos surveyed + new baseline SHAs.
- 1-line bullets for each substantive finding.
- Items added to working TODOs by anchor.
- Ask the user which (if any) should be **promoted** from "Open Research
  Items" into a real backlog item in that same TODO. Don't promote
  without explicit user OK.

## Common pitfalls

- **Don't `git pull`** in the temp repos. The whole point of the diff
  procedure is that the local HEAD stays at the previous baseline until
  the user decides to advance it.
- **Submodules don't fetch implicitly.** If a submodule pointer matters
  for the survey, compare the gitlink SHAs at HEAD vs `origin/<branch>`;
  don't assume a `git fetch` populated them.
- **Active release branch ≠ `main`.** DuckLake's substantive work
  currently lands on `v1.5-variegata` before back-merging to `main`. Always
  survey the release branch in addition to `main`.
- **`vendor/ducklake-web` and `vendor/duckdb-web` use `docs/stable/`** as
  the single source of truth — versioned dirs (`docs/0.4/`, etc.) are
  snapshots of older releases. When checking for new feature docs, look
  at `stable` first.
- **`datafusion-ducklake` releases version-tag often.** Their `CHANGELOG.md`
  is the cheapest signal — read it before diving into the diff.
- **`pg_ducklake` mostly ships PG-side glue.** Most diffs there are not
  portable to us, but the vendored `third_party/ducklake/` snapshot is —
  if that submodule/vendor bumps, treat it as a `ducklake/` survey.

## Latest baselines

Most-recent SHA for each tracked repo. Update these after each refresh
run. For the long-form historical record see
[`archive/RESEARCH-LOG.md`](archive/RESEARCH-LOG.md).

| Repo | Branch | Baseline SHA | Surveyed on |
|---|---|---|---|
| `datafusion-ducklake/` | `main` | `7a15d31` | 2026-07-05 |
| `ducklake/` | `v1.5-variegata` | `c23aca43` (unchanged; confirmed via ls-remote — real upstream tip, 2026-06-17) | 2026-07-05 |
| `ducklake/` | `main` | `6e90e9d3` | 2026-07-05 |
| `ducklake-web/` | `main` | `82231c20` (unchanged) | 2026-07-05 |
| `ducklake-web/` | `quack` | `bb393710` (unchanged from 2026-05-22) | 2026-07-05 |
| `pg_ducklake/` | `main` | `f1a3475` (PG19 support; mostly PG glue) | 2026-07-05 |
| `pg_ducklake/` | `v1.0` | `d538bf8` (two portable commit-protocol fixes #216/#217) | 2026-07-05 |
| `duckdb-quack/` | `main` | `b9f841c` (async insert stream; protocol bump) | 2026-07-05 |
| `duckdb-quack/` | `v1.5-variegata` | `29fc039` (bump to v1.5.4) | 2026-07-05 |
| `duckdb-web/` | `main` | (see archive entry 2026-05-22; not refreshed this run) | 2026-05-22 |

Older baselines and the per-run substantive findings are preserved
verbatim in [`archive/RESEARCH-LOG.md`](archive/RESEARCH-LOG.md). The
research items found during those runs were folded into the working
TODOs (see "Open Research Items" sections in `TODO-WRITE-MODE.md` and
`TODO-READ-MODE.md`); the longer per-item rationale is in
[`archive/RESEARCH-TODO.md`](archive/RESEARCH-TODO.md).
