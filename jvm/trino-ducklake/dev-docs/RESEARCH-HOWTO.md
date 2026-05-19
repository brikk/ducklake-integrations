# Upstream-Tracking Research Playbook

The repo root has a `vendor/` directory that holds **read-only working
copies** of upstream projects we audit against. Nothing under `vendor/`
is built or shipped, and the sub-directories themselves are git-ignored
(see the root `.gitignore`). This file is the playbook an agent runs on
each refresh.

To bootstrap a fresh checkout (or refresh an existing one):

```sh
./vendor/clone-related-projects.sh          # clone any missing, fetch the rest
./vendor/clone-related-projects.sh --pull   # additionally fast-forward
```

Paths in this document are written **relative to the repo root** (e.g.
`vendor/ducklake/`, `jvm/trino-ducklake/dev-docs/RESEARCH-LOG.md`). The
research docs themselves live in `jvm/trino-ducklake/dev-docs/` alongside
the `COMPARE-*.md` and `TODO-*.md` files; refer to them by sibling path:

- [`RESEARCH-LOG.md`](RESEARCH-LOG.md) — append-only log, one entry per
  refresh run. Records what was surveyed, what was found, and the SHAs
  the survey rested on so the next run has a baseline.
- [`RESEARCH-TODO.md`](RESEARCH-TODO.md) — open research questions and
  proposed action items. Items here are *candidates*; the user escalates
  them to a real working TODO when they want them picked up.

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

Open [`RESEARCH-LOG.md`](RESEARCH-LOG.md), find the most recent entry for
this repo, and read the recorded "last-surveyed SHA" (and branch) for it.
If no prior entry exists, the baseline is the current local `HEAD` before
fetching.

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

- **Parity** — we already do this. Note as confirmation in the log, no
  TODO needed.
- **Gap** — we don't do this; could matter. Add a `RESEARCH-TODO.md`
  entry under "Action items pending escalation".
- **Research** — unclear if it matters until we look closer. Add a
  `RESEARCH-TODO.md` entry under "Research questions".
- **Bug-shaped on our side** — if upstream just fixed something we may
  also have wrong, add it as a high-priority action item.
- **Doc-only** — write/read mode unaffected; just update the relevant
  `dev-docs/COMPARE-*.md` files in place (the agent has authority to
  do this directly — these are *our* docs, not user code).

### 5. Write the log entry

Append one entry to `RESEARCH-LOG.md`. Template:

```markdown
## YYYY-MM-DD — refresh run

**Surveyed repos:**

- `ducklake/` — baseline `<prev-sha>` → `origin/main@<new-sha>`,
  `origin/v1.5-variegata@<sha>`; N substantive commits.
- `ducklake-web/` — baseline `<prev-sha>` → `origin/main@<new-sha>`;
  N substantive commits.
- `pg_ducklake/` — baseline `<prev-sha>` → `origin/main@<new-sha>`;
  vendored `third_party/ducklake/` (un)changed.
- `datafusion-ducklake/` — baseline `<prev-sha>` → `origin/main@<new-sha>`;
  vN.M.P (vN.M.Q).
- `duckdb-web/` — checked, no DuckLake-relevant changes / N entries.

**Substantive findings:**

- [parity / gap / research / bug] — short description, repo + PR/commit,
  what we did about it (logged-only, COMPARE updated, RESEARCH-TODO added,
  …).

**TODOs created this run:** links to `RESEARCH-TODO.md` items by anchor.

**Next baseline:** record the new SHAs the next run should diff against.
```

### 6. Append/update RESEARCH-TODO entries

For each gap / research / bug item, append (or update if related) an
entry in `RESEARCH-TODO.md`. Use a short stable anchor so the log can
link to it. Template:

```markdown
### <anchor> — short title

**Source:** repo `<repo>`, PR/commit `<ref>`, dated YYYY-MM-DD.
**Kind:** [gap | research | bug-shaped]
**Impact:** which paths (read / write / catalog / type / lifecycle).
**Our state:** what we do today (file:line if known).
**Proposed next step:** scoped task, often a quick verification or a
spike before committing to real work.
```

### 7. Summarize to the user

End the run with a concise summary in chat (not a doc):

- Repos surveyed + new baseline SHAs
- 1-line bullets for each substantive finding
- "Items added to RESEARCH-TODO: …"
- Ask the user which (if any) should be **escalated** to a working TODO
  in `TODO-WRITE-MODE.md` or `TODO-READ-MODE.md` (siblings in this same
  directory). Don't escalate without explicit user OK.

## Working-TODO escalation

When the user says "escalate item X to the working TODO":

1. Move the item from `RESEARCH-TODO.md` into the appropriate
   `TODO-WRITE-MODE.md` or `TODO-READ-MODE.md` section (siblings in
   this same directory).
   Convert "proposed next step" into the proper TODO format (priority,
   scope, checklist).
2. Strike through the entry in `RESEARCH-TODO.md` with a back-reference
   to where it landed (so the log history stays intact).

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

## Inventory at last update

See `RESEARCH-LOG.md` for the most-recent baseline SHAs to diff against.
