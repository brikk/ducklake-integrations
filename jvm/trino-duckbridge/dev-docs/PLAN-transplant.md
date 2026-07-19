# PLAN — transplant to the standalone `brikk/duckbridge` repo

**Decided 2026-07-19 (user directive).** This module was designed transplant-ready
(no `project(":...")` deps, own package tree `dev.brikk.duckbridge.*`, module-local
dev-docs). At the next good stopping point it moves out of the ducklake monorepo.

## Target

- Local dir: `/home/jayson/brokk/duckbridge`
- Remote: `https://github.com/brikk/duckbridge` (branch `main`)
- Vendor in the parity extension so it's always at hand:
  `https://github.com/brikk/duckdb-trino-parity-extension` (already a git submodule
  of the ducklake monorepo; carry it over as a submodule of the new repo unless a
  hard copy proves more practical).
- Module naming keeps `trino-` in it (`trino-duckbridge`) because a Doris plugin
  sibling (`doris-duckbridge` or similar) may join later — packages stay separated
  per engine.

## Build toolchain

**Decided: stay Gradle** (user call, 2026-07-19 — matches the existing Trino
connector setup; the Kotlin Toolchain/Amper option was considered and dropped as
not worth the migration risk). The new repo carries over the monorepo's Gradle
shape:

1. Wrapper + version catalog (`gradle/libs.versions.toml` — trim to what the
   module uses: trino BOM, duckdb-jdbc, quack-jdbc, arrow, testcontainers, kotlin,
   detekt).
2. The `buildlogic.kotlin.*` convention plugins — inline the parts the module
   needs into the new repo's `build-logic` (toolchain/JDK 25, kotlin config; drop
   monorepo-only bits like the brikk-sql GitHub Packages wiring if unused).
3. detekt + the root `config/detekt/detekt.yml` — copy into the new repo.
4. Parity-extension resource bundling copy task — moves as-is.
5. Test JVM flags (`--add-modules=jdk.incubator.vector`, `--add-opens`, unsafe
   allow flags) and testcontainers integration tests — move as-is.
6. Trino plugin packaging (deploys as a directory of jars): keep whatever the
   monorepo does today; a proper plugin-dir/zip assembly task can be added in the
   new repo when deployment needs it.

## What moves

- `jvm/trino-duckbridge/**` (src, test, resources, dev-docs, build config —
  re-expressed in the new toolchain as decided above).
- The parity submodule reference + the bundling recipe.
- NOT: anything under `jvm/trino-ducklake` (that module stays and gets stripped in
  moveout phase P6 — see the monorepo's
  `jvm/trino-ducklake/dev-docs/PLAN-duckdb-parity-moveout.md`).

## Sequencing (proposed)

1. P5 (lance/vortex port) completes in the monorepo — fast verification loop, and
   the DuckLake sources being adjacent makes the port cheaper.
2. Transplant (this doc): new repo init, toolchain decision, submodule vendoring,
   CI-less first commit, all tests green in the new home.
3. P6 (ducklake strip-down) + P7 (docs) happen back in the monorepo; trino-ducklake
   README points at github.com/brikk/duckbridge.

After the transplant, the monorepo copy of this module is deleted (or tombstoned)
so there is exactly one source of truth.
