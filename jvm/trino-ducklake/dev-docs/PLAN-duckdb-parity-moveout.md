# PLAN — DuckDB-format moveout: `trino-duckdb-parity` connector

**Status: APPROVED — P0 resolved 2026-07-19. P1/P2/P3/P5 SHIPPED and TRANSPLANTED.**
The new connector was built in-tree as `jvm/trino-duckbridge` (naming evolved from the
draft's `trino-duckdb-parity`: module `trino-duckbridge`, connector name `duckbridge`,
package `dev.brikk.duckbridge.trino.plugin`) and then moved to its permanent home:
**`/home/jayson/brokk/duckbridge` → github.com/brikk/duckbridge** (see that repo's
`trino-duckbridge/dev-docs/` for P2/P3/P5 notes and the transplant plan). P4 (T4 codec
evaluation) is deferred pending upstream gizmo FSST work. Remaining in THIS repo:
**P6 (trino-ducklake strip-down, §4.2 + §7 guard) and P7 (docs)**.

### P0 resolution record

- **Gates:** G1(a→c), G2 (T3 control / T3-passthrough interim data plane, T2 gated on
  Quack pool rework, T4 strategic, T1 test-only, T5 parked), G3 (move search + scan
  PTFs, drop writes), G4 (none), G5 (`duckdb-parity`), G6 (clean cut) — all accepted
  as recommended.
- **Prereq checks:** Trino pin is 483; `io.trino:trino-base-jdbc:483` is on Maven
  Central. Upstream `trino-duckdb` was published only for 470–476 (dropped upstream
  after 476) — use its source as a skeleton reference only, never a dependency; the
  G5 collision concern is thereby weakened but distinct naming stands. gizmo
  `quack-jdbc` is **not** on Maven Central — resolve coordinates/vendoring before P3.
  `duckdb-trino-parity-extension` submodule initialized; per-platform binaries still
  require `make` before bundling produces non-empty resources.
- **§4.4 audit verdicts (all confirmed DuckDB-engine-only; zero parquet-side
  consumers):**
  - `DucklakeMaterializedFileCache` — dies with `.db` read path; delete files never
    touch the cache (`DucklakeDeleteFileReader`/`DucklakePuffinDeleteReader` read
    streams directly).
  - `DuckDbCatalogWriteRetry` — clean MOVE with `QuackDuckDbExecutor` (its only code
    consumers). The `DucklakePageSourceProvider` "consumer" is a comment, not code.
    `ducklake-catalog` has its own unrelated `WriteTransactionRetry`.
  - `NestedFieldReshapePlanner`/`StructFieldPlan` — MOVES entirely. Parquet self-heals
    nested evolution in the reader; the `DucklakeMetadata` "consumer" is comments
    only. SQL emission lives in `DuckDbSelectSqlBuilder.appendStructPack`.
  - `DucklakeColumnStatsAccumulator` — DIES (instantiated only for vortex/lance COPY
    writers). Parquet writer never touches `DuckDbWriterSupport`; no helper extraction
    needed. Sole `DuckDbWriterSupport` member used: `formatStatValue`.
  - `DucklakeArrowToPageConverter` — MOVES; parquet add_files is footer-based
    (`buildFragment`) and never calls the converter.
  - `pushedExpressions` — read-consumed only via the DuckDB-engine chain
    (PageSourceProvider DuckDB branch → `DuckDbFilePageSource` → executor →
    `DuckDbSelectSqlBuilder`); parquet page source takes no such argument.

## 1. Why

The DuckLake "duckdb" data-file format (`.db` DuckDB database files as lake data files)
does not perform well enough to keep or continue pursuing. Measured (TPC-H-style CTAS +
Q1/Q6):

| Format  | Files | Size      | CTAS | Q1 first | Q1 warm | Q6 warm |
|---------|-------|-----------|------|----------|---------|---------|
| Parquet | 120   | 14.89 GiB | 301s | 3.13s    | 2.16s   | 0.96s   |
| DuckDB  | 171   | 16.87 GiB | 276s | 15.98s   | 7.96s   | 1.09s   |
| Vortex  | 683   | 17.54 GiB | 320s | 12.29s   | 7.69s   | 2.11s   |

**As a parquet replacement inside DuckLake: not worth it** (2.5–7× slower reads, larger
on disk, more files). **As a standalone capability — using DuckDB from within Trino —
worth it on its own merit.** So we remove the formats from `trino-ducklake` — along
with the other DuckDB-engine-read formats that ride on the same machinery (vortex,
lance) — returning that connector to what the DuckLake spec intends: **parquet data
files + parquet/puffin delete files**, plus inlined data.

These formats were experimental and have **no users and no deployed data** — this is a
code moveout, not a data migration (see §7).

The machinery built for the `.db` path is independently valuable and moves to a new
standalone Trino connector, working name **`trino-duckdb-parity`**:

- the parity-backed **function/expression pushdown** layer
  (`DuckDbExpressionTranslator` + the `trino_parity` DuckDB extension),
- the **executor seam** (embedded DuckDB, Quack remote),
- the **Arrow → Trino Page** columnar decode path,
- the **lance/vortex** query surfaces (scope in G3).

The new connector is a generic **Trino → DuckDB** connector: point it at a DuckDB the
user operates (remote via Quack, primarily), query it with real pushdown. We do not
manage the DuckDB server (no sidecar); users run `quack_serve` themselves. Embedded
DuckDB in the Trino worker remains possible but is a testing convenience, not the
recommended deployment (native memory + crash blast radius inside the worker JVM).

**Guiding posture:** the Quack ecosystem (protocol, server pool behavior, gizmo's
JVM codec) is young and actively maturing. The goal now is **functionally working +
retain all our hard-won code**, with parallel transport channels we can benchmark and
swap as upstream improves — not perfection on day one.

## 2. End state

### trino-ducklake (after strip-down)
- Data files: **parquet only**. Delete files: parquet/puffin. Inlined data unchanged.
- `applyFilter` keeps TupleDomain pushdown + partition-transform classification;
  **drops** function-shape expression pushdown (`pushedExpressions`) — its only
  consumer was the DuckDB-engine read path.
- No `duckdb_jdbc` dependency in the plugin, no parity-extension bundling, no
  `ducklake.execution-engine` / `ducklake.quack.*` config.
- `ducklake-catalog` is untouched: its DuckDB/Quack usage
  (`QuackWrappedMetadataQuery`, `QuackBackedDuckDbCatalogUrl`, `DuckDbCatalogWriteRetry`
  users on the catalog side) is the **metadata/catalog plane**, orthogonal to data-file
  formats. DuckDB-as-catalog-backend stays.
- Loud-fail guard for tables whose catalog rows reference non-parquet data files (§7).

### trino-duckdb-parity (new module `jvm/trino-duckdb-parity`)
- Trino plugin, connector name `duckdb-parity` (G5).
- Transports (G2): Quack remote (primary), embedded (testing / opt-in), Flight/Swanlake
  parked.
- Parity-backed pushdown: projection + domain predicates (free), function-shape
  expression pushdown via `trino_*` parity macros, LIMIT/TopN, aggregates later.
- Lance/vortex surfaces move here (search PTFs, scans) — survival scope per G3.
- Bundles the `trino_parity` extension binaries for embedded use; for Quack remote the
  extension must be installed server-side (probed at startup, fail loud).

## 3. Decision gates ⚖️

Sign off each before the corresponding phase starts.

**G1 — Connector shape.**
- (a) base-jdbc derived (upstream `trino-duckdb` shape, `DuckDbClient extends
  BaseJdbcClient`): metadata listing, DDL, writes, `query()` passthrough PTF come free;
  reads are row-based `ResultSet`; pushdown slots (`connectorExpressionRewriter`,
  `limitFunction`, `implementAggregation`) fit our translator well.
- (b) native connector: full control (today's model), but hand-rolls metadata/DDL.
- (c) hybrid: base-jdbc control plane (metadata/DDL/writes) + **custom data plane**
  (own `ConnectorPageSourceProvider` replacing `JdbcRecordSetProvider`), enabling the
  columnar paths (Arrow export or Quack codec §6.2 T4).
- **Recommendation: (a) first, structured so the data plane can be swapped to (c) in a
  later phase.** Correctness ships on rows; columnar is a perf phase, not a blocker.

**G2 — Transport priority.** Recommendation (details §6.2):
control plane **T3 (gizmo quack-jdbc)**; data plane **T2 (embedded-delegate → Quack)
first, migrating to T4 (pure-JVM Quack codec → Trino Blocks) as the target**; T1
embedded test-only; T5 Flight/Swanlake parked. Running parallel transport channels and
benchmarking them against each other is explicitly fine — pick winners empirically.

⚠ **T2 operational gate:** T2's code path works, but it is **not operationally
proven**. Under per-split churn, Quack **1.5.4 exhausted its fixed server-side
connection pool** and added substantial overhead. An upcoming Quack release replaces
the fixed pool (1.5.5-ish; not in 1.5.4). P3 must gate on that release (or demonstrate
a split-batching/connection-reuse pattern that lives within the fixed pool). Until
then, T3's plain pass-through query model is the safe interim data plane.

**G3 — Lance/vortex survival scope.** What must survive: (i) search PTFs
(vector/FTS/hybrid), (ii) plain scans (`__lance_scan`, `read_vortex`) as PTFs, (iii)
writes (`COPY … FORMAT lance/vortex`)? Working assumption from the decision note:
"maybe nothing survives other than some pushdown things". Recommendation: move (i) and
(ii) mechanically (they are self-contained), drop (iii) writes until a concrete need,
revisit exposure once the connector exists.

**G4 — Migration policy. RESOLVED: none needed.** The duckdb/vortex/lance formats were
experimental only — **no users, no deployed data**. Clean delete: no migration tooling,
no transition release, no rewrite playbook. We keep only a cheap fail-loud guard (§7)
so an unexpected non-parquet `file_format` in a catalog surfaces as a named error
rather than silently wrong results (AGENTS.md).

**G5 — Naming.** Module `jvm/trino-duckdb-parity`, plugin/connector name
`duckdb-parity` (avoids colliding with upstream Trino's `duckdb` connector so both can
be loaded side by side). Class prefix: `DuckDbParity*`.

**G6 — Shared code between the two connectors.** After the strip, trino-ducklake needs
none of the moved code (parity, translator, executors, Arrow converter all go).
**Recommendation: clean cut, no shared library.** If the audit (§4.4) finds a genuinely
shared piece (e.g. `NestedFieldReshapePlanner`), copy-don't-share — the two connectors
version independently.

## 4. Inventory — `trino-ducklake` footprint

Source root: `src/dev/brikk/ducklake/trino/plugin/`. ~9.2k lines of DuckDB-engine core
src + ~1.3k lines lance src + ~38 test classes (of 155) touch the moved surface.

### 4.1 MOVE (seeds the new connector; strip `Ducklake` prefixes on the way)

Pushdown brain (the moat — port behind the new connector's SPI, §6.3):
- `DuckDbExpressionTranslator.kt` — ConnectorExpression → `trino_*` SQL; per-conjunct
  partial pushdown; `PUSHABLE_FUNCTIONS` mirrored from `trino-function-aliases.sql`
  with a drift test against `trino_meta()`.
- `DuckDbWhereClauseTranslator.kt` — TupleDomain → DuckDB `WHERE` fragments.
- `DuckDbSelectSqlBuilder.kt` — generalize; the schema-evolution projection parts
  (file-name maps, typed-NULL defaults, struct reshapes) are DuckLake-specific and
  stay behind (die with the `.db` read path).
- `TrinoTimeZoneNormaliser.kt` — Trino session zone → DuckDB `SET TimeZone`.

Parity extension integration:
- `TrinoFunctionAliases.kt`, `TrinoParityExtensionResolver.kt`.
- Gradle bundling block (`build.gradle.kts:135–216`): copies
  `duckdb-trino-parity-extension/build/**/trino_parity.duckdb_extension` per platform
  into jar resources. Moves wholesale to the new module.

Executor seam and transports:
- `DucklakeDuckDbExecutor.kt` (interface), `InProcessDuckDbExecutor.kt`,
  `QuackDuckDbExecutor.kt`, `DucklakeDuckDbExecutorFactory.kt`,
  `DucklakeExecutionEngine.kt`.
- `DuckDbAttachTarget.kt`, `DuckDbTuning.kt`, `DuckDbTuningSql.kt`, `DuckDbS3Config.kt`.

Columnar decode:
- `DucklakeArrowToPageConverter.kt` — Arrow vectors → Trino Blocks/Pages. Also the
  template for the future Quack-codec converter (§6.2 T4).

Lance (9 src files):
- `AbstractLanceSearchTableFunction.kt`, `LanceVectorSearchTableFunction.kt`,
  `LanceFtsTableFunction.kt`, `LanceHybridSearchTableFunction.kt`,
  `LanceSearchProcessor.kt`, `LanceSearchHandle.kt`, `LanceSearchSplit.kt`,
  `LanceSearchTableHandle.kt`, `LanceVectorSearchFunctionHandle.kt`.

Config/session-property slices (relocate, don't delete):
- `DucklakeConfig`: `ducklake.execution-engine`, `ducklake.quack.host/port/token`,
  DuckDB tuning (memory_limit etc.), `parity-extension-path`.
- `DucklakeSessionProperties`: `pushdown_timestamp_with_timezone`, engine/tuning
  session overrides.

Test/dev infra and docs:
- `compose/` Quack server bits used by Quack tests.
- `README-duckdb-format-pushdown-reference.md`, `README-lance-format.md`.
- dev-docs: `TODO-pushdown-duckdb.md`, `TODO-lance.md`, `TODO-vortex.md`,
  `RESEARCH-lance-index-lifecycle.md` move; `TODO-duckdb-lake-format.md`,
  `TODO-duckdb-silent-cache.md` → archive with a tombstone note.

### 4.2 DELETE from trino-ducklake (no longer needed anywhere)

Write path (`.db` / vortex / lance writers):
- `DuckDbFileWriter.kt`, `DuckDbArrowStreamFileWriter.kt`,
  `DuckDbComplexVectorWriter.kt`, `DuckDbWriterSupport.kt` (audit §4.4 first),
  `duckdb_writer_mode` session property, the `openLanceWriter` / `openVortexWriter` /
  duckdb-writer branches in `DucklakePageSink.kt` (~lines 435–530).

Read path:
- `DuckDbFilePageSource.kt`, `DucklakeMaterializedFileCache` *if* duckdb-only (§4.4),
  `createDuckDbPageSource` + `DuckDbAttachTarget.FileScan` dispatch in
  `DucklakePageSourceProvider.kt` (~lines 278–330, 1451–1683).

Format plumbing:
- `FORMAT_DUCKDB` / `FORMAT_VORTEX` / `FORMAT_LANCE`
  (`DucklakeSessionProperties.kt:108–117`), non-parquet acceptance in
  `validateDataFileFormat` (table + session properties), non-parquet format resolution
  in `DucklakeMetadata` (lines ~866, 1225, 1277–1346).
- `pushedExpressions` on `DucklakeTableHandle` and the expression branch of
  `applyFilter` (`DucklakeMetadata.kt:744` area).
- `add_files` lance/vortex/duckdb modes in `DucklakeAddFilesProcedure` (parquet
  add_files stays).
- `ducklake-catalog`'s `CatalogFileFormat` shrinks: `toStored` no longer emits
  namespaced formats (writes are parquet-only); `fromStored` **stays** to power the
  guard (§7).

### 4.3 STAYS (unchanged)

Everything parquet/puffin/metadata: split manager (minus `.db` fields), parquet page
source/writer, puffin delete read/write, inlined data (`DucklakeInlinedSplit`,
`DucklakeInlinedValueConverter`, flush procedure), partitioning, time travel, metadata
tables, change feed, maintenance procedures, and the whole `ducklake-catalog` module
including its Quack-backed *catalog* access.

### 4.4 AUDIT before cutting (shared consumers found by grep — verify, then classify)

| File | Known consumers | Question |
|---|---|---|
| `DucklakeMaterializedFileCache` | PageSourceProvider + delete-file handling tests | Used only to materialize `.db` files, or also remote delete files? Keep if the latter. |
| `DuckDbCatalogWriteRetry` | `DucklakePageSourceProvider`, `QuackDuckDbExecutor` | Split: executor usage moves; page-source usage dies with `.db` path — confirm nothing catalog-side in the plugin needs it. |
| `NestedFieldReshapePlanner` / `StructFieldPlan` | `DucklakeMetadata` (generic) + `.db` read path | Does the parquet path consume reshape plans? If yes, planner stays and only the SQL-emission half moves/dies. |
| `DucklakeColumnStatsAccumulator` | uses `DuckDbWriterSupport` | Extract the type-rendering helper it needs, or confirm accumulator is writer-only for non-parquet and dies. |
| `DucklakeArrowToPageConverter` | `.db` reads, `LanceSearchProcessor`, `DucklakeAddFilesProcedure` | After add_files goes parquet-only, confirm no residual ducklake consumer; then MOVE. |

### 4.5 Tests (~38 of 155 classes move or die with their feature)

- **Move (duckdb-engine/quack/parity/pushdown):** `TestDucklakeDuckDbExecutorBackends`,
  `TestDucklakeBackendDispatchSmoke`, `TestDuckDbExpressionTranslator`,
  `TestDuckDbSelectSqlBuilder`, `TestTrinoFunctionAliases`,
  `TestDucklakeUnicodeStringRoundTrip`, `TestDucklakeArithmeticPushdownParity`,
  `TestDucklakeQuackS3InitRace`, `TestDuckDbCatalogWriteRetry` (per audit),
  `TestDucklakeDuckDbComplexTypeRead`, datetime/tz pushdown suites.
- **Move (lance/vortex):** `TestDucklakeLance*` (7), `TestLanceSearchSql`,
  `TestLanceExtensionCanary`, `TestDucklakeVortex*` (3), `BenchLanceRouteAVsB`,
  row-level/schema-evolution/nested-field format variants
  (`TestDucklakeRowLevel{Lance,Vortex}Format`,
  `TestDucklakeSchemaEvolution{Lance,Vortex}Format`,
  `TestDucklakeNestedFieldEvolutionVortexFormat`).
  Note: these currently test *DuckLake tables in lance/vortex format*; they get
  rewritten as *connector-native* tests (query a plain DuckDB/lance dataset), not
  ported verbatim.
- **Die:** `.db` writer-mode tests, `TestDucklakeDuckDbFormatWrite`,
  `TestDucklakeDuckDbAddFiles`, format-dispatch tests for non-parquet.
- **Stay:** everything parquet/inlined/metadata; `ducklake-corpus-replay` is
  parquet-oriented and must stay green throughout (per-corpus-dir runs are the
  verification loop; never the full corpus unless asked).

## 5. New connector architecture

### 5.1 Shape (per G1 recommendation)

Start from the upstream `trino-duckdb` skeleton (`BaseJdbcClient` subclass — see
`io.trino.plugin.duckdb.DuckDbClient`), which today implements **zero** pushdown beyond
the base-jdbc floor (projection + simple domain predicates) and no capability probing.
We are building "the better version of that": same familiar frame, real pushdown,
pluggable transports, parity contract.

Module layout mirrors trino-ducklake conventions (Kotlin, same gradle plugin
packaging, `resources/META-INF/services/io.trino.spi.Plugin`).

### 5.2 Transports

| # | Transport | What it is | Status / role |
|---|---|---|---|
| T1 | Embedded `jdbc:duckdb:` | DuckDB in the worker JVM | Test-only; opt-in flag. Native memory + crash risk in the worker; explicitly not the deployment story. |
| T2 | Embedded-delegate → Quack | Today's `QuackDuckDbExecutor`: local embedded DuckDB `LOAD quack`, `ATTACH` the remote server, execute server-side via `quack_query_by_name(...)`, read locally via `arrowExportStream` | Working code, **not operationally proven** (see G2 gate): Quack 1.5.4's fixed server-side connection pool exhausted under per-split churn, with substantial overhead. Gated on the upcoming pool rework (1.5.5-ish). Also still embeds native DuckDB in the worker as a protocol client — a stepping stone, not the end state. |
| T3 | gizmo `quack-jdbc` (as-is) | Pure-JVM JDBC driver for `jdbc:quack://` (MIT, zero runtime deps, JDK-17 HttpClient); full `DatabaseMetaData` surface modeled on duckdb-java | **Control plane** (metadata listing, DDL, writes) and **interim pass-through data plane** while T2 is pool-gated and T4 matures. Alpha; validate type fidelity against our corpus before trusting. |
| T4 | Quack codec → Trino Blocks | Fork/reuse quack-jdbc's `codec/type/message/transport` layers (explicitly designed reusable) and decode DataChunk vectors **directly into Trino Blocks** — a `QuackVectorToBlockConverter` sibling of `DucklakeArrowToPageConverter`. Skips JDBC `ResultSet` row-pivot *and* embedded DuckDB entirely: pure-JVM, columnar end-to-end. | **Strategic data plane.** FSST decode is WiP upstream at gizmo with support expected over the coming weeks — use the Quack pass-through query model (T3) in the interim, watch, and move here for efficiency when ready. Nested types also maturing. Protocol stabilizes with DuckDB v2.0 (Sept 2026), so pin/vendor the codec against the server version and keep a protocol-canary test. |
| T5 | Swanlake / Arrow Flight SQL | `swanlake-io/swanlake` | Parked — upstream appears dormant since Quack landed; revisit post-DuckDB-2.0 only if a Flight-native server matters. Flight's columnar advantage is anyway captured by T4 without a new server dependency. |

Deployment model: **remote-first**. The user runs their own Quack server
(`CALL quack_serve(...)`, their token, their filesystem/S3); we connect. No sidecar,
no lifecycle management. T1 exists for tests and the brave.

### 5.3 Pushdown design

- **Free floor:** base-jdbc projection + TupleDomain→WHERE (keep varchar pushdown
  case-sensitive as upstream does).
- **Function-shape expressions:** port `DuckDbExpressionTranslator` semantics into
  `JdbcConnectorExpressionRewriterBuilder` rules; each `PUSHABLE_FUNCTIONS` (name,
  arity) pair becomes a rule emitting `trino_<name>(...)`. Per-conjunct partial
  pushdown maps naturally onto base-jdbc's converted/remaining expression split. Keep
  the tz-gated rules behind the ported `pushdown_timestamp_with_timezone` session
  property.
- **LIMIT / TopN:** `limitFunction()` / `supportsTopN()` — DuckDB dialect is trivial.
- **Aggregates:** later phase via `implementAggregation` — each aggregate must be
  parity-verified (Trino vs DuckDB semantics on NULLs, overflow, decimal scale) before
  enabling; start with COUNT/MIN/MAX/SUM on exact types.
- **Capability probing:** static, corpus-verified allowlist (no dynamic rule engine —
  AGENTS.md, avoid over-engineering). At connection init, probe `trino_meta()` (parity
  present + version) and `duckdb_extensions()` for whatever G3 keeps (lance/vortex);
  on mismatch **fail loud** with install instructions — never silently skip pushdown
  the planner already promised, and never push through a missing parity layer.
- **Parity loading:** T1 embedded — `allow_unsigned_extensions` connection property +
  `LOAD` from bundled per-platform resources (existing resolver). T2/T3/T4 remote —
  extension must be installed on the user's server; startup probe enforces it. The
  existing `TestTrinoFunctionAliases` drift test moves with the translator.

### 5.4 Lance/vortex (per G3)

Whatever survives becomes connector-native: search PTFs (`lance_vector_search`,
`lance_fts`, `lance_hybrid_search`) and scan PTFs issuing `__lance_scan('<dir>')` /
`read_vortex('<path>')` through the active transport, with domain predicates AND-ed
into the generated SQL (the `FileScan` target machinery already does this). Lance's
directory-dataset semantics and extension availability are server-side concerns —
probed, not managed.

## 6. Phasing

Each phase: verify with the module's test suite (fast, targeted runs), commit at
meaningful intervals, no push until asked. Any temporary shim goes in
`dev-docs/CRUTCHES-AND-SHORTCUTS.md` (doris convention; create the module-local
equivalent).

- **P0 — Decisions.** Resolve G1–G6 (this doc reviewed). Run the §4.4 audit and pin the
  final move/delete lists.
- **P1 — Scaffold.** `jvm/trino-duckdb-parity` module: settings include, plugin
  skeleton, upstream-shape client, T1 embedded transport, parity gradle bundling moved
  over, smoke tests (`SHOW TABLES`, scalar round-trip).
- **P2 — Pushdown port.** Translator + normaliser + parity aliases + drift test;
  expression rewriter rules; LIMIT/TopN; pushdown test suite ported (unicode
  round-trip, arithmetic parity, datetime/tz gating).
- **P3 — Quack data plane.** Interim: T3 pass-through query model (send generated SQL
  over `jdbc:quack://`, read rows) + control-plane on the same driver. Port
  `QuackDuckDbExecutor` (T2) + tuning + attach-alias scheme, but **gate T2 activation
  on the Quack pool rework** (G2 gate: 1.5.4's fixed pool exhausts under split churn) —
  keep it behind config as a parallel channel to benchmark, not the default. Move
  compose Quack test env; S3-credential story for server-side reads.
- **P4 — T4 evaluation.** Type-fidelity matrix for quack-jdbc against our test corpus;
  track gizmo's FSST decode work (expected over the coming weeks); decide
  fork-vs-depend for the codec layers; when ready, spike the `DataChunk → Block` page
  source behind the G1(c) seam and benchmark T3-passthrough vs T2 vs T4 (prior art:
  `BenchLanceRouteAVsB`). Move the default to whichever channel wins.
- **P5 — Lance/vortex.** Move the G3-approved surfaces + rewritten tests.
- **P6 — DuckLake strip-down.** Delete §4.2, add the §7 guard, full trino-ducklake
  suite + per-corpus-dir replay green. Remove `duckdb_jdbc`/arrow deps from
  trino-ducklake's gradle if nothing residual needs them.
- **P7 — Docs & close-out.** Root README module table, doc moves (§4.1), tombstones in
  dev-docs, upstream-notes update (`RESEARCH-upstreams.md`).

P1–P5 are purely additive (new module) and can proceed while trino-ducklake is
untouched; P6 is the only destructive phase and lands last, after the new connector's
suite is green.

## 7. DuckLake guard

**No migration** (G4 resolved: experimental formats, no users, no deployed data).
Note for the record: the earlier draft's playbook was invalid anyway —
`rewrite_data_files` only selects **parquet** inputs, so it could never have converted
DuckDB/Vortex/Lance files; a real conversion would have required a CTAS workflow.
Moot now.

**Guard (fail loud, never silently wrong):** when split generation encounters a data
file whose `file_format` (after `CatalogFileFormat.fromStored`) is not `parquet`, throw
`TrinoException(NOT_SUPPORTED)` naming the table and the format found. Same guard on
`data_file_format` table settings at metadata load. Never skip such files —
under-returning rows is the failure mode this repo's rules exist to prevent.

## 8. Sizing

~9.2k lines executor/pushdown/writer core + ~1.3k lance src + ~38 test classes moving
or rewritten; gradle bundling block; docs. The parity extension repo
(`duckdb-trino-parity-extension`) is already a standalone submodule and needs **no
change** — only its bundling location moves. Largest genuinely new code: the base-jdbc
integration of the translator (P2) and the T4 codec page source (P7-adjacent spike).

## 9. Risks / open questions

- **Quack protocol churn** until DuckDB v2.0 (Sept 2026): T2 insulates via the duckdb
  client extension; T4 must pin/vendor the codec per server version + canary test.
- **quack-jdbc maturity** (alpha, WiP with active gizmo support expected over the
  coming weeks): FSST decode in progress (interim: T3 pass-through; move to T4 for
  efficiency once landed), nested types decode to Java collections, client-side
  literal substitution (acceptable: our pushdown emits literals), metadata fidelity
  unproven → corpus-validate before trusting (never blame-the-harness; verify).
- **Perf baseline:** recorded in §1 (parquet vs duckdb vs vortex, CTAS/Q1/Q6). The new
  connector's success criterion is *standalone* usefulness, not beating parquet;
  transport channels (T3-passthrough vs T2 vs T4) are benchmarked against each other
  and the default follows the winner (P4).
- **Quack server pool (1.5.4):** fixed connection pool exhausts under per-split churn
  with substantial overhead — the G2/P3 gate. Watch the upcoming pool rework
  (1.5.5-ish) and re-test before making T2 a default.
- **Embedded risk:** T1 shares the worker JVM with native DuckDB — keep opt-in,
  document memory tuning (`DuckDbTuning` moves for this reason).
- **Upstream overlap:** upstream Trino now ships a (pushdown-less) `duckdb` connector —
  keep our connector name distinct (G5); long-term, pieces of P2 (expression rules on
  a DuckDB dialect) are plausible upstream contributions.
- **Swanlake:** dormant upstream; parked (T5). Re-evaluate only on real demand.

## 10. Doc moves

| Doc | Disposition |
|---|---|
| `README-duckdb-format-pushdown-reference.md` | → new module (becomes the pushdown reference) |
| `README-lance-format.md` | → new module (per G3 scope) |
| `dev-docs/TODO-pushdown-duckdb.md`, `TODO-lance.md`, `TODO-vortex.md`, `RESEARCH-lance-index-lifecycle.md` | → new module dev-docs |
| `dev-docs/TODO-duckdb-lake-format.md`, `TODO-duckdb-silent-cache.md` | → archive w/ tombstone (format removed) |
| trino-ducklake `README.md`, `SAMPLES.md`, root `README.md` | strip duckdb/vortex/lance format claims; add pointer to new connector |
