# PLAN - `trino-doris`: read-only Doris connector with verified pushdown

**Status: DRAFT for review.** The decision gates in section 3 require sign-off before
implementation. This plan intentionally starts read-only and treats every pushdown as a
correctness claim, not merely a SQL-rendering convenience.

**Initial compatibility baseline:** Apache Doris **4.1.3** and Trino **483**. All protocol,
type, function, parser, and optimizer claims must be verified against Doris 4.1.3 rather than
inferred from the rolling `dev` documentation. Later Doris versions require an explicit re-probe.

## 1. Why

Apache Doris can hold a large amount of locally optimized analytical data: sorted and
partitioned tables, inverted indexes, Bloom filters, vector indexes, materialized views,
pre-aggregated models, and engine-native functions. Trino cannot currently query that data
through a Doris-aware connector.

Doris accepts MySQL-protocol connections on the FE, so Trino's stock MySQL connector can
establish a basic connection. That is not enough for the intended use:

- Doris is an MPP analytical engine, not MySQL. Its type system, metadata, functions,
  collation behavior, DDL, indexes, and optimizer all diverge from MySQL.
- A generic JDBC scan leaves many predicates and expressions in Trino. That prevents Doris
  from applying inverted indexes and other native acceleration.
- High-value examples such as Trino `contains(array, value)` and `arrays_overlap(a, b)` need
  explicit Doris mappings (`array_contains`, `arrays_overlap`) and boolean-shape handling.
- Blind same-name pushdown is unsafe. `element_at`, date/time functions, string length,
  domain errors, NULL behavior, precision, and return types all contain known differences.

The repository already has a major part of the semantic evidence in
`/home/jayson/DEV/brikk/brikk-house`: live-probe-verified Trino-to-Doris function mappings,
hazard verdicts, Doris grammar verification, and target SQL rewrites.

The goal is a standalone Trino connector, working name **`trino-doris`**, that lets Trino
query a user-operated Doris cluster while preserving safe, typed pushdown into Doris.

## 2. End state

### First production milestone

- New module: `jvm/trino-doris`.
- Trino plugin/connector name: `doris`.
- Connection: MySQL Connector/J to one or more Doris FEs on port 9030.
- Read-only, enforced by both connector code and a SELECT-only Doris account.
- One remote Doris statement per logical table scan. Doris performs its own distributed FE/BE
  execution; Trino does not split a scan by tablet in v1.
- Correct scalar, date/time, JSON, and native ARRAY mappings.
- Streaming JDBC results into Trino pages.
- Pushdown of numeric/date domains, selected typed scalar predicates, LIMIT, and safe TopN.
- High-value ARRAY predicates translated so Doris can use array inverted indexes.
- Unknown or semantically unproven expressions remain in Trino.
- Generated remote SQL is observable and testable with Doris `EXPLAIN`/profile evidence.

### Later milestones

- Broader scalar projection pushdown.
- Verified aggregate pushdown, then cost-aware join pushdown.
- Doris-native search/vector query surfaces.
- MAP/STRUCT/VARIANT fidelity.
- Optional Arrow Flight SQL data plane, only after its production and networking gaps close.
- Writes and Doris-specific DDL only after the read path is mature and separately designed.

### Explicit non-goals for v1

- No INSERT/UPDATE/DELETE/MERGE, CREATE TABLE, ALTER, DROP, MV creation, or index creation.
- No tablet-level parallel split generation.
- No arbitrary SQL passthrough that can mutate the remote database.
- No assumption that Doris is MySQL beyond protocol/driver compatibility.
- No runtime full-SQL parsing/transpilation in the page-source hot path.
- No Arrow Flight dependency in the first release.

## 3. Decision gates

Sign off each gate before the corresponding phase begins.

### G1 - Connector shape

Options:

1. Fork Trino's MySQL connector wholesale.
2. Build a native connector from the SPI.
3. Use Trino `trino-base-jdbc`, modeled primarily on `SingleStoreClient` and selectively on
   `MySqlClient`/`PostgreSqlClient`.

**Recommendation: option 3.** SingleStore is the closest architectural analogy: a distributed
non-MySQL engine behind a MySQL-family protocol. Base JDBC supplies metadata, prepared SQL,
dynamic filtering, result-to-page conversion, caching, LIMIT/TopN/aggregate/join hooks, and
connection lifecycle. Doris-specific code remains focused on correctness, types, and pushdown.

Do not subclass or copy `MySqlClient` blindly. In particular, Doris integer signedness,
DATETIME semantics, complex types, and metadata differ from MySQL.

### G2 - Read topology

Options:

1. One JDBC statement per scan (the Base JDBC default).
2. Parse tablet IDs from Doris plans and issue parallel `TABLET(...)` scans.
3. Use Arrow Flight to obtain BE endpoints and parallel result streams.

**Recommendation: option 1 for v1.** Doris already distributes a statement across BEs. A single
statement also gives one Doris read snapshot. Multiple independently issued tablet statements can
observe different table versions if a write commits between splits. Tablet splitting is a later
feature and requires an explicit snapshot/version contract before implementation.

### G3 - brikk-house integration

Options:

1. Depend on the full `brikk-sql` parser/generator and transpile SQL at runtime.
2. Copy the current JSON mappings and generator rules into the connector.
3. Use the lightweight `brikk-sql-metadata` artifact as the evidence source, while implementing
   explicit typed Trino `ConnectorExpression` rules in this connector.

**Recommendation: option 3.** `brikk-sql-metadata` is intentionally the small embeddable module,
which makes it appropriate as a production dependency. The connector receives a typed expression
tree, not source SQL. Round-tripping it through the separate full transpiler would lose Trino type
information and parameter binding. Typed rules should render `ParameterizedExpression`s directly.

Before P2, assess whether `brikk-sql-metadata` needs an additive mapping API that exposes both
source and target function names together with verdict, areas, hazard text, and provenance. The
current `HazardRegistry.lookup(...)` exposes verdict evidence but not the target name/shape. Pin a
released brikk artifact; do not ship a production connector against a mutable SNAPSHOT.

`brikk-house` is release-status software in a separate repository. The implementation agent must
**stop and ask before modifying it, changing its public API, publishing a release, or changing this
repository's production dependency on it**. P0 may inspect the existing API and write a concrete
proposal; approval is required before applying that proposal. Do not copy internal brikk data into
this repository as a way around that review.

Using `brikk-sql-metadata` in production is intentional: that module is kept small specifically for
embedding by other projects. This is distinct from the full `brikk-sql` transpiler, which remains
test/tooling-only. Update the JVM build convention's broad "brikk-sql must never enter production"
comment when the dependency lands so it names the full transpiler rather than appearing to prohibit
the metadata module too.

Keep the module boundaries deliberate:

- Production: `brikk-sql-metadata-jvm` only.
- Connector implementation: typed Trino-to-Doris expression rules.
- Test/tooling only: the full `brikk-sql` transpiler for cross-checking emitted target shapes and
  generating fixtures when useful.
- Native grammar tests: use the pure-Java Doris parser directly on the test classpath; do not pull
  `brikk-sql-verify` into production or connector tests.

The metadata verdict is a gate, not an automatic rule generator:

- `IDENTICAL`: eligible only when a typed/arity-specific connector rule exists.
- `CONDITIONALLY_EQUIVALENT`: requires a connector rule with explicit guards or wrappers.
- `DIVERGENT`: denied by default. A divergence proven to be solely a target-name or return-shape
  mismatch may be implemented as a separate explicit rewrite after a dedicated live test. This is
  the path for `contains -> array_contains(...)=1`; it is not direct function equivalence.
- `NO_EQUIVALENT`, `UNCLEAR`, or unknown: never push.

### G4 - Type scope

Options:

1. Scalar types only, map ARRAY/MAP/STRUCT to VARCHAR.
2. A safe scalar subset plus ARRAY in v1; MAP/STRUCT/VARIANT later.
3. All Doris types immediately.

**Recommendation: option 2.** ARRAY cannot be deferred because the primary value proposition
includes `array_contains` and `arrays_overlap` index acceleration. If an ARRAY column is exposed as
VARCHAR, Trino cannot type-check those functions and no expression rule can fire.

ARRAY wire decoding is a correctness gate. Probe the exact Connector/J result representation for
each element type and nested/null/string shape. Do not invent a permissive parser that silently
changes NULL, escaping, or numeric precision.

### G5 - String/collation policy

Doris collation and comparison behavior must not be assumed to match Trino. String predicates can
otherwise over-return or under-return.

**Recommendation:** start with NULL-only string domains. Disable string range/inequality/LIKE and
equality pushdown until live probes establish a safe policy. A remote equality used only as a
superset pre-filter must leave the exact Trino predicate as a remaining filter; never mark it fully
pushed merely because Doris accepted the SQL.

### G6 - Doris-native indexed predicates

There are two classes:

- Standard Trino expressions with Doris equivalents (`contains`, `arrays_overlap`, selected JSON
  and regex calls). These belong in typed pushdown rules.
- Doris-only operators (`MATCH_ANY`, `MATCH_ALL`, `MATCH_PHRASE`, `MULTI_MATCH`, vector search).
  Trino has no standard source expression for these.

**Recommendation:** implement the standard mappings first. Do not invent local scalar semantics for
Doris-only search operators. Keep a future decision between read-only validated `query()`
passthrough and connector-native table functions. Arbitrary passthrough is excluded from v1 because
SQL that returns no rows can still mutate Doris before the connector notices.

### G7 - Read-only enforcement

**Recommendation: defense in depth.** All layers are required:

1. Document and test a Doris account with SELECT/SHOW/EXPLAIN-only privileges.
2. A `ReadOnlyDorisClient`/`ForwardingJdbcClient` rejects every mutating `JdbcClient` method.
3. Connector access control rejects write/DDL/procedure operations.
4. Do not bind the JDBC `query()` table function in v1.
5. Verify that Base JDBC's automatically installed `system.execute` procedure is denied before
   release; if access control cannot make that airtight, replace/fork the module binding rather
   than shipping a writable escape hatch.

### G8 - Result transport

**Recommendation:** MySQL protocol/JDBC first. It is row-based, but stable, supports prepared
parameters, and only requires FE connectivity. Connector/J streaming must be enabled so large
results are not buffered in the worker.

Arrow Flight SQL is parked: Doris currently marks it experimental; parameterized prepared
statements have gaps; clients may need direct BE reachability; nested Java type behavior and
multi-BE parallel result delivery remain version-sensitive.

### G9 - Doris catalog scope

**Recommendation:** Doris `internal` catalog only in v1, exposed as Trino schemas (Doris
databases). Doris external catalogs add a third namespace level and source-specific type/semantic
behavior. Add an explicit configured Doris catalog later after the metadata and session-switching
contract is proven.

### G10 - Naming

**Recommendation:** module `jvm/trino-doris`, connector name `doris`, class prefix `Doris*`.
This is distinct from `jvm/doris-ducklake`, which is a Doris FE plugin for reading DuckLake. The
new module runs inside Trino and queries Doris.

Implementation language is **Kotlin only**. Upstream Trino and StarRocks Java connectors are design
references to translate into idiomatic Kotlin; do not add Java source files to the new module.

### G11 - StarRocks prior art

StarRocks maintains `contrib/trino-connector`, but it is reference material rather than a base:

- Its current POM is pinned to Trino **418**, while the mirrored documentation is for 435 and does
  not consistently match the checked-in implementation.
- It uses Base JDBC and a MariaDB/MySQL-family driver for reads, which supports G1.
- Useful pieces to study: module wiring, connection properties, LIMIT/TopN NULL ordering, aggregate
  and join hooks, cancellation, and remote-statistics queries.
- Do not copy its type policy: checked-in code has no native ARRAY/MAP/STRUCT reader and maps
  LARGEINT to `DECIMAL(20,0)`, which is far too narrow.
- Its write/Stream Load code and any old FE-plan/BE-pull design are StarRocks-specific and out of
  scope for the read-only Doris connector.
- Doris and StarRocks function/index semantics have diverged; brikk's Doris evidence and live Doris
  probes remain authoritative.

**Recommendation:** audit the source once during P0, extract implementation lessons into a short
ledger, and port no code wholesale.

### G12 - Plugin packaging and cross-repository prerequisites

P1 adds `trino-base-jdbc` and Connector/J as runtime plugin dependencies, unlike the current custom
Trino connector module. Before scaffolding, prove the Gradle plugin assembly includes the correct
483-aligned jars without duplicating Trino-provided SPI classes.

P2 may depend on an additive brikk metadata API and a released artifact from a separate repository.
P0 must first determine whether the existing released API is sufficient. If it is not, produce the
smallest API/release proposal and stop for user approval before touching `brikk-house`. Once
approved, treat "mapping API released and pinned" as a hard P0 exit criterion. Do not copy an
unpublished SNAPSHOT into the connector as a schedule workaround.

## 4. Architecture

### 4.1 Module layout

Proposed initial classes:

- `DorisPlugin` extends `JdbcPlugin`.
- `DorisClientModule` installs Base JDBC, Connector/J, Doris configs, and the read-only wrapper.
- `DorisClient` extends `BaseJdbcClient` and owns metadata, types, SQL dialect, and pushdown hooks.
- `ReadOnlyDorisClient` extends `ForwardingJdbcClient` and rejects mutation methods.
- `DorisJdbcConfig` validates FE/MySQL connection URLs.
- `DorisConfig` holds Doris session variables and feature gates.
- `DorisSessionProperties` exposes only safe per-query controls.
- `DorisTypeMappings` centralizes remote type handling.
- `DorisExpressionRewriter` builds typed predicate/projection rules.
- `DorisPushdownEvidence` bridges each rule to pinned brikk metadata/provenance.
- `DorisRemoteQueryModifier` applies query labels/comments if useful for profiles.
- `DorisReadOnlyAccessControl` provides connector-level write denial.

All classes are implemented in Kotlin. Java class names in upstream references describe API roles,
not source-language choices.

The module uses the project Trino 483 BOM and `io.trino:trino-base-jdbc`, MySQL Connector/J, and a
released `brikk-sql-metadata-jvm`. The full transpiler is at most a test/tool dependency. Native
Doris grammar tests consume the pure-Java Doris parser directly. The module does not depend on
`trino-ducklake`, `doris-ducklake`, or `ducklake-catalog`.

### 4.2 Reference implementations

Use upstream Trino 483 sources as design references, not copied assumptions:

- `SingleStorePlugin`, `SingleStoreClientModule`, `SingleStoreClient`: MySQL-wire distributed
  engine, catalog-as-schema mapping, expression/limit/TopN hooks.
- `MySqlClient`: streaming Connector/J result handling, cancellation/abort behavior, case-aware
  predicate controls, aggregate rewriters.
- `PostgreSqlClient`: typed expression/projection rules and ARRAY Block-construction patterns. Its
  JDBC reader uses `java.sql.Array`, which Connector/J does not provide for Doris; only the Trino
  Block-building pattern transfers.
- `JdbcModule`: default metadata, split manager, dynamic filtering, page source, and procedures.
- StarRocks `contrib/trino-connector` (Trino 418): stale but useful Base JDBC/statistics/TopN prior
  art; never a Doris semantic source.

### 4.3 Connection lifecycle

Initial Connector/J properties to probe and pin:

- `tinyInt1isBit=false` so Doris TINYINT is not globally mistaken for BOOLEAN.
- Streaming result mode (`JdbcStatement.enableStreamingResults()` or a proven fetch-size mode).
- Connection and socket timeouts.
- Server prepared statements only if live Doris tests confirm parameter fidelity for all supported
  types; otherwise use Connector/J's safe emulation.
- Character encoding UTF-8.
- TLS configuration passed through explicitly.
- Multi-FE URL/failover support or a documented TCP load balancer.

Apply Doris settings at session scope only, never globally. Candidate connector properties:

- `doris.query-timeout`
- `doris.exec-mem-limit`
- `doris.time-zone` (default UTC after compatibility probe)
- `doris.workload-group`
- `doris.enable-profile` (test/diagnostic use)

Use `Statement.cancel()` plus server `query_timeout` as the cancellation contract. Test that
cancellation removes the Doris query from `SHOW PROCESSLIST` promptly.

### 4.4 Metadata and table exposure

- Map a Doris database to a Trino schema.
- Expose tables, views, and queryable materialized views only after confirming JDBC metadata type
  labels and duplicate-name behavior.
- Hide Doris system schemas by default.
- Use `information_schema` queries where Connector/J metadata is incomplete or misleading.
- Preserve identifier case through Trino's standard identifier mapping support.
- Do not expose external Doris catalogs in v1.

### 4.5 Split behavior and consistency

Base JDBC should produce one split for a table query in v1. Doris distributes the SQL internally
and the FE streams the result. This favors pushed predicates/aggregates because fewer rows cross the
row protocol.

Record this limitation explicitly: large unfiltered result scans funnel through the FE and incur
row serialization. The connector is optimized for pushdown, not bulk extraction.

Tablet-level splits are not just a performance toggle. Before adding them, prove:

- how to obtain stable tablet lists;
- how to pin all statements to one visible table version/snapshot;
- how replica routing and failures work;
- how dynamic filters interact with per-tablet SQL;
- that tablet hints do not bypass row-level security or MVs.

## 5. Type contract

Every mapping needs live MySQL-wire evidence. Metadata type codes alone are insufficient.

| Doris type | Initial Trino mapping | Policy |
|---|---|---|
| BOOLEAN | BOOLEAN | Normalize Doris/Connector-J TINYINT/BIT reporting explicitly. |
| TINYINT/SMALLINT/INT/BIGINT | matching Trino integer | Doris integers are signed; do not copy MySQL unsigned widening. |
| LARGEINT | DECIMAL(38,0) | Deliberately misses the extreme top/bottom of Doris's signed 128-bit range. In-range values work; an out-of-range row fails the query loudly. |
| FLOAT/DOUBLE | REAL/DOUBLE | Disable exact predicate pushdown where approximation can change equality. |
| DECIMAL(p,s), p <= 38 | DECIMAL(p,s) | Preserve precision/scale exactly. |
| DECIMAL precision > 38 | unsupported or VARCHAR | No silent rounding/overflow in the default mode. |
| DATE | DATE | Direct after live boundary tests. |
| DATETIME(p) | TIMESTAMP(p), max proven precision | Doris DATETIME has no zone. |
| TIMESTAMPTZ | TIMESTAMP WITH TIME ZONE | Version-gated; pin connection time zone and test DST/offset boundaries. |
| CHAR/VARCHAR/STRING | CHAR/VARCHAR | Conservative domain pushdown due collation/padding. |
| JSON | JSON | Parse textual wire value with strict JSON handling. |
| ARRAY<T> | ARRAY<T> | Required in v1 for index-aware predicates; strict parser and supported element allowlist. |
| MAP/STRUCT | unsupported or JSON/VARCHAR | Native mapping deferred until wire grammar is proven. |
| VARIANT | JSON/VARCHAR | Dynamic shape cannot become a fixed Trino type without a contract. |
| IPv4/IPv6 | IPADDRESS if exact, otherwise VARCHAR | Probe Connector/J representation. |
| BITMAP/HLL/QUANTILE_STATE/AGG_STATE | hidden/unsupported | Opaque engine states are consumed by Doris functions, not exposed as rows. |

Trino's internal `Int128` carrier backs long DECIMAL values, but the public DECIMAL type contract
still stops at precision 38. It does not create a connector-visible signed 128-bit integer type.
LARGEINT values outside DECIMAL(38,0) therefore fail during conversion; they are never clamped or
truncated. Numeric predicate pushdown is allowed only for literals/domains representable in
DECIMAL(38,0), so the unsupported extreme range is explicit.

### ARRAY implementation gate

P0 must capture Connector/J metadata and raw values for:

- ARRAY of each supported primitive;
- empty array versus NULL array;
- NULL elements;
- strings containing comma, quotes, brackets, backslash, Unicode, and newlines;
- nested arrays if Doris permits them;
- decimal/date/datetime elements;
- malformed/truncated wire values.

The read function should call Connector/J `getString`, parse the proven Doris text representation,
and produce Trino Blocks through an `ObjectReadFunction`. Reuse only PostgreSQL's
`ObjectReadFunction -> Block` construction pattern; Connector/J does not implement
`java.sql.Array`. Unknown element types fail loud or follow the configured unsupported-type policy.
No best-effort coercion.

## 6. Pushdown design

### 6.1 Layers

1. **Domain pushdown:** numeric, boolean, date/time, and null predicates whose column mappings permit
   exact remote comparison.
2. **Expression predicate pushdown:** typed `ConnectorExpression` rules returning
   `ParameterizedExpression`.
3. **Projection pushdown:** later, because return-type/shape mismatches must be normalized.
4. **Aggregate pushdown:** later, one verified aggregate/type family at a time.
5. **Join pushdown:** cost-aware and off by default until statistics and semantics are proven.

Unknown subexpressions remain in Trino. Partial conjunction pushdown is allowed only when the
remaining Trino filter still enforces exact semantics.

### 6.2 First high-value rules

Predicate rules should prioritize index-relevant shapes:

| Trino expression | Doris SQL | Initial policy |
|---|---|---|
| `contains(array, value)` | `array_contains(array, value) = 1` | Connector-original name/boolean-shape rewrite; brikk supplies the divergent-hazard evidence. Prove NULL and element coercion. |
| `arrays_overlap(left, right)` | `arrays_overlap(left, right) = 1` | Connector-original predicate normalization over brikk's conditional value-equivalence evidence. |
| `array_position(array, value)` in comparison | `array_position(array, value)` | Proven 1-based and zero-if-absent; typed comparisons only. |
| `array_intersect/except/union/remove` | same Doris names | Projection phase after return-type tests. |
| selected JSON predicates | `json_contains`, `json_extract`, `json_unquote(...)` | Use brikk semantic/rewrite evidence; connector owns parameterized rendering. Predicate context first. |
| `regexp_like` | Doris `regexp` predicate | Conditional boolean normalization and regex-dialect tests. |
| safe numeric/date functions | mapped Doris functions | Only typed rules whose domain/error behavior is proven. |

Explicit deny examples:

- `element_at` without an in-bounds proof: Trino throws out-of-bounds; Doris returns NULL.
- `length(varchar)` mapped to Doris `length`: Doris counts bytes; Trino counts characters. A
  separate proven `char_length` rewrite may be allowed.
- domain-sensitive math where Trino and Doris disagree on throw/NULL/NaN.
- `array_sort` until NULL ordering is proven for the element type.
- nanosecond timestamps mapped to Doris microsecond DATETIME.

### 6.3 brikk evidence contract

Relevant source assets:

- `brikk-sql/testResources/semantics/trino-doris-hazards.json` (216 pairs at the current pin).
- `brikk-sql-metadata/.../GeneratedTrinoDorisHazards.kt` and `HazardRegistry`.
- `brikk-sql/.../dialects/DorisGenerator.kt` typed target transforms.
- `docs/research/REPORT-doris-differential-probe-2026-07-13.md`.
- `docs/research/BUGS-doris-generator-mappings-2026-07-13.md`.

Connector tests must pin:

- the brikk artifact/version and probed Doris/Trino versions;
- every registered pushdown rule to an evidence entry;
- target function name and wrapper shape;
- accepted Trino argument types/arity;
- return/predicate type normalization;
- live Doris result equivalence, including NULL and error behavior;
- Doris grammar acceptance.

No function is pushed merely because it appears in both function catalogs. brikk supplies evidence;
the connector remains responsible for the exact typed, parameterized SQL rendering. In particular,
brikk's Doris generator does not currently supply the connector's `contains` rule or the `= 1`
predicate wrappers.

### 6.4 Inverted-index verification

For ARRAY predicates, tests need both result equivalence and physical-plan evidence:

1. Create a table large enough that index use matters.
2. Create the appropriate Doris inverted index outside the connector (v1 remains read-only).
3. Run the Trino query and capture generated remote SQL.
4. Run Doris `EXPLAIN`/profile for that exact SQL.
5. Assert the expected predicate reached the scan and record whether the index was selected.
6. Compare indexed versus non-indexed timings and scanned rows/bytes.

The connector guarantees correct SQL pushdown. Doris optimizer index selection is observable and
benchmarked, but not hard-coded into query generation and not a deterministic P2 correctness gate.

### 6.5 LIMIT, TopN, aggregate, and join

- LIMIT: supported and guaranteed after a direct Doris probe.
- TopN: implement Doris/MySQL NULL ordering explicitly. Disable textual TopN until collation is
  proven. Never push a bare remote ORDER BY without LIMIT; Doris can cap sorted output by default.
- Aggregates: begin with COUNT, MIN/MAX on non-text, and exact SUM types. Verify NULL, overflow,
  DECIMAL scale, and approximate-function behavior before each rule.
- Join: keep disabled in the first release. Later use Base JDBC cost-aware join pushdown with FULL
  OUTER and textual-condition gates. A pushed whole join/aggregate can benefit from Doris MVs far
  more than independent table scans can. Base JDBC renders a pushed join as one remote Doris
  statement, so the v1 single-statement snapshot posture remains intact.

### 6.6 Doris-native search surfaces

`MATCH_ANY`, `MATCH_ALL`, phrase/regexp search, vector search, and other Doris-only operators have no
standard Trino expression mapping in brikk. Options for a later phase:

- a SELECT-only validated `query()` table function;
- connector table functions for full-text/vector searches;
- connector scalar functions with a correct local implementation plus remote rewrite.

Do not choose until the read-only boundary and API ergonomics are reviewed. A pushdown-only scalar
that cannot execute locally is unsafe if the optimizer ever declines the pushdown.

## 7. Read-only behavior

The first release should fail these operations before sending SQL to Doris:

- CREATE/DROP/ALTER schema or table;
- INSERT/UPDATE/DELETE/MERGE/TRUNCATE;
- comments and property changes;
- arbitrary procedures;
- arbitrary SQL passthrough.

Tests must prove both denial and absence of side effects. The remote test account should independently
lack mutation privileges so a connector regression cannot write.

`SHOW`, metadata reads, `EXPLAIN`, SELECT, cancellation, and harmless metadata-cache flush remain
available. Whether Doris views/MVs appear is a metadata decision, not write support.

## 8. Observability

Every pushed query needs enough evidence to diagnose correctness and performance:

- Trino query ID attached to the remote query through a safe SQL comment or Doris query tag.
- Generated remote SQL visible at DEBUG and through Trino query diagnostics without logging secrets.
- Count of predicates/projections/aggregates accepted and rejected, with rejection reason categories.
- Doris query/profile ID when available.
- JDBC read rows/bytes and elapsed time.
- Cancellation outcome.
- Optional test-only `EXPLAIN` capture.

Pushdown decisions should distinguish: unsupported shape, unknown brikk evidence, divergent semantics,
conditional guard failed, unsupported type, collation risk, or connector feature disabled.

## 9. Validation strategy

### 9.1 P0 protocol/type probe

Against the exact target Doris release and MySQL Connector/J version, record:

- `DatabaseMetaData` rows for every Doris type;
- `ResultSetMetaData` and getter behavior;
- prepared parameter behavior;
- ARRAY/MAP/STRUCT/JSON wire values;
- BOOLEAN/TINYINT reporting;
- DATETIME/TIMESTAMPTZ under multiple session zones;
- streaming fetch memory behavior;
- cancel/query-timeout behavior;
- FE follower/observer routing and multi-host failover.

Check these findings into the new module's dev-docs as evidence, not ephemeral notes.

### 9.2 Correctness suites

- Base JDBC connector smoke: schemas, tables, columns, views, SELECT, cancellation.
- Type matrix including extremes, NULL, Unicode, precision, and unsupported types.
- Read-only denial suite using both connector API and remote audit checks.
- Pushdown rule tests over typed Trino expressions.
- brikk registry/rule drift test.
- Direct pure-Java Doris-parser acceptance for every generated SQL rule.
- Live Trino-versus-Doris differential queries for each pushed rule.
- String collation/padding tests.
- ARRAY parser and high-value predicate tests.
- Doris `EXPLAIN` evidence tests for inverted indexes.
- LIMIT/TopN cardinality and NULL-order tests.
- ORDER BY without LIMIT over more than 65,535 rows to prevent remote truncation mistakes.

Reuse the corpus replay pattern from this repository, but do not conflate it with DuckLake: this
connector's oracle is the same Doris table queried directly versus through Trino.

### 9.3 Performance benchmarks

Compare three paths over identical Doris tables:

1. Doris-native SQL through MySQL protocol.
2. Trino stock MySQL connector against Doris.
3. `trino-doris` with controlled pushdown settings.

Measure:

- elapsed and Doris execution time;
- rows/bytes crossing FE to Trino;
- Trino CPU and memory;
- Doris scanned rows/bytes and index use;
- cold/warm latency;
- concurrency and FE connection count;
- cancellation latency.

Primary workloads:

- `array_contains` and `arrays_overlap` on indexed and non-indexed arrays;
- numeric/date selective filters;
- JSON and regex predicates;
- full scans to quantify row-protocol tax;
- LIMIT/TopN;
- aggregate pushdown;
- later, joins/MV rewrite.

Success is not "all SQL pushed." Success is identical answers plus a material reduction in rows and
work crossing from Doris to Trino on high-value query shapes.

## 10. Phasing

### P0 - Decisions and live spike

- Sign off G1-G12.
- Audit the existing released brikk metadata API. If an additive API/release is needed, write the
  proposal and ask before modifying `brikk-house`; after approval, cut/pin the lightweight release.
- Verify `trino-base-jdbc` and Connector/J plugin assembly/classloader contents.
- Audit the StarRocks connector once and check in the keep/reject ledger from G11.
- Point stock Trino MySQL/SingleStore-shaped code at the Doris 4.1.3 FE.
- Run the protocol/type probe in section 9.1.
- Implement a throwaway ARRAY text-decoder spike over every ARRAY fixture in section 5; native ARRAY
  feasibility is a P0 exit criterion, not deferred P2 risk.
- Confirm single-statement snapshot behavior, streaming, cancellation, and FE failover.
- Produce a concrete type and read-only capability ledger.

### P1 - Read-only JDBC foundation

- Scaffold `jvm/trino-doris`, Gradle module, plugin service, and package assembly.
- Implement metadata and scalar type mappings.
- Enable streaming MySQL results and cancellation.
- Add read-only client wrapper/access control and SELECT-only fixture user.
- Prove `system.execute` is denied before P1 completes; replace the Base JDBC procedure binding if
  access control cannot make the denial airtight.
- No expression/function pushdown beyond exact numeric/date domains.
- Smoke and type/read-only suites green.

### P2 - ARRAY and index-aware predicates

- Implement strict ARRAY metadata/wire decoding for an explicit element-type allowlist.
- Add typed `contains -> array_contains(...)=1` and `arrays_overlap(...)=1` rules.
- Add `array_position` comparisons where safe.
- Add brikk evidence drift tests.
- Add Doris inverted-index EXPLAIN/profile fixtures. Correct emitted SQL and predicate-at-scan
  evidence gate P2; optimizer index selection and timing are recorded benchmarks, not flaky
  connector correctness gates.

This is the first milestone that demonstrates why this connector exists instead of using Trino's
stock MySQL connector.

### P3 - Broader predicate/projection pushdown

- Add proven identical scalar predicate rules by semantic family.
- Add explicit guarded rewrites for selected conditional/name-shape mappings.
- Add projection pushdown only where return type/shape is exact.
- Add LIMIT and safe TopN.
- Keep unknown and unverified mappings local.

### P4 - Aggregates and Doris optimizer leverage

- Add exact aggregate/type families one at a time.
- Prove overflow/NULL/DECIMAL behavior.
- Confirm Doris MV rewrites occur for pushed aggregate queries.
- Add remote statistics sufficient for later cost-aware join decisions.

### P5 - Join pushdown

- Add cost-aware join pushdown, disabled by default initially.
- Exclude unsupported join types and textual/collation-risk conditions.
- Test snapshot consistency and MV rewrites.
- Enable by default only after a representative correctness/performance corpus.

### P6 - Doris-native query surfaces

- Decide SELECT-only passthrough versus typed table functions.
- Add full-text/vector/index-native surfaces only with a reviewed read-only boundary.
- Keep mutation and Doris DDL out of scope unless a separate write plan is approved.

### P7 - Optional high-throughput transport

- Re-evaluate Doris Arrow Flight SQL against then-current releases.
- Require production status, parameter support, type fidelity, cancellation, BE reachability, and
  multi-BE result behavior.
- Keep MySQL/JDBC as the correctness reference and fallback.

## 11. Acceptance bar for the first useful release

- Read-only operations are enforced in connector code and by fixture privileges.
- Scalar and supported ARRAY values round-trip exactly.
- Unsupported/overflowing types fail loud or follow an explicit VARCHAR policy. LARGEINT maps to
  DECIMAL(38,0) and fails loudly on the unrepresentable extreme range.
- Every pushed function has pinned brikk evidence and a live Doris equivalence test.
- `contains` and `arrays_overlap` reach Doris with exact predicate semantics.
- Indexed and non-indexed results agree; index use is visible in Doris plans/profiles.
- String/collation hazards cannot silently under-return rows.
- Cancellation and query timeouts release Doris work.
- No tablet-split snapshot inconsistency exists because v1 uses one statement per scan.
- Benchmarks show reduced transferred rows/work for at least the target ARRAY predicates.
- Stock-MySQL comparison demonstrates concrete value beyond branding/configuration.

## 12. Risks and open questions

- **ARRAY wire format:** the core v1 risk. Native typing is required for the value proposition, but
  Connector/J may expose only text. The parser must be proven, not inferred.
- **Collation:** unsafe string pushdown can silently change rows. Conservative default required.
- **brikk verdict granularity:** current evidence is primarily function-name based; connector rules
  also need arity/type/argument-shape constraints.
- **LARGEINT domain loss:** DECIMAL(38,0) is the practical Trino mapping but cannot represent the
  extreme Doris signed-128 range. Reads and predicates must expose that boundary loudly.
- **Doris version skew:** function catalogs and semantics change. Pin and re-probe on upgrades.
- **Initial version pin:** Doris 4.1.3 is the only initial compatibility target; rolling `dev`
  documentation is research input, not runtime evidence.
- **Row protocol ceiling:** full scans can be CPU-heavy in FE, network, driver, and Trino.
- **Single statement:** correct and simple, but all results stream through one FE connection.
- **Complex types:** MAP/STRUCT/VARIANT require separate wire/type work.
- **Read-only escape hatches:** Base JDBC procedure bindings must be audited, not assumed safe.
- **Doris-only search syntax:** no standard Trino mapping; API design deferred.
- **Statistics:** poor remote statistics can make join pushdown choices harmful.
- **External Doris catalogs:** namespace and semantics intentionally deferred.

## 13. Key references

Repository/brikk:

- `/home/jayson/DEV/brikk/brikk-house/brikk-sql/testResources/semantics/trino-doris-hazards.json`
- `/home/jayson/DEV/brikk/brikk-house/brikk-sql-metadata/src/dev.brikk.house.sql.metadata/Hazards.kt`
- `/home/jayson/DEV/brikk/brikk-house/brikk-sql/src/dev.brikk.house.sql/dialects/DorisGenerator.kt`
- `/home/jayson/DEV/brikk/brikk-house/docs/research/REPORT-doris-differential-probe-2026-07-13.md`
- `/home/jayson/DEV/brikk/brikk-house/docs/research/BUGS-doris-generator-mappings-2026-07-13.md`
- `jvm/doris-ducklake` for Doris deployment/testing knowledge only; no runtime code dependency.

Trino 483:

- `plugin/trino-base-jdbc/.../JdbcClient.java`, `JdbcModule.java`, `JdbcPageSource.java`
- `plugin/trino-singlestore/.../SingleStoreClient.java`, `SingleStoreClientModule.java`
- `plugin/trino-mysql/.../MySqlClient.java`
- `plugin/trino-postgresql/.../PostgreSqlClient.java` (ARRAY/projection patterns)

StarRocks prior art:

- https://github.com/StarRocks/starrocks/tree/main/contrib/trino-connector (currently Trino 418)
- https://ta.thinkingdata.cn/presto-docs/connector/starrocks.html (mirrored Trino 435 docs; verify
  every claim against source because docs and implementation have drifted)

Doris docs:

- MySQL protocol: https://doris.apache.org/docs/dev/connection-integration/mysql-proto/
- Data types: https://doris.apache.org/docs/dev/sql-manual/basic-element/sql-data-types/data-type-overview/
- SELECT: https://doris.apache.org/docs/dev/sql-manual/sql-statements/data-query/SELECT/
- EXPLAIN: https://doris.apache.org/docs/dev/sql-manual/sql-statements/data-query/EXPLAIN/
- Variables: https://doris.apache.org/docs/dev/sql-manual/basic-element/variables/
- Arrow Flight SQL: https://doris.apache.org/docs/dev/connection-integration/arrow-flight-sql/

The links above target rolling documentation for research convenience. P0 must use Doris 4.1.3
documentation/source and live 4.1.3 probes for accepted behavior.
