# Design thoughts — managed Parquet soft-scan materialization

Status: planning input. This is deliberately a proposed direction, not an
implementation design that has already been accepted.

## The narrow first problem worth solving

Start with one problem:

> A BI workload repeatedly scans a large, mostly-stable collection of Parquet
> files. Make the repeated scans materially faster without making the derived
> representation authoritative.

Use DuckDB as the only derived representation in this first effort. Treat the
result as a **soft scan materialization**: roughly a materialized view at the
file-scan boundary, but disposable and always subordinate to the source
Parquet/DuckLake snapshot.

This is not yet a general cache framework, nor a reason to silently convert
every file format. In particular, Vortex and Lance should remain explicit
conversion/rendering workflows for now. They have different storage and
execution characteristics, and mixing them into a transparent cache policy
would make correctness, operability, and format choice much harder to reason
about.

## The useful model

For a canonical scan shape over a set of Parquet source objects:

```text
authoritative Parquet objects / DuckLake snapshot
                 |
                 | direct read or asynchronous build
                 v
       derived DuckDB scan artifact (soft-MV)
                 |
                 | exact matching future scan
                 v
              query result
```

The artifact can contain a projection and a filter applied to a defined source
snapshot. On a later query, it may cover some source files, while new or
changed files continue through the normal Parquet path:

```text
requested source snapshot F
  cached-covered sources C  ----> DuckDB artifact read
  remaining sources F - C   ----> native Trino Parquet read
                                      |
                                      v
                           union at the scan boundary
                                      |
                                      v
                       ordinary Trino planning thereafter
```

That makes the artifact useful without pretending it is a catalog table or a
permanent MV. It is an optimization whose absence, eviction, or build failure
must leave the direct Parquet result unchanged.

## Keep three kinds of cache separate

The current implementation already has distinct mechanisms. The plan should
not collapse them into one vague “cache.”

| Layer | What it is good for | What it must not be asked to do |
| --- | --- | --- |
| Trino filesystem cache | Transparent cached source bytes, remote filesystem semantics, worker affinity. Materialize mode already uses this path. | Expose cache membership or an addressable local filename to a connector; Trino's public API does not provide that. |
| Connector artifact/staging cache | A known local path that DuckDB/Quack can attach; derived DuckDB artifacts; accounting, leases, quotas and eviction that the connector controls. | Be unbounded temp storage or rely on `path + size` as source identity. |
| DuckDB external-file cache | A bounded secondary external-file cache when DuckDB owns remote reads, principally valuable for a long-lived Quack process. | Be the product's correctness, observability, cache-admission, or artifact-reuse control plane. |

The connector staging/artifact cache is therefore still needed even if Trino's
filesystem cache is enabled. In materialize mode, the initial remote pull can
benefit from Trino's cache, then the connector creates an addressable local
copy for DuckDB. That duplicate is intentional when DuckDB needs a local file;
it needs an explicit policy and bounded lifecycle.

## Current facts that should shape the plan

The cache note documents several facts that are easy to get backwards:

- Native Trino reads Parquet. `.db`, Vortex, and Lance are routed to the
  DuckDB-oriented page source.
- `duckdb_read_mode=httpfs` is the present default. DuckDB fetches the remote
  object itself, so it bypasses Trino's `TrinoFileSystem` and its filesystem
  cache entirely.
- In the local embedded-DuckDB engine, DuckDB is created per split, so its
  external-file cache is short-lived. It is not a durable warm query cache.
- In Quack, the DuckDB process is long-lived, so its external-file cache can
  survive across queries. Its limits and settings are process-wide when the
  sidecar is shared, rather than cleanly catalog-specific.
- Materialize mode does use `TrinoFileSystem`, and therefore can use Trino's
  configured filesystem cache. It then writes a connector-owned local
  materialized-file copy for DuckDB to attach.
- The current connector materialized-file cache is keyed by remote path and
  size and has no quota, TTL, or eviction. That is not sufficient identity or
  lifecycle management for a semantic artifact cache.
- `enable_object_cache` is a dead/no-op setting in the presently bundled
  DuckDB version. Any DuckDB cache configuration work should use the actual
  external-file-cache settings (for example `enable_external_file_cache`) and
  should be verified against the bundled version.
- Quack materialization needs a worker cache directory that is deliberately
  shared/bind-mounted into the sidecar. This must be configured and validated,
  never assumed through a hard-coded temp path.

## Correctness contract: source snapshots win

The direct Parquet scan is the reference answer. A soft materialization may be
chosen only when its declared coverage is known to be compatible with the
query's source snapshot and semantics.

At minimum, the artifact identity needs:

- each source object identity: URI plus immutable version information such as
  object-store version ID, ETag/generation, and length where available;
- the DuckLake/catalog snapshot or equivalent table snapshot identity when
  one exists;
- canonical scan shape: selected columns, normalized predicates, and any
  operations intentionally pushed into the artifact;
- output schema, type coercion policy, collation/time-zone and other semantic
  settings that can affect results;
- a builder/format version, so that engine or writer changes can invalidate
  old artifacts safely;
- the relevant security/tenant/policy identity if predicates or visibility are
  policy-sensitive.

`path + size` is not a valid source version: replacing an object in place with
different contents of the same size would otherwise return stale data.

Artifacts should be built in a private location, validated, and atomically
published with their manifest. Readers must never observe a partly written
DuckDB file or a manifest claiming coverage that the file does not have.

## Make the first rewrite deliberately conservative

The first rewrite should support **exact match only**:

- the same canonical scan shape;
- exactly declared source snapshot identities;
- compatible schema and semantic context.

Do not begin with predicate implication/containment (for example, deciding an
artifact for `date >= X` can answer `date >= Y`). That turns a storage/cache
project into a query-containment prover and gives us a large, subtle
correctness surface.

For partial coverage, rewrite the scan into a union of:

1. DuckDB reads of the artifact for source objects it covers; and
2. native Parquet reads for new, changed, missing, or incompatible objects.

The union should occur at a stage where the result still has ordinary table
scan semantics. Do not depend on file order unless the query has an explicit
ordering later in the plan.

## Promotion levels

There are three related ideas, but they should not all ship at once.

1. **Raw source staging** — retain an addressable local copy of a popular
   Parquet object. This is useful infrastructure and may reduce remote reads,
   but it does not change the representation.
2. **Filtered/projection scan artifact** — materialize the scan shape actually
   seen repeatedly, into DuckDB. This is the recommended first product target.
3. **Full query-result MV** — materialize joins, aggregates, and arbitrary
   query results. Defer this; invalidation and dependency tracking become a
   different product.

The scan artifact is a nice middle ground: it can have meaningful BI impact
without taking responsibility for whole-query MV semantics.

## Policy and admission

There should be an explicit policy surface, not an opaque heuristic only.

Useful initial modes:

- `direct`: always use native Parquet for the eligible scan.
- `auto`: observe repeated compatible scans and admit after thresholds.
- `warm`: request build/prefetch without requiring the current query to use it.
- `pin`: retain a named artifact subject to an explicit administrative quota.

The normal first request should generally read direct while an asynchronous
builder creates the artifact. Blocking a user request for a conversion is
appropriate only as an explicit requested mode and only with clear failure
behavior.

Admission should use evidence, not just file size: frequency of the canonical
scan shape, bytes scanned, remote-read cost, selectivity, build cost, expected
reuse, available local capacity, and source churn. A file that changes every
few minutes should rarely be promoted.

## Where the artifact should live

Start worker-local. It is operationally simpler and aligns with Trino's
worker-local cache and scheduling affinity. It does require the planner/cache
controller to know that an artifact is only useful on workers that hold it.

Do not make “parallel mirror tree in object storage” a first requirement.
That becomes a distributed artifact store with publication, garbage collection,
access control, cross-worker visibility, and cost questions. It is a plausible
later phase when local artifacts have proven the model and when reuse across
workers justifies it.

For Quack, the first version must explicitly define whether the artifact is
owned by the worker or sidecar and validate the shared mount. A local path that
is invisible to the process doing `ATTACH` is worse than a cache miss.

## Observability is part of correctness

Every eligible scan should be explainable. At a minimum expose/query-log:

- decision: direct, building, artifact hit, partial hit, bypassed, rejected;
- reason: no artifact, stale source, shape mismatch, no local worker copy,
  capacity, policy, unsupported semantics, or build in progress;
- source and artifact byte counts, local/remote bytes where measurable,
  build duration, reuse count, and eviction reason;
- artifact identity, coverage count, state, owner worker, and lease/build ID.

The present DuckDB page source reports `getCompletedBytes() = 0`; the plan
should include a path to meaningful page-source or connector-level byte
accounting. Otherwise cache policy and operator diagnosis are blind precisely
on the optimized path.

## Concurrency, resource safety, and failure cases

These need to be designed before broad automatic admission:

- A per-artifact build lease prevents a burst of identical BI queries from
  creating the same conversion on many workers.
- A reader lease prevents eviction/deletion while DuckDB is attached to an
  artifact.
- Quotas need at least global worker capacity, per-catalog/tenant attribution,
  high/low watermarks, and an eviction policy such as weighted recency/future
  value. Temp-directory growth is not a policy.
- Failed builds must clean up private files and publish no manifest.
- A source object changing during a build must cause the build to be discarded
  or revalidated before publish.
- Cache exhaustion and unavailable native DuckDB/Quack must degrade to the
  normal direct Parquet path, not make reads unavailable.

## Suggested phases

### Phase 0 — make existing behavior safe and visible

- Replace path-and-size cache identity with a real source fingerprint.
- Add caps, accounting, eviction/TTL, build/read leases, and metrics to the
  connector-owned materialized-file cache.
- Make Quack shared cache directories explicit configuration with startup
  validation.
- Correct DuckDB cache configuration to the real external-file cache controls.
- Add tests for replacement-in-place with the same length, concurrent readers,
  Quack mount visibility, and graceful fallback.

### Phase 1 — managed source staging

Introduce a small connector-owned staging abstraction with a ledger and
addressable local paths. It should intentionally sit above Trino's filesystem
cache: source transfer through `TrinoFileSystem` may hit the Trino cache, while
the staging layer owns the local copy DuckDB needs.

### Phase 2 — explicit/manual soft scan materialization

Build a DuckDB artifact for an exact canonical scan shape, record a manifest,
and allow an explicit warm/pin or test-only lookup. Establish artifact format,
atomic publication, cleanup, and direct-result equivalence before heuristics.

### Phase 3 — automatic reuse and partial coverage

Add repeated-scan observation, admission thresholds, exact-match rewrite, and
the “artifact for known sources + direct Parquet for delta sources” union.
Require an explainable decision record for every attempted rewrite.

### Phase 4 — distributed reuse, only if justified

Evaluate worker-affinity improvements, a shared/mirrored artifact store, or
prewarming. This is the point to decide whether cross-worker reuse is worth
the additional distributed-catalog and lifecycle machinery.

Vortex and Lance conversion should be a separate, explicitly invoked project
after the Parquet/DuckDB path proves its correctness and value.

## Questions for the planning agent to resolve

1. What is the smallest artifact representation that DuckDB can reliably read
   and that is easy to version: a standalone DuckDB database, an attached
   table in one per-artifact database, or another DuckDB-supported form?
2. At which connector/planner boundary can a scan be replaced by a partial
   artifact/direct union while preserving projection, filter, split, and
   dynamic-filter semantics?
3. What exact snapshot identity is available for each supported filesystem?
   What is the safe fallback when a filesystem cannot supply strong version
   metadata?
4. Is initial scope only DuckLake tables with a catalog snapshot, or arbitrary
   Parquet locations too? The former is much safer.
5. How will worker locality be represented and used without promising a global
   artifact that exists only on one worker?
6. Which policy controls are session hints versus catalog configuration versus
   an operator API? Session hints must never alter correctness.
7. Can a materialization safely use DuckDB's SQL semantics for the pushed
   filter/projection, or must it be produced from Trino-evaluated rows to avoid
   semantic differences? The initial scope should exclude expressions where
   equivalence is not proven.

## Acceptance bar

Before automatic promotion is considered successful:

- Direct and artifact/partial-artifact paths agree for eligible queries,
  including source additions, removals, same-size replacements, schema changes,
  nulls, timestamps, and predicate edge cases.
- Disk use is bounded and attributable; no cache is an unbounded temp folder.
- A burst of identical requests results in one build, not one build per query.
- Every optimization decision has a queryable explanation.
- An unavailable sidecar, absent local artifact, failed build, or unsupported
  platform cleanly falls back to normal Parquet reads.
- Quack's local-path visibility and process-wide DuckDB cache behavior are
  tested, not assumed.

The guiding rule is simple: **source snapshots are authoritative; a cached scan
is only a hint.** If that remains true in every failure and invalidation path,
this can become a very useful BI acceleration feature without becoming a
second, hidden storage system.
