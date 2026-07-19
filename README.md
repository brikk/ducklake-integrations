# ducklake-integrations

[DuckLake](https://ducklake.select) integration layer for JVM-based query engines.

Both modules below are full-featured implementations of the DuckLake catalog with only minor
gaps against the spec that are actively being resolved.

## Modules

### [ducklake-catalog](jvm/ducklake-catalog/)

Shared JVM library providing the DuckLake catalog abstraction — JDBC-based metadata access,
connection pooling, write transactions, type conversion, and partition computation. Used as a
dependency by engine-specific connectors.

### [trino-ducklake](jvm/trino-ducklake/)

Trino connector plugin for DuckLake. Complete read and write support including DDL, DML,
row-level mutations (DELETE/UPDATE/MERGE), ALTER TABLE, partitioned writes, time travel,
metadata tables, and bidirectional DuckDB compatibility.

See the [trino-ducklake README](jvm/trino-ducklake/README.md) for features, build
instructions, and configuration.

### [doris-ducklake](jvm/doris-ducklake/) (in development)

Apache Doris connector plugin for DuckLake, built as an out-of-tree plugin against Doris's
new **catalog SPI**. That SPI is not yet in a stable Doris release — it is landing upstream
over the coming months (tracked on the `branch-catalog-spi` line) and ships in an upcoming
Doris release — so this module targets that forthcoming release rather than a current one.

Read-side support is well advanced (schema/type mapping, predicate and count pushdown,
partition and bucket pruning, time travel, MVCC snapshot pinning), with write support
progressing (DDL, INSERT/CTAS including partitioned and bucketed writes, and the
`expire_snapshots` maintenance procedure). Some features remain gated on upstream Doris work
(notably merge-on-read DELETE/UPDATE/MERGE, which needs row-identity infrastructure the plugin
SPI does not yet expose). Read correctness is validated against the same upstream corpus via a
Doris engine adapter in the conformance harness below.

See the [doris-ducklake README](jvm/doris-ducklake/README.md) for current status, the build,
and the local FE+BE compose smoke setup.

### [ducklake-corpus-replay](jvm/ducklake-corpus-replay/)

Conformance harness that replays **DuckLake's upstream sqllogictest corpus** (the reference
C++ implementation's own `test/sql/` suite, pinned as a git submodule) through an embedded
DuckDB oracle, validating results against the upstream golden text — then mirrors every lake
read through an engine adapter (Trino and Doris) live-vs-live against the oracle
on a shared PostgreSQL catalog. Engine-agnostic core (`ReplayReadEngine` seam); adapters live
in the engine modules. The corpus grows with every upstream release, so spec-conformance
coverage compounds automatically.

## Getting Started

This repo carries two git submodules:

- `jvm/trino-ducklake/ducklake-web` — the DuckLake specification, used as reference docs.
- `jvm/ducklake-corpus-replay/ducklake` — the upstream `duckdb/ducklake` reference
  implementation, pinned to the release matching our DuckDB version; its `test/sql/` corpus is
  replayed by the conformance harness above.

Clone with all submodules in one shot:

```shell
git clone --recurse-submodules https://github.com/<org>/ducklake-integrations.git
```

If already cloned without submodules, initialize them:

```shell
git submodule update --init --recursive
```

### Building

The `trino-ducklake` plugin is parquet-only and needs no native extension:

```shell
cd jvm
./gradlew build
```

See the [JVM project README](jvm/README.md) for JVM-side build prerequisites
(Java 25, Docker).

The native `trino_parity` DuckDB extension (predicate-pushdown functions) now lives with the
standalone Trino → DuckDB connector at
[github.com/brikk/duckbridge](https://github.com/brikk/duckbridge).

## License

See [LICENSE](LICENSE).
