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

## Getting Started

Clone with submodules to include the [DuckLake specification](https://github.com/duckdb/ducklake-web):

```shell
git clone --recurse-submodules https://github.com/<org>/ducklake-integrations.git
```

If already cloned, initialize submodules with:

```shell
git submodule update --init --recursive
```

See the [JVM project README](jvm/README.md) for build prerequisites (Java 25, Docker).

```shell
cd jvm
./gradlew build
```

## License

See [LICENSE](LICENSE).
