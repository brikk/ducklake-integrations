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

### [duckdb-trino-parity-extension](duckdb-trino-parity-extension/) (git submodule)

Native DuckDB extension that provides `trino_<name>(...)` functions with semantics matching
Trino's documented behaviour on Unicode and other edge cases where DuckDB's built-ins
diverge. Required at attach time by the trino-ducklake plugin's predicate-pushdown layer;
bundled into the plugin jar as platform-specific resources.

## Getting Started

This repo carries two git submodules:

- `duckdb-trino-parity-extension` — the native DuckDB extension consumed by trino-ducklake at
  attach time. Itself carries the `duckdb` and `extension-ci-tools` submodules from the
  upstream DuckDB extension template.
- `jvm/trino-ducklake/ducklake-web` — the DuckLake specification, used as reference docs.

Clone with all submodules in one shot:

```shell
git clone --recurse-submodules https://github.com/<org>/ducklake-integrations.git
```

If already cloned without submodules, initialize them (recursive picks up the nested
submodules inside the extension):

```shell
git submodule update --init --recursive
```

### Building

The trino-ducklake plugin jar bundles the trino_parity DuckDB extension binary for each
platform it can find at build time. Build the extension first, then the plugin:

```shell
# Build the host-platform extension (~30 min first time — DuckDB + statically linked ICU)
cd duckdb-trino-parity-extension
GEN=ninja make

# Optional: cross-build for Linux to enable the connector's Quack-engine tests on macOS
make linux-arm64    # native on Apple Silicon
make linux-amd64    # via Rosetta/qemu emulation, slower

# Build the Trino plugin (gradle picks up every available build/<platform>/... binary)
cd ../jvm
./gradlew build
```

See [duckdb-trino-parity-extension/README.md](duckdb-trino-parity-extension/README.md) for
the extension's prerequisites (vcpkg, ninja, ccache) and platform notes. See the
[JVM project README](jvm/README.md) for JVM-side build prerequisites (Java 25, Docker).

## License

See [LICENSE](LICENSE).
