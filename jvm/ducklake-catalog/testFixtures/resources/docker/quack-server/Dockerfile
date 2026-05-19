# DuckDB CLI hosting a Quack RPC listener that serves as the remote DuckLake
# catalog backend in tests. Client side is the DuckDB JDBC driver running
# in-process inside our JVM; this container is the out-of-process DatabaseInstance
# that breaks the connection-id self-RPC loop documented upstream in
# duckdb/ducklake's scripts/run_quack_tests.py.
#
# Pinned to match duckdb_jdbc:1.5.2.0 on the client side. Bumping this version
# requires the matching bump in jvm/gradle/libs.versions.toml (key: `duckdb`).

FROM debian:bookworm-slim

ARG DUCKDB_VERSION=v1.5.2

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl unzip \
 && rm -rf /var/lib/apt/lists/*

# Detect arch via `uname -m` rather than relying on $TARGETARCH — the latter is
# a buildkit-only built-in and isn't reliably populated under podman / vanilla
# `docker build`. DuckDB's GitHub release assets use `amd64` / `arm64` (not
# `aarch64`).
RUN set -eu; \
    case "$(uname -m)" in \
        x86_64)  DUCKDB_ARCH=amd64 ;; \
        aarch64) DUCKDB_ARCH=arm64 ;; \
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-${DUCKDB_ARCH}.zip" -o /tmp/duckdb.zip; \
    unzip /tmp/duckdb.zip -d /usr/local/bin; \
    rm /tmp/duckdb.zip; \
    chmod +x /usr/local/bin/duckdb

# Pre-pull the Quack extension at image build time so container startup doesn't
# pay the network round-trip. core_nightly is required: Quack as a DuckLake
# catalog backend was merged upstream 2026-05-12 (duckdb/ducklake#1151) and the
# stable extension channel hadn't picked it up at the time this image was pinned.
RUN duckdb -unsigned -c "INSTALL quack FROM core_nightly;"

COPY entrypoint.sh /usr/local/bin/quack-entrypoint.sh
RUN chmod +x /usr/local/bin/quack-entrypoint.sh

EXPOSE 9494
ENTRYPOINT ["/usr/local/bin/quack-entrypoint.sh"]
