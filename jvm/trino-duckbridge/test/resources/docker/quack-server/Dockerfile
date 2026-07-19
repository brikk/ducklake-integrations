# DuckDB CLI hosting a Quack RPC listener — the remote server the duckbridge T3
# (quack-jdbc) transport connects to in integration tests. The in-process client
# is gizmo's quack-jdbc driver running inside the test JVM; this container is the
# out-of-process DuckDB the driver speaks the Quack wire protocol to.
#
# Base image: debian:trixie-slim (GLIBC 2.41). The DuckLake test image uses
# bookworm (GLIBC 2.36), but the locally-built trino_parity.duckdb_extension
# links against GLIBC 2.38, so it fails to LOAD on bookworm ("GLIBC_2.38 not
# found"). trixie's 2.41 satisfies it. Pin duckdb to match duckdb_jdbc:1.5.4.0
# on the client side.

FROM debian:trixie-slim

ARG DUCKDB_VERSION=v1.5.4

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl unzip \
 && rm -rf /var/lib/apt/lists/*

# Detect arch via `uname -m` (TARGETARCH is buildkit-only and unreliable under
# podman / vanilla docker build). DuckDB release assets use amd64 / arm64.
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

# Pre-pull the Quack core extension at build time so container startup pays no
# network round-trip. Quack is a core extension as of DuckDB 1.5.3.
RUN duckdb -c "INSTALL quack;"

COPY entrypoint.sh /usr/local/bin/quack-entrypoint.sh
RUN chmod +x /usr/local/bin/quack-entrypoint.sh

EXPOSE 9494
ENTRYPOINT ["/usr/local/bin/quack-entrypoint.sh"]
