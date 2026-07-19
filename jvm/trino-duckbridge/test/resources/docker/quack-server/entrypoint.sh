#!/bin/sh
# Stand up a Quack RPC listener on 0.0.0.0:${QUACK_PORT} backed by an in-process
# DuckDB. Clients (the test JVM's quack-jdbc driver) connect via
# jdbc:quack://host:port and run SQL directly server-side.
#
# The trailing `sleep infinity` inside the brace group holds the writer side of
# the pipe open so duckdb (running in batch mode when stdin is a pipe) blocks on
# read forever, keeping the listener alive. Container teardown tears both down.

set -eu

PORT="${QUACK_PORT:-9494}"
TOKEN="${QUACK_TOKEN:-duckbridge-test-token}"

{
    printf "LOAD quack;\n"
    # allow_other_hostname is required to bind to 0.0.0.0 — Quack refuses
    # non-localhost binds by default. Safe here because the container network
    # namespace makes 0.0.0.0 mean "this container's interfaces only".
    printf "SELECT * FROM quack_serve('quack://0.0.0.0:%s/', token := '%s', allow_other_hostname := true);\n" "$PORT" "$TOKEN"
    exec sleep infinity
# -unsigned: server-side LOAD of the locally-built (unsigned)
# trino_parity.duckdb_extension requires allow_unsigned_extensions at process
# start (a runtime SET is rejected). No-op for a server never asked to LOAD one.
} | duckdb -unsigned
