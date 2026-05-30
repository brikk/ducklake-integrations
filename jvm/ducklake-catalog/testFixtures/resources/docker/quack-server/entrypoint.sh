#!/bin/sh
# Stand up a Quack RPC listener on 0.0.0.0:${QUACK_PORT} backed by an in-process
# DuckDB. The container's DuckDB hosts the DuckLake catalog metadata; clients
# attach via `ATTACH 'ducklake:quack:host:port'` from a separate DuckDB instance.
#
# Why the trailing `sleep infinity` inside the brace group: DuckDB's CLI runs in
# non-interactive batch mode when stdin is a pipe — it processes the script and
# exits on EOF, taking the listener down with it. Holding the writer side of the
# pipe open via `sleep infinity` blocks duckdb on read forever, keeping the
# listener alive. Container teardown (SIGTERM/SIGKILL) tears both processes down.

set -eu

PORT="${QUACK_PORT:-9494}"
TOKEN="${QUACK_TOKEN:-ducklake-test-token}"

{
    printf "LOAD quack;\n"
    # allow_other_hostname is required to bind to 0.0.0.0 — Quack refuses
    # non-localhost binds by default. Safe here because the container's network
    # namespace makes 0.0.0.0 mean "this container's interfaces only".
    printf "SELECT * FROM quack_serve('quack://0.0.0.0:%s/', token := '%s', allow_other_hostname := true);\n" "$PORT" "$TOKEN"
    exec sleep infinity
# -unsigned: client-side LOAD of trino_parity.duckdb_extension (forwarded over
# Quack RPC from the trino-ducklake connector) requires allow_unsigned_extensions
# at server startup. Setting `SET allow_unsigned_extensions=true` after the DB
# is running is rejected — must be a CLI flag at process start. No-op for any
# server that never gets asked to LOAD an unsigned extension.
} | duckdb -unsigned
