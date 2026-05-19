#!/usr/bin/env bash
# Live-FE smoke driver: rebuilds the plugin zip, drops it into the bind mount,
# brings the substrate + Doris cluster up, waits for BE registration, and runs
# the SHOW CATALOGS / SHOW DATABASES / SHOW TABLES smoke SQL.
#
# Usage:
#   ./smoke.sh             # one-shot: build → drop plugin → up → drive
#   ./smoke.sh --no-build  # skip plugin rebuild (assume zip is current)
#   ./smoke.sh --down      # tear everything down
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JVM_ROOT="$(cd "${HERE}/../.." && pwd)"
TRINO_COMPOSE="${JVM_ROOT}/trino-ducklake/compose/docker-compose.yml"
PLUGIN_DIR="${HERE}/plugin/ducklake"
PLUGIN_ZIP_GLOB="${JVM_ROOT}/doris-ducklake/build/distributions/doris-ducklake-*-plugin.zip"

DO_BUILD=1
DOWN=0
for arg in "$@"; do
    case "$arg" in
        --no-build) DO_BUILD=0 ;;
        --down)     DOWN=1 ;;
        -h|--help)
            sed -n '2,9p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

log() { printf '\033[1;36m[smoke]\033[0m %s\n' "$*"; }

if [[ $DOWN -eq 1 ]]; then
    log "Tearing down doris-ducklake stack…"
    docker compose -f "${HERE}/docker-compose.yml" down -v
    log "(Substrate at ${TRINO_COMPOSE} left running — use its own compose to stop it.)"
    exit 0
fi

# 1. Substrate.
log "Bringing up DuckLake substrate (Postgres + MinIO + seed)…"
docker compose -f "${TRINO_COMPOSE}" up -d postgres minio create-bucket bootstrap

# Wait for the substrate's bootstrap container to exit successfully — it's the
# one that seeds TPC-H into DuckLake. After it exits (0) the substrate is ready.
log "Waiting for substrate bootstrap to complete…"
deadline=$((SECONDS + 300))
while :; do
    state=$(docker inspect -f '{{.State.Status}}' trino-ducklake-bootstrap 2>/dev/null || echo missing)
    exit_code=$(docker inspect -f '{{.State.ExitCode}}' trino-ducklake-bootstrap 2>/dev/null || echo -1)
    if [[ "$state" == "exited" && "$exit_code" == "0" ]]; then
        log "Substrate ready."
        break
    fi
    if (( SECONDS >= deadline )); then
        log "Substrate bootstrap did not finish in time."
        docker logs trino-ducklake-bootstrap | tail -40 || true
        exit 1
    fi
    sleep 2
done

# 2. Plugin zip — rebuild and unpack into the bind mount.
if [[ $DO_BUILD -eq 1 ]]; then
    log "Building plugin zip…"
    (cd "${JVM_ROOT}" && ./gradlew :doris-ducklake:assemble -q)
fi

zip_path=$(ls -t ${PLUGIN_ZIP_GLOB} 2>/dev/null | head -1 || true)
if [[ -z "$zip_path" ]]; then
    log "No plugin zip found under ${PLUGIN_ZIP_GLOB}"
    exit 1
fi
log "Installing plugin from $(basename "$zip_path") into named volume…"
# Podman on macOS doesn't reliably propagate bind-mount contents from host
# into the VM, so we use a named volume + docker cp (daemon-API streaming,
# bypasses host↔VM file sharing). The FE service mounts the same volume at
# /opt/apache-doris/fe/plugins/connector/ducklake.
VOL_NAME=doris-ducklake-dev_fe-plugin-ducklake
docker volume create "${VOL_NAME}" >/dev/null
# Create a helper container that we'll cp into, then run unzip in.
helper=$(docker create -v "${VOL_NAME}:/dest" alpine \
    sh -c 'apk add --no-cache unzip >/dev/null 2>&1 && rm -rf /dest/* /dest/.[!.]* 2>/dev/null; cd /dest && unzip -oq /tmp/plugin.zip')
docker cp "$zip_path" "$helper":/tmp/plugin.zip
docker start -a "$helper"
docker rm "$helper" >/dev/null

# 3. Bring up FE + BE.
log "Starting Doris FE + BE…"
docker compose -f "${HERE}/docker-compose.yml" up -d

# 4. Wait for FE health (mysql:9030 SELECT 1).
log "Waiting for FE to accept SQL…"
deadline=$((SECONDS + 180))
while :; do
    if docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "SELECT 1" >/dev/null 2>&1; then
        log "FE up."
        break
    fi
    if (( SECONDS >= deadline )); then
        log "FE never came up — last 80 lines of fe.log:"
        docker exec doris-ducklake-fe sh -c 'tail -80 /opt/apache-doris/fe/log/fe.log' 2>&1 || true
        exit 1
    fi
    sleep 2
done

# 5. Wait for BE to register.
log "Waiting for BE registration (SHOW BACKENDS → IsAlive)…"
deadline=$((SECONDS + 180))
while :; do
    alive=$(docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "SHOW BACKENDS" 2>/dev/null | awk -F'\t' '$10=="true"{n++} END{print n+0}')
    if [[ "${alive:-0}" -ge 1 ]]; then
        log "BE registered and alive."
        break
    fi
    if (( SECONDS >= deadline )); then
        log "BE failed to register — SHOW BACKENDS output:"
        docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>&1 || true
        exit 1
    fi
    sleep 3
done

# 6. Wire-compat sanity gates.
log "Sanity gates (SHOW BACKENDS / SHOW FRONTENDS / SELECT 1):"
docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "
    SHOW BACKENDS\G
    SHOW FRONTENDS\G
    SELECT 1 AS wire_compat_ok;
" 2>&1 | tail -40

# 7. Drive the plugin.
#
# CREATE CATALOG carries s3.* credentials so the BE's parquet reader can reach
# MinIO. DuckLakeScanPlanProvider extracts the s3.* / AWS_* / use_path_style
# subset of catalog properties and forwards them through populateScanLevelParams
# onto TFileScanRangeParams.properties; the BE's S3ObjStorage normaliser
# canonicalises (s3.access_key → AWS_ACCESS_KEY, etc.).
log "Creating DuckLake catalog + listing schemas/tables…"
docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "
    DROP CATALOG IF EXISTS dl;
    CREATE CATALOG dl PROPERTIES (
        'type'              = 'ducklake',
        'metadata.url'      = 'jdbc:postgresql://trino-ducklake-postgres:5432/ducklake',
        'metadata.user'     = 'ducklake',
        'metadata.password' = 'ducklake',
        'storage.warehouse' = 's3://ducklake/data/',
        's3.endpoint'       = 'http://trino-ducklake-minio:9000',
        's3.region'         = 'us-east-1',
        's3.access_key'     = 'minioadmin',
        's3.secret_key'     = 'minioadmin',
        'use_path_style'    = 'true'
    );
    SHOW CATALOGS;
    SHOW DATABASES FROM dl;
    SWITCH dl;
    SHOW DATABASES;
    SHOW TABLES FROM tpch;
    DESC dl.tpch.orders;
    DESC dl.tpch.lineitem;
" 2>&1

# 8. Step 5 of the roadmap: live SELECT * through to the BE parquet reader.
log "SELECT * FROM dl.tpch.orders LIMIT 5 …"
docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "
    USE dl.tpch;
    SELECT * FROM orders LIMIT 5;
" 2>&1

log "Smoke complete."
