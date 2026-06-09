#!/usr/bin/env bash
# Live-FE smoke driver: rebuilds the plugin zip, drops it into the bind mount,
# brings the substrate + Doris cluster up, waits for BE registration, and runs
# the read smoke (SHOW/DESC/SELECT *), the Step-7 delete exercise, and the W2
# INSERT end-to-end (Doris writes a DuckLake table; read back via Doris + DuckDB).
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

# Doris's init_fe.sh appends `priority_networks` to fe.conf at every boot. The
# compose mounts a gitignored runtime copy (not the tracked file) so those appends
# never pollute the repo. Stage it fresh from the pristine tracked fe.conf here, so
# both the `up` and `--down` compose calls below find the mount source present.
cp "${HERE}/fe.conf" "${HERE}/.fe.conf.runtime"

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

# 9. Step 7 of the roadmap: position-delete plumbing exercise.
#
# Plan (against a dedicated tpch.step7_orders table so tpch.orders stays
# clean for the Step 5 SELECT *):
#   (a) issue DELETE through DuckDB+DuckLake. step7-delete.py first sets
#       data_inlining_row_limit=0 to force the file-based delete path
#       for THIS SMOKE only — production users will produce both inline
#       and file-based deletes and the connector must handle whatever
#       DuckDB writes. The smoke flips the option as fixture setup,
#       not as a usage prescription. Inline-delete handling is tracked
#       as Step 7.5 (see friction log 2026-05-19 entry on inline deletes);
#   (b) verify ducklake_delete_file got a fresh row pointing at a
#       position-delete parquet — that's the FE-observable signal that
#       the Step 7 path is reachable from the catalog side;
#   (c) drop+recreate the catalog so Doris re-resolves snapshot fresh
#       (REFRESH CATALOG was not enough on the first bring-up);
#   (d) try SELECT COUNT(*) through Doris. As of 2026-05-19 this fails
#       with `[CORRUPTION]Not nullable column has null values` because
#       DuckLake writes the position-delete file with OPTIONAL columns
#       and the BE iceberg reader expects REQUIRED (friction log entry
#       on DuckLake delete-file parquet nullability). We log the
#       expected failure but do not fail the smoke — the FE work shipped;
#       end-to-end correctness is blocked on the BE/DuckLake fix.
DELETE_COUNT=7

log "Issuing DELETE of $DELETE_COUNT rows through DuckDB+DuckLake (on tpch.step7_orders)…"
# step7-delete.py is a single file so a bind mount survives the
# host→podman-VM file-sharing path (the previous-bring-up lesson about bind
# mounts failing applies to directory trees with jars, not single files).
docker run --rm \
    --network trino-ducklake-dev_default \
    -v "${HERE}/step7-delete.py:/script.py:ro" \
    -e PG_HOST=trino-ducklake-postgres \
    -e PG_DB=ducklake \
    -e PG_USER=ducklake \
    -e PG_PASSWORD=ducklake \
    -e S3_ENDPOINT=trino-ducklake-minio:9000 \
    -e S3_KEY_ID=minioadmin \
    -e S3_SECRET=minioadmin \
    -e DATA_PATH=s3://ducklake/data/ \
    -e DELETE_COUNT="$DELETE_COUNT" \
    python:3.12-slim sh -c '
        set -e
        pip install --quiet --no-cache-dir "duckdb==1.5.2"
        python /script.py
    ' 2>&1 | sed "s/^/  [duckdb] /"

log "Verifying ducklake_delete_file got a fresh row (active at the latest snapshot)…"
active_deletes=$(docker exec trino-ducklake-postgres psql -U ducklake -d ducklake -tA -c "
    SELECT COUNT(*) FROM ducklake_delete_file WHERE end_snapshot IS NULL;
" 2>&1 | tail -1)
if [[ "$active_deletes" -lt 1 ]]; then
    log "FAIL: no active delete file row in ducklake_delete_file. DELETE may have inlined instead — check ducklake_inlined_delete_*."
    exit 1
fi
log "  ducklake_delete_file has $active_deletes active row(s) — FE-side Step 7 plumbing is exercised."

log "Refreshing Doris catalog so the new snapshot is visible…"
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
" 2>&1 | tail -5

log "Attempting SELECT COUNT(*) via Doris on the table with the new delete file…"
# Capture output; either success (count == 100 - DELETE_COUNT) or the known
# BE error. Don't propagate failure to smoke exit code — the FE wire-format
# work is what this smoke verifies; downstream BE parquet-nullability fix
# is tracked in ducklake-doris-friction.md (2026-05-19 entry).
set +e
output=$(docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "
    SELECT COUNT(*) FROM dl.tpch.step7_orders;
" 2>&1)
status=$?
set -e
if [[ $status -eq 0 ]]; then
    expected=$((100 - DELETE_COUNT))
    if [[ "$output" == "$expected" ]]; then
        log "Step 7 GREEN: Doris saw $output rows (expected $expected). Deletes propagated end-to-end."
    else
        log "Step 7 PARTIAL: Doris returned $output rows, expected $expected."
    fi
else
    if echo "$output" | grep -q "Not nullable column has null values"; then
        log "Step 7 FE-side OK; BE-side blocked on known parquet-nullability gap."
        log "  See ducklake-doris-friction.md (2026-05-19) for the upstream fix."
        log "  BE error: $(echo "$output" | grep -oE 'reason = \[[^]]+\][^[:cntrl:]]*' | head -1)"
    else
        log "Step 7 unexpected error from Doris:"
        echo "$output" | tail -10
        exit 1
    fi
fi

# 10. W2 (INSERT) end-to-end: Doris writes to a DuckLake table via the BE Iceberg
# sink (DuckLakeWritePlanProvider → TIcebergTableSink), the connector commits the
# file (DuckLakeConnectorTransaction → catalog.commitInsert), then we read it back
# BOTH through Doris and through DuckDB+DuckLake — the cross-engine check that
# proves Doris wrote DuckLake-compatible Parquet (field_id == column_id, footer ok).
#
# Requires the FE to carry "ducklake" in CatalogFactory.SPI_READY_TYPES, else INSERT
# is rejected as unsupported. Doris can't CREATE TABLE on a plugin catalog yet (W1),
# so the empty target is created via DuckDB first.
w2_helper() {  # $1 = MODE (create|verify)
    docker run --rm \
        --network trino-ducklake-dev_default \
        -v "${HERE}/w2-insert.py:/script.py:ro" \
        -e MODE="$1" \
        -e PG_HOST=trino-ducklake-postgres -e PG_DB=ducklake \
        -e PG_USER=ducklake -e PG_PASSWORD=ducklake \
        -e S3_ENDPOINT=trino-ducklake-minio:9000 \
        -e S3_KEY_ID=minioadmin -e S3_SECRET=minioadmin \
        -e DATA_PATH=s3://ducklake/data/ \
        python:3.12-slim sh -c '
            set -e
            pip install --quiet --no-cache-dir "duckdb==1.5.2"
            python /script.py
        ' 2>&1 | sed "s/^/  [duckdb] /"
}

refresh_dl_catalog() {  # DROP+CREATE so Doris re-resolves the latest snapshot/schema
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
    " 2>&1 | tail -3
}

log "W2: creating empty target lake.tpch.doris_w via DuckDB…"
w2_helper create
log "W2: refreshing Doris catalog so it sees tpch.doris_w…"
refresh_dl_catalog

log "W2: INSERT INTO dl.tpch.doris_w via Doris (BE Iceberg sink writes the Parquet)…"
set +e
insert_out=$(docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "
    INSERT INTO dl.tpch.doris_w VALUES (1,'alice'),(2,'bob'),(3,'charlie');
" 2>&1)
insert_status=$?
set -e
echo "$insert_out" | tail -8

if [[ $insert_status -ne 0 ]]; then
    if echo "$insert_out" | grep -qiE "does not support|not ready|SPI_READY|no.*write"; then
        log "W2 BLOCKED: INSERT not routed — FE is missing \"ducklake\" in CatalogFactory.SPI_READY_TYPES."
        log "  Add it to the FE build (see doris-fe-build-macos memory + ducklake-doris-todo-write.md), rebuild, rerun."
    else
        log "W2 ERROR: INSERT failed at the FE/BE. See the output above + fe.log/be.log for the sink/commit error."
    fi
else
    log "W2: INSERT accepted — reading back through Doris…"
    doris_rows=$(docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "
        SELECT COUNT(*) FROM dl.tpch.doris_w;
    " 2>&1 | tail -1)
    docker exec doris-ducklake-fe mysql -h127.0.0.1 -P9030 -uroot -e "
        SELECT * FROM dl.tpch.doris_w ORDER BY id;
    " 2>&1
    log "W2: cross-engine read-back via DuckDB+DuckLake (proves the Parquet is DuckLake-readable)…"
    w2_helper verify
    log "W2: catalog cross-check — active data files for doris_w…"
    docker exec trino-ducklake-postgres psql -U ducklake -d ducklake -tA -c "
        SELECT COUNT(*) FROM ducklake_data_file f
        JOIN ducklake_table t ON t.table_id = f.table_id
        WHERE t.table_name = 'doris_w' AND f.end_snapshot IS NULL;
    " 2>&1 | tail -1 | sed 's/^/  active data files: /'
    if [[ "${doris_rows:-0}" == "3" ]]; then
        log "W2 GREEN: Doris INSERT wrote a DuckLake file and read back 3 rows end-to-end. 🎉"
    else
        log "W2 PARTIAL: Doris read back ${doris_rows:-?} rows (expected 3) — inspect the cross-engine output above."
    fi
fi

log "Smoke complete."
