# Trino + DuckLake local dev stack

A self-contained Docker Compose stack for hacking on the `trino-ducklake` connector:

- **Trino 480** with this repo's plugin mounted from `build/trino-plugin/`
- **PostgreSQL 18** as the DuckLake metadata catalog
- **MinIO** as the S3-compatible object store for Parquet data
- A one-shot **bootstrap** container that uses DuckDB's `ducklake` extension to
  initialize the catalog schema in Postgres, optionally seeded with TPC-H data
  via DuckDB's `tpch` extension

Internal hostnames (`postgres`, `minio`, `trino`) are used for service-to-service
traffic; host ports are exposed for local debugging tools.

## Prerequisites

- Docker Desktop, Podman+Docker compose, or Colima
- Java 25 + Gradle for assembling the plugin (see the parent
  [README](../README.md))

## Start it

The Trino container mounts the assembled plugin directory directly, so build
the plugin first:

```bash
cd ../..                         # jvm/
./gradlew :trino-ducklake:pluginAssemble
cd trino-ducklake/compose
docker compose up -d
```

Startup order is enforced by `depends_on` health gates:

1. `postgres` and `minio` come up and pass health checks
2. `create-bucket` (one-shot `mc`) creates the `ducklake` bucket
3. `bootstrap` (one-shot Python + duckdb) installs the `ducklake` extension,
   `ATTACH`es the catalog (which writes ~28 metadata tables into Postgres),
   then optionally generates TPC-H data into `lake.tpch.*`
4. `trino` starts, loads the plugin, and registers the `ducklake` catalog

Re-running `docker compose up` is safe — the bootstrap detects an
already-initialized catalog and skips both `ATTACH` setup and TPC-H generation.

## Exposed ports

| Service           | Host port | Container port | Notes                                         |
|-------------------|-----------|----------------|-----------------------------------------------|
| Trino             | **9080**  | 8080           | No auth, plain HTTP                           |
| PostgreSQL        | **9432**  | 5432           | `ducklake` / `ducklake` / `ducklake`          |
| MinIO API (S3)    | 9000      | 9000           | `minioadmin` / `minioadmin`                   |
| MinIO console     | 9001      | 9001           | http://localhost:9001                         |

The non-default Trino (9080) and Postgres (9432) host ports are intentional —
8080 collides with practically every Java server and 5432 with any local
Postgres install.

## Connecting

### Trino CLI from host

```bash
# If you have the trino CLI installed locally
trino --server http://localhost:9080 --catalog ducklake

# Or shell into the container and use the bundled CLI
docker exec -it trino-ducklake-trino trino --catalog ducklake
```

Try a query:

```sql
SHOW SCHEMAS FROM ducklake;
SELECT count(*) FROM ducklake.tpch.lineitem;

SELECT n.n_name, count(*) AS orders
FROM ducklake.tpch.nation n
JOIN ducklake.tpch.customer c ON c.c_nationkey = n.n_nationkey
JOIN ducklake.tpch.orders   o ON o.o_custkey   = c.c_custkey
GROUP BY n.n_name
ORDER BY orders DESC
LIMIT 5;
```

### Trino over JDBC (DBeaver, IntelliJ, JDBC apps)

There is no authentication on this stack, but Trino still requires a user
identity on every request. The JDBC driver does not auto-fill one, so include
`sessionUser` in the URL (or set it as a connection property):

```
jdbc:trino://localhost:9080?sessionUser=trino
```

Without `sessionUser`, the driver throws "Username property (`user`) must be
set" or the server rejects the request. The user value is arbitrary — Trino
accepts any string when authentication is disabled.

To default the catalog and schema so you don't have to fully-qualify table
names:

```
jdbc:trino://localhost:9080?sessionUser=trino&catalog=ducklake&schema=tpch
```

### PostgreSQL (catalog metadata)

```bash
psql 'postgres://ducklake:ducklake@localhost:9432/ducklake'
\dt ducklake_*
```

The DuckLake spec tables (`ducklake_snapshot`, `ducklake_data_file`, etc.) live
in the `public` schema.

### DuckDB from host (cross-engine)

DuckDB on your host can attach to the same lake as Trino — the data lives in
S3 (MinIO) and the metadata in Postgres, both reachable from localhost:

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);

ATTACH 'ducklake:postgres:dbname=ducklake host=localhost port=9432 user=ducklake password=ducklake'
    AS lake (DATA_PATH 's3://ducklake/data/');

SELECT count(*) FROM lake.tpch.lineitem;
```

Trino-written tables show up in DuckDB and vice versa — that's the point of
the cross-engine compatibility tests.

### MinIO

Browse data files at http://localhost:9001 (login `minioadmin` / `minioadmin`)
or with `mc`:

```bash
docker exec trino-ducklake-minio sh -c \
  "mc alias set me http://localhost:9000 minioadmin minioadmin && \
   mc ls --recursive me/ducklake/"
```

## Configuration knobs

All in `.env`:

| Variable               | Default               | Purpose                                   |
|------------------------|-----------------------|-------------------------------------------|
| `PLUGIN_VERSION`       | `480-1-ALPHA`         | Must match `:trino-ducklake` Gradle version (the assembled directory name) |
| `TRINO_VERSION`        | `480`                 | `trinodb/trino` image tag                 |
| `POSTGRES_VERSION`     | `18-alpine`           |                                           |
| `MINIO_TAG`            | release tag           | `minio/minio` image tag                   |
| `MC_TAG`               | release tag           | `minio/mc` image tag (separate from server!) |
| `DUCKDB_VERSION`       | `1.5.2`               | `duckdb` pip package version, matches the version the plugin is tested against |
| `POSTGRES_DB/USER/PASSWORD` | `ducklake` x3    | Catalog DB credentials                    |
| `MINIO_ROOT_USER/PASSWORD`  | `minioadmin` x2  | MinIO root credentials                    |
| `MINIO_BUCKET`         | `ducklake`            | Bucket holding all Parquet                |
| `TPCH_SCALE_FACTOR`    | `0.01`                | DuckDB `dbgen` scale factor; `0` skips    |
| `TPCH_SCHEMA`          | `tpch`                | Schema in the lake to receive seed tables |

### TPC-H scale factors (rough sizes)

| `sf`  | Lineitem rows | On-disk Parquet |
|-------|---------------|-----------------|
| 0.01  | ~60K          | ~6 MB           |
| 0.1   | ~600K         | ~60 MB          |
| 1     | ~6M           | ~250 MB         |
| 10    | ~60M          | ~2.5 GB         |

If you change `TPCH_SCALE_FACTOR` after the lake has been seeded, the
bootstrap will *not* regenerate — it skips when `lake.tpch.*` already exists.
Wipe the volumes (below) to reseed at a different scale.

## Common operations

```bash
# Tail Trino logs
docker compose logs -f trino

# Re-run only the bootstrap (e.g. after wiping the postgres volume)
docker compose up bootstrap

# Stop the stack, keeping data volumes
docker compose stop

# Wipe everything (postgres data + minio bucket)
docker compose down -v
```

## How the catalog gets created

The bootstrap container runs `bootstrap/init_ducklake.py`, which:

1. Installs and loads DuckDB's `ducklake` and `httpfs` extensions
2. Creates an S3 secret pointing at the MinIO container
3. Calls `ATTACH 'ducklake:postgres:...' AS lake (DATA_PATH 's3://ducklake/data/')`
   — DuckDB writes the DuckLake spec tables into Postgres on first attach
4. If `TPCH_SCALE_FACTOR > 0` and `lake.tpch.*` is empty, calls
   `CALL dbgen(sf = ?)` and CTAS-copies each TPC-H table into the lake
5. `DETACH`es and exits

This mirrors how the test suite generates fixtures
(`test/src/dev/brikk/ducklake/trino/plugin/DucklakeCatalogGenerator.java`),
so the layout you get from `docker compose up` is the same shape the tests
exercise.

## Trino catalog properties

`trino/etc/catalog/ducklake.properties` is mounted read-only into the Trino
container at `/etc/trino/catalog/ducklake.properties`. The S3 block points at
the in-network `minio:9000` endpoint:

```properties
connector.name=ducklake

ducklake.catalog.database-url=jdbc:postgresql://postgres:5432/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=ducklake
ducklake.data-path=s3://ducklake/data/

fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.path-style-access=true
```

If you change this file, restart only Trino: `docker compose restart trino`.
