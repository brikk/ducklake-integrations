# DuckLake-on-Doris — Friction log

Running log of SPI / FE / BE surprises hit while implementing the DuckLake
`fe-connector` plugin against [apache/doris#62767](https://github.com/apache/doris/pull/62767).

For Doris fe-connector maintainers — each entry has a pickable upstream
fix. For future plugin authors — read top-to-bottom before starting; saves
hours of debugging.

Sister docs: [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md)
(canonical plan), [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md)
(one-shot architectural review), [`ducklake-doris-todo.md`](./ducklake-doris-todo.md)
(working state).

Entry shape: **Symptom** → **Root cause** (file:line) → **Workaround**
→ **Fix** (small, pickable). Newest first.

---

## 2026-05-19 · DuckLake position-delete files use OPTIONAL columns; Doris BE rejects them

**Symptom.** Step 7 smoke: DuckDB issues DELETE on `tpch.orders` (with
`data_inlining_row_limit = 0` so the DELETE materialises as a
`ducklake_delete_file` row + a position-delete parquet file). The FE plugin
correctly packs the delete file into `iceberg_params.delete_files`. BE
errors on read:

```
[INTERNAL_ERROR]Read parquet file s3://…/orders/…-delete.parquet failed,
reason = [CORRUPTION]Not nullable column has null values in parquet file
```

The delete file has zero actual nulls; the error is about parquet schema
nullability metadata, not row content.

**Root cause.** Two-layer schema mismatch between DuckLake's delete-file
parquet and what the BE iceberg reader expects:

1. **Column shape matches.** DuckLake writes `(file_path: VARCHAR, pos:
   BIGINT)` — same column names + types as the Iceberg
   [position-delete spec](https://iceberg.apache.org/spec/#position-delete-files).
2. **Nullability disagrees.** DuckLake writes both columns as
   `repetition_type = OPTIONAL`. Iceberg's spec marks them `required`,
   and Doris's BE parquet reader (`ScalarColumnReader<false, false>`,
   the not-nullable fast path) trusts the schema's NOT-NULL contract.
   The DuckLake-written parquet declares the columns as nullable, so
   the reader's NOT-NULL fast path raises rather than fall back to the
   nullable path.

Confirmed via `parquet_schema()`:

```
('file_path', 'BYTE_ARRAY', None, 'OPTIONAL', None, None, 'UTF8')
('pos',       'INT64',      None, 'OPTIONAL', None, None, 'INT_64')
```

**Workaround.** None on the FE side. Three honest options:
- Patch DuckDB to write `REQUIRED` repetition_type on DuckLake delete files.
- Patch the BE iceberg reader to widen the nullability contract on the
  Iceberg-spec'd delete-file columns (treat OPTIONAL-but-no-nulls as
  REQUIRED, falling through to the nullable path).
- Rewrite the delete file FE-side before handing it to the BE.
  Cost-prohibitive (per-query parquet rewrite).

**Fix (pickable, upstream coordination).**
- **DuckDB / DuckLake**: write delete-file parquet with `REQUIRED`
  columns for `file_path` + `pos`. The schema matches Iceberg's spec by
  intent (column names + types); nullability is the only deviation.
  One-line fix at the parquet writer for delete files.
- **Doris BE** (`ParquetReader` / `ScalarColumnReader`): treat
  OPTIONAL columns whose definition levels are all `1` as effectively
  required, OR explicitly route Iceberg-spec'd delete files through the
  nullable column path.

---

## 2026-05-19 · DuckLake defaults inline DELETEs into Postgres rows, not parquet files

**Symptom.** Issue `DELETE FROM lake.tpch.orders WHERE …` through DuckDB.
DuckDB sees the rows gone. `ducklake_delete_file` stays empty. Doris
sees no change. Catalog has a row in
`ducklake_inlined_delete_<table_id>(file_id, row_id, begin_snapshot)`.

**Root cause.** DuckLake's `DATA_INLINING_ROW_LIMIT` defaults non-zero.
DELETEs smaller than the limit are materialised as rows in per-table
`ducklake_inlined_delete_<tableId>` tables in the catalog database
rather than as `ducklake_delete_file` + position-delete parquet.

The Doris BE iceberg reader has no way to consume Postgres-side delete
rows; only file-based deletes route through `iceberg_params.delete_files`.

**Workaround.** Disable inlining for the test/dev catalog:

```sql
CALL lake.set_option('data_inlining_row_limit', '0');
```

(Syntax discovered by probing — `CALL lake.set_option(<key>, <value>)`.
`ducklake_options('lake')` lists the four built-in keys but doesn't show
this one until it's been set.)

After the option is set, subsequent DELETEs write to
`ducklake_delete_file` and a position-delete parquet — the format
[the FE plugin's Step 7 path expects](./ducklake-doris-todo.md#step-7--position-deletes-path-still-v1-scope).
Existing inline-delete rows stay where they are; they're not migrated
to files.

**Fix (FE plugin work).** Implement inline-delete handling on the FE
side. The library already exposes
`DucklakeCatalog#hasInlinedDeletes(tableId, snapshotId)` and
`getInlinedDeletes(tableId, snapshotId)` returning `Map<fileId,
Set<rowId>>`. Three options for honouring them at scan time:
- **Synthesize a parquet delete file** with `(file_path, pos)` rows on
  the FE side, drop it under a scratch location, hand it to the BE
  through `iceberg_params.delete_files`. Per-query parquet write.
- **Push the row IDs as a predicate** — Doris would need to support
  "exclude row indexes" pushdown; it doesn't today (file_scanner has no
  such hook).
- **Block reads when inline deletes are present** until upstream
  resolves a path. Honest, ergonomically poor.

The synthesize-parquet path is the only one that works without an
upstream change. Track as Step 7.5.

---

## 2026-05-19 · `ConnectorScanRange.getFileFormat()` is dead on the plugin-driven path

**Symptom.** Returning `"parquet"` from `ConnectorScanRange.getFileFormat()`
isn't enough. First live `SELECT *` failed:

```
[NOT_IMPLEMENTED_ERROR]Not supported create reader for table format:
iceberg / file format: FORMAT_JNI. cur path: s3://.../foo.parquet
```

**Root cause.** `PluginDrivenScanNode.getFileFormatType()` (~line 198)
reads `"file_format_type"` from `getScanNodeProperties()`, not from
`range.getFileFormat()`. Missing key silently defaults to `FORMAT_JNI`
in `mapFileFormatType()` (~line 689). The range-level method's javadoc
("determines the BE reader") is misleading.

**Workaround.** Emit the key from `getScanNodeProperties()`:

```java
props.put("file_format_type", "parquet");
```

**Fix.**
- Fall back to `range.getFileFormat()` when the property is absent, OR
- Add a `PROP_FILE_FORMAT_TYPE` constant on the API side + fix the
  range-level javadoc, OR
- Raise instead of silently defaulting to `FORMAT_JNI`.

---

## 2026-05-19 · BE S3 reader needs `AWS_*` keys verbatim; `s3.*` is silently dropped

**Symptom.** `CREATE CATALOG` and the scan range carried
`s3.endpoint=http://minio:9000`, `s3.access_key=minioadmin`, etc. BE rejected:

```
[INVALID_ARGUMENT]Invalid s3 conf, empty endpoint. cur path: s3://...
```

even with `params.properties["s3.endpoint"]` set on the wire. The error
reads like missing config, not an alias mismatch.

**Root cause.** `S3ClientFactory::convert_properties_to_s3_conf`
(`be/src/util/s3_util.cpp:541-619`) does literal lookups against:

```cpp
constexpr char S3_AK[]       = "AWS_ACCESS_KEY";
constexpr char S3_SK[]       = "AWS_SECRET_KEY";
constexpr char S3_ENDPOINT[] = "AWS_ENDPOINT";
constexpr char S3_REGION[]   = "AWS_REGION";
```

FE-side `S3ObjStorage.normalizeProperties` knows the `s3.*` aliases but
runs on a different path (FE-direct S3 opens, e.g. HMS metadata reads).
The parquet reader doesn't pass through it.

**Workaround.** In `populateScanLevelParams`, emit both forms:

```java
out.put("s3.endpoint",  "http://minio:9000");  // FE-form
out.put("AWS_ENDPOINT", "http://minio:9000");  // BE-form (the one that actually works)
```

`DuckLakeScanPlanProvider.canonicalAwsAlias()` is the static mapping.

**Fix.**
- Normalise `s3.*` → `AWS_*` engine-side after `populateScanLevelParams`, OR
- Make `convert_properties_to_s3_conf` accept both (~5-line BE switch), OR
- Document the contract in `populateScanLevelParams` javadoc:
  *"S3 creds must be `AWS_*`, HDFS `dfs.*`, …"*

---

## 2026-05-19 · `SPI_READY_TYPES` whitelist silently drops unknown ConnectorProviders

**Symptom.** Provider correctly registered via
`META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`,
`getType()` returns `"ducklake"`, jar on classpath. `CREATE CATALOG`
still fails:

```
ERROR: Unknown catalog type: ducklake
```

No FE log line saying the provider was discovered-but-rejected.

**Root cause.** `CatalogFactory.java` hardcodes
`SPI_READY_TYPES = {"jdbc", "es", "iceberg"}`. Providers outside that
set fall through to the legacy switch.

**Workaround.** One-line worktree patch (`+ "ducklake"`) re-applied
on every Doris release we deploy.

**Fix.**
- End state: drop the whitelist; any registered `ConnectorProvider`
  wins.
- Until then: log a warning when discovery skips a registered provider.
  The silent drop is the worst part.

---

## 2026-05-19 · BE position-delete dispatch keyed on the literal string `"iceberg"`

**Symptom.** Setting `TTableFormatFileDesc.table_format_type = "ducklake"`
with correctly-populated `iceberg_params` produces no reader: `_cur_reader`
stays unset, scan errors as "Not supported".

**Root cause.** `be/src/exec/scan/file_scanner.cpp:1252` (Parquet) and
`:1343` (ORC) gate on `table_format_params.table_format_type == "iceberg"`
with no default branch.

**Workaround.** Emit `table_format_type = "iceberg"` and reuse
`iceberg_params.delete_files`. DuckLake parquet carries field-ids, so the
Iceberg reader's column resolution works unchanged. EXPLAIN VERBOSE shows
"iceberg" for DuckLake tables — operationally confusing.

**Fix.** Five-line BE PR:

```cpp
} else if (range.__isset.table_format_params &&
           (range.table_format_params.table_format_type == "iceberg" ||
            range.table_format_params.table_format_type == "ducklake")) {
    // existing IcebergParquetReader path
}
```

Also tracked in `ducklake-doris-sanity-check.md` §2.1.

---

## 2026-05-19 · `populateScanLevelParams` vs `populateRangeParams` boundary is unenforced

**Symptom.** First implementation put `iceberg_params` in
`populateScanLevelParams` and storage creds in `populateRangeParams`.
Result: BE saw an empty `iceberg_params` per range and duplicated S3
properties at the node level.

**Root cause.** The two overrides look symmetric but cover different
scopes — and the SPI doesn't enforce it.

| Method | Scope | Goes here |
|---|---|---|
| `populateRangeParams(TTableFormatFileDesc, TFileRangeDesc)` | per range | `iceberg_params` / `paimon_params` / per-file `columns_from_path` |
| `populateScanLevelParams(TFileScanRangeParams, Map)` | per node | shared `properties` (S3 creds, JDBC info), `serialized_table` |

**Workaround.** Move per-file thrift fragments to `populateRangeParams`;
keep `populateScanLevelParams` for shared state. The iceberg reference
plugin gets this right but doesn't call out the rule.

**Fix.** Add the scope table above to `ConnectorScanPlanProvider` /
`ConnectorScanRange` javadoc or the fe-connector handbook.

---

## 2026-05-19 · Doris injects engine-level keys into every `CREATE CATALOG`

**Symptom.** Strict unknown-property check in `validateProperties()`
rejected valid usage. Engine adds keys like `enable.mapping.varbinary`
before the plugin sees the map:

```
ERROR: unknown property: enable.mapping.varbinary
```

Not documented in the SPI handbook.

**Root cause.** Engine-side property injection at the DDL layer runs
before `validateProperties`.

**Workaround.** Plugin's `validateProperties` enforces required keys
only; ignores unknowns.

**Fix.** Document the contract — *"validate required + known keys;
tolerate arbitrary additional keys; the engine may inject its own"* —
OR strip engine-injected keys before calling the plugin.

---

## 2026-05-19 · `DriverManager.getDriver` doesn't see plugin-classloader JDBC drivers

**Symptom.** `postgresql-42.7.8.jar` present in the deployed plugin
`lib/` dir. Connection still fails:

```
SQLException: No suitable driver found for jdbc:postgresql://...
```

**Root cause.** `DriverManager`'s registry is populated by
`ServiceLoader` at JVM startup using the **system** classloader.
Plugin jars live on a child classloader, so their
`META-INF/services/java.sql.Driver` isn't discovered.

**Workaround.** Force the driver's static initialiser to run under the
plugin classloader:

```java
Class.forName("org.postgresql.Driver");  // inside DuckLakeConnector.buildCatalog()
```

This triggers `DriverManager.registerDriver(this)` from within the
plugin CL.

**Fix.** Document this pattern in the fe-connector handbook. Every
JDBC-using plugin will need it; currently it's lore.

---

## 2026-05-19 · FE healthcheck on `SELECT 1` deadlocks BE startup

**Symptom.** `docker compose` with `BE depends_on: doris-fe service_healthy`
and `healthcheck: mysql -e "SELECT 1"` on FE: neither ever comes up.

**Root cause.** `SELECT 1` requires BE dispatch for query planning, even
in the trivial case. BE waits for FE-healthy → FE-healthy waits for a BE.

**Workaround.** FE-local healthcheck:

```yaml
healthcheck:
  test: ["CMD-SHELL", "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW FRONTENDS' >/dev/null 2>&1"]
```

`SHOW FRONTENDS` reads FE state only.

**Fix.** Either add a dedicated FE-local probe (`SHOW SELF`?), or
document `SHOW FRONTENDS` as the canonical container healthcheck in the
Docker deploy docs. Official `apache/doris` images hit this same
deadlock when chained.

---

## 2026-05-19 · `start_fe.sh` sources `fe.conf` for JVM flags; env-vars don't override

**Symptom.** Setting `JAVA_OPTS_FOR_JDK_17=-Xmx2g -Xms2g` as a Docker
env-var had no effect — FE still booted with the 8GB default and got
OOM-killed on Docker Desktop.

**Root cause.** `start_fe.sh` reads `JAVA_OPTS_FOR_JDK_17` from
`fe.conf` only.

**Workaround.** Ship a tuned `fe.conf` as a writable bind mount
(`init_fe.sh` appends `priority_networks` at boot, so `:ro` deadlocks
init):

```
# fe.conf
JAVA_OPTS_FOR_JDK_17="-Xmx2g -Xms2g ..."
```

**Fix.** `start_fe.sh` should prefer env-vars when present, fall through
to `fe.conf` otherwise. Standard Twelve-Factor expectation for
containers.

---

## 2026-05-19 · `connector_plugin_root` default is hardcoded in `Config.java`

**Symptom.** Plugins land in `${DORIS_HOME}/plugins/connector/<name>/`
(singular) but `conf/fe.conf` doesn't mention the path. Sibling
`plugins/connectors/` (plural) is the legacy Trino-bridge loader
(`TrinoConnectorPluginLoader.java:92`) — both coexist. Wrong directory
→ silent no-op, no log.

**Root cause.** `Config.java:3541` hardcodes the default without
surfacing it in `fe.conf`.

**Workaround.** Read the source.

**Fix.** Add a commented-out line in `conf/fe.conf` documenting the
default. Zero behaviour change.

---

## How to add an entry

When you hit the next one:

1. Date the entry; insert at the top.
2. **Symptom** — paste the literal error / SQL output. No paraphrasing.
3. **Root cause** — file path + line. Quote the offending code if small.
4. **Workaround** — code snippet or config line on our side.
5. **Fix** — bullet list of pickable upstream changes. Keep each one
   small enough to be a single PR.
