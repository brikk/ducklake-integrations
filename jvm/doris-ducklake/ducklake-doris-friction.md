# DuckLake-on-Doris — Friction log

A running log of SPI / FE / BE friction we hit while implementing the
DuckLake `fe-connector` plugin against PR
[apache/doris#62767](https://github.com/apache/doris/pull/62767).

Intended audience:

- **Doris fe-connector maintainers** — places the SPI surface, BE
  contract, or operator docs are quietly ambiguous. Each entry calls
  out what we expected vs. what we found, and where in code the
  mismatch lives, so it can be triaged without re-reproducing.
- **Future plugin authors** — the workarounds. Read top-to-bottom
  before authoring a new fe-connector plugin and you'll skip a few
  hours of debugging.

Sister docs:

- [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md)
  — canonical implementation plan.
- [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md)
  — one-shot architectural review (deployment posture, capability set,
  BE workaround for position deletes, JDK 17 ABI rules). The friction
  log is the running counterpart.
- [`ducklake-doris-todo.md`](./ducklake-doris-todo.md) — our working
  state and roadmap.

Each entry is dated (so the log doubles as a changelog of when each
surprise surfaced) and has the same shape:

> **Symptom** — what we saw  
> **Root cause** — where in code  
> **Workaround** — what we did  
> **Suggested upstream fix** — what would prevent the next person hitting it

---

## 2026-05-19 · `ConnectorScanRange.getFileFormat()` is dead on the plugin-driven scan path

**Symptom.** Plugin returned `"parquet"` from
`ConnectorScanRange.getFileFormat()`. BE rejected the scan with:

```
[NOT_IMPLEMENTED_ERROR]Not supported create reader for table format:
iceberg / file format: FORMAT_JNI. cur path: s3://.../foo.parquet
```

**Root cause.** `PluginDrivenScanNode` never consumes
`ConnectorScanRange.getFileFormat()`. Its
`getFileFormatType()` (`fe-core/.../datasource/PluginDrivenScanNode.java`,
lines ~198-205) reads the property key `"file_format_type"` out of
`getScanNodeProperties()` and runs it through
`mapFileFormatType()` (lines ~689-707). Missing key → defaults to
`TFileFormatType.FORMAT_JNI` silently.

`ConnectorScanRange.getFileFormat()`'s javadoc says "this determines
the BE reader" — that's accurate for non-plugin-driven scan nodes,
but it's NOT how the plugin-driven path resolves format. The method
is only consumed by the default `populateRangeParams` implementation
that stuffs it into `jdbc_params` as `"connector_file_format"`.

**Workaround.**
`DuckLakeScanPlanProvider.getScanNodeProperties()` emits
`"file_format_type" = "parquet"`. The scan-range-level method stays
overridden for completeness but doesn't influence dispatch.

**Suggested upstream fix.** One of:

1. Have `PluginDrivenScanNode.getFileFormatType()` fall back to
   `range.getFileFormat()` when `file_format_type` is absent from the
   scan-node properties.
2. Update `ConnectorScanRange.getFileFormat()`'s javadoc to call out
   that the plugin-driven path uses `getScanNodeProperties()` instead,
   and add a `PROP_FILE_FORMAT_TYPE` constant on the API side that
   plugins can import.
3. Make the default `FORMAT_JNI` fallback raise instead of silently
   selecting a reader the iceberg/parquet path can't open — a missing
   format declaration is always a planning bug, not a runtime
   condition.

---

## 2026-05-19 · BE S3 reader requires `AWS_*` keys verbatim; FE-side `s3.*` form is invisible

**Symptom.** Plugin emitted `s3.endpoint`, `s3.access_key`,
`s3.secret_key`, `s3.region` (the FE-canonical names users write into
`CREATE CATALOG`) into `TFileScanRangeParams.properties`. BE rejected
the scan with:

```
[INVALID_ARGUMENT]Invalid s3 conf, empty endpoint. cur path: s3://...
```

even though the properties map carried `s3.endpoint = http://minio:9000`.

**Root cause.** `S3ClientFactory::convert_properties_to_s3_conf`
(`be/src/util/s3_util.cpp`, lines ~541-619) does literal map lookups
against these constants:

```cpp
constexpr char S3_AK[]       = "AWS_ACCESS_KEY";
constexpr char S3_SK[]       = "AWS_SECRET_KEY";
constexpr char S3_ENDPOINT[] = "AWS_ENDPOINT";
constexpr char S3_REGION[]   = "AWS_REGION";
constexpr char S3_TOKEN[]    = "AWS_TOKEN";
```

The FE-side `S3ObjStorage.normalizeProperties`
(`fe-filesystem-s3/.../S3ObjStorage.java`, lines ~127-135) knows about
the `s3.*` aliases — but it does NOT run on the parquet-reader path.
It's invoked from FE-side code that opens S3 directly (HMS / Iceberg
metadata reads), not from `populateScanLevelParams` → wire → BE.

So the FE has two parallel S3-key vocabularies: one the user writes
(`s3.*`), one the BE understands (`AWS_*`), and they're only
reconciled if the call site happens to go through `S3ObjStorage`.

**Workaround.** `DuckLakeScanPlanProvider.canonicalAwsAlias()` is a
static switch from FE-form keys to BE-form aliases; called from
`populateScanLevelParams` after the location prefix is stripped.
Both forms get emitted into `params.properties` (belt + suspenders).

**Suggested upstream fix.** One of:

1. Move `S3ObjStorage.normalizeProperties` (or an equivalent) onto
   the `TFileScanRangeParams.properties` path on the FE side so all
   plugins benefit automatically — i.e. wrap the call to
   `populateScanLevelParams` with a post-normalisation step.
2. Have the BE-side `convert_properties_to_s3_conf` recognise both
   forms (a five-line switch).
3. At minimum, document the contract loudly in the
   `ConnectorScanPlanProvider.populateScanLevelParams` javadoc:
   "keys for S3 must be in `AWS_*` form; for HDFS in `dfs.*` form; etc."
   so plugin authors don't have to read the BE source to find out.

We hit this on the first live `SELECT *` end-to-end. The error
message ("Invalid s3 conf, empty endpoint") doesn't hint that
*alternate keys exist and were ignored* — it reads like missing
configuration.

---

## 2026-05-19 · `connector_plugin_root` default is hardcoded in `Config.java`

**Symptom.** Plugins land in `${DORIS_HOME}/plugins/connector/<name>/`
but neither `conf/fe.conf` nor the docs mention the path. Operators
have to grep `Config.java` to learn where to deploy.

**Root cause.** `Config.java:3541` hardcodes the default; the value is
not surfaced as a commented-out line in `conf/fe.conf` like other
defaults.

Sibling directory `plugins/connectors/` (plural) is the legacy
Trino-bridge loader (`TrinoConnectorPluginLoader.java:92`) — both
coexist, which compounds the confusion ("which `plugins/connector*` do
I drop my jar into?").

**Workaround.** Documented in `ducklake-doris-todo.md` (Reference
section).

**Suggested upstream fix.** Surface the default as a commented-out
line in `conf/fe.conf`. Zero behaviour change; saves every new plugin
author a grep.

---

## 2026-05-19 · `SPI_READY_TYPES` whitelist silently drops unknown ConnectorProviders

**Symptom.** `CREATE CATALOG dl PROPERTIES('type'='ducklake', ...)`
fails with `Unknown catalog type: ducklake` even though our
`META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`
file is correct, the jar is on the classpath, and the provider's
`getType()` returns `"ducklake"`. No log line acknowledging the
provider was discovered-but-rejected.

**Root cause.** `CatalogFactory.java` carries a hardcoded
`SPI_READY_TYPES = {"jdbc", "es", "iceberg"}` set; providers whose
`getType()` isn't in this set are silently ignored and the factory
falls through to its legacy `switch`.

**Workaround.** We carry a one-line worktree patch (`+ "ducklake"`)
on top of every Doris release we deploy. Re-applied on every refresh
of PR #62767.

**Suggested upstream fix.** Already flagged in
`ducklake-doris-sanity-check.md` §3.5 / `ducklake-doris-todo.md`. The
correct end state is "any `ConnectorProvider` discoverable via
`ServiceLoader` wins". Until that lands, *at minimum* log a warning
when discovery skips a registered provider — the silent-drop is the
worst part.

---

## 2026-05-19 · BE position-delete dispatch keyed on the literal string `"iceberg"`

**Symptom.** Setting `TTableFormatFileDesc.table_format_type =
"ducklake"` and populating `iceberg_params` correctly results in BE
silently producing no reader: `_cur_reader` stays unset and the scan
errors as "Not supported".

**Root cause.** `be/src/exec/scan/file_scanner.cpp:1252` (Parquet)
and `:1343` (ORC) gate on `table_format_params.table_format_type ==
"iceberg"` with no default branch. An unknown discriminator falls off
the end.

**Workaround.** Plugin emits `table_format_type = "iceberg"` and uses
the existing Iceberg reader path with `iceberg_params.delete_files`
to carry positional deletes. DuckLake's parquet files carry field-ids,
so the Iceberg reader's column resolution works unchanged.

Operational cost: EXPLAIN VERBOSE, BE profile output, and any
operator-facing error message says `"iceberg"` for DuckLake tables.

**Suggested upstream fix.** Five-line BE PR adding `||
table_format_type == "ducklake"` to the iceberg branch in both call
sites. Already documented in `ducklake-doris-sanity-check.md` §2.1.
A registry-based dispatch is the right *eventual* answer but not v1.

---

## 2026-05-19 · FE `start_fe.sh` sources `fe.conf` for JVM flags; env-vars don't override

**Symptom.** Setting `JAVA_OPTS_FOR_JDK_17=-Xmx2g -Xms2g` as a Docker
environment variable had no effect — FE still booted with the 8GB
heap default and OOM-killed on Docker Desktop's typical memory ceiling.

**Root cause.** `start_fe.sh` reads `JAVA_OPTS_FOR_JDK_17` from
`fe.conf`, not from the environment. Env-var override never lands.

**Workaround.** Ship a tuned `fe.conf` as a bind mount and set the
flag there. Caveat: `init_fe.sh` appends `priority_networks` at boot,
so the mount must be writable — `:ro` deadlocks the init script.

**Suggested upstream fix.** Have `start_fe.sh` honour the env-var
when present (fall through to `fe.conf` otherwise). Standard
Twelve-Factor expectation for containerised deployments.

---

## 2026-05-19 · `DriverManager.getDriver` can't see plugin-classloader JDBC drivers

**Symptom.** Plugin jar includes `postgresql-42.7.8.jar` (verified
present in the deployed `lib/` directory), but
`DriverManager.getConnection("jdbc:postgresql://...")` raises
`SQLException: No suitable driver found`.

**Root cause.** `DriverManager` consults its registry, which is
populated by `ServiceLoader` at JVM startup with the **system**
classloader. Plugin jars live on a child classloader, so their
`META-INF/services/java.sql.Driver` files aren't discovered without
explicit help from the plugin code.

**Workaround.** `Class.forName("org.postgresql.Driver")` from inside
the plugin's `Connector` (must be called from a plugin-loaded class so
the driver's static initialiser runs under the plugin classloader and
registers itself via `DriverManager.registerDriver(this)`).

**Suggested upstream fix.** The fe-connector handbook should call
out this pattern explicitly — every JDBC-using plugin needs it.
Currently it's lore: each plugin author rediscovers it through a
runtime error.

---

## 2026-05-19 · FE healthcheck deadlocks BE startup when based on `SELECT 1`

**Symptom.** `depends_on: service_healthy` for BE was waiting on FE
healthcheck. FE healthcheck was `mysql ... -e "SELECT 1"`. Neither
ever came up — `SELECT 1` needs a BE to dispatch the plan, BE waits
for FE health, FE waits for a BE.

**Root cause.** `SELECT 1` is not FE-local; it goes through query
planning + BE dispatch even in the trivial case.

**Workaround.** Healthcheck on `SHOW FRONTENDS` (FE-local, no BE
required). Documented in `compose/fe.conf` and the smoke loop.

**Suggested upstream fix.** Either:

1. Add an explicit FE-local-only health probe SQL (`SELECT VERSION()`
   or a dedicated `SHOW SELF`).
2. Document the canonical container healthcheck SQL in the Docker
   deploy docs; the official `apache/doris` images currently
   include healthchecks that hit this same deadlock when chained.

---

## 2026-05-19 · Doris injects engine-level keys into every `CREATE CATALOG`

**Symptom.** A strict unknown-property check in
`ConnectorProvider.validateProperties()` rejected valid usage because
Doris's DDL layer added `enable.mapping.varbinary` (and friends) on
top of the user-supplied properties.

**Root cause.** Engine-side property injection at the DDL layer
happens before the provider's `validateProperties` is called. Not
documented in the SPI handbook.

**Workaround.** Plugin's `validateProperties` enforces the required
set but doesn't reject unknowns.

**Suggested upstream fix.** Document the contract: providers should
treat their property map as "required-keys-must-be-present +
known-keys-validated; arbitrary additional keys may be present and
must be tolerated". Or, alternatively, strip engine-injected keys
before calling `validateProperties` so the plugin sees only the
user-supplied set.

---

## 2026-05-19 · `populateScanLevelParams` vs `populateRangeParams` boundary is subtle

**Symptom.** Initial implementation put per-range thrift content
(`iceberg_params`) into `populateScanLevelParams` and node-level
properties into `populateRangeParams`. Result: BE saw an empty
`iceberg_params` on every range and either nothing or duplicated
S3 properties at the node level.

**Root cause.** The two methods look symmetric (both take a thrift
struct + properties) but operate on different scopes:

- `populateRangeParams(TTableFormatFileDesc, TFileRangeDesc)` —
  **per scan range**, fills the format-specific descriptor for ONE
  file (iceberg_params, paimon_params, etc.) plus
  `columns_from_path` on the range desc.
- `populateScanLevelParams(TFileScanRangeParams, Map)` —
  **per scan node**, fills shared state across all ranges
  (`properties` map for storage creds, JDBC connection info,
  serialized table, etc.).

The boundary makes sense once you read both javadocs side by side, but
the SPI doesn't enforce it (nothing stops you from setting node-level
state inside `populateRangeParams` — the result is wasted bytes per
range or silently-ignored properties).

**Workaround.** Plugin separates them cleanly:
`populateRangeParams` builds `iceberg_params` per file;
`populateScanLevelParams` strips the location prefix off node
properties and emits S3 keys (plus AWS_* aliases).

**Suggested upstream fix.** The
`ConnectorScanPlanProvider`/`ConnectorScanRange` package-info or
handbook section could lay out the boundary explicitly with a
"node-level vs range-level fields" table. Right now plugin authors
infer it from reading the iceberg implementation.

---

## How to add an entry

When you hit the next gotcha:

1. Date the entry (top of file is most-recent-first).
2. Symptom: paste the actual error / SQL output.
3. Root cause: file path + line range. Don't paraphrase code; quote it.
4. Workaround: what we did. Cite the class / method on our side.
5. Suggested upstream fix: a small ask the Doris team could pick up
   without a deep refactor — "log a warning instead of silently dropping",
   "add a constant on the API side", "honour env-var override", etc.
   The friction log is most useful when the upstream-fix column is
   small and pickable.
