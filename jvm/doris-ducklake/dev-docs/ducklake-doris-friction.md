# DuckLake-on-Doris — Friction log

Running log of SPI / FE / BE surprises hit while implementing the DuckLake
`fe-connector` plugin against [apache/doris#62767](https://github.com/apache/doris/pull/62767).

For Doris fe-connector maintainers — each entry has a pickable upstream
fix. For future plugin authors — read top-to-bottom before starting; saves
hours of debugging.

Sister docs: [`ducklake-doris-integration-spi-plan.md`](./ducklake-doris-integration-spi-plan.md)
(canonical plan), [`ducklake-doris-sanity-check.md`](./ducklake-doris-sanity-check.md)
(one-shot architectural review), [`TODO-read.md`](./TODO-read.md)
(working state), [`CRUTCHES-AND-SHORTCUTS.md`](./CRUTCHES-AND-SHORTCUTS.md)
(audited inventory of every non-production-clean path — read before trusting a
feature is deployment-ready).

Entry shape: **Symptom** → **Root cause** (file:line) → **Workaround**
→ **Fix** (small, pickable). Newest first.

---

## 2026-07-07 · No way to hand a small FE-built payload to the BE without shared storage (inlined-data reads)

**Symptom.** DuckLake keeps small tables' rows inline in the metadata catalog
(`ducklake_inlined_data_*`), not in data files. To let the BE — which reads
files only — see them, the FE synthesizes a temp Parquet from the catalog rows
and emits a normal `FILE_SCAN` range pointing at it. **This only works when
the FE and BE share a filesystem at the same path.** In compose they do (a
bind mount); in a real cluster the FE and BE are separate machines, so a
temp file the FE writes to its local disk (or `java.io.tmpdir`) does not
exist on the BE — the scan fails to open the file. The current
implementation is therefore a **dev/compose-only crutch**, gated to
local-filesystem warehouses, and is NOT production-viable.

**Root cause.** The `fe-connector` SPI has no channel for a connector to ship
a small FE-computed payload (a few KB of rows) to the BE for a scan. Every
read path assumes the bytes already live in storage the BE can reach (S3,
HDFS, or a genuinely shared FS). Confirmed against the P6 SPI: no in-memory /
values scan-range type, `META_SCAN_NODE`/`fetchSchemaTableData` is a closed
enum with lossy `TCell` type coverage, `DATA_GEN_SCAN_NODE` is NUMBERS-only,
and the iceberg/paimon JNI sys-table path is a hardcoded `table_format_type`
dispatch (see the 2026-07-06 "No SPI path for a connector to hand FE-computed
rows to the BE" entry — same root gap, this is the concrete write-then-read
consequence).

**Workaround (compose only).** Write the temp Parquet under
`java.io.tmpdir`, which the compose `corpusReplayTest` task pins into the
shared FE↔BE bind mount. Real deployments have no such shared path, so
inlined reads must remain gated off there.

**Fix (pickable upstream changes, ranked).**
1. **Have the FE write the temp payload to the table's own object store**
   (the warehouse the BE already reads — S3/HDFS), not the FE's local disk.
   Needs the connector to reach the warehouse filesystem from the FE (creds +
   an FE-side FS write), plus a lifecycle/GC story. Production-viable but
   heavy, and still a write-per-query.
2. **A small-payload SPI channel:** let a `ConnectorScanRange` carry literal
   rows (or a serialized parquet blob) that the coordinator streams to the BE
   over the existing fragment RPC — no shared storage. This is the clean
   answer for the KB-scale inlined case; needs an in-memory/values scan-range
   type + full `TCell`/nested type coverage on the BE side.
3. **A BE-side "read inlined from catalog" hook** (BE queries the metadata DB
   directly for the inlined table). Couples the BE to the catalog; least
   attractive.

Until one lands, inlined-data reads work only where FE and BE share storage;
tracked in `TODO-read.md`.

---

## 2026-07-07 · BE parquet reader can't read timestamptz into a TimeStampTz slot

**Symptom.** A DuckLake `timestamptz` column mapped to Doris `TIMESTAMPTZ`
(`ScalarType.createTimeStampTzType`) fails at read time on the BE — for a real
DuckLake-written file AND a connector-synthesized one:

```
[INTERNAL_ERROR]The column type of 'ts' is not supported: Unsupported type
change: Nullable(DateTimeV2(6)) => Nullable(TimeStampTz(6)),
src_logical_type: Nullable(DateTimeV2(6)), dst_logical_type: Nullable(TimeStampTz(6))
```

(There is also an FE-side prerequisite: the connector must emit the type name
`TIMESTAMPTZ`, not `TIMESTAMPTZV2` — the latter falls through
`ConnectorColumnConverter` to `UNSUPPORTED` and Nereids `CheckDataTypes`
rejects the plan with "type UNSUPPORTED is unsupported for Nereids". Fixed on
our side.)

**Root cause.** DuckLake writes `timestamptz` as parquet `INT64` /
`TIMESTAMP_MICROS` with `isAdjustedToUtc=true` (verified via
`parquet_schema`). The 4.1.0 BE parquet reader surfaces that column's logical
type as `DateTimeV2` and has no conversion path to a `TimeStampTz` scan slot
(`be/src/vec/exec/format/parquet/...` "Unsupported type change"). So a
zone-aware Doris slot over a UTC-micros parquet column is unreadable, whether
the file was written by DuckDB or by our inlined-data synthesizer.

**Workaround.** Map `timestamptz` → naive `DATETIMEV2(6)`
(`DuckLakeTypeMapping`). The stored micros are already UTC, so the wall-clock
values are correct; only the zone-aware TYPING is lost (a documented
degradation — trino keeps it zone-aware via its own reader). The inlined
writer writes the column as `isAdjustedToUtc=false` micros to match, so the BE
sees `DateTimeV2 => DateTimeV2`.

**Fix (BE).** Add a `DateTimeV2 -> TimeStampTz` conversion to the parquet
reader's type-change handling for `isAdjustedToUtc` timestamp columns (read
the UTC micros into the tz slot). Then the connector can restore the
`TIMESTAMPTZ` mapping for zone-aware semantics. Tracked in `TODO-read.md`.

---

## 2026-07-06 · Schema dictionary is scan-node-level; can't express per-FILE column mapping

**Symptom.** A DuckLake table with a DROP-then-re-ADD of a column, over
externally-registered (`add_files`) **id-less** parquet files whose physical
column names are reused, reads wrong or errors. Concretely (corpus
`add_files/add_files.test`): `col2` (field-id 2) is dropped, then a new `col2`
(field-id 4) is added; older files physically named `col2` mean the DROPPED
field-id 2, so a read of the new `col2` must return NULL for old rows. Doris
either mis-binds the old file's physical `col2` onto the new column (silent
wrong rows), or — once we conflict-filter the ambiguous name — errors:

```
[DATA_QUALITY_ERROR]name_mapping must be set when read missing field id data
file.. cur path: .../my_file3.parquet
```

**Root cause.** The native-reader schema dictionary (`current_schema_id` +
`history_schema_info`, `TFileScanRangeParams` fields 25/26) is emitted **once
per scan node** by the connector's `getScanNodeProperties`, describing the
TABLE. But each DuckLake data file carries its OWN
`ducklake_data_file.mapping_id` → `ducklake_name_mapping` row (a per-file
`target_field_id → source_name`), and for an id-less file the BE binds columns
by those names (`be/src/format/table/table_schema_change_helper.cpp:551`
`by_parquet_field_id_with_name_mapping` → `find_file_field_idx_by_name_mapping`
`:58`, which tries the field's `name_mapping` names then its table `name`).
A single table-level dictionary can't say "file A's `col2` → field-id 2, file
B's `col2` → field-id 4": the `name_mapping` list and the table-name fallback
are applied uniformly to every file. Rename and simple `add_files` cases work
(one unambiguous name per field id); only the reuse-across-a-field-id-boundary
case is unsolvable table-level.

**Workaround.** Conflict-aware `name_mapping` union
(`DuckLakeSchemaDictionary.safeAlternateNames`): a source-name that maps to
more than one field id across files is dropped from the alternates, so we
never silently mis-bind — the BE then errors loudly instead. The two affected
corpus files stay skip-listed. Rename / simple-add_files reads are correct.

**Fix (pickable upstream changes).**
- **Per-range schema info:** let `ConnectorScanRange.populateRangeParams`
  attach a per-file schema/field-id mapping (the file's own `mapping_id`
  resolution), so each split carries its correct file↔table column map instead
  of relying on one scan-node dictionary. This is the general fix and also
  helps any connector with heterogeneous per-file schemas.
- **Or:** a BE hook to consult a per-file name→field-id map supplied on the
  `TFileRangeDesc` (parallel to how `iceberg_params` rides per-range), instead
  of only the scan-node `history_schema_info`.

Until then the two DROP+re-ADD-over-id-less-files corpus cases are documented
skips; tracked in [`TODO-read.md`](./TODO-read.md).

---

## 2026-07-06 · No SPI path for a connector to hand FE-computed rows to the BE (inlined DATA reads)

**Symptom.** DuckLake keeps small tables' rows *inline in the catalog DB*
(per-table `ducklake_inlined_data_<tableId>_<schemaVersion>` tables), not in
any data file. A read of such a table has rows the FE can fetch but no data
file for the BE to scan. Today the Doris plugin refuses at plan time:

```
errCode = 2, detailMessage = DuckLake table has live inlined data rows in the
metadata catalog; reading them is not supported by the Doris connector yet
(rows live in ducklake_inlined_* tables, not parquet).
```

(This is the DATA analogue of the 2026-05-19 inline-DELETE entry below. The
default `DATA_INLINING_ROW_LIMIT` is non-zero, so *any* small INSERT lands
here — it is the common case, not an edge case.)

**Root cause.** The `fe-connector` SPI has no mechanism for a connector to
emit rows it computed FE-side; every scan path assumes the rows live in a
file (or a format the BE has a hardcoded reader for). Surveyed at P6 baseline
`8b391c7` (paths + refs):

1. `ConnectorScanRangeType` (`fe/fe-connector/fe-connector-api/.../scan/ConnectorScanRangeType.java:34`)
   has only `FILE_SCAN` / `JDBC_SCAN` / `REMOTE_OLAP_SCAN` / `CUSTOM` — no
   in-memory / values / metadata variant. `ConnectorScanRange.populateRangeParams`
   (`.../scan/ConnectorScanRange.java:190`) only ever fills a
   `TTableFormatFileDesc` / `TFileRangeDesc`.
2. The BE `META_SCAN_NODE` path (the literal "FE holds rows, BE emits them"
   mechanism used by `information_schema` / `backends` / `partitions`) is
   closed: `TMetadataType` (`gensrc/thrift/Types.thrift:751`) is a fixed enum,
   the BE `MetaScanner::_fetch_metadata` switch (`be/src/exec/scan/meta_scanner.cpp:234`)
   has no generic case, and the master-FE callback `fetchSchemaTableData`
   (`FrontendServiceImpl.java:3190` → `MetadataGenerator`) is keyed on that
   enum. Its `TCell`/`TRow` fill (`meta_scanner.cpp:164-223`) also handles only
   BOOL/INT family/FLOAT/DOUBLE/DATEV2/DATETIMEV2/STRING — **no DECIMAL, no
   nested** — so it would be lossy for DuckLake columns even if it were open.
3. `DATA_GEN_SCAN_NODE` is **not** a VALUES node — `TDataGenFunctionName` has
   the single member `NUMBERS` (`gensrc/thrift/PlanNodes.thrift:616`).
4. The iceberg/paimon *system-table* path DOES make the BE materialize
   FE-supplied rows, but via `FORMAT_JNI` + a `serialized_split` +
   a **hardcoded** `table_format_type` dispatch
   (`be/src/exec/scan/file_scanner.cpp:1067-1156`, only
   `max_compute`/`paimon`/`hudi`/`trino_connector`/`jdbc`/`iceberg`) reading
   through a per-format be-java-extension JNI scanner
   (`be-java-extensions/iceberg-metadata-scanner/.../IcebergSysTableJniScanner.java`).
   No generic `plugin_driven` case exists, so an SPI connector can't reach it
   without a BE patch + its own scanner extension.

**Workaround (FE plugin side, the only path reachable at P6).** Synthesize a
temp **Parquet** file from the inlined rows at plan time and emit a normal
`FILE_SCAN` / `FORMAT_PARQUET` `ConnectorScanRange` pointing at it (shared
scratch dir both FE and BE resolve). The BE's native Parquet branch
(`file_scanner.cpp:1165`, `_init_parquet_reader`) is `table_format_type`-
agnostic, so this needs no BE change. Two costs we absorb: (a) a per-query
Parquet write, and (b) **temp-file GC is on us** — the SPI has no
end-of-scan/close hook (`ConnectorScanPlanProvider` / `ConnectorScanRange`
expose none; `ConnectorSplitSource.close()` fires at end of split
*generation*, not end of *read*), so the file must outlive `planScan` and be
reaped out-of-band.

**Fix (pickable upstream changes, in rising order of scope).**
- **Smallest:** a generic `FORMAT_JNI` `table_format_type == "plugin_driven"`
  dispatch in `file_scanner.cpp` that routes to a connector-provided JNI
  scanner (mirrors the iceberg sys-table path) — lets a connector return
  arbitrary FE-computed rows without a temp file.
- **Alternatively:** an SPI in-memory/values `ConnectorScanRangeType` that
  carries literal rows to the BE (needs a real values scan node + full type
  coverage incl. DECIMAL/nested in the `TCell` fill).
- **Ergonomic add either way:** an end-of-scan / temp-resource cleanup
  callback on `ConnectorScanPlanProvider` (or a scan-scoped `Closeable`) so a
  connector materializing temp files can delete them deterministically
  instead of running a reaper.

Until one of those lands, the temp-Parquet workaround is the plan; tracked in
[`TODO-read.md`](./TODO-read.md) (serve inlined data rows) and
[`PLAN.md`](./PLAN.md).

---

## 2026-07-05 · P6-baseline FE plans `LOCAL_EXCHANGE_NODE` (38); 4.1.0 BE rejects the fragment

**Symptom.** On the P6 iceberg-cutover FE baseline (`branch-catalog-spi` @
`8b391c7`), the first aggregate query of the smoke —
`SELECT COUNT(*) FROM dl.tpch.step7_orders` — dies with

```
(be)[INTERNAL_ERROR]Unsupported exec type in pipeline: Invalid plan node type
```

while plain `SELECT … LIMIT 5` scans work fine. `EXPLAIN` shows a perfectly
ordinary plan (`VPluginDrivenScanNode` → partial agg → exchange → merge agg),
because the offending node is inserted *after* explain rendering.

**Root cause.** The rebased P-series line is based on a newer master whose
planner does FE-side within-fragment local exchange:
`NereidsPlanner.addLocalExchangeAfterDistribute()` → `AddLocalExchange` inserts
`LocalExchangeNode` (`TPlanNodeType.LOCAL_EXCHANGE_NODE = 38`) into agg
fragments (gated by session var `enable_local_shuffle_planner`, default
**true**). The stock `apache/doris:be-4.1.0` image's thrift enum tops out at
`REC_CTE_SCAN_NODE = 36`; `pipeline_fragment_context.cpp` hits its `default:`
arm and `print_plan_node_type()` can't even name the value → "Invalid plan node
type". Pure FE/BE version skew — nothing ducklake- or P6-specific (P6 itself
touches zero thrift).

**Workaround.** `SET GLOBAL enable_local_shuffle_planner = false;` — the BE
then plans local exchange itself (`runtime_state.h::plan_local_shuffle()`),
which is exactly the pre-38 behavior 4.1.0 implements. The FE and BE paths are
mutually exclusive by design, so this is behaviorally safe. `compose/smoke.sh`
now sets it right after FE health (§6b).

**Fix.** Use a BE image built from the same line as the FE (once one exists),
then drop the shim. Not upstream-pickable — it's an artifact of running a
master-based FE against a release BE.

---

## 2026-06-10 · Narrow-int (TINYINT/SMALLINT) write crashes the BE Iceberg writer

**Symptom.** A `CREATE TABLE … AS SELECT 1 AS id, 'alice' AS name` (CTAS) takes the
**BE down** mid-write. `SHOW BACKENDS` → `Alive: false`,
`ErrMsg: java.net.SocketTimeoutException: Read timed out`; the BE log shows an
`assert_cast` abort:

```
assert_cast<arrow::NumericBuilder<arrow::Int8Type>&>(arrow::ArrayBuilder&)   assert_cast.h:75
doris::DataTypeNumberSerDe<(PrimitiveType)3 /*TINYINT*/>::write_column_to_arrow
doris::FromBlockConverter::convert                    arrow_block_convertor.cpp:113
doris::VParquetTransformer::write                     vparquet_transformer.cpp:262
doris::VIcebergPartitionWriter::write                 viceberg_partition_writer.cpp:136
doris::VIcebergTableWriter::_write_prepared_block      viceberg_table_writer.cpp:329
```

**Root cause.** CTAS infers the literal `1` as **TINYINT** (`int8`). Iceberg has no
8/16-bit integer, so the table's iceberg schema represents the column as
`int` (32-bit) → the Arrow builder is `Int32`. But the BE picks the column
serializer by the **source Doris column type** (TINYINT), and
`DataTypeNumberSerDe<TINYINT>::write_column_to_arrow` `assert_cast`s the builder to
`Int8Type` — which it isn't (it's Int32) → abort. It is **not CTAS-specific**: a
direct `INSERT` into a `TINYINT`/`SMALLINT` column through the Iceberg sink crashes
identically. (Our connector's type mapping is fine: TINYINT→`int8` on create,
`int8`→iceberg `int` on read are both correct; the bug is purely the BE writer's
serde-vs-builder type pick.)

**Workaround.** Use `INT`/`BIGINT` (32/64-bit) columns when writing through the
sink; cast narrow ints (`CAST(x AS INT)`) in the SELECT. The compose smoke's W3
CTAS sources INT32/VARCHAR columns and never inflicts the narrow-int crash on the
BE.

**Fix.** In `VIcebergTableWriter`/`FromBlockConverter`, up-cast int8/int16 source
columns to the Arrow type declared by the Iceberg schema (int32) before
`write_column_to_arrow`, or select the serde by the *target* Arrow field type
rather than the source Doris column type. (BE-side; separate from the FE/connector.)

---

## 2026-06-10 · `CreateTableInfo.pluginCatalogTypeToEngine` only knows `"max_compute"` → plugin `CREATE TABLE` rejected

**Symptom.** On the live FE, `CREATE DATABASE` on the `dl` (ducklake) plugin
catalog succeeds, but `CREATE TABLE` is rejected before the connector is ever
called:

```
ERROR 1105 (HY000) at line 4: errCode = 2,
detailMessage = Current catalog does not support create table: dl
```

(W2/W2c INSERT and CREATE/DROP DATABASE were already routed fine — only
table-level CREATE/DROP failed.)

**Root cause.** `CreateTableInfo.paddingEngineName` pads a legacy engine name for
a no-ENGINE `CREATE TABLE` on a plugin catalog by calling
`pluginCatalogTypeToEngine()`, whose `switch` only maps `"max_compute"` →
`ENGINE_MAXCOMPUTE`; every other plugin type (incl. `"ducklake"`) hits
`default → null`, so the `else` branch throws
(`CreateTableInfo.java:928`, switch at `:942`). The rejection is purely FE
engine-padding — `PluginDrivenExternalCatalog.createTable()` is generic (it
converts the request and calls `metadata.createTable`), and the connector mapping
is headless-green (`DuckLakeDdlTest`). The catalog-engine consistency check
(`checkEngineWithCatalog`, `:396`) calls the *same* function, so it stays
consistent automatically.

**Workaround.** Pad `ENGINE_ICEBERG` for `"ducklake"` (working-tree patch on the
local FE; tracked in `jvm/doris-ducklake/fe-patches/ducklake-fe.patch`). Iceberg
is the right engine: DuckLake is Iceberg-shaped (TIcebergTableSink, Iceberg
transform partitioning), `checkEngineName` accepts `ENGINE_ICEBERG`, and the
iceberg path is the one that accepts `PARTITION BY (bucket(N,col))` and rejects
`DISTRIBUTE BY` — matching the connector's own DDL contract. Routing is by catalog
*instance* (a `PluginDrivenExternalCatalog`), not the engine string, so the pad
never diverts CREATE TABLE to the native Iceberg DDL handler. Read-side engine
display (`PluginDrivenExternalTable.getEngine`) is left generic so the shipped
read path is untouched. **Validated GREEN** in `compose/smoke.sh` W1 DDL step.

**Fix.** Generalize `pluginCatalogTypeToEngine` (and the read-side switches in
`PluginDrivenExternalTable`) to consult the connector's declared
capability/engine instead of a hardcoded per-type `switch`, so a new
CREATE-TABLE-capable SPI full-adopter doesn't need an FE edit. As a minimal
interim, add `case "ducklake": return ENGINE_ICEBERG;`.

---

## 2026-06-10 · External partitioned `CREATE TABLE` arrives as `Style.LIST`/`RANGE`, never `Style.TRANSFORM`

**Symptom.** With the engine-padding fix above in place, an unpartitioned
`CREATE TABLE` works, but the iceberg-transform partitioned form first fails to
*parse*:

```
CREATE TABLE dl.ddl_smoke.t (id INT, name STRING) PARTITION BY (bucket(4, name));
-- mismatched input '<EOF>' expecting '(' (line 1, pos 87)
```

and once written in the grammar Doris *does* accept —
`PARTITION BY LIST (bucket(4, name)) ()` — the connector then rejected it:

```
DuckLake supports only Iceberg-style partitioning (...);
PARTITION BY LIST is not supported
```

**Root cause.** Two compounding facts. (1) Doris's only accepted grammar for an
external/iceberg partitioned `CREATE TABLE` is
`PARTITION BY [LIST|RANGE] (transform(col), …) ()` — the bare
`PARTITION BY (expr)` and `PARTITIONED BY (...)` forms don't parse. (2) The FE
converter keys the connector `ConnectorPartitionSpec.style` off the grammar
*keyword*: `CreateTableInfoToConnectorRequestConverter.convertPartition` sets
`Style.LIST`/`RANGE` whenever the `LIST`/`RANGE` keyword is present, putting the
real transform (`bucket(4, name)`) into the per-field data and only using
`Style.TRANSFORM` for the keyword-less expression form. So every live partitioned
external CREATE arrives tagged `LIST`/`RANGE`, **not** `TRANSFORM`. The W1b(a)
connector mapper assumed `TRANSFORM` and rejected `LIST`/`RANGE` outright.

**Workaround.** Map by each field's transform regardless of
`spec.style` in `DuckLakeCreatePartitionMapper.fromPartitionSpec` (the converter
drops the empty value-lists, so the style keyword carries no extra meaning for an
iceberg/ducklake target; per-field `toFieldSpec` still rejects transforms DuckLake
can't represent, e.g. `truncate`). Connector-side only, headless-tested. The smoke
uses `PARTITION BY LIST (bucket(4, name)) ()`. **Validated GREEN** (W1 DDL records
`bucket(4)`; W2c bucket-equivalence `{1,2,3}` round-trips on a Doris-created table).

**Fix.** In `convertPartition`, prefer `Style.TRANSFORM` when every partition
expression is a transform/identity function even under the `LIST`/`RANGE` keyword
(the keyword is grammar boilerplate for external tables, not list/range
semantics) — then connectors need only handle `IDENTITY`/`TRANSFORM`.

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
(Same root SPI gap as the 2026-07-06 inlined-DATA entry above: no path for a
connector to hand FE-computed rows/positions to the BE.)

**Workaround.** Disable inlining for the test/dev catalog:

```sql
CALL lake.set_option('data_inlining_row_limit', '0');
```

(Syntax discovered by probing — `CALL lake.set_option(<key>, <value>)`.
`ducklake_options('lake')` lists the four built-in keys but doesn't show
this one until it's been set.)

After the option is set, subsequent DELETEs write to
`ducklake_delete_file` and a position-delete parquet — the format
[the FE plugin's Step 7 path expects](./TODO-read.md#step-7--position-deletes-path-still-v1-scope).
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
