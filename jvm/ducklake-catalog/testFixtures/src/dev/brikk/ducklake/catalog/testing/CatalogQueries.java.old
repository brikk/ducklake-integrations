/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.catalog.testing;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeAt;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeTableNamed;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive;
import static java.util.Objects.requireNonNull;

/**
 * Named, type-safe jOOQ replacements for the catalog-introspection raw SQL that recurs in
 * tests — primarily lookups against {@code ducklake_snapshot}, {@code ducklake_table},
 * {@code ducklake_data_file}, {@code ducklake_schema_versions}, and the inlined-data tables.
 *
 * <p>Each method takes a {@link DSLContext} so it composes cleanly with caller-managed
 * transactions and connection lifecycles. The DSLContext should normally come from
 * {@link CatalogTestSupport#dsl(java.sql.Connection)} so test queries render identical SQL
 * to the runtime catalog.
 *
 * <p>The dynamic {@code ducklake_inlined_data_<tableId>_<schemaVersion>} tables can't be
 * modeled by codegen, so {@link #inlinedDataTable(long, long)} returns a
 * {@link DSL#table(Name) name-built} {@link Table} reference and
 * {@link #activeInlinedRowCount(DSLContext, long, long)} uses it together with
 * {@link CatalogPredicates#activeAt(Table, long)} — that predicate is the one place where
 * dynamic-table reads still need {@code begin_snapshot}/{@code end_snapshot} resolved by
 * column name rather than from generated metadata.
 */
public final class CatalogQueries
{
    private CatalogQueries() {}

    // ---- snapshots --------------------------------------------------------------------

    /**
     * {@code SELECT max(snapshot_id) FROM ducklake_snapshot}. Throws if the catalog has no
     * snapshots — the empty-catalog case almost always indicates a test setup bug.
     */
    public static long latestSnapshotId(DSLContext dsl)
    {
        return latestSnapshotIdOrEmpty(dsl)
                .orElseThrow(() -> new AssertionError("No snapshots in catalog"));
    }

    /**
     * Non-throwing variant of {@link #latestSnapshotId} for assertions that need to verify
     * the catalog is empty.
     */
    public static OptionalLong latestSnapshotIdOrEmpty(DSLContext dsl)
    {
        requireNonNull(dsl, "dsl is null");
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long id = dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long.class);
        return id == null ? OptionalLong.empty() : OptionalLong.of(id);
    }

    /**
     * Latest-snapshot id as a scalar sub-query, for use in {@code WHERE snapshot_id = (...)}
     * patterns without round-tripping the value into Java first.
     */
    public static Select<Record1<Long>> latestSnapshotIdSubquery(DSLContext dsl)
    {
        requireNonNull(dsl, "dsl is null");
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        return dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap);
    }

    /**
     * {@code schema_version} on the latest snapshot row. Used to verify that schema-changing
     * DDL bumped the version and that pure DML did not.
     */
    public static long currentSchemaVersion(DSLContext dsl)
    {
        requireNonNull(dsl, "dsl is null");
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long version = dsl.select(snap.SCHEMA_VERSION)
                .from(snap)
                .orderBy(snap.SNAPSHOT_ID.desc())
                .limit(1)
                .fetchOne(0, Long.class);
        if (version == null) {
            throw new AssertionError("No snapshots in catalog");
        }
        return version;
    }

    /**
     * {@code schema_version} on a specific snapshot. Returns empty when the snapshot id is
     * unknown — callers asserting consistency should treat that as a hard failure.
     */
    public static OptionalLong snapshotSchemaVersion(DSLContext dsl, long snapshotId)
    {
        requireNonNull(dsl, "dsl is null");
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long version = dsl.select(snap.SCHEMA_VERSION)
                .from(snap)
                .where(snap.SNAPSHOT_ID.eq(snapshotId))
                .fetchOne(0, Long.class);
        return version == null ? OptionalLong.empty() : OptionalLong.of(version);
    }

    // ---- tables -----------------------------------------------------------------------

    /**
     * {@code table_id} of the currently-active row for {@code tableName}. Throws if no
     * active row exists — DROP TABLE plus a stale lookup is the typical cause.
     */
    public static long activeTableId(DSLContext dsl, String tableName)
    {
        return activeTableIdOrEmpty(dsl, tableName)
                .orElseThrow(() -> new AssertionError("Missing active table: " + tableName));
    }

    /** Non-throwing variant of {@link #activeTableId}. */
    public static OptionalLong activeTableIdOrEmpty(DSLContext dsl, String tableName)
    {
        requireNonNull(dsl, "dsl is null");
        var tab = DUCKLAKE_TABLE.as("tab");
        Long id = dsl.select(tab.TABLE_ID)
                .from(tab)
                .where(activeTableNamed(tab, tableName))
                .fetchOne(0, Long.class);
        return id == null ? OptionalLong.empty() : OptionalLong.of(id);
    }

    // ---- data files -------------------------------------------------------------------

    /**
     * Count of currently-active Parquet data files for a table — i.e. files whose
     * {@code end_snapshot IS NULL}. Used to assert that a write went to (or stayed out of)
     * the on-disk format.
     */
    public static long activeDataFileCount(DSLContext dsl, long tableId)
    {
        requireNonNull(dsl, "dsl is null");
        var file = DUCKLAKE_DATA_FILE.as("file");
        Integer count = dsl.selectCount()
                .from(file)
                .where(file.TABLE_ID.eq(tableId)
                        .and(currentlyActive(file.END_SNAPSHOT)))
                .fetchOne(0, Integer.class);
        return count == null ? 0L : count.longValue();
    }

    // ---- schema versions --------------------------------------------------------------

    /**
     * Schema-version rows for a table, ordered by {@code begin_snapshot}. Each row records
     * one schema-changing event (CREATE / ALTER / DROP), so the list length equals the
     * number of DDL ops in the table's history.
     */
    public static List<SchemaVersionRow> schemaVersionsByTable(DSLContext dsl, long tableId)
    {
        requireNonNull(dsl, "dsl is null");
        var schver = DUCKLAKE_SCHEMA_VERSIONS.as("schver");
        return dsl.select(
                        schver.BEGIN_SNAPSHOT,
                        schver.SCHEMA_VERSION,
                        schver.TABLE_ID)
                .from(schver)
                .where(schver.TABLE_ID.eq(tableId))
                .orderBy(schver.BEGIN_SNAPSHOT)
                .fetch(r -> new SchemaVersionRow(
                        r.value1() == null ? 0L : r.value1(),
                        r.value2() == null ? 0L : r.value2(),
                        r.value3() == null ? 0L : r.value3()));
    }

    public record SchemaVersionRow(long beginSnapshot, long schemaVersion, long tableId) {}

    // ---- inlined data -----------------------------------------------------------------

    /**
     * Schema-version values for which an inlined-data side table exists for {@code tableId}.
     * Each value names a {@code ducklake_inlined_data_<tableId>_<schemaVersion>} table that
     * a row count would need to query.
     */
    public static List<Long> inlinedDataSchemaVersions(DSLContext dsl, long tableId)
    {
        requireNonNull(dsl, "dsl is null");
        var inlined = DUCKLAKE_INLINED_DATA_TABLES.as("inlined");
        return dsl.select(inlined.SCHEMA_VERSION)
                .from(inlined)
                .where(inlined.TABLE_ID.eq(tableId))
                .fetch(r -> r.value1());
    }

    /**
     * Build a {@link Table} reference for the dynamic
     * {@code ducklake_inlined_data_<tableId>_<schemaVersion>} table. The columns aren't
     * known to codegen (the table is created on demand by the runtime), so callers resolve
     * fields by name — typically via {@link CatalogPredicates#activeAt(Table, long)}.
     */
    public static Table<?> inlinedDataTable(long tableId, long schemaVersion)
    {
        return DSL.table(DSL.name("ducklake_inlined_data_" + tableId + "_" + schemaVersion));
    }

    /**
     * Inlined-row count visible at {@code snapshotId} for a single
     * {@code (tableId, schemaVersion)} inlined-data table. Use this when the test cares about
     * the per-schema-version breakdown (e.g. asserting that ALTER TABLE ADD COLUMN created a
     * second inlined table).
     *
     * <p>Implementation note: the dynamic table built via {@link #inlinedDataTable} has no
     * declared {@code Field}s — codegen never saw it — so the {@code begin_snapshot} /
     * {@code end_snapshot} columns are referenced unqualified via {@link DSL#field(Name, Class)}
     * and resolved by Postgres at execution time. Mirrors the runtime's
     * {@code InlinedDataTable} pattern in {@code JdbcDucklakeCatalog}.
     */
    public static long activeInlinedRowCountForSchemaVersion(
            DSLContext dsl,
            long tableId,
            long schemaVersion,
            long snapshotId)
    {
        requireNonNull(dsl, "dsl is null");
        Table<?> inlined = inlinedDataTable(tableId, schemaVersion);
        Field<Long> beginSnapshot = DSL.field(DSL.name("begin_snapshot"), Long.class);
        Field<Long> endSnapshot = DSL.field(DSL.name("end_snapshot"), Long.class);
        Integer count = dsl.select(DSL.count())
                .from(inlined)
                .where(activeAt(beginSnapshot, endSnapshot, snapshotId))
                .fetchOne(0, Integer.class);
        return count == null ? 0L : count.longValue();
    }

    /**
     * Per-schema-version inlined-row counts visible at {@code snapshotId} for {@code tableId},
     * keyed by {@code schema_version} in ascending order. Empty when the table has no
     * inlined-data tables.
     */
    public static Map<Long, Long> activeInlinedRowCountsBySchemaVersion(
            DSLContext dsl,
            long tableId,
            long snapshotId)
    {
        requireNonNull(dsl, "dsl is null");
        Map<Long, Long> result = new LinkedHashMap<>();
        List<Long> schemaVersions = new ArrayList<>(inlinedDataSchemaVersions(dsl, tableId));
        schemaVersions.sort(Long::compareTo);
        for (Long schemaVersion : schemaVersions) {
            result.put(schemaVersion,
                    activeInlinedRowCountForSchemaVersion(dsl, tableId, schemaVersion, snapshotId));
        }
        return result;
    }

    /**
     * Total inlined-row count visible at {@code snapshotId}, summed across every
     * inlined-data table for {@code tableId}. Returns 0 when {@code tableId} has no
     * inlined-data tables (i.e. the table has only ever written to Parquet).
     */
    public static long activeInlinedRowCount(DSLContext dsl, long tableId, long snapshotId)
    {
        requireNonNull(dsl, "dsl is null");
        long total = 0;
        for (Long schemaVersion : inlinedDataSchemaVersions(dsl, tableId)) {
            total += activeInlinedRowCountForSchemaVersion(dsl, tableId, schemaVersion, snapshotId);
        }
        return total;
    }
}
