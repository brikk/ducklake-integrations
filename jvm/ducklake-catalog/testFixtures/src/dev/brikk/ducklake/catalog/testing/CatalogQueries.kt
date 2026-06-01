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
package dev.brikk.ducklake.catalog.testing

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeAt
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeTableNamed
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Record1
import org.jooq.Select
import org.jooq.Table
import org.jooq.impl.DSL
import java.util.LinkedHashMap
import java.util.OptionalLong

/**
 * Named, type-safe jOOQ replacements for the catalog-introspection raw SQL that recurs in
 * tests — primarily lookups against `ducklake_snapshot`, `ducklake_table`,
 * `ducklake_data_file`, `ducklake_schema_versions`, and the inlined-data tables.
 *
 * Each method takes a [DSLContext] so it composes cleanly with caller-managed
 * transactions and connection lifecycles. The DSLContext should normally come from
 * [CatalogTestSupport.dsl] so test queries render identical SQL to the runtime catalog.
 *
 * The dynamic `ducklake_inlined_data_<tableId>_<schemaVersion>` tables can't be
 * modeled by codegen, so [inlinedDataTable] returns a [DSL.table] name-built
 * [Table] reference and [activeInlinedRowCount] uses it together with
 * [CatalogPredicates.activeAt] — that predicate is the one place where
 * dynamic-table reads still need `begin_snapshot`/`end_snapshot` resolved by
 * column name rather than from generated metadata.
 */
class CatalogQueries private constructor() {
    companion object {
        // ---- snapshots --------------------------------------------------------------------

        /**
         * `SELECT max(snapshot_id) FROM ducklake_snapshot`. Throws if the catalog has no
         * snapshots — the empty-catalog case almost always indicates a test setup bug.
         */
        @JvmStatic
        fun latestSnapshotId(dsl: DSLContext): Long {
            val opt = latestSnapshotIdOrEmpty(dsl)
            if (!opt.isPresent) {
                throw AssertionError("No snapshots in catalog")
            }
            return opt.asLong
        }

        /**
         * Non-throwing variant of [latestSnapshotId] for assertions that need to verify
         * the catalog is empty.
         */
        @JvmStatic
        fun latestSnapshotIdOrEmpty(dsl: DSLContext): OptionalLong {
            requireNotNull(dsl) { "dsl is null" }
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val id: Long? = dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long::class.java)
            return if (id == null) OptionalLong.empty() else OptionalLong.of(id)
        }

        /**
         * Latest-snapshot id as a scalar sub-query, for use in `WHERE snapshot_id = (...)`
         * patterns without round-tripping the value into Java first.
         */
        @JvmStatic
        fun latestSnapshotIdSubquery(dsl: DSLContext): Select<Record1<Long>> {
            requireNotNull(dsl) { "dsl is null" }
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            return dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
        }

        /**
         * `schema_version` on the latest snapshot row. Used to verify that schema-changing
         * DDL bumped the version and that pure DML did not.
         */
        @JvmStatic
        fun currentSchemaVersion(dsl: DSLContext): Long {
            requireNotNull(dsl) { "dsl is null" }
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val version: Long? = dsl.select(snap.SCHEMA_VERSION)
                .from(snap)
                .orderBy(snap.SNAPSHOT_ID.desc())
                .limit(1)
                .fetchOne(0, Long::class.java)
            if (version == null) {
                throw AssertionError("No snapshots in catalog")
            }
            return version
        }

        /**
         * `schema_version` on a specific snapshot. Returns empty when the snapshot id is
         * unknown — callers asserting consistency should treat that as a hard failure.
         */
        @JvmStatic
        fun snapshotSchemaVersion(dsl: DSLContext, snapshotId: Long): OptionalLong {
            requireNotNull(dsl) { "dsl is null" }
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val version: Long? = dsl.select(snap.SCHEMA_VERSION)
                .from(snap)
                .where(snap.SNAPSHOT_ID.eq(snapshotId))
                .fetchOne(0, Long::class.java)
            return if (version == null) OptionalLong.empty() else OptionalLong.of(version)
        }

        // ---- tables -----------------------------------------------------------------------

        /**
         * `table_id` of the currently-active row for `tableName`. Throws if no
         * active row exists — DROP TABLE plus a stale lookup is the typical cause.
         */
        @JvmStatic
        fun activeTableId(dsl: DSLContext, tableName: String): Long {
            val opt = activeTableIdOrEmpty(dsl, tableName)
            if (!opt.isPresent) {
                throw AssertionError("Missing active table: $tableName")
            }
            return opt.asLong
        }

        /** Non-throwing variant of [activeTableId]. */
        @JvmStatic
        fun activeTableIdOrEmpty(dsl: DSLContext, tableName: String): OptionalLong {
            requireNotNull(dsl) { "dsl is null" }
            val tab = DUCKLAKE_TABLE.`as`("tab")
            val id: Long? = dsl.select(tab.TABLE_ID)
                .from(tab)
                .where(activeTableNamed(tab, tableName))
                .fetchOne(0, Long::class.java)
            return if (id == null) OptionalLong.empty() else OptionalLong.of(id)
        }

        // ---- data files -------------------------------------------------------------------

        /**
         * Count of currently-active Parquet data files for a table — i.e. files whose
         * `end_snapshot IS NULL`. Used to assert that a write went to (or stayed out of)
         * the on-disk format.
         */
        @JvmStatic
        fun activeDataFileCount(dsl: DSLContext, tableId: Long): Long {
            requireNotNull(dsl) { "dsl is null" }
            val file = DUCKLAKE_DATA_FILE.`as`("file")
            val count: Int? = dsl.selectCount()
                .from(file)
                .where(
                    file.TABLE_ID.eq(tableId)
                        .and(currentlyActive(file.END_SNAPSHOT))
                )
                .fetchOne(0, Int::class.java)
            return count?.toLong() ?: 0L
        }

        // ---- schema versions --------------------------------------------------------------

        /**
         * Schema-version rows for a table, ordered by `begin_snapshot`. Each row records
         * one schema-changing event (CREATE / ALTER / DROP), so the list length equals the
         * number of DDL ops in the table's history.
         */
        @JvmStatic
        fun schemaVersionsByTable(dsl: DSLContext, tableId: Long): List<SchemaVersionRow> {
            requireNotNull(dsl) { "dsl is null" }
            val schver = DUCKLAKE_SCHEMA_VERSIONS.`as`("schver")
            return dsl.select(
                schver.BEGIN_SNAPSHOT,
                schver.SCHEMA_VERSION,
                schver.TABLE_ID
            )
                .from(schver)
                .where(schver.TABLE_ID.eq(tableId))
                .orderBy(schver.BEGIN_SNAPSHOT)
                .fetch { r ->
                    SchemaVersionRow(
                        r.value1() ?: 0L,
                        r.value2() ?: 0L,
                        r.value3() ?: 0L
                    )
                }
        }

        // ---- inlined data -----------------------------------------------------------------

        /**
         * Schema-version values for which an inlined-data side table exists for `tableId`.
         * Each value names a `ducklake_inlined_data_<tableId>_<schemaVersion>` table that
         * a row count would need to query.
         */
        @JvmStatic
        fun inlinedDataSchemaVersions(dsl: DSLContext, tableId: Long): List<Long> {
            requireNotNull(dsl) { "dsl is null" }
            val inlined = DUCKLAKE_INLINED_DATA_TABLES.`as`("inlined")
            return dsl.select(inlined.SCHEMA_VERSION)
                .from(inlined)
                .where(inlined.TABLE_ID.eq(tableId))
                .fetch { r -> r.value1() }
        }

        /**
         * Build a [Table] reference for the dynamic
         * `ducklake_inlined_data_<tableId>_<schemaVersion>` table. The columns aren't
         * known to codegen (the table is created on demand by the runtime), so callers resolve
         * fields by name — typically via [CatalogPredicates.activeAt].
         */
        @JvmStatic
        fun inlinedDataTable(tableId: Long, schemaVersion: Long): Table<*> {
            return DSL.table(DSL.name("ducklake_inlined_data_" + tableId + "_" + schemaVersion))
        }

        /**
         * Inlined-row count visible at `snapshotId` for a single
         * `(tableId, schemaVersion)` inlined-data table. Use this when the test cares about
         * the per-schema-version breakdown (e.g. asserting that ALTER TABLE ADD COLUMN created a
         * second inlined table).
         *
         * Implementation note: the dynamic table built via [inlinedDataTable] has no
         * declared `Field`s — codegen never saw it — so the `begin_snapshot` /
         * `end_snapshot` columns are referenced unqualified via [DSL.field]
         * and resolved by Postgres at execution time. Mirrors the runtime's
         * `InlinedDataTable` pattern in `JdbcDucklakeCatalog`.
         */
        @JvmStatic
        fun activeInlinedRowCountForSchemaVersion(
            dsl: DSLContext,
            tableId: Long,
            schemaVersion: Long,
            snapshotId: Long,
        ): Long {
            requireNotNull(dsl) { "dsl is null" }
            val inlined: Table<*> = inlinedDataTable(tableId, schemaVersion)
            val beginSnapshot: Field<Long> = DSL.field(DSL.name("begin_snapshot"), Long::class.java)
            val endSnapshot: Field<Long> = DSL.field(DSL.name("end_snapshot"), Long::class.java)
            val count: Int? = dsl.select(DSL.count())
                .from(inlined)
                .where(activeAt(beginSnapshot, endSnapshot, snapshotId))
                .fetchOne(0, Int::class.java)
            return count?.toLong() ?: 0L
        }

        /**
         * Per-schema-version inlined-row counts visible at `snapshotId` for `tableId`,
         * keyed by `schema_version` in ascending order. Empty when the table has no
         * inlined-data tables.
         */
        @JvmStatic
        fun activeInlinedRowCountsBySchemaVersion(
            dsl: DSLContext,
            tableId: Long,
            snapshotId: Long,
        ): Map<Long, Long> {
            requireNotNull(dsl) { "dsl is null" }
            val result: MutableMap<Long, Long> = LinkedHashMap()
            val schemaVersions: MutableList<Long> = ArrayList(inlinedDataSchemaVersions(dsl, tableId))
            schemaVersions.sortWith(Comparator.naturalOrder())
            for (schemaVersion in schemaVersions) {
                result[schemaVersion] =
                    activeInlinedRowCountForSchemaVersion(dsl, tableId, schemaVersion, snapshotId)
            }
            return result
        }

        /**
         * Total inlined-row count visible at `snapshotId`, summed across every
         * inlined-data table for `tableId`. Returns 0 when `tableId` has no
         * inlined-data tables (i.e. the table has only ever written to Parquet).
         */
        @JvmStatic
        fun activeInlinedRowCount(dsl: DSLContext, tableId: Long, snapshotId: Long): Long {
            requireNotNull(dsl) { "dsl is null" }
            var total: Long = 0
            for (schemaVersion in inlinedDataSchemaVersions(dsl, tableId)) {
                total += activeInlinedRowCountForSchemaVersion(dsl, tableId, schemaVersion, snapshotId)
            }
            return total
        }
    }

    @JvmRecord
    data class SchemaVersionRow(val beginSnapshot: Long, val schemaVersion: Long, val tableId: Long)
}
