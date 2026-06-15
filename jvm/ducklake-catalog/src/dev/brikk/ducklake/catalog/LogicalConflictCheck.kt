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
package dev.brikk.ducklake.catalog

import dev.brikk.ducklake.catalog.SnapshotRange.activeAt
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_VIEW
import org.jooq.DSLContext
import org.jooq.impl.DSL

/**
 * Validates that every catalog entity an in-flight write transaction
 * references in its payload is still active at the current snapshot. Runs
 * after `action.execute(tx)` but before the new snapshot row is
 * inserted, inside the same JDBC transaction so reads see a consistent
 * post-action snapshot.
 *
 * Why this is necessary even with strict lineage checking + retry:
 * the retry loop re-runs the action against fresh state, but per-call
 * arguments captured by the caller before `commitInsert` /
 * `dropColumn` / etc. (table IDs, fragment column / file IDs) are
 * *not* re-validated. A retry will happily insert
 * `ducklake_file_column_stats` rows pointing at columns an
 * intervening `DROP COLUMN` end-snapshotted, or end-snapshot rows
 * for a table an intervening `DROP TABLE` already removed.
 *
 * Conflicts thrown here are non-retryable (see
 * [LogicalConflictException]): the same stale references would
 * feed every retry, so re-running burns the retry budget on a
 * guaranteed-fail.
 *
 * `Created*` variants are skipped — duplicate-name races are
 * already PK-protected on the underlying catalog row INSERTs.
 */
object LogicalConflictCheck {
    fun run(tx: DucklakeWriteTransaction, operationDescription: String) {
        val snapshotId = tx.getCurrentSnapshotId()
        val ctx = tx.dsl()
        for (change in tx.getChanges()) {
            when (change) {
                is WriteChange.InsertedIntoTable ->
                    checkInsertedIntoTable(ctx, snapshotId, change, operationDescription)
                is WriteChange.DeletedFromTable ->
                    checkDeletedFromTable(ctx, snapshotId, change, operationDescription)
                is WriteChange.AlteredTable ->
                    checkTableActive(ctx, snapshotId, change.tableId, "altered", operationDescription)
                is WriteChange.FlushedInlinedData ->
                    checkTableActive(ctx, snapshotId, change.tableId, "flushed inlined data for", operationDescription)
                is WriteChange.DroppedTable ->
                    checkTableActive(ctx, snapshotId, change.tableId, "dropped", operationDescription)
                is WriteChange.DroppedSchema ->
                    checkSchemaActive(ctx, snapshotId, change.schemaId, operationDescription)
                is WriteChange.AlteredView ->
                    checkViewActive(ctx, snapshotId, change.viewId, "altered", operationDescription)
                is WriteChange.DroppedView ->
                    checkViewActive(ctx, snapshotId, change.viewId, "dropped", operationDescription)
                // CreatedSchema / CreatedTable / CreatedView: no logical-conflict check
                // (duplicate-name races are PK-protected by the catalog row INSERTs).
                is WriteChange.CreatedSchema,
                is WriteChange.CreatedTable,
                is WriteChange.CreatedView -> {
                    // intentionally skipped
                }
            }
        }
    }

    private fun checkInsertedIntoTable(
        ctx: DSLContext,
        snapshotId: Long,
        change: WriteChange.InsertedIntoTable,
        operationDescription: String,
    ) {
        checkTableActive(ctx, snapshotId, change.tableId, "inserted into", operationDescription)

        val referenced = change.referencedColumnIds
        if (referenced.isEmpty()) {
            return
        }
        val col = DUCKLAKE_COLUMN.`as`("col")
        val active: Set<Long> = ctx.select(col.COLUMN_ID)
            .from(col)
            .where(col.TABLE_ID.eq(change.tableId))
            .and(col.COLUMN_ID.`in`(referenced))
            .and(activeAt(col, snapshotId))
            .fetchSet(col.COLUMN_ID)
        val missing = referenced.filterNot { it in active }.sorted()
        if (missing.isNotEmpty()) {
            throw LogicalConflictException(
                "Failed to $operationDescription: concurrent commit end-snapshotted" +
                    " column_id(s) $missing on table_id=${change.tableId}" +
                    " (likely ALTER TABLE DROP COLUMN). The in-flight INSERT fragments" +
                    " reference these now-dropped columns; re-running with the same" +
                    " column-stats payload would fail identically, so this conflict" +
                    " is not retried.",
            )
        }
    }

    private fun checkDeletedFromTable(
        ctx: DSLContext,
        snapshotId: Long,
        change: WriteChange.DeletedFromTable,
        operationDescription: String,
    ) {
        checkTableActive(ctx, snapshotId, change.tableId, "deleted from", operationDescription)

        val referenced = change.referencedDataFileIds
        if (referenced.isEmpty()) {
            return
        }
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val active: Set<Long> = ctx.select(file.DATA_FILE_ID)
            .from(file)
            .where(file.TABLE_ID.eq(change.tableId))
            .and(file.DATA_FILE_ID.`in`(referenced))
            .and(activeAt(file, snapshotId))
            .fetchSet(file.DATA_FILE_ID)
        val missing = referenced.filterNot { it in active }.sorted()
        if (missing.isNotEmpty()) {
            throw LogicalConflictException(
                "Failed to $operationDescription: concurrent commit end-snapshotted" +
                    " data_file_id(s) $missing on table_id=${change.tableId}" +
                    " (likely DROP TABLE or compaction). The in-flight DELETE fragments" +
                    " reference these now-removed data files; re-running with the same" +
                    " delete-target payload would fail identically, so this conflict" +
                    " is not retried.",
            )
        }
    }

    private fun checkTableActive(
        ctx: DSLContext,
        snapshotId: Long,
        tableId: Long,
        verb: String,
        operationDescription: String,
    ) {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val exists = ctx.fetchExists(
            DSL.selectOne()
                .from(tab)
                .where(tab.TABLE_ID.eq(tableId))
                .and(activeAt(tab, snapshotId)),
        )
        if (!exists) {
            throw LogicalConflictException(
                "Failed to $operationDescription: concurrent commit end-snapshotted" +
                    " table_id=$tableId (likely DROP TABLE) before this transaction's" +
                    " $verb could commit. The in-flight payload's table reference is" +
                    " stale; re-running with the same table_id would fail identically," +
                    " so this conflict is not retried.",
            )
        }
    }

    private fun checkSchemaActive(
        ctx: DSLContext,
        snapshotId: Long,
        schemaId: Long,
        operationDescription: String,
    ) {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        val exists = ctx.fetchExists(
            DSL.selectOne()
                .from(sch)
                .where(sch.SCHEMA_ID.eq(schemaId))
                .and(activeAt(sch, snapshotId)),
        )
        if (!exists) {
            throw LogicalConflictException(
                "Failed to $operationDescription: concurrent commit end-snapshotted" +
                    " schema_id=$schemaId (likely DROP SCHEMA) before this" +
                    " transaction's drop could commit." +
                    " The in-flight payload's schema reference is stale.",
            )
        }
    }

    private fun checkViewActive(
        ctx: DSLContext,
        snapshotId: Long,
        viewId: Long,
        verb: String,
        operationDescription: String,
    ) {
        val view = DUCKLAKE_VIEW.`as`("view")
        val exists = ctx.fetchExists(
            DSL.selectOne()
                .from(view)
                .where(view.VIEW_ID.eq(viewId))
                .and(activeAt(view, snapshotId)),
        )
        if (!exists) {
            throw LogicalConflictException(
                "Failed to $operationDescription: concurrent commit end-snapshotted" +
                    " view_id=$viewId (likely DROP VIEW) before this transaction's" +
                    " $verb could commit. The in-flight payload's view reference is stale.",
            )
        }
    }
}
