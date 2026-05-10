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
package dev.brikk.ducklake.catalog;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static dev.brikk.ducklake.catalog.SnapshotRange.activeAt;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_VIEW;

/**
 * Validates that every catalog entity an in-flight write transaction
 * references in its payload is still active at the current snapshot. Runs
 * after {@code action.execute(tx)} but before the new snapshot row is
 * inserted, inside the same JDBC transaction so reads see a consistent
 * post-action snapshot.
 *
 * <p>Why this is necessary even with strict lineage checking + retry:
 * the retry loop re-runs the action against fresh state, but per-call
 * arguments captured by the caller before {@code commitInsert} /
 * {@code dropColumn} / etc. (table IDs, fragment column / file IDs) are
 * <em>not</em> re-validated. A retry will happily insert
 * {@code ducklake_file_column_stats} rows pointing at columns an
 * intervening {@code DROP COLUMN} end-snapshotted, or end-snapshot rows
 * for a table an intervening {@code DROP TABLE} already removed.
 *
 * <p>Conflicts thrown here are non-retryable (see
 * {@link LogicalConflictException}): the same stale references would
 * feed every retry, so re-running burns the retry budget on a
 * guaranteed-fail.
 *
 * <p>{@code Created*} variants are skipped — duplicate-name races are
 * already PK-protected on the underlying catalog row INSERTs.
 */
final class LogicalConflictCheck
{
    private LogicalConflictCheck() {}

    static void run(DucklakeWriteTransaction tx, String operationDescription)
    {
        long snapshotId = tx.getCurrentSnapshotId();
        DSLContext ctx = tx.dsl();
        for (WriteChange change : tx.getChanges()) {
            switch (change) {
                case WriteChange.InsertedIntoTable c ->
                        checkInsertedIntoTable(ctx, snapshotId, c, operationDescription);
                case WriteChange.DeletedFromTable c ->
                        checkDeletedFromTable(ctx, snapshotId, c, operationDescription);
                case WriteChange.AlteredTable c ->
                        checkTableActive(ctx, snapshotId, c.tableId(), "altered", operationDescription);
                case WriteChange.DroppedTable c ->
                        checkTableActive(ctx, snapshotId, c.tableId(), "dropped", operationDescription);
                case WriteChange.DroppedSchema c ->
                        checkSchemaActive(ctx, snapshotId, c.schemaId(), operationDescription);
                case WriteChange.AlteredView c ->
                        checkViewActive(ctx, snapshotId, c.viewId(), "altered", operationDescription);
                case WriteChange.DroppedView c ->
                        checkViewActive(ctx, snapshotId, c.viewId(), "dropped", operationDescription);
                case WriteChange.CreatedSchema ignored -> {}
                case WriteChange.CreatedTable ignored -> {}
                case WriteChange.CreatedView ignored -> {}
            }
        }
    }

    private static void checkInsertedIntoTable(
            DSLContext ctx,
            long snapshotId,
            WriteChange.InsertedIntoTable change,
            String operationDescription)
    {
        checkTableActive(ctx, snapshotId, change.tableId(), "inserted into", operationDescription);

        Set<Long> referenced = change.referencedColumnIds();
        if (referenced.isEmpty()) {
            return;
        }
        var col = DUCKLAKE_COLUMN.as("col");
        Set<Long> active = ctx.select(col.COLUMN_ID)
                .from(col)
                .where(col.TABLE_ID.eq(change.tableId()))
                .and(col.COLUMN_ID.in(referenced))
                .and(activeAt(col, snapshotId))
                .fetchSet(col.COLUMN_ID);
        TreeSet<Long> missing = referenced.stream()
                .filter(id -> !active.contains(id))
                .collect(Collectors.toCollection(TreeSet::new));
        if (!missing.isEmpty()) {
            throw new LogicalConflictException(
                    "Failed to " + operationDescription
                            + ": concurrent commit end-snapshotted column_id(s) " + missing
                            + " on table_id=" + change.tableId()
                            + " (likely ALTER TABLE DROP COLUMN). The in-flight INSERT fragments"
                            + " reference these now-dropped columns; re-running with the same"
                            + " column-stats payload would fail identically, so this conflict"
                            + " is not retried.");
        }
    }

    private static void checkDeletedFromTable(
            DSLContext ctx,
            long snapshotId,
            WriteChange.DeletedFromTable change,
            String operationDescription)
    {
        checkTableActive(ctx, snapshotId, change.tableId(), "deleted from", operationDescription);

        Set<Long> referenced = change.referencedDataFileIds();
        if (referenced.isEmpty()) {
            return;
        }
        var file = DUCKLAKE_DATA_FILE.as("file");
        Set<Long> active = ctx.select(file.DATA_FILE_ID)
                .from(file)
                .where(file.TABLE_ID.eq(change.tableId()))
                .and(file.DATA_FILE_ID.in(referenced))
                .and(activeAt(file, snapshotId))
                .fetchSet(file.DATA_FILE_ID);
        TreeSet<Long> missing = referenced.stream()
                .filter(id -> !active.contains(id))
                .collect(Collectors.toCollection(TreeSet::new));
        if (!missing.isEmpty()) {
            throw new LogicalConflictException(
                    "Failed to " + operationDescription
                            + ": concurrent commit end-snapshotted data_file_id(s) " + missing
                            + " on table_id=" + change.tableId()
                            + " (likely DROP TABLE or compaction). The in-flight DELETE fragments"
                            + " reference these now-removed data files; re-running with the same"
                            + " delete-target payload would fail identically, so this conflict"
                            + " is not retried.");
        }
    }

    private static void checkTableActive(
            DSLContext ctx,
            long snapshotId,
            long tableId,
            String verb,
            String operationDescription)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        boolean exists = ctx.fetchExists(
                DSL.selectOne()
                        .from(tab)
                        .where(tab.TABLE_ID.eq(tableId))
                        .and(activeAt(tab, snapshotId)));
        if (!exists) {
            throw new LogicalConflictException(
                    "Failed to " + operationDescription
                            + ": concurrent commit end-snapshotted table_id=" + tableId
                            + " (likely DROP TABLE) before this transaction's " + verb
                            + " could commit. The in-flight payload's table reference is stale;"
                            + " re-running with the same table_id would fail identically, so this"
                            + " conflict is not retried.");
        }
    }

    private static void checkSchemaActive(
            DSLContext ctx,
            long snapshotId,
            long schemaId,
            String operationDescription)
    {
        var sch = DUCKLAKE_SCHEMA.as("sch");
        boolean exists = ctx.fetchExists(
                DSL.selectOne()
                        .from(sch)
                        .where(sch.SCHEMA_ID.eq(schemaId))
                        .and(activeAt(sch, snapshotId)));
        if (!exists) {
            throw new LogicalConflictException(
                    "Failed to " + operationDescription
                            + ": concurrent commit end-snapshotted schema_id=" + schemaId
                            + " (likely DROP SCHEMA) before this transaction's drop could commit."
                            + " The in-flight payload's schema reference is stale.");
        }
    }

    private static void checkViewActive(
            DSLContext ctx,
            long snapshotId,
            long viewId,
            String verb,
            String operationDescription)
    {
        var view = DUCKLAKE_VIEW.as("view");
        boolean exists = ctx.fetchExists(
                DSL.selectOne()
                        .from(view)
                        .where(view.VIEW_ID.eq(viewId))
                        .and(activeAt(view, snapshotId)));
        if (!exists) {
            throw new LogicalConflictException(
                    "Failed to " + operationDescription
                            + ": concurrent commit end-snapshotted view_id=" + viewId
                            + " (likely DROP VIEW) before this transaction's " + verb
                            + " could commit. The in-flight payload's view reference is stale.");
        }
    }
}
