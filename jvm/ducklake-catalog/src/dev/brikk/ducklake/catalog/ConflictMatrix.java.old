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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Change-vs-change conflict check between this transaction's
 * {@link WriteChange} list and the {@link InterveningChanges} committed by
 * other transactions in the snapshot range we're racing against.
 *
 * <p>Direct port of upstream
 * {@code DuckLakeTransaction::CheckForConflicts(TransactionChangeInformation,
 * SnapshotChangeInformation, ...)} at
 * {@code temp/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction.cpp:1184-1314}.
 * Keep this file in lock-step with upstream — the matrix entries are the
 * spec.
 *
 * <p>Complements {@link LogicalConflictCheck} (which is state-based, per-call
 * args): this check catches dueling-name commits (two concurrent
 * {@code createSchema(name)}, etc.) that the state-based check misses
 * because catalog rows are snapshot-versioned and there's no DDL UNIQUE
 * constraint on {@code (schema_id, name)}.
 *
 * <p>Conflicts thrown here are non-retryable — the in-flight payload's
 * stale references would feed every retry, so re-running burns the retry
 * budget on a guaranteed-fail. Upstream achieves the same via the
 * {@code can_retry = false} flag at
 * {@code ducklake_transaction.cpp:2463}; we use
 * {@link LogicalConflictException#retryable()} returning {@code false}.
 */
final class ConflictMatrix
{
    private ConflictMatrix() {}

    /**
     * Run the matrix. Throws {@link LogicalConflictException} on the first
     * mismatch found.
     *
     * @param myChanges   the in-flight transaction's recorded changes
     * @param other       changes committed in the snapshot range
     *                    {@code (transactionStartSnapshotId, currentSnapshotId]}
     */
    static void check(List<WriteChange> myChanges, InterveningChanges other)
    {
        for (WriteChange change : myChanges) {
            if (change instanceof WriteChange.DroppedTable) {
                checkDroppedTable((WriteChange.DroppedTable) change, other);
            } else if (change instanceof WriteChange.DroppedView) {
                checkDroppedView((WriteChange.DroppedView) change, other);
            } else if (change instanceof WriteChange.DroppedSchema) {
                checkDroppedSchema((WriteChange.DroppedSchema) change, other);
            } else if (change instanceof WriteChange.CreatedSchema) {
                checkCreatedSchema((WriteChange.CreatedSchema) change, other);
            } else if (change instanceof WriteChange.CreatedTable) {
                checkCreatedTable((WriteChange.CreatedTable) change, other);
            } else if (change instanceof WriteChange.CreatedView) {
                checkCreatedView((WriteChange.CreatedView) change, other);
            } else if (change instanceof WriteChange.InsertedIntoTable) {
                checkInsertedIntoTable((WriteChange.InsertedIntoTable) change, other);
            } else if (change instanceof WriteChange.DeletedFromTable) {
                checkDeletedFromTable((WriteChange.DeletedFromTable) change, other);
            } else if (change instanceof WriteChange.AlteredTable) {
                checkAlteredTable((WriteChange.AlteredTable) change, other);
            } else if (change instanceof WriteChange.AlteredView) {
                checkAlteredView((WriteChange.AlteredView) change, other);
            }
        }
    }

    // ducklake_transaction.cpp:1188-1190
    private static void checkDroppedTable(WriteChange.DroppedTable c, InterveningChanges other)
    {
        conflictIfMember(c.tableId(), other.droppedTables,
                "drop table", "dropped it already");
    }

    // ducklake_transaction.cpp:1191-1194
    private static void checkDroppedView(WriteChange.DroppedView c, InterveningChanges other)
    {
        conflictIfMember(c.viewId(), other.droppedViews,
                "drop view", "dropped it already");
    }

    // ducklake_transaction.cpp:1202-1210
    private static void checkDroppedSchema(WriteChange.DroppedSchema c, InterveningChanges other)
    {
        conflictIfMember(c.schemaId(), other.droppedSchemas,
                "drop schema", "dropped it already");
        // Upstream: another transaction created an entry (table/view) in this schema.
        Map<String, String> nameMap = other.createdTablesByName.get(c.schemaName());
        if (nameMap != null && !nameMap.isEmpty()) {
            String firstName = nameMap.keySet().iterator().next();
            String kind = nameMap.get(firstName);
            throw conflict("drop schema \"" + c.schemaName() + "\"",
                    "another transaction created " + kind + " \"" + firstName
                            + "\" in this schema");
        }
    }

    // ducklake_transaction.cpp:1212-1215
    private static void checkCreatedSchema(WriteChange.CreatedSchema c, InterveningChanges other)
    {
        if (other.createdSchemas.contains(c.schemaName())) {
            throw conflict(
                    "create schema \"" + c.schemaName() + "\"",
                    "another transaction created a schema with this name already");
        }
    }

    // ducklake_transaction.cpp:1218-1243 (the created_table loop)
    private static void checkCreatedTable(WriteChange.CreatedTable c, InterveningChanges other)
    {
        // Schema this table is being created in must not have been dropped.
        if (other.droppedSchemas.contains(c.schemaId())) {
            throw conflict(
                    "create table \"" + c.tableName() + "\" in schema \"" + c.schemaName() + "\"",
                    "another transaction dropped this schema");
        }
        // No other transaction created an entry with this (schema, name) pair.
        Map<String, String> nameMap = other.createdTablesByName.get(c.schemaName());
        if (nameMap != null) {
            String existingKind = nameMap.get(c.tableName());
            if (existingKind != null) {
                throw conflict(
                        "create table \"" + c.tableName() + "\" in schema \"" + c.schemaName() + "\"",
                        "this " + existingKind + " has been created by another transaction already");
            }
        }
    }

    // Same shape as checkCreatedTable, for views.
    private static void checkCreatedView(WriteChange.CreatedView c, InterveningChanges other)
    {
        if (other.droppedSchemas.contains(c.schemaId())) {
            throw conflict(
                    "create view \"" + c.viewName() + "\" in schema \"" + c.schemaName() + "\"",
                    "another transaction dropped this schema");
        }
        Map<String, String> nameMap = other.createdTablesByName.get(c.schemaName());
        if (nameMap != null) {
            String existingKind = nameMap.get(c.viewName());
            if (existingKind != null) {
                throw conflict(
                        "create view \"" + c.viewName() + "\" in schema \"" + c.schemaName() + "\"",
                        "this " + existingKind + " has been created by another transaction already");
            }
        }
    }

    // ducklake_transaction.cpp:1245-1248
    private static void checkInsertedIntoTable(WriteChange.InsertedIntoTable c, InterveningChanges other)
    {
        conflictIfMember(c.tableId(), other.droppedTables,
                "insert into table", "dropped it");
        conflictIfMember(c.tableId(), other.alteredTables,
                "insert into table", "altered it");
    }

    // ducklake_transaction.cpp:1253-1257
    // The delete-vs-delete file-overlap check (upstream :1259-1283) is
    // intentionally omitted: state-based LogicalConflictCheck.checkDeletedFromTable
    // already catches the case where any referenced data_file_id is no longer
    // active at the current snapshot, which subsumes both DROP and concurrent-delete.
    private static void checkDeletedFromTable(WriteChange.DeletedFromTable c, InterveningChanges other)
    {
        conflictIfMember(c.tableId(), other.droppedTables,
                "delete from table", "dropped it");
        conflictIfMember(c.tableId(), other.alteredTables,
                "delete from table", "altered it");
        conflictIfMember(c.tableId(), other.tablesMergeAdjacent,
                "delete from table", "compacted it");
        conflictIfMember(c.tableId(), other.tablesRewriteDelete,
                "delete from table", "compacted it");
    }

    // ducklake_transaction.cpp:1307-1310
    private static void checkAlteredTable(WriteChange.AlteredTable c, InterveningChanges other)
    {
        conflictIfMember(c.tableId(), other.droppedTables,
                "alter table", "dropped it");
        conflictIfMember(c.tableId(), other.alteredTables,
                "alter table", "altered it");
    }

    // ducklake_transaction.cpp:1311-1313
    private static void checkAlteredView(WriteChange.AlteredView c, InterveningChanges other)
    {
        conflictIfMember(c.viewId(), other.alteredViews,
                "alter view", "altered it");
    }

    private static void conflictIfMember(long id, Set<Long> otherSet, String myAction, String theirAction)
    {
        if (otherSet.contains(id)) {
            throw conflict(myAction + " (id=" + id + ")", "another transaction " + theirAction);
        }
    }

    private static LogicalConflictException conflict(String myAction, String theirAction)
    {
        return new LogicalConflictException(
                "Transaction conflict - attempting to " + myAction
                        + " - but " + theirAction
                        + ". This conflict is not retried (re-running with the same payload"
                        + " would fail identically).");
    }
}
