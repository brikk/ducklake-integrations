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

/**
 * Change-vs-change conflict check between this transaction's
 * [WriteChange] list and the [InterveningChanges] committed by
 * other transactions in the snapshot range we're racing against.
 *
 * Direct port of upstream
 * `DuckLakeTransaction::CheckForConflicts(TransactionChangeInformation,
 * SnapshotChangeInformation, ...)` at
 * `temp/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction.cpp:1184-1314`.
 * Keep this file in lock-step with upstream — the matrix entries are the spec.
 *
 * Complements [LogicalConflictCheck] (which is state-based, per-call args):
 * this check catches dueling-name commits (two concurrent
 * `createSchema(name)`, etc.) that the state-based check misses because
 * catalog rows are snapshot-versioned and there's no DDL UNIQUE constraint
 * on `(schema_id, name)`.
 *
 * Conflicts thrown here are non-retryable — the in-flight payload's
 * stale references would feed every retry, so re-running burns the retry
 * budget on a guaranteed-fail. Upstream achieves the same via the
 * `can_retry = false` flag at `ducklake_transaction.cpp:2463`; we use
 * [LogicalConflictException.retryable] returning `false`.
 */
object ConflictMatrix {
    /**
     * Run the matrix. Throws [LogicalConflictException] on the first
     * mismatch found.
     *
     * @param myChanges the in-flight transaction's recorded changes
     * @param other     changes committed in the snapshot range
     *                  `(transactionStartSnapshotId, currentSnapshotId]`
     */
    @JvmStatic
    fun check(myChanges: List<WriteChange>, other: InterveningChanges) {
        for (change in myChanges) {
            when (change) {
                is WriteChange.DroppedTable -> checkDroppedTable(change, other)
                is WriteChange.DroppedView -> checkDroppedView(change, other)
                is WriteChange.DroppedSchema -> checkDroppedSchema(change, other)
                is WriteChange.CreatedSchema -> checkCreatedSchema(change, other)
                is WriteChange.CreatedTable -> checkCreatedTable(change, other)
                is WriteChange.CreatedView -> checkCreatedView(change, other)
                is WriteChange.InsertedIntoTable -> checkInsertedIntoTable(change, other)
                is WriteChange.DeletedFromTable -> checkDeletedFromTable(change, other)
                is WriteChange.AlteredTable -> checkAlteredTable(change, other)
                is WriteChange.AlteredView -> checkAlteredView(change, other)
            }
        }
    }

    // ducklake_transaction.cpp:1188-1190
    private fun checkDroppedTable(c: WriteChange.DroppedTable, other: InterveningChanges) {
        conflictIfMember(
            c.tableId, other.droppedTables,
            "drop table", "dropped it already",
        )
    }

    // ducklake_transaction.cpp:1191-1194
    private fun checkDroppedView(c: WriteChange.DroppedView, other: InterveningChanges) {
        conflictIfMember(
            c.viewId, other.droppedViews,
            "drop view", "dropped it already",
        )
    }

    // ducklake_transaction.cpp:1202-1210
    private fun checkDroppedSchema(c: WriteChange.DroppedSchema, other: InterveningChanges) {
        conflictIfMember(
            c.schemaId, other.droppedSchemas,
            "drop schema", "dropped it already",
        )
        // Upstream: another transaction created an entry (table/view) in this schema.
        val nameMap = other.createdTablesByName[c.schemaName]
        if (nameMap != null && nameMap.isNotEmpty()) {
            val firstName = nameMap.keys.iterator().next()
            val kind = nameMap[firstName]
            throw conflict(
                "drop schema \"" + c.schemaName + "\"",
                "another transaction created " + kind + " \"" + firstName + "\" in this schema",
            )
        }
    }

    // ducklake_transaction.cpp:1212-1215
    private fun checkCreatedSchema(c: WriteChange.CreatedSchema, other: InterveningChanges) {
        if (other.createdSchemas.contains(c.schemaName)) {
            throw conflict(
                "create schema \"" + c.schemaName + "\"",
                "another transaction created a schema with this name already",
            )
        }
    }

    // ducklake_transaction.cpp:1218-1243 (the created_table loop)
    private fun checkCreatedTable(c: WriteChange.CreatedTable, other: InterveningChanges) {
        // Schema this table is being created in must not have been dropped.
        if (other.droppedSchemas.contains(c.schemaId)) {
            throw conflict(
                "create table \"" + c.tableName + "\" in schema \"" + c.schemaName + "\"",
                "another transaction dropped this schema",
            )
        }
        // No other transaction created an entry with this (schema, name) pair.
        val nameMap = other.createdTablesByName[c.schemaName]
        if (nameMap != null) {
            val existingKind = nameMap[c.tableName]
            if (existingKind != null) {
                throw conflict(
                    "create table \"" + c.tableName + "\" in schema \"" + c.schemaName + "\"",
                    "this " + existingKind + " has been created by another transaction already",
                )
            }
        }
    }

    // Same shape as checkCreatedTable, for views.
    private fun checkCreatedView(c: WriteChange.CreatedView, other: InterveningChanges) {
        if (other.droppedSchemas.contains(c.schemaId)) {
            throw conflict(
                "create view \"" + c.viewName + "\" in schema \"" + c.schemaName + "\"",
                "another transaction dropped this schema",
            )
        }
        val nameMap = other.createdTablesByName[c.schemaName]
        if (nameMap != null) {
            val existingKind = nameMap[c.viewName]
            if (existingKind != null) {
                throw conflict(
                    "create view \"" + c.viewName + "\" in schema \"" + c.schemaName + "\"",
                    "this " + existingKind + " has been created by another transaction already",
                )
            }
        }
    }

    // ducklake_transaction.cpp:1245-1248
    private fun checkInsertedIntoTable(c: WriteChange.InsertedIntoTable, other: InterveningChanges) {
        conflictIfMember(
            c.tableId, other.droppedTables,
            "insert into table", "dropped it",
        )
        conflictIfMember(
            c.tableId, other.alteredTables,
            "insert into table", "altered it",
        )
    }

    // ducklake_transaction.cpp:1253-1257
    // The delete-vs-delete file-overlap check (upstream :1259-1283) is
    // intentionally omitted: state-based LogicalConflictCheck.checkDeletedFromTable
    // already catches the case where any referenced data_file_id is no longer
    // active at the current snapshot, which subsumes both DROP and concurrent-delete.
    private fun checkDeletedFromTable(c: WriteChange.DeletedFromTable, other: InterveningChanges) {
        conflictIfMember(
            c.tableId, other.droppedTables,
            "delete from table", "dropped it",
        )
        conflictIfMember(
            c.tableId, other.alteredTables,
            "delete from table", "altered it",
        )
        conflictIfMember(
            c.tableId, other.tablesMergeAdjacent,
            "delete from table", "compacted it",
        )
        conflictIfMember(
            c.tableId, other.tablesRewriteDelete,
            "delete from table", "compacted it",
        )
    }

    // ducklake_transaction.cpp:1307-1310
    private fun checkAlteredTable(c: WriteChange.AlteredTable, other: InterveningChanges) {
        conflictIfMember(
            c.tableId, other.droppedTables,
            "alter table", "dropped it",
        )
        conflictIfMember(
            c.tableId, other.alteredTables,
            "alter table", "altered it",
        )
    }

    // ducklake_transaction.cpp:1311-1313
    private fun checkAlteredView(c: WriteChange.AlteredView, other: InterveningChanges) {
        conflictIfMember(
            c.viewId, other.alteredViews,
            "alter view", "altered it",
        )
    }

    private fun conflictIfMember(id: Long, otherSet: Set<Long>, myAction: String, theirAction: String) {
        if (otherSet.contains(id)) {
            throw conflict("$myAction (id=$id)", "another transaction $theirAction")
        }
    }

    private fun conflict(myAction: String, theirAction: String): LogicalConflictException =
        LogicalConflictException(
            "Transaction conflict - attempting to " + myAction +
                " - but " + theirAction +
                ". This conflict is not retried (re-running with the same payload" +
                " would fail identically).",
        )
}
