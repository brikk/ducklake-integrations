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
 * Typed record of a mutation a write transaction performed. Captured during
 * [DucklakeWriteTransaction] alongside (and replacing the role of) the
 * older string-based `addChange` channel. Two consumers:
 *
 *  1. **`ducklake_snapshot_changes.changes_made` serializer.**
 *     [formatChangesMade] renders the list as the comma-separated text
 *     upstream DuckDB parses via `ParseChangesList`; quoting matches
 *     `DuckLakeUtil::ParseQuotedValue` (wrap in `"..."`, escape `"` by
 *     doubling). Numeric-id forms (`dropped_*`, `altered_*`,
 *     `inserted_into_table`, `deleted_from_table`) are unquoted per spec.
 *  2. **Logical conflict checking.** The variants carry the structured
 *     payload (target IDs, referenced column / file IDs) needed by
 *     `LogicalConflictCheck` to validate that intervening commits didn't
 *     strip the entities this transaction depends on.
 */
sealed interface WriteChange {
    /**
     * Render this change as one entry in the
     * `ducklake_snapshot_changes.changes_made` text column.
     */
    fun toChangesMadeEntry(): String

    @JvmRecord
    data class CreatedSchema(val schemaName: String) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "created_schema:${WriteChange.writeQuotedValue(schemaName)}"
    }

    /**
     * @param schemaName carried alongside the ID so the conflict matrix can
     *        cross-check intervening `created_table:"S".*` entries
     *        against this drop. Not serialized to `changes_made`.
     */
    @JvmRecord
    data class DroppedSchema(val schemaId: Long, val schemaName: String) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "dropped_schema:$schemaId"
    }

    /**
     * @param schemaId carried alongside the names so the conflict matrix can
     *        cross-check this create against intervening
     *        `dropped_schema:<id>` entries. Not serialized to
     *        `changes_made`.
     */
    @JvmRecord
    data class CreatedTable(
        val schemaId: Long,
        val schemaName: String,
        val tableName: String,
    ) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "created_table:${WriteChange.writeQuotedValue(schemaName)}.${WriteChange.writeQuotedValue(tableName)}"
    }

    @JvmRecord
    data class DroppedTable(val tableId: Long) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "dropped_table:$tableId"
    }

    /**
     * Covers `addColumn` / `dropColumn` / `renameColumn`. Upstream's
     * `ParseChangeType` groups these as `altered_table`.
     */
    @JvmRecord
    data class AlteredTable(val tableId: Long) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "altered_table:$tableId"
    }

    /**
     * @param referencedColumnIds column IDs the data files depend on (drawn from
     *        each fragment's `columnStats`). Captured so a logical
     *        conflict check can verify they are still active at commit time.
     *        Defensively copied to an immutable [Set] on construction
     *        (mirrors the Java record's compact-constructor `Set.copyOf`).
     */
    class InsertedIntoTable private constructor(
        @get:JvmName("tableId") val tableId: Long,
        @get:JvmName("referencedColumnIds") val referencedColumnIds: Set<Long>,
        @Suppress("UNUSED_PARAMETER") marker: Unit,
    ) : WriteChange {
        constructor(tableId: Long, referencedColumnIds: Set<Long>) :
            this(tableId, referencedColumnIds.toSet(), Unit)

        override fun toChangesMadeEntry(): String =
            "inserted_into_table:$tableId"

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is InsertedIntoTable) return false
            return tableId == other.tableId && referencedColumnIds == other.referencedColumnIds
        }

        override fun hashCode(): Int {
            var result = tableId.hashCode()
            result = 31 * result + referencedColumnIds.hashCode()
            return result
        }

        override fun toString(): String =
            "InsertedIntoTable[tableId=$tableId, referencedColumnIds=$referencedColumnIds]"
    }

    /**
     * @param referencedDataFileIds `data_file_id`s the delete fragments
     *        target. Captured so a logical conflict check can verify they are
     *        still active at commit time. Defensively copied to an immutable
     *        [Set] on construction (mirrors the Java record's compact-constructor
     *        `Set.copyOf`).
     */
    class DeletedFromTable private constructor(
        @get:JvmName("tableId") val tableId: Long,
        @get:JvmName("referencedDataFileIds") val referencedDataFileIds: Set<Long>,
        @Suppress("UNUSED_PARAMETER") marker: Unit,
    ) : WriteChange {
        constructor(tableId: Long, referencedDataFileIds: Set<Long>) :
            this(tableId, referencedDataFileIds.toSet(), Unit)

        override fun toChangesMadeEntry(): String =
            "deleted_from_table:$tableId"

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is DeletedFromTable) return false
            return tableId == other.tableId && referencedDataFileIds == other.referencedDataFileIds
        }

        override fun hashCode(): Int {
            var result = tableId.hashCode()
            result = 31 * result + referencedDataFileIds.hashCode()
            return result
        }

        override fun toString(): String =
            "DeletedFromTable[tableId=$tableId, referencedDataFileIds=$referencedDataFileIds]"
    }

    /**
     * @param schemaId carried alongside the names so the conflict matrix can
     *        cross-check this create against intervening
     *        `dropped_schema:<id>` entries. Not serialized to
     *        `changes_made`.
     */
    @JvmRecord
    data class CreatedView(
        val schemaId: Long,
        val schemaName: String,
        val viewName: String,
    ) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "created_view:${WriteChange.writeQuotedValue(schemaName)}.${WriteChange.writeQuotedValue(viewName)}"
    }

    @JvmRecord
    data class DroppedView(val viewId: Long) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "dropped_view:$viewId"
    }

    /**
     * Covers `replaceViewMetadata` and `renameView`. Upstream's
     * `ParseChangeType` does not recognize `renamed_view`, so a
     * rename also serializes as `altered_view`.
     */
    @JvmRecord
    data class AlteredView(val viewId: Long) : WriteChange {
        override fun toChangesMadeEntry(): String =
            "altered_view:$viewId"
    }

    companion object {
        /**
         * Comma-separated render for `ducklake_snapshot_changes.changes_made`.
         * Per upstream `ParseChangesList` (DuckDB
         * `ducklake_transaction_changes.cpp`), entries are split on unquoted
         * commas; balanced double quotes are tracked so commas inside quoted values
         * are literal.
         */
        @JvmStatic
        fun formatChangesMade(changes: List<WriteChange>): String =
            changes.joinToString(",") { it.toChangesMadeEntry() }

        /**
         * Matches upstream's `KeywordHelper::WriteQuoted` (DuckDB) as consumed by
         * `DuckLakeUtil::ParseQuotedValue` in
         * `third_party/ducklake/src/common/ducklake_util.cpp`: wrap in double
         * quotes and escape embedded `"` by doubling.
         */
        @JvmStatic
        fun writeQuotedValue(value: String): String =
            "\"${value.replace("\"", "\"\"")}\""
    }
}
