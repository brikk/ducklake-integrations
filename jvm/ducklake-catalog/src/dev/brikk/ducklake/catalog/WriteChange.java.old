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
import java.util.Set;

/**
 * Typed record of a mutation a write transaction performed. Captured during
 * {@link DucklakeWriteTransaction} alongside (and replacing the role of) the
 * older string-based {@code addChange} channel. Two consumers:
 *
 * <ol>
 *   <li><b>{@code ducklake_snapshot_changes.changes_made} serializer.</b>
 *       {@link #formatChangesMade(List)} renders the list as the
 *       comma-separated text upstream DuckDB parses via
 *       {@code ParseChangesList}; quoting matches
 *       {@code DuckLakeUtil::ParseQuotedValue} (wrap in {@code "..."},
 *       escape {@code "} by doubling). Numeric-id forms ({@code dropped_*},
 *       {@code altered_*}, {@code inserted_into_table},
 *       {@code deleted_from_table}) are unquoted per spec.
 *   <li><b>Logical conflict checking.</b> The variants carry the structured
 *       payload (target IDs, referenced column / file IDs) needed by
 *       {@code LogicalConflictCheck} to validate that intervening commits
 *       didn't strip the entities this transaction depends on.
 * </ol>
 */
sealed interface WriteChange
{
    /**
     * Render this change as one entry in the
     * {@code ducklake_snapshot_changes.changes_made} text column.
     */
    String toChangesMadeEntry();

    record CreatedSchema(String schemaName)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "created_schema:" + writeQuotedValue(schemaName);
        }
    }

    /**
     * @param schemaName carried alongside the ID so the conflict matrix can
     *        cross-check intervening {@code created_table:"S".*} entries
     *        against this drop. Not serialized to {@code changes_made}.
     */
    record DroppedSchema(long schemaId, String schemaName)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "dropped_schema:" + schemaId;
        }
    }

    /**
     * @param schemaId carried alongside the names so the conflict matrix can
     *        cross-check this create against intervening
     *        {@code dropped_schema:<id>} entries. Not serialized to
     *        {@code changes_made}.
     */
    record CreatedTable(long schemaId, String schemaName, String tableName)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "created_table:" + writeQuotedValue(schemaName) + "." + writeQuotedValue(tableName);
        }
    }

    record DroppedTable(long tableId)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "dropped_table:" + tableId;
        }
    }

    /**
     * Covers {@code addColumn} / {@code dropColumn} / {@code renameColumn}.
     * Upstream's {@code ParseChangeType} groups these as {@code altered_table}.
     */
    record AlteredTable(long tableId)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "altered_table:" + tableId;
        }
    }

    /**
     * @param referencedColumnIds column IDs the data files depend on (drawn from
     *        each fragment's {@code columnStats}). Captured so a logical
     *        conflict check can verify they are still active at commit time.
     */
    record InsertedIntoTable(long tableId, Set<Long> referencedColumnIds)
            implements WriteChange
    {
        public InsertedIntoTable
        {
            referencedColumnIds = Set.copyOf(referencedColumnIds);
        }

        @Override
        public String toChangesMadeEntry()
        {
            return "inserted_into_table:" + tableId;
        }
    }

    /**
     * @param referencedDataFileIds {@code data_file_id}s the delete fragments
     *        target. Captured so a logical conflict check can verify they are
     *        still active at commit time.
     */
    record DeletedFromTable(long tableId, Set<Long> referencedDataFileIds)
            implements WriteChange
    {
        public DeletedFromTable
        {
            referencedDataFileIds = Set.copyOf(referencedDataFileIds);
        }

        @Override
        public String toChangesMadeEntry()
        {
            return "deleted_from_table:" + tableId;
        }
    }

    /**
     * @param schemaId carried alongside the names so the conflict matrix can
     *        cross-check this create against intervening
     *        {@code dropped_schema:<id>} entries. Not serialized to
     *        {@code changes_made}.
     */
    record CreatedView(long schemaId, String schemaName, String viewName)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "created_view:" + writeQuotedValue(schemaName) + "." + writeQuotedValue(viewName);
        }
    }

    record DroppedView(long viewId)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "dropped_view:" + viewId;
        }
    }

    /**
     * Covers {@code replaceViewMetadata} and {@code renameView}. Upstream's
     * {@code ParseChangeType} does not recognize {@code renamed_view}, so a
     * rename also serializes as {@code altered_view}.
     */
    record AlteredView(long viewId)
            implements WriteChange
    {
        @Override
        public String toChangesMadeEntry()
        {
            return "altered_view:" + viewId;
        }
    }

    /**
     * Comma-separated render for {@code ducklake_snapshot_changes.changes_made}.
     * Per upstream {@code ParseChangesList} (DuckDB
     * {@code ducklake_transaction_changes.cpp}), entries are split on unquoted
     * commas; balanced double quotes are tracked so commas inside quoted values
     * are literal.
     */
    static String formatChangesMade(List<WriteChange> changes)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < changes.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(changes.get(i).toChangesMadeEntry());
        }
        return sb.toString();
    }

    /**
     * Matches upstream's {@code KeywordHelper::WriteQuoted} (DuckDB) as consumed by
     * {@code DuckLakeUtil::ParseQuotedValue} in
     * {@code third_party/ducklake/src/common/ducklake_util.cpp}: wrap in double
     * quotes and escape embedded {@code "} by doubling.
     */
    static String writeQuotedValue(String value)
    {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
