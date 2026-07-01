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
package dev.brikk.ducklake.trino.plugin

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * The three DuckLake change-feed table functions exposed under `<catalog>.system.*`
 * (F9 — the data change feed; see vendor/ducklake-web .../data_change_feed.md):
 *
 *  - `table_insertions(schema, table, ...)` — rows INSERTED in the window;
 *  - `table_deletions(schema, table, ...)`  — rows DELETED in the window;
 *  - `table_changes(schema, table, ...)`    — both, tagged with a `change_type`.
 *
 * [hasChangeType] governs the output layout: `table_changes` prepends `snapshot_id`, `rowid`,
 * `change_type`; the two single-sided functions prepend only `snapshot_id`, `rowid` (there is no
 * `change_type` because every returned row is the one kind — matching the DuckLake spec).
 */
enum class ChangeFeedType(val functionName: String, val hasChangeType: Boolean) {
    INSERTIONS("table_insertions", false),
    DELETIONS("table_deletions", false),
    CHANGES("table_changes", true),
}

/**
 * The synthetic metadata columns the change feed prepends to a table's own columns, plus the
 * `change_type` value vocabulary. Their `columnId`s are private negative sentinels well away from
 * real catalog columns (>= 0), the queryable virtuals (-101..-105), and the MERGE row-id channel
 * (-100) — the change-feed page source recognizes them by id when projecting output blocks.
 */
object ChangeFeedColumns {
    val SNAPSHOT_ID: DucklakeColumnHandle = DucklakeColumnHandle(-201L, "snapshot_id", BIGINT, false)
    val ROWID: DucklakeColumnHandle = DucklakeColumnHandle(-202L, "rowid", BIGINT, false)
    val CHANGE_TYPE: DucklakeColumnHandle = DucklakeColumnHandle(-203L, "change_type", VARCHAR, false)

    const val CHANGE_INSERT: String = "insert"
    const val CHANGE_DELETE: String = "delete"
    const val CHANGE_UPDATE_PREIMAGE: String = "update_preimage"
    const val CHANGE_UPDATE_POSTIMAGE: String = "update_postimage"

    /** The full output layout: the prepended metadata columns followed by the table's columns. */
    fun outputColumns(type: ChangeFeedType, dataColumns: List<DucklakeColumnHandle>): List<DucklakeColumnHandle> {
        val prefix: List<DucklakeColumnHandle> = if (type.hasChangeType) {
            listOf(SNAPSHOT_ID, ROWID, CHANGE_TYPE)
        }
        else {
            listOf(SNAPSHOT_ID, ROWID)
        }
        return prefix + dataColumns
    }
}
