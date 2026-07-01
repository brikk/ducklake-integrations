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

import io.airlift.slice.Slices
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Direct unit coverage of [ChangeFeedPageSource]'s per-page transform: constant `snapshot_id`,
 * `rowid` passthrough, `change_type` classification (including the `update_preimage` /
 * `update_postimage` pairing — which real SQL rarely reaches because the `row_id_start + position`
 * rowid vocabulary doesn't carry cross-file lineage), delete-position filtering, unit
 * concatenation, and projection.
 */
class TestChangeFeedPageSource {
    private val valColumn = DucklakeColumnHandle(1L, "val", VARCHAR, true)

    /** A one-page base source producing [val, rowid] blocks (rowid at the last channel). */
    private class SinglePageSource(private val page: Page) : ConnectorPageSource {
        private var done = false
        override fun getCompletedBytes(): Long = 0L
        override fun getReadTimeNanos(): Long = 0L
        override fun isFinished(): Boolean = done
        override fun getMemoryUsage(): Long = 0L
        override fun getNextSourcePage(): SourcePage? {
            if (done) return null
            done = true
            return SourcePage.create(page)
        }
        override fun close() {
            // no resources to release
        }
    }

    private fun varchar(vararg values: String): Block {
        val builder = VARCHAR.createBlockBuilder(null, values.size)
        values.forEach { VARCHAR.writeSlice(builder, Slices.utf8Slice(it)) }
        return builder.build()
    }

    private fun bigint(vararg values: Long): Block {
        val builder = BIGINT.createBlockBuilder(null, values.size)
        values.forEach { BIGINT.writeLong(builder, it) }
        return builder.build()
    }

    private fun basePage(vals: Array<String>, rowids: LongArray): () -> ConnectorPageSource =
        { SinglePageSource(Page(vals.size, varchar(*vals), bigint(*rowids))) }

    private fun readRows(source: ChangeFeedPageSource, columns: List<DucklakeColumnHandle>): List<List<Any?>> {
        val out = mutableListOf<List<Any?>>()
        while (!source.isFinished) {
            val page: SourcePage = source.nextSourcePage ?: continue
            for (position in 0 until page.positionCount) {
                out.add(columns.indices.map { channel ->
                    val block: Block = page.getBlock(channel)
                    when (columns[channel].columnType) {
                        BIGINT -> BIGINT.getLong(block, position)
                        VARCHAR -> VARCHAR.getSlice(block, position).toStringUtf8()
                        else -> null
                    }
                })
            }
        }
        return out
    }

    private fun changeColumns(): List<DucklakeColumnHandle> =
        listOf(ChangeFeedColumns.SNAPSHOT_ID, ChangeFeedColumns.ROWID, ChangeFeedColumns.CHANGE_TYPE, valColumn)

    @Test
    fun insertUnitStampsSnapshotRowidAndInsertType() {
        val columns = changeColumns()
        val unit = ChangeFeedUnit(
                baseSource = basePage(arrayOf("a", "b"), longArrayOf(0L, 1L)),
                snapshotId = 7L,
                keepRowids = null,
                updatedRowids = emptySet(),
                isDelete = false)
        val rows = readRows(ChangeFeedPageSource(listOf(unit), columns, listOf(valColumn), true), columns)
        assertThat(rows).containsExactly(
                listOf(7L, 0L, "insert", "a"),
                listOf(7L, 1L, "insert", "b"))
    }

    @Test
    fun deleteUnitKeepsOnlyDeletedRowidsWithDeleteType() {
        val columns = changeColumns()
        val unit = ChangeFeedUnit(
                baseSource = basePage(arrayOf("a", "b", "c"), longArrayOf(10L, 11L, 12L)),
                snapshotId = 9L,
                keepRowids = setOf(11L),
                updatedRowids = emptySet(),
                isDelete = true)
        val rows = readRows(ChangeFeedPageSource(listOf(unit), columns, listOf(valColumn), true), columns)
        assertThat(rows).containsExactly(listOf(9L, 11L, "delete", "b"))
    }

    @Test
    fun updatedRowidsProduceUpdatePreAndPostImage() {
        val columns = changeColumns()
        val postImage = ChangeFeedUnit(
                baseSource = basePage(arrayOf("new"), longArrayOf(5L)),
                snapshotId = 3L,
                keepRowids = null,
                updatedRowids = setOf(5L),
                isDelete = false)
        val preImage = ChangeFeedUnit(
                baseSource = basePage(arrayOf("old", "gone"), longArrayOf(5L, 6L)),
                snapshotId = 3L,
                keepRowids = setOf(5L, 6L),
                updatedRowids = setOf(5L),
                isDelete = true)
        val rows = readRows(ChangeFeedPageSource(listOf(postImage, preImage), columns, listOf(valColumn), true), columns)
        assertThat(rows).containsExactly(
                listOf(3L, 5L, "update_postimage", "new"),
                listOf(3L, 5L, "update_preimage", "old"),
                listOf(3L, 6L, "delete", "gone"))
    }

    @Test
    fun projectionSelectsRequestedColumnsOnly() {
        // table_deletions layout without change_type, projecting only rowid + val.
        val columns = listOf(ChangeFeedColumns.ROWID, valColumn)
        val unit = ChangeFeedUnit(
                baseSource = basePage(arrayOf("x", "y"), longArrayOf(0L, 1L)),
                snapshotId = 2L,
                keepRowids = setOf(1L),
                updatedRowids = emptySet(),
                isDelete = true)
        val rows = readRows(ChangeFeedPageSource(listOf(unit), columns, listOf(valColumn), false), columns)
        assertThat(rows).containsExactly(listOf(1L, "y"))
    }

    @Test
    fun emptyDeleteUnitYieldsNoRows() {
        val columns = changeColumns()
        val unit = ChangeFeedUnit(
                baseSource = basePage(arrayOf("a", "b"), longArrayOf(0L, 1L)),
                snapshotId = 1L,
                keepRowids = setOf(99L),
                updatedRowids = emptySet(),
                isDelete = true)
        val rows = readRows(ChangeFeedPageSource(listOf(unit), columns, listOf(valColumn), true), columns)
        assertThat(rows).isEmpty()
    }
}
