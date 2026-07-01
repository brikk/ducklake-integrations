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
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException

/**
 * One data file to read for the change feed. [baseSource] lazily opens the connector's normal
 * data-file read pipeline projecting exactly [ChangeFeedPageSource]'s requested data columns
 * followed by the positional `$row_id` virtual (`rowIdStart + file position` = the DuckLake
 * `rowid`), reading as of the feed's END-snapshot schema (so schema evolution / all file formats
 * are handled for free).
 *
 * - [snapshotId] is the snapshot to report for every row of this unit (the file's `begin_snapshot`
 *   for an insert unit; the deletion's snapshot for a delete unit).
 * - [keepRowids] = null keeps every row (insert unit); otherwise only rows whose `rowid` is in the
 *   set are emitted (delete unit — the positions newly deleted at [snapshotId]).
 * - [updatedRowids] are the `rowid`s at [snapshotId] that are BOTH inserted and deleted — i.e.
 *   updates. Their `change_type` becomes [ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE] on the insert
 *   side and [ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE] on the delete side; all other rows get the
 *   plain [insert] / [delete] change type.
 *
 * NOTE on update pairing: this connector's `rowid` is `row_id_start + file position`, which does
 * NOT carry DuckLake's cross-file row lineage — an UPDATE (in Trino OR DuckDB) rewrites the moved
 * row into a new file with a FRESH `row_id_start`, so its `rowid` differs from the deleted row's.
 * Consequently [updatedRowids] is empty in practice and UPDATEs surface as a separate `delete` +
 * `insert` (which is a faithful description of DuckLake's delete-then-insert UPDATE). The pairing
 * remains correct and will fire whenever a deleted `rowid` is re-inserted with the same value in
 * one snapshot; [TestChangeFeedPageSource] exercises that path directly.
 */
class ChangeFeedUnit(
        val baseSource: () -> ConnectorPageSource,
        val snapshotId: Long,
        val keepRowids: Set<Long>?,
        val updatedRowids: Set<Long>,
        val isDelete: Boolean)

/**
 * Streams the DuckLake change feed as `snapshot_id, rowid[, change_type]` + table columns
 * (F9). Reads each [ChangeFeedUnit]'s data file through the connector's ordinary read pipeline
 * (reused for schema evolution + every file format), then per page: filters to the deleted
 * positions (delete units), stamps `snapshot_id`, exposes `rowid`, and classifies `change_type`
 * (pairing insert+delete of the same `rowid` in one snapshot into update pre/post-image — see the
 * NOTE on [ChangeFeedUnit] for when that fires; in practice UPDATEs surface as a separate
 * `delete` + `insert` under this connector's `row_id_start + position` rowid vocabulary).
 *
 * Output blocks are projected in [requestedColumns] order; the data columns are read in
 * [dataColumns] order followed by `$row_id`, so `$row_id` sits at channel `dataColumns.size`.
 */
class ChangeFeedPageSource(
        units: List<ChangeFeedUnit>,
        private val requestedColumns: List<DucklakeColumnHandle>,
        private val dataColumns: List<DucklakeColumnHandle>,
        private val emitChangeType: Boolean) : ConnectorPageSource {
    private val units: List<ChangeFeedUnit> = units.toList()
    private val rowIdChannel: Int = dataColumns.size
    private val dataChannelByColumnId: Map<Long, Int> = dataColumns.withIndex()
            .associate { (i, col) -> col.columnId to i }

    private var unitIndex: Int = 0
    private var currentUnit: ChangeFeedUnit? = null
    private var currentSource: ConnectorPageSource? = null
    private var finished: Boolean = false
    private var completedBytes: Long = 0
    private var readTimeNanos: Long = 0

    override fun getCompletedBytes(): Long = completedBytes

    override fun getReadTimeNanos(): Long = readTimeNanos

    override fun isFinished(): Boolean = finished

    override fun getMemoryUsage(): Long = currentSource?.memoryUsage ?: 0L

    override fun getNextSourcePage(): SourcePage? {
        while (!finished) {
            val source: ConnectorPageSource = currentSource ?: run {
                if (unitIndex >= units.size) {
                    finished = true
                    return null
                }
                currentUnit = units[unitIndex]
                units[unitIndex].baseSource().also { currentSource = it }
            }

            val page: SourcePage? = source.nextSourcePage
            if (page == null) {
                completedBytes += source.completedBytes
                readTimeNanos += source.readTimeNanos
                source.close()
                currentSource = null
                currentUnit = null
                unitIndex++
                continue
            }
            val transformed: SourcePage? = transform(currentUnit!!, page)
            if (transformed != null) {
                return transformed
            }
            // Every row of this page was filtered out (delete unit); try the next page.
        }
        return null
    }

    private fun transform(unit: ChangeFeedUnit, page: SourcePage): SourcePage? {
        val positionCount: Int = page.positionCount
        val rowIdBlock: Block = page.getBlock(rowIdChannel)

        val keepRowids: Set<Long>? = unit.keepRowids
        val keptPositions: IntArray
        val keptCount: Int
        if (keepRowids == null) {
            keptPositions = IntArray(0)
            keptCount = positionCount
        }
        else {
            val positions = IntArray(positionCount)
            var count = 0
            for (i in 0 until positionCount) {
                if (!rowIdBlock.isNull(i) && keepRowids.contains(BIGINT.getLong(rowIdBlock, i))) {
                    positions[count] = i
                    count++
                }
            }
            keptPositions = positions
            keptCount = count
        }
        if (keptCount == 0) {
            return null
        }
        val selectAll: Boolean = keepRowids == null

        val blocks: Array<Block?> = arrayOfNulls(requestedColumns.size)
        for ((outIndex, column) in requestedColumns.withIndex()) {
            blocks[outIndex] = when (column.columnId) {
                ChangeFeedColumns.SNAPSHOT_ID.columnId ->
                    RunLengthEncodedBlock.create(writeNativeValue(BIGINT, unit.snapshotId), keptCount)
                ChangeFeedColumns.ROWID.columnId ->
                    selectPositions(rowIdBlock, keptPositions, keptCount, selectAll)
                ChangeFeedColumns.CHANGE_TYPE.columnId ->
                    changeTypeBlock(unit, rowIdBlock, keptPositions, keptCount, selectAll, positionCount)
                else -> {
                    val channel: Int = dataChannelByColumnId[column.columnId]
                            ?: throw IllegalStateException("Change-feed column not in read plan: ${column.columnName}")
                    selectPositions(page.getBlock(channel), keptPositions, keptCount, selectAll)
                }
            }
        }
        @Suppress("UNCHECKED_CAST")
        return SourcePage.create(Page(keptCount, *(blocks as Array<Block>)))
    }

    private fun selectPositions(block: Block, keptPositions: IntArray, keptCount: Int, selectAll: Boolean): Block =
        if (selectAll) block else block.getPositions(keptPositions, 0, keptCount)

    private fun changeTypeBlock(
            unit: ChangeFeedUnit,
            rowIdBlock: Block,
            keptPositions: IntArray,
            keptCount: Int,
            selectAll: Boolean,
            positionCount: Int): Block {
        val updatedLabel: String = if (unit.isDelete) {
            ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE
        }
        else {
            ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE
        }
        val plainLabel: String = if (unit.isDelete) ChangeFeedColumns.CHANGE_DELETE else ChangeFeedColumns.CHANGE_INSERT
        val builder = VARCHAR.createBlockBuilder(null, keptCount)
        val limit: Int = if (selectAll) positionCount else keptCount
        for (i in 0 until limit) {
            val position: Int = if (selectAll) i else keptPositions[i]
            val rowId: Long = BIGINT.getLong(rowIdBlock, position)
            val label: String = if (unit.updatedRowids.contains(rowId)) updatedLabel else plainLabel
            VARCHAR.writeSlice(builder, Slices.utf8Slice(label))
        }
        return builder.build()
    }

    init {
        require(emitChangeType || requestedColumns.none { it.columnId == ChangeFeedColumns.CHANGE_TYPE.columnId }) {
            "change_type requested but this feed does not emit it"
        }
    }

    @Throws(IOException::class)
    override fun close() {
        currentSource?.close()
        currentSource = null
    }
}
