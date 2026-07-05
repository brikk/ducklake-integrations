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
 * followed by the positional `$row_id` virtual (`rowIdStart + file position`), reading as of the
 * feed's END-snapshot schema (so schema evolution / all file formats are handled for free).
 *
 * - [snapshotId] is the snapshot to report for every row of this unit (the file's `begin_snapshot`
 *   for an insert unit; the deletion's snapshot for a delete unit).
 * - [keepPositions] = null keeps every row (insert unit); otherwise only rows at the given FILE
 *   positions are emitted (delete unit — the positions newly deleted at [snapshotId]).
 * - The emitted `rowid` is [lineageRowIds]`[position]` when the file carries DuckLake's embedded
 *   row-lineage column (UPDATE/compaction output — see [DucklakeDeleteFileReader.readInternalRowIds]),
 *   else `rowIdStart + position`. Using the preserved rowid is what lets an UPDATE's delete and
 *   re-insert land on the SAME rowid in one snapshot and pair into update pre/post-image.
 * - [updatedRowids] are the `rowid`s at [snapshotId] that are BOTH deleted and re-inserted — i.e.
 *   updates. Their `change_type` becomes [ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE] on the insert
 *   side and [ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE] on the delete side; all others get the
 *   plain [insert] / [delete] change type. (Trino-written UPDATEs allocate a fresh rowid and write
 *   no lineage column, so they still surface as delete+insert — a faithful description of Trino's
 *   delete-then-insert UPDATE.)
 */
class ChangeFeedUnit(
        val baseSource: () -> ConnectorPageSource,
        val snapshotId: Long,
        val rowIdStart: Long,
        val lineageRowIds: LongArray?,
        val keepPositions: Set<Long>?,
        val updatedRowids: Set<Long>,
        val isDelete: Boolean)

/**
 * Streams the DuckLake change feed as `snapshot_id, rowid[, change_type]` + table columns.
 * Reads each [ChangeFeedUnit]'s data file through the connector's ordinary read pipeline (reused
 * for schema evolution + every file format), then per page: derives each row's FILE position from
 * the positional `$row_id`, filters to the deleted positions (delete units), computes the true
 * `rowid` (embedded lineage column when present, else positional), stamps `snapshot_id`, and
 * classifies `change_type` (pairing an UPDATE's delete + re-insert of the same rowid in one
 * snapshot into update pre/post-image).
 *
 * The data columns are read in [dataColumns] order followed by `$row_id`, so `$row_id` sits at
 * channel `dataColumns.size`; output blocks are projected in [requestedColumns] order.
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

        val keptIdx = IntArray(positionCount)
        val keptRowIds = LongArray(positionCount)
        var keptCount = 0
        for (i in 0 until positionCount) {
            val positional: Long = BIGINT.getLong(rowIdBlock, i)
            val filePosition: Long = positional - unit.rowIdStart
            if (unit.keepPositions != null && !unit.keepPositions.contains(filePosition)) {
                continue
            }
            keptIdx[keptCount] = i
            keptRowIds[keptCount] = rowIdFor(unit, filePosition, positional)
            keptCount++
        }
        if (keptCount == 0) {
            return null
        }
        // keepPositions == null means the whole page is kept in order, so data blocks pass through.
        val selectAll: Boolean = unit.keepPositions == null

        val blocks: Array<Block?> = arrayOfNulls(requestedColumns.size)
        for ((outIndex, column) in requestedColumns.withIndex()) {
            blocks[outIndex] = when (column.columnId) {
                ChangeFeedColumns.SNAPSHOT_ID.columnId ->
                    RunLengthEncodedBlock.create(writeNativeValue(BIGINT, unit.snapshotId), keptCount)
                ChangeFeedColumns.ROWID.columnId -> rowIdBlock(keptRowIds, keptCount)
                ChangeFeedColumns.CHANGE_TYPE.columnId -> changeTypeBlock(unit, keptRowIds, keptCount)
                else -> {
                    val channel: Int = dataChannelByColumnId[column.columnId]
                            ?: throw IllegalStateException("Change-feed column not in read plan: ${column.columnName}")
                    val block: Block = page.getBlock(channel)
                    if (selectAll) block else block.getPositions(keptIdx, 0, keptCount)
                }
            }
        }
        @Suppress("UNCHECKED_CAST")
        return SourcePage.create(Page(keptCount, *(blocks as Array<Block>)))
    }

    /** The row's DuckLake rowid: preserved lineage value when the file embeds it, else positional. */
    private fun rowIdFor(unit: ChangeFeedUnit, filePosition: Long, positional: Long): Long {
        val lineage: LongArray = unit.lineageRowIds ?: return positional
        return if (filePosition in 0 until lineage.size.toLong()) lineage[filePosition.toInt()] else positional
    }

    private fun rowIdBlock(keptRowIds: LongArray, keptCount: Int): Block {
        val builder = BIGINT.createBlockBuilder(null, keptCount)
        for (j in 0 until keptCount) {
            BIGINT.writeLong(builder, keptRowIds[j])
        }
        return builder.build()
    }

    private fun changeTypeBlock(unit: ChangeFeedUnit, keptRowIds: LongArray, keptCount: Int): Block {
        val updatedLabel: String = if (unit.isDelete) {
            ChangeFeedColumns.CHANGE_UPDATE_PREIMAGE
        }
        else {
            ChangeFeedColumns.CHANGE_UPDATE_POSTIMAGE
        }
        val plainLabel: String = if (unit.isDelete) ChangeFeedColumns.CHANGE_DELETE else ChangeFeedColumns.CHANGE_INSERT
        val builder = VARCHAR.createBlockBuilder(null, keptCount)
        for (j in 0 until keptCount) {
            val label: String = if (unit.updatedRowids.contains(keptRowIds[j])) updatedLabel else plainLabel
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
