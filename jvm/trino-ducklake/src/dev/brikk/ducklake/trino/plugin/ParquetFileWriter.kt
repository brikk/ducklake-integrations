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

import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_PARQUET
import io.airlift.slice.DynamicSliceOutput
import io.trino.parquet.writer.ParquetWriter
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.RealType.REAL
import org.apache.parquet.format.FileMetaData
import org.apache.parquet.format.Util
import java.io.IOException
import java.io.OutputStream
import java.lang.Float.intBitsToFloat

/**
 * [DucklakeFileWriter] that delegates to Trino's [ParquetWriter], streaming
 * directly to the destination via [TrinoOutputFile] (no local staging).
 */
class ParquetFileWriter(
    private val parquetWriter: ParquetWriter,
    private val outputStream: OutputStream,
    private val relativePath: String,
    partitionValues: Map<Int, String?>,
    private val partitionId: Long?,
    columns: List<DucklakeColumnHandle>,
    allCatalogColumns: List<DucklakeColumn>)
        : DucklakeFileWriter {
    private val partitionValues: MutableMap<Int, String?> = partitionValues.toMutableMap()
    private val leafStatsTargets: List<LeafStatsTarget> =
        DucklakeStatsLeafProjector.projectFromCatalogTree(columns, allCatalogColumns)

    // Parquet footer stats carry min/max but NOT a NaN flag, and NaN is excluded from min/max —
    // so DuckLake would prune a NaN-bearing file. We observe values as they stream through to set
    // contains_nan for TOP-LEVEL REAL/DOUBLE columns (keyed by field_id == top-level columnId).
    // Nested float leaves (struct/array/map) are not scanned here — a documented follow-up.
    private val floatChannels: List<FloatChannel> = columns.withIndex()
        .filter { (_, c) -> c.columnType == REAL || c.columnType == DOUBLE }
        .map { (idx, c) -> FloatChannel(idx, c.columnId, c.columnType == REAL) }
    private val nanColumnIds: MutableSet<Long> = mutableSetOf()

    private data class FloatChannel(val channel: Int, val columnId: Long, val isReal: Boolean)

    @Throws(IOException::class)
    override fun write(page: Page) {
        recordNaNs(page)
        parquetWriter.write(page)
    }

    private fun recordNaNs(page: Page) {
        for (fc in floatChannels) {
            if (fc.columnId in nanColumnIds) {
                continue // already known to contain NaN — no need to keep scanning this column
            }
            if (blockContainsNaN(page.getBlock(fc.channel), fc.isReal)) {
                nanColumnIds.add(fc.columnId)
            }
        }
    }

    private fun blockContainsNaN(block: Block, isReal: Boolean): Boolean {
        for (position in 0 until block.positionCount) {
            if (block.isNull(position)) {
                continue
            }
            val isNan = if (isReal) {
                intBitsToFloat(REAL.getInt(block, position)).isNaN()
            }
            else {
                DOUBLE.getDouble(block, position).isNaN()
            }
            if (isNan) {
                return true
            }
        }
        return false
    }

    override fun getApproximateWrittenBytes(): Long {
        return parquetWriter.estimatedWrittenBytes
    }

    override fun getRetainedBytes(): Long {
        return parquetWriter.retainedBytes
    }

    @Throws(IOException::class)
    override fun finishAndBuildFragment(): DucklakeWriteFragment {
        parquetWriter.close()

        val fileMetaData: FileMetaData = parquetWriter.fileMetaData
        val recordCount = fileMetaData.num_rows
        val fileSize = parquetWriter.estimatedWrittenBytes

        // We need the Thrift FileMetaData length (excluding the 8-byte trailer) for the write
        // fragment. ParquetWriter.close() already computed this exact value, but it lives in a
        // private OptionalInt footerReadSize with no getter (and serializeFooter is private
        // static), so there is no way to reuse it without patching upstream trino-parquet —
        // out of scope here. Re-serializing the footer once per closed file is the deliberate,
        // accepted cost: the footer is metadata-only and tiny relative to the full-file
        // encoding/compression that just finished, and this runs per file close, not per page.
        var footerSize: Long
        try {
            val footerOutput = DynamicSliceOutput(40)
            Util.writeFileMetaData(fileMetaData, footerOutput)
            footerSize = footerOutput.size().toLong()
        }
        catch (e: IOException) {
            footerSize = 0
        }

        val extracted: List<DucklakeFileColumnStats> =
                DucklakeStatsExtractor.extractStats(fileMetaData, leafStatsTargets)
        // Footer min/max can't express NaN; fold in the contains_nan bits observed during write
        // (top-level REAL/DOUBLE columns, keyed by field_id).
        val columnStats: List<DucklakeFileColumnStats> =
                if (nanColumnIds.isEmpty()) extracted
                else extracted.map { if (it.columnId in nanColumnIds) it.copy(containsNan = true) else it }

        return DucklakeWriteFragment(
                relativePath,
                FORMAT_PARQUET,
                fileSize,
                footerSize,
                recordCount,
                columnStats,
                partitionValues,
                partitionId)
    }

    override fun abort() {
        try {
            parquetWriter.close()
        }
        catch (ignored: IOException) {
            // best-effort
        }
        try {
            outputStream.close()
        }
        catch (ignored: IOException) {
            // best-effort
        }
    }
}
