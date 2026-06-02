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
import org.apache.parquet.format.FileMetaData
import org.apache.parquet.format.Util
import java.io.IOException
import java.io.OutputStream
import java.util.HashMap
import java.util.OptionalLong

/**
 * {@link DucklakeFileWriter} that delegates to Trino's {@link ParquetWriter}, streaming
 * directly to the destination via {@link TrinoOutputFile} (no local staging).
 */
class ParquetFileWriter(
        parquetWriter: ParquetWriter,
        outputStream: OutputStream,
        relativePath: String,
        partitionValues: Map<Int, String?>,
        partitionId: OptionalLong,
        columns: List<DucklakeColumnHandle>,
        allCatalogColumns: List<DucklakeColumn>)
        : DucklakeFileWriter {
    private val parquetWriter: ParquetWriter = parquetWriter
    private val outputStream: OutputStream = outputStream
    private val relativePath: String = relativePath
    private val partitionValues: MutableMap<Int, String?> = HashMap(partitionValues)
    private val partitionId: OptionalLong = partitionId
    private val leafStatsTargets: List<LeafStatsTarget>

    init {
        this.leafStatsTargets = DucklakeStatsLeafProjector.projectFromCatalogTree(columns, allCatalogColumns)
    }

    @Throws(IOException::class)
    override fun write(page: Page) {
        parquetWriter.write(page)
    }

    override fun getApproximateWrittenBytes(): Long {
        return parquetWriter.estimatedWrittenBytes
    }

    @Throws(IOException::class)
    override fun finishAndBuildFragment(): DucklakeWriteFragment {
        parquetWriter.close()

        val fileMetaData: FileMetaData = parquetWriter.fileMetaData
        val recordCount = fileMetaData.num_rows
        val fileSize = parquetWriter.estimatedWrittenBytes

        var footerSize: Long
        try {
            val footerOutput = DynamicSliceOutput(40)
            Util.writeFileMetaData(fileMetaData, footerOutput)
            footerSize = footerOutput.size().toLong()
        }
        catch (e: IOException) {
            footerSize = 0
        }

        val columnStats: List<DucklakeFileColumnStats> =
                DucklakeStatsExtractor.extractStats(fileMetaData, leafStatsTargets)

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
