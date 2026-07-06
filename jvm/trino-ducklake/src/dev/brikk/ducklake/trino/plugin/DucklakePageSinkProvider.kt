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

import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.json.JsonCodec
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.plugin.hive.parquet.ParquetWriterConfig
import io.trino.spi.NodeVersion
import io.trino.spi.PageIndexerFactory
import io.trino.spi.connector.ConnectorInsertTableHandle
import io.trino.spi.connector.ConnectorMergeSink
import io.trino.spi.connector.ConnectorMergeTableHandle
import io.trino.spi.connector.ConnectorOutputTableHandle
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.connector.ConnectorPageSinkId
import io.trino.spi.connector.ConnectorPageSinkProvider
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableCredentials
import io.trino.spi.connector.ConnectorTransactionHandle
import java.util.Optional

class DucklakePageSinkProvider @Inject constructor(
    private val fileSystemFactory: DucklakeFileSystemFactory,
    private val fragmentCodec: JsonCodec<DucklakeWriteFragment>,
    private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
    private val parquetWriterConfig: ParquetWriterConfig,
    parquetReaderConfig: ParquetReaderConfig,
    private val fileFormatDataSourceStats: FileFormatDataSourceStats,
    ducklakeConfig: DucklakeConfig,
    nodeVersion: NodeVersion,
    private val pageIndexerFactory: PageIndexerFactory
)
        : ConnectorPageSinkProvider {
    private val parquetReaderOptions: ParquetReaderOptions = parquetReaderConfig.toParquetReaderOptions()
    private val duckdbTargetWriteBytes: Long = ducklakeConfig
            .getDuckdbTargetWriteBytes().toBytes()
    private val trinoVersion: String = nodeVersion.toString()

    override fun createPageSink(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            outputTableHandle: ConnectorOutputTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            pageSinkId: ConnectorPageSinkId): ConnectorPageSink =
            createPageSink(outputTableHandle as DucklakeWritableTableHandle, session)

    override fun createPageSink(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            insertTableHandle: ConnectorInsertTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            pageSinkId: ConnectorPageSinkId): ConnectorPageSink =
            createPageSink(insertTableHandle as DucklakeWritableTableHandle, session)

    override fun createMergeSink(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            mergeHandle: ConnectorMergeTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            pageSinkId: ConnectorPageSinkId): ConnectorMergeSink {
        val ducklakeMergeHandle = mergeHandle as DucklakeMergeTableHandle

        val writerOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(parquetWriterConfig.blockSize)
                .setMaxPageSize(parquetWriterConfig.pageSize)
                .setMaxPageValueCount(parquetWriterConfig.pageValueCount)
                .setBatchSize(parquetWriterConfig.batchSize)
                .build()

        val insertSink = createPageSink(ducklakeMergeHandle.insertHandle, session)

        // Lineage-preserving rewrites (session write_row_lineage): a SECOND insert
        // sink embeds the original rowid column into UPDATE-rewritten rows.
        // Parquet-only — other data-file formats keep today's delete+insert shape.
        val lineageSink: DucklakePageSink? =
                if (DucklakeSessionProperties.isWriteRowLineage(session) &&
                        DucklakeSessionProperties.FORMAT_PARQUET.equals(
                                ducklakeMergeHandle.insertHandle.fileFormat, ignoreCase = true)) {
                    createPageSink(ducklakeMergeHandle.insertHandle, session, writeRowLineage = true)
                } else {
                    null
                }

        return DucklakeMergeSink(
                ducklakeMergeHandle,
                fileSystemFactory.create(session),
                deleteFragmentCodec,
                writerOptions,
                parquetReaderOptions,
                fileFormatDataSourceStats,
                trinoVersion,
                DucklakeSessionProperties.isWriteDeletionVectors(session),
                insertSink,
                lineageSink)
    }

    private fun createPageSink(
            handle: DucklakeWritableTableHandle,
            session: ConnectorSession,
            writeRowLineage: Boolean = false): DucklakePageSink =
            DucklakePageSink(
                    handle,
                    fileSystemFactory.create(session),
                    fragmentCodec,
                    parquetWriterConfig,
                    duckdbTargetWriteBytes,
                    trinoVersion,
                    pageIndexerFactory,
                    writeRowLineage)
}
