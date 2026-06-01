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
import java.util.Objects.requireNonNull
import java.util.Optional

class DucklakePageSinkProvider @Inject constructor(
        fileSystemFactory: DucklakeFileSystemFactory,
        fragmentCodec: JsonCodec<DucklakeWriteFragment>,
        deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        parquetWriterConfig: ParquetWriterConfig,
        parquetReaderConfig: ParquetReaderConfig,
        fileFormatDataSourceStats: FileFormatDataSourceStats,
        ducklakeConfig: DucklakeConfig,
        nodeVersion: NodeVersion,
        pageIndexerFactory: PageIndexerFactory)
        : ConnectorPageSinkProvider {
    private val fileSystemFactory: DucklakeFileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
    private val fragmentCodec: JsonCodec<DucklakeWriteFragment> = requireNonNull(fragmentCodec, "fragmentCodec is null")
    private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment> = requireNonNull(deleteFragmentCodec, "deleteFragmentCodec is null")
    private val parquetWriterConfig: ParquetWriterConfig = requireNonNull(parquetWriterConfig, "parquetWriterConfig is null")
    private val parquetReaderOptions: ParquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions()
    private val fileFormatDataSourceStats: FileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null")
    private val duckdbTargetWriteBytes: Long = requireNonNull(ducklakeConfig, "ducklakeConfig is null")
            .getDuckdbTargetWriteBytes().toBytes()
    private val trinoVersion: String = requireNonNull(nodeVersion, "nodeVersion is null").toString()
    private val pageIndexerFactory: PageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null")

    override fun createPageSink(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            outputTableHandle: ConnectorOutputTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            pageSinkId: ConnectorPageSinkId): ConnectorPageSink {
        return createPageSink(outputTableHandle as DucklakeWritableTableHandle, session)
    }

    override fun createPageSink(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            insertTableHandle: ConnectorInsertTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            pageSinkId: ConnectorPageSinkId): ConnectorPageSink {
        return createPageSink(insertTableHandle as DucklakeWritableTableHandle, session)
    }

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

        val insertSink = createPageSink(ducklakeMergeHandle.insertHandle(), session)

        return DucklakeMergeSink(
                ducklakeMergeHandle,
                fileSystemFactory.create(session),
                deleteFragmentCodec,
                fragmentCodec,
                writerOptions,
                parquetReaderOptions,
                fileFormatDataSourceStats,
                trinoVersion,
                insertSink)
    }

    private fun createPageSink(handle: DucklakeWritableTableHandle, session: ConnectorSession): DucklakePageSink {
        return DucklakePageSink(
                handle,
                fileSystemFactory.create(session),
                fragmentCodec,
                parquetWriterConfig,
                duckdbTargetWriteBytes,
                trinoVersion,
                pageIndexerFactory)
    }
}
