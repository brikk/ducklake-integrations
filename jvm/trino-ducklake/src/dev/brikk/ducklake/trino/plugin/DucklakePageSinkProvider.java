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
package dev.brikk.ducklake.trino.plugin;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.parquet.writer.ParquetWriterOptions;
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class DucklakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DucklakeFileSystemFactory fileSystemFactory;
    private final JsonCodec<DucklakeWriteFragment> fragmentCodec;
    private final JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec;
    private final ParquetWriterConfig parquetWriterConfig;
    private final String trinoVersion;
    private final PageIndexerFactory pageIndexerFactory;

    @Inject
    public DucklakePageSinkProvider(
            DucklakeFileSystemFactory fileSystemFactory,
            JsonCodec<DucklakeWriteFragment> fragmentCodec,
            JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec,
            ParquetWriterConfig parquetWriterConfig,
            NodeVersion nodeVersion,
            PageIndexerFactory pageIndexerFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fragmentCodec = requireNonNull(fragmentCodec, "fragmentCodec is null");
        this.deleteFragmentCodec = requireNonNull(deleteFragmentCodec, "deleteFragmentCodec is null");
        this.parquetWriterConfig = requireNonNull(parquetWriterConfig, "parquetWriterConfig is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink((DucklakeWritableTableHandle) outputTableHandle, session);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink((DucklakeWritableTableHandle) insertTableHandle, session);
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId)
    {
        DucklakeMergeTableHandle ducklakeMergeHandle = (DucklakeMergeTableHandle) mergeHandle;

        ParquetWriterOptions writerOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(parquetWriterConfig.getBlockSize())
                .setMaxPageSize(parquetWriterConfig.getPageSize())
                .setMaxPageValueCount(parquetWriterConfig.getPageValueCount())
                .setBatchSize(parquetWriterConfig.getBatchSize())
                .build();

        ConnectorPageSink insertSink = createPageSink(ducklakeMergeHandle.insertHandle(), session);

        return new DucklakeMergeSink(
                ducklakeMergeHandle,
                fileSystemFactory.create(session),
                deleteFragmentCodec,
                fragmentCodec,
                writerOptions,
                trinoVersion,
                insertSink);
    }

    private DucklakePageSink createPageSink(DucklakeWritableTableHandle handle, ConnectorSession session)
    {
        return new DucklakePageSink(
                handle,
                fileSystemFactory.create(session),
                fragmentCodec,
                parquetWriterConfig,
                trinoVersion,
                pageIndexerFactory);
    }
}
