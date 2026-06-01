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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Reads positional rows from a DuckLake delete parquet file. Delete files contain a
 * single primitive column (INT64 or INT32) whose values are either global row IDs or
 * file-local row offsets — the page source decides which interpretation applies at
 * filter time, so this reader returns the raw values verbatim.
 *
 * <p>Centralized so both the read path (apply-deletes during scan) and the write
 * path (merge sink unioning prior positions into a superseding file) call the same
 * code. Keeping the reader in one place is a B3a invariant: if the two paths drift,
 * the union could mis-interpret values and either lose or re-introduce deletions.
 */
final class DucklakeDeleteFileReader
{
    private DucklakeDeleteFileReader() {}

    /**
     * Reads all non-null positions from a single parquet delete file. The returned set
     * is unordered and dedup'd (the read path tolerates duplicate aliasing — see
     * {@code TestDeleteRowFilterTransformOverlap}).
     */
    static Set<Long> readPositions(
            TrinoFileSystem fileSystem,
            String deleteFilePath,
            long footerSizeHint,
            ParquetReaderOptions parquetReaderOptions,
            FileFormatDataSourceStats stats)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(toLocation(deleteFilePath));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetDataSource dataSource = null;
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.empty(),
                    parquetReaderOptions,
                    memoryContext,
                    stats);

            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(dataSource, footerSizeHint);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            ParquetDataSourceId dataSourceId = dataSource.getId();
            MessageType fileSchema = fileMetadata.getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

            DeleteFileColumn deleteFileColumn = getDeleteFileColumn(fileSchema, messageColumnIO);
            java.util.Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.all();
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    ImmutableList.of(new Column(deleteFileColumn.columnName(), deleteFileColumn.field())),
                    false,
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Set<Long> deletedRows = new HashSet<>();
            try (ConnectorPageSource pageSource = new ParquetPageSource(parquetReader)) {
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }
                    Block block = page.getBlock(0);
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        if (block.isNull(position)) {
                            continue;
                        }
                        deletedRows.add(readDeleteValue(deleteFileColumn.columnType(), block, position));
                    }
                }
            }
            return deletedRows;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to read delete file: " + deleteFilePath, e);
        }
    }

    private static DeleteFileColumn getDeleteFileColumn(MessageType fileSchema, MessageColumnIO messageColumnIO)
    {
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (!field.isPrimitive()) {
                continue;
            }
            ColumnIO columnIO = messageColumnIO.getChild(field.getName());
            if (!(columnIO instanceof PrimitiveColumnIO primitiveColumnIO)) {
                continue;
            }

            PrimitiveTypeName primitiveTypeName = primitiveColumnIO.getPrimitive();
            Type columnType = switch (primitiveTypeName) {
                case INT64 -> BIGINT;
                case INT32 -> INTEGER;
                default -> null;
            };

            if (columnType != null) {
                Field fieldDefinition = DucklakeParquetTypeUtils.constructField(columnType, columnIO)
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Could not construct field for delete file column: " + field.getName()));
                return new DeleteFileColumn(field.getName(), columnType, fieldDefinition);
            }
        }

        throw new TrinoException(NOT_SUPPORTED, "Delete file must contain at least one INT32/INT64 primitive column");
    }

    private static long readDeleteValue(Type type, Block block, int position)
    {
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        throw new IllegalArgumentException("Unsupported delete file value type: " + type);
    }

    private static RuntimeException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        return new TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: " + dataSourceId,
                exception);
    }

    private static Location toLocation(String path)
    {
        Location location = Location.of(path);
        if (location.scheme().isPresent()) {
            return location;
        }
        return Location.of(Path.of(path).toUri().toString());
    }

    private record DeleteFileColumn(String columnName, Type columnType, Field field) {}
}
