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

import dev.brikk.ducklake.catalog.DucklakePartitionField;
import dev.brikk.ducklake.catalog.DucklakePartitionSpec;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

/**
 * Partitions pages by computing DuckLake partition values and routing rows to writer indexes.
 * Uses Trino's {@link PageIndexerFactory} for efficient partition-to-index mapping.
 */
public class DucklakePagePartitioner
{
    private final DucklakePartitionSpec partitionSpec;
    private final List<DucklakeColumnHandle> allColumns;
    private final DucklakeTemporalPartitionEncoding encoding;
    private final PageIndexer pageIndexer;
    private final List<PartitionColumnMapping> partitionColumnMappings;

    public DucklakePagePartitioner(
            PageIndexerFactory pageIndexerFactory,
            DucklakePartitionSpec partitionSpec,
            List<DucklakeColumnHandle> allColumns,
            DucklakeTemporalPartitionEncoding encoding)
    {
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.allColumns = List.copyOf(requireNonNull(allColumns, "allColumns is null"));
        this.encoding = requireNonNull(encoding, "encoding is null");

        // Build mappings: for each partition field, find the source column index and type
        this.partitionColumnMappings = new ArrayList<>();
        for (DucklakePartitionField field : partitionSpec.fields()) {
            int sourceColumnIndex = -1;
            DucklakeColumnHandle sourceColumn = null;
            for (int i = 0; i < allColumns.size(); i++) {
                if (allColumns.get(i).columnId() == field.columnId()) {
                    sourceColumnIndex = i;
                    sourceColumn = allColumns.get(i);
                    break;
                }
            }
            if (sourceColumnIndex < 0) {
                throw new IllegalArgumentException("Partition field references unknown column ID: " + field.columnId());
            }

            // For the page indexer, the partition column type is:
            // - IDENTITY: the source column type
            // - Temporal: INTEGER (the computed partition value is an integer)
            Type indexerType = field.transform().isIdentity() ? sourceColumn.columnType() : INTEGER;

            partitionColumnMappings.add(new PartitionColumnMapping(field, sourceColumnIndex, sourceColumn, indexerType));
        }

        // Create page indexer for the partition column types
        List<Type> partitionTypes = partitionColumnMappings.stream()
                .map(PartitionColumnMapping::indexerType)
                .toList();
        this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionTypes);
    }

    /**
     * Partition a page into writer indexes. Returns an array where element[i] is the writer index
     * for row position i.
     */
    public int[] partitionPage(Page page)
    {
        Page partitionPage = buildPartitionPage(page);
        return pageIndexer.indexPage(partitionPage);
    }

    public int getMaxIndex()
    {
        return pageIndexer.getMaxIndex();
    }

    /**
     * Compute partition values for a given row position. Returns a map of partitionKeyIndex -> value string.
     */
    public Map<Integer, String> getPartitionValues(Page page, int position)
    {
        Map<Integer, String> values = new HashMap<>();
        for (PartitionColumnMapping mapping : partitionColumnMappings) {
            Block sourceBlock = page.getBlock(mapping.sourceColumnIndex());
            String value = DucklakePartitionComputer.computePartitionValue(
                    mapping.sourceColumn().columnType(),
                    sourceBlock,
                    position,
                    mapping.field().transform(),
                    encoding);
            values.put(mapping.field().partitionKeyIndex(), value);
        }
        return values;
    }

    /**
     * Get the column name for a partition field (used for directory naming).
     */
    public String getPartitionColumnName(int partitionKeyIndex)
    {
        for (PartitionColumnMapping mapping : partitionColumnMappings) {
            if (mapping.field().partitionKeyIndex() == partitionKeyIndex) {
                return mapping.sourceColumn().columnName();
            }
        }
        throw new IllegalArgumentException("Unknown partition key index: " + partitionKeyIndex);
    }

    public long getPartitionId()
    {
        return partitionSpec.partitionId();
    }

    private Page buildPartitionPage(Page page)
    {
        Block[] partitionBlocks = new Block[partitionColumnMappings.size()];
        for (int i = 0; i < partitionColumnMappings.size(); i++) {
            PartitionColumnMapping mapping = partitionColumnMappings.get(i);
            Block sourceBlock = page.getBlock(mapping.sourceColumnIndex());

            if (mapping.field().transform().isIdentity()) {
                partitionBlocks[i] = sourceBlock;
            }
            else {
                // Transform temporal values into integer partition values
                partitionBlocks[i] = transformTemporalBlock(
                        mapping.sourceColumn().columnType(),
                        sourceBlock,
                        mapping.field().transform());
            }
        }
        return new Page(page.getPositionCount(), partitionBlocks);
    }

    private Block transformTemporalBlock(Type sourceType, Block sourceBlock, dev.brikk.ducklake.catalog.DucklakePartitionTransform transform)
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(null, sourceBlock.getPositionCount());
        for (int position = 0; position < sourceBlock.getPositionCount(); position++) {
            if (sourceBlock.isNull(position)) {
                builder.appendNull();
            }
            else {
                String valueStr = DucklakePartitionComputer.computePartitionValue(
                        sourceType, sourceBlock, position, transform, encoding);
                if (valueStr == null) {
                    builder.appendNull();
                }
                else {
                    INTEGER.writeLong(builder, Long.parseLong(valueStr));
                }
            }
        }
        return builder.build();
    }

    private record PartitionColumnMapping(
            DucklakePartitionField field,
            int sourceColumnIndex,
            DucklakeColumnHandle sourceColumn,
            Type indexerType) {}
}
