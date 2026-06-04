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

import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import io.trino.spi.Page
import io.trino.spi.PageIndexer
import io.trino.spi.PageIndexerFactory
import io.trino.spi.block.Block
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.Type
import java.util.OptionalInt

/**
 * Partitions pages by computing DuckLake partition values and routing rows to writer indexes.
 * Uses Trino's [PageIndexerFactory] for efficient partition-to-index mapping.
 */
open class DucklakePagePartitioner(
        pageIndexerFactory: PageIndexerFactory,
        private val partitionSpec: DucklakePartitionSpec,
        allColumns: List<DucklakeColumnHandle>,
        private val encoding: DucklakeTemporalPartitionEncoding) {
    private val allColumns: List<DucklakeColumnHandle> = allColumns.toList()
    private val pageIndexer: PageIndexer
    private val partitionColumnMappings: List<PartitionColumnMapping>

    init {
        // Build mappings: for each partition field, find the source column index and type
        val mappings: MutableList<PartitionColumnMapping> = ArrayList()
        for (field in partitionSpec.fields) {
            var sourceColumnIndex = -1
            var sourceColumn: DucklakeColumnHandle? = null
            for (i in this.allColumns.indices) {
                if (this.allColumns[i].columnId == field.columnId) {
                    sourceColumnIndex = i
                    sourceColumn = this.allColumns[i]
                    break
                }
            }
            if (sourceColumnIndex < 0) {
                throw IllegalArgumentException("Partition field references unknown column ID: ${field.columnId}")
            }

            // For the page indexer, the partition column type is:
            // - IDENTITY: the source column type
            // - Temporal / BUCKET: INTEGER (the computed partition value is an integer)
            val indexerType: Type = if (field.transform.isIdentity()) sourceColumn!!.columnType else INTEGER

            mappings.add(PartitionColumnMapping(field, sourceColumnIndex, sourceColumn!!, indexerType))
        }
        this.partitionColumnMappings = mappings

        // Create page indexer for the partition column types
        val partitionTypes: List<Type> = partitionColumnMappings.map { it.indexerType }
        this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionTypes)
    }

    /**
     * Partition a page into writer indexes. Returns an array where element[i] is the writer index
     * for row position i.
     */
    open fun partitionPage(page: Page): IntArray {
        val partitionPage = buildPartitionPage(page)
        return pageIndexer.indexPage(partitionPage)
    }

    open fun getMaxIndex(): Int {
        return pageIndexer.maxIndex
    }

    /**
     * Compute partition values for a given row position. Returns a map of partitionKeyIndex -> value string.
     */
    open fun getPartitionValues(page: Page, position: Int): Map<Int, String?> {
        val values: MutableMap<Int, String?> = HashMap()
        for (mapping in partitionColumnMappings) {
            val sourceBlock = page.getBlock(mapping.sourceColumnIndex)
            val value = DucklakePartitionComputer.computePartitionValue(
                    mapping.sourceColumn.columnType,
                    sourceBlock,
                    position,
                    mapping.field.transform,
                    mapping.field.arity,
                    encoding)
            values[mapping.field.partitionKeyIndex] = value
        }
        return values
    }

    /**
     * Get the column name for a partition field (used for directory naming).
     */
    open fun getPartitionColumnName(partitionKeyIndex: Int): String {
        for (mapping in partitionColumnMappings) {
            if (mapping.field.partitionKeyIndex == partitionKeyIndex) {
                return mapping.sourceColumn.columnName
            }
        }
        throw IllegalArgumentException("Unknown partition key index: $partitionKeyIndex")
    }

    open fun getPartitionId(): Long {
        return partitionSpec.partitionId
    }

    private fun buildPartitionPage(page: Page): Page {
        val partitionBlocks: Array<Block?> = arrayOfNulls(partitionColumnMappings.size)
        for (i in partitionColumnMappings.indices) {
            val mapping = partitionColumnMappings[i]
            val sourceBlock = page.getBlock(mapping.sourceColumnIndex)

            if (mapping.field.transform.isIdentity()) {
                partitionBlocks[i] = sourceBlock
            }
            else {
                // Transform temporal / bucket values into integer partition values
                partitionBlocks[i] = transformBlockToInteger(
                        mapping.sourceColumn.columnType,
                        sourceBlock,
                        mapping.field.transform,
                        mapping.field.arity)
            }
        }
        @Suppress("UNCHECKED_CAST")
        return Page(page.positionCount, *(partitionBlocks as Array<Block>))
    }

    private fun transformBlockToInteger(
            sourceType: Type,
            sourceBlock: Block,
            transform: DucklakePartitionTransform,
            arity: OptionalInt): Block {
        val builder = INTEGER.createBlockBuilder(null, sourceBlock.positionCount)
        for (position in 0 until sourceBlock.positionCount) {
            if (sourceBlock.isNull(position)) {
                builder.appendNull()
            }
            else {
                val valueStr = DucklakePartitionComputer.computePartitionValue(
                        sourceType, sourceBlock, position, transform, arity, encoding)
                if (valueStr == null) {
                    builder.appendNull()
                }
                else {
                    INTEGER.writeLong(builder, valueStr.toLong())
                }
            }
        }
        return builder.build()
    }

    private data class PartitionColumnMapping(
            val field: DucklakePartitionField,
            val sourceColumnIndex: Int,
            val sourceColumn: DucklakeColumnHandle,
            val indexerType: Type)
}
