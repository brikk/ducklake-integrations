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

import com.google.common.collect.ImmutableList
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import java.util.HashMap

/**
 * Builds the flat list of parquet-leaf stats targets that
 * {@link DucklakeStatsExtractor} consumes, for the write path that funnels
 * through Trino's parquet writer (INSERT / CTAS / MERGE).
 *
 * <p>The walk mirrors Trino's parquet writer: depth-first over each
 * top-level column's Trino type, descending STRUCT fields in declaration
 * order, ARRAY element, then MAP key/value. Trino's
 * {@code ParquetSchemaConverter} produces leaves in exactly this order,
 * which is what {@code RowGroup.columns} indexes against.
 *
 * <p>Each emitted leaf carries the DuckLake catalog field_id at that path
 * (top-level columnId for flat columns; the child column_id from the
 * catalog tree for nested leaves).
 */
object DucklakeStatsLeafProjector {
    @JvmStatic
    fun projectFromCatalogTree(
            topLevelColumns: List<DucklakeColumnHandle>,
            allCatalogColumns: List<DucklakeColumn>): List<LeafStatsTarget> {
        val childrenByParent = buildChildrenByParent(allCatalogColumns)

        val leaves = ImmutableList.builder<LeafStatsTarget>()
        val runningIndex = intArrayOf(0)
        for (top in topLevelColumns) {
            if (top.isRowIdColumn()) {
                continue
            }
            collect(top.columnId, top.columnType, childrenByParent, leaves, runningIndex)
        }
        return leaves.build()
    }

    private fun buildChildrenByParent(allCatalogColumns: List<DucklakeColumn>): Map<Long, MutableMap<String, DucklakeColumn>> {
        val childrenByParent: MutableMap<Long, MutableMap<String, DucklakeColumn>> = HashMap()
        for (column in allCatalogColumns) {
            column.parentColumn.ifPresent { parentId ->
                childrenByParent
                        .computeIfAbsent(parentId) { _ -> HashMap() }
                        .put(column.columnName, column)
            }
        }
        return childrenByParent
    }

    private fun collect(
            fieldId: Long,
            type: Type,
            childrenByParent: Map<Long, MutableMap<String, DucklakeColumn>>,
            out: ImmutableList.Builder<LeafStatsTarget>,
            runningIndex: IntArray) {
        if (type is RowType) {
            val children: Map<String, DucklakeColumn> = childrenByParent[fieldId] ?: emptyMap()
            for (field in type.fields) {
                val childName = field.name.orElseThrow {
                    IllegalArgumentException("Anonymous row fields are not supported (parent field_id=$fieldId)")
                }
                val child = children[childName]
                if (child == null) {
                    throw IllegalStateException(
                            "Catalog tree missing ROW child '$childName' for parent field_id=$fieldId")
                }
                collect(child.columnId, field.type, childrenByParent, out, runningIndex)
            }
            return
        }
        if (type is ArrayType) {
            val children: Map<String, DucklakeColumn> = childrenByParent[fieldId] ?: emptyMap()
            if (children.size != 1) {
                throw IllegalStateException(
                        "Catalog tree expected one ARRAY child for field_id=$fieldId, found ${children.size}")
            }
            val element = children.values.first()
            collect(element.columnId, type.elementType, childrenByParent, out, runningIndex)
            return
        }
        if (type is MapType) {
            val children: Map<String, DucklakeColumn> = childrenByParent[fieldId] ?: emptyMap()
            val keyChild = children["key"]
            val valueChild = children["value"]
            if (keyChild == null || valueChild == null) {
                throw IllegalStateException(
                        "Catalog tree missing MAP key/value children for field_id=$fieldId")
            }
            collect(keyChild.columnId, type.keyType, childrenByParent, out, runningIndex)
            collect(valueChild.columnId, type.valueType, childrenByParent, out, runningIndex)
            return
        }
        out.add(LeafStatsTarget(fieldId, type, runningIndex[0]))
        runningIndex[0]++
    }
}
