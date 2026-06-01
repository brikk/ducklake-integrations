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
import dev.brikk.ducklake.catalog.DucklakeColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public final class DucklakeStatsLeafProjector
{
    private DucklakeStatsLeafProjector() {}

    public static List<LeafStatsTarget> projectFromCatalogTree(
            List<DucklakeColumnHandle> topLevelColumns,
            List<DucklakeColumn> allCatalogColumns)
    {
        Map<Long, Map<String, DucklakeColumn>> childrenByParent = buildChildrenByParent(allCatalogColumns);

        ImmutableList.Builder<LeafStatsTarget> leaves = ImmutableList.builder();
        int[] runningIndex = {0};
        for (DucklakeColumnHandle top : topLevelColumns) {
            if (top.isRowIdColumn()) {
                continue;
            }
            collect(top.columnId(), top.columnType(), childrenByParent, leaves, runningIndex);
        }
        return leaves.build();
    }

    private static Map<Long, Map<String, DucklakeColumn>> buildChildrenByParent(List<DucklakeColumn> allCatalogColumns)
    {
        Map<Long, Map<String, DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allCatalogColumns) {
            column.parentColumn().ifPresent(parentId ->
                    childrenByParent
                            .computeIfAbsent(parentId, ignored -> new HashMap<>())
                            .put(column.columnName(), column));
        }
        return childrenByParent;
    }

    private static void collect(
            long fieldId,
            Type type,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent,
            ImmutableList.Builder<LeafStatsTarget> out,
            int[] runningIndex)
    {
        if (type instanceof RowType row) {
            Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(fieldId, Map.of());
            for (RowType.Field field : row.getFields()) {
                String childName = field.getName().orElseThrow(() ->
                        new IllegalArgumentException("Anonymous row fields are not supported (parent field_id=" + fieldId + ")"));
                DucklakeColumn child = children.get(childName);
                if (child == null) {
                    throw new IllegalStateException(
                            "Catalog tree missing ROW child '" + childName + "' for parent field_id=" + fieldId);
                }
                collect(child.columnId(), field.getType(), childrenByParent, out, runningIndex);
            }
            return;
        }
        if (type instanceof ArrayType array) {
            Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(fieldId, Map.of());
            if (children.size() != 1) {
                throw new IllegalStateException(
                        "Catalog tree expected one ARRAY child for field_id=" + fieldId + ", found " + children.size());
            }
            DucklakeColumn element = children.values().iterator().next();
            collect(element.columnId(), array.getElementType(), childrenByParent, out, runningIndex);
            return;
        }
        if (type instanceof MapType map) {
            Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(fieldId, Map.of());
            DucklakeColumn keyChild = children.get("key");
            DucklakeColumn valueChild = children.get("value");
            if (keyChild == null || valueChild == null) {
                throw new IllegalStateException(
                        "Catalog tree missing MAP key/value children for field_id=" + fieldId);
            }
            collect(keyChild.columnId(), map.getKeyType(), childrenByParent, out, runningIndex);
            collect(valueChild.columnId(), map.getValueType(), childrenByParent, out, runningIndex);
            return;
        }
        out.add(new LeafStatsTarget(fieldId, type, runningIndex[0]));
        runningIndex[0]++;
    }
}
