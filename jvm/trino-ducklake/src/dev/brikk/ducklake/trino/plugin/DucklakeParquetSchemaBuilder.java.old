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

import dev.brikk.ducklake.catalog.DucklakeColumn;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Builds a Parquet {@link MessageType} with {@code field_id} annotations from DuckLake column IDs.
 * <p>
 * DuckLake's spec states that {@code ducklake_column.column_id} corresponds to the Parquet
 * {@code field_id}. DuckDB uses these IDs for column mapping when reading Parquet files.
 * Trino's standard {@link io.trino.parquet.writer.ParquetSchemaConverter} does not set field IDs,
 * so we rebuild the schema with IDs after conversion.
 */
public final class DucklakeParquetSchemaBuilder
{
    private DucklakeParquetSchemaBuilder() {}

    /**
     * Rebuilds the given MessageType with field_id annotations from DuckLake column metadata.
     *
     * @param topLevelColumns top-level column handles with columnId (in schema field order)
     * @param allColumns all catalog columns including nested (flat list with parentColumn references)
     * @param sourceMessageType the MessageType from ParquetSchemaConverter (without field IDs)
     * @return a new MessageType with field_id set on each field
     */
    public static MessageType buildMessageType(
            List<DucklakeColumnHandle> topLevelColumns,
            List<DucklakeColumn> allColumns,
            MessageType sourceMessageType)
    {
        requireNonNull(topLevelColumns, "topLevelColumns is null");
        requireNonNull(allColumns, "allColumns is null");
        requireNonNull(sourceMessageType, "sourceMessageType is null");

        // Build lookup: parentColumnId -> childName -> DucklakeColumn
        Map<Long, Map<String, DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allColumns) {
            column.parentColumn().ifPresent(parentId ->
                    childrenByParent
                            .computeIfAbsent(parentId, _ -> new HashMap<>())
                            .put(column.columnName(), column));
        }

        // Build top-level name -> columnId map
        Map<String, Long> topLevelColumnIds = new HashMap<>();
        for (DucklakeColumnHandle handle : topLevelColumns) {
            topLevelColumnIds.put(handle.columnName(), handle.columnId());
        }

        // Rebuild each field with field_id
        List<Type> annotatedFields = new ArrayList<>();
        for (Type field : sourceMessageType.getFields()) {
            Long columnId = topLevelColumnIds.get(field.getName());
            if (columnId != null) {
                annotatedFields.add(annotateType(field, columnId, childrenByParent));
            }
            else {
                // No column ID found — keep original (shouldn't happen for well-formed schemas)
                annotatedFields.add(field);
            }
        }

        return new MessageType(sourceMessageType.getName(), annotatedFields);
    }

    /**
     * Overload for simple schemas without nested columns.
     */
    public static MessageType buildMessageType(
            List<DucklakeColumnHandle> topLevelColumns,
            MessageType sourceMessageType)
    {
        return buildMessageType(topLevelColumns, List.of(), sourceMessageType);
    }

    private static Type annotateType(
            Type sourceType,
            long columnId,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        if (sourceType.isPrimitive()) {
            return annotatePrimitive(sourceType.asPrimitiveType(), columnId);
        }
        return annotateGroup(sourceType.asGroupType(), columnId, childrenByParent);
    }

    private static PrimitiveType annotatePrimitive(PrimitiveType source, long columnId)
    {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(
                source.getPrimitiveTypeName(),
                source.getRepetition());

        if (source.getLogicalTypeAnnotation() != null) {
            builder = builder.as(source.getLogicalTypeAnnotation());
        }
        if (source.getTypeLength() > 0) {
            builder = builder.length(source.getTypeLength());
        }

        return builder.id((int) columnId).named(source.getName());
    }

    private static GroupType annotateGroup(
            GroupType source,
            long columnId,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(columnId, Map.of());

        List<Type> annotatedChildren = new ArrayList<>();
        for (Type child : source.getFields()) {
            DucklakeColumn childColumn = children.get(child.getName());
            if (childColumn != null) {
                annotatedChildren.add(annotateType(child, childColumn.columnId(), childrenByParent));
            }
            else {
                // Structural fields (list/element, key_value/key/value) may not have direct column IDs.
                // For these, recurse into their children looking for mapped columns.
                annotatedChildren.add(annotateStructuralField(child, columnId, childrenByParent));
            }
        }

        Types.GroupBuilder<GroupType> builder = Types.buildGroup(source.getRepetition());
        if (source.getLogicalTypeAnnotation() != null) {
            builder = builder.as(source.getLogicalTypeAnnotation());
        }
        for (Type annotatedChild : annotatedChildren) {
            builder.addField(annotatedChild);
        }
        return builder.id((int) columnId).named(source.getName());
    }

    /**
     * Handles Parquet structural wrapper fields (e.g., "list" in arrays, "key_value" in maps)
     * that don't have direct DuckLake column ID mappings. Recursively annotates children
     * using the parent column ID's children map.
     */
    private static Type annotateStructuralField(
            Type field,
            long parentColumnId,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        if (field.isPrimitive()) {
            return field; // No ID for unmapped primitives inside structural wrappers
        }

        GroupType group = field.asGroupType();
        Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(parentColumnId, Map.of());

        List<Type> annotatedChildren = new ArrayList<>();
        for (Type child : group.getFields()) {
            DucklakeColumn childColumn = children.get(child.getName());
            if (childColumn != null) {
                annotatedChildren.add(annotateType(child, childColumn.columnId(), childrenByParent));
            }
            else {
                annotatedChildren.add(annotateStructuralField(child, parentColumnId, childrenByParent));
            }
        }

        Types.GroupBuilder<GroupType> builder = Types.buildGroup(group.getRepetition());
        if (group.getLogicalTypeAnnotation() != null) {
            builder = builder.as(group.getLogicalTypeAnnotation());
        }
        for (Type annotatedChild : annotatedChildren) {
            builder.addField(annotatedChild);
        }
        return builder.named(group.getName());
    }
}
