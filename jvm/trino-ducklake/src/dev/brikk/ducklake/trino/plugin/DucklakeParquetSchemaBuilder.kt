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
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

/**
 * Builds a Parquet [MessageType] with `field_id` annotations from DuckLake column IDs.
 *
 *
 * DuckLake's spec states that `ducklake_column.column_id` corresponds to the Parquet
 * `field_id`. DuckDB uses these IDs for column mapping when reading Parquet files.
 * Trino's standard [io.trino.parquet.writer.ParquetSchemaConverter] does not set field IDs,
 * so we rebuild the schema with IDs after conversion.
 */
public class DucklakeParquetSchemaBuilder private constructor() {
    public companion object {
        /**
         * Rebuilds the given MessageType with field_id annotations from DuckLake column metadata.
         *
         * @param topLevelColumns top-level column handles with columnId (in schema field order)
         * @param allColumns all catalog columns including nested (flat list with parentColumn references)
         * @param sourceMessageType the MessageType from ParquetSchemaConverter (without field IDs)
         * @return a new MessageType with field_id set on each field
         */
        @JvmStatic
        public fun buildMessageType(
                topLevelColumns: List<DucklakeColumnHandle>,
                allColumns: List<DucklakeColumn>,
                sourceMessageType: MessageType): MessageType {
            // Build lookup: parentColumnId -> childName -> DucklakeColumn
            val childrenByParent: MutableMap<Long, MutableMap<String, DucklakeColumn>> = HashMap()
            for (column in allColumns) {
                column.parentColumn.ifPresent { parentId ->
                    childrenByParent
                            .computeIfAbsent(parentId) { HashMap() }
                            .put(column.columnName, column)
                }
            }

            // Build top-level name -> columnId map
            val topLevelColumnIds: MutableMap<String, Long> = HashMap()
            for (handle in topLevelColumns) {
                topLevelColumnIds.put(handle.columnName(), handle.columnId())
            }

            // Rebuild each field with field_id
            val annotatedFields: MutableList<Type> = ArrayList()
            for (field in sourceMessageType.fields) {
                val columnId = topLevelColumnIds[field.name]
                if (columnId != null) {
                    annotatedFields.add(annotateType(field, columnId, childrenByParent))
                }
                else {
                    // No column ID found — keep original (shouldn't happen for well-formed schemas)
                    annotatedFields.add(field)
                }
            }

            return MessageType(sourceMessageType.name, annotatedFields)
        }

        /**
         * Overload for simple schemas without nested columns.
         */
        @JvmStatic
        public fun buildMessageType(
                topLevelColumns: List<DucklakeColumnHandle>,
                sourceMessageType: MessageType): MessageType {
            return buildMessageType(topLevelColumns, listOf(), sourceMessageType)
        }

        private fun annotateType(
                sourceType: Type,
                columnId: Long,
                childrenByParent: Map<Long, Map<String, DucklakeColumn>>): Type {
            if (sourceType.isPrimitive) {
                return annotatePrimitive(sourceType.asPrimitiveType(), columnId)
            }
            return annotateGroup(sourceType.asGroupType(), columnId, childrenByParent)
        }

        private fun annotatePrimitive(source: PrimitiveType, columnId: Long): PrimitiveType {
            var builder: Types.PrimitiveBuilder<PrimitiveType> = Types.primitive(
                    source.primitiveTypeName,
                    source.repetition)

            if (source.logicalTypeAnnotation != null) {
                builder = builder.`as`(source.logicalTypeAnnotation)
            }
            if (source.typeLength > 0) {
                builder = builder.length(source.typeLength)
            }

            return builder.id(columnId.toInt()).named(source.name)
        }

        private fun annotateGroup(
                source: GroupType,
                columnId: Long,
                childrenByParent: Map<Long, Map<String, DucklakeColumn>>): GroupType {
            val children = childrenByParent.getOrDefault(columnId, mapOf())

            val annotatedChildren: MutableList<Type> = ArrayList()
            for (child in source.fields) {
                val childColumn = children[child.name]
                if (childColumn != null) {
                    annotatedChildren.add(annotateType(child, childColumn.columnId, childrenByParent))
                }
                else {
                    // Structural fields (list/element, key_value/key/value) may not have direct column IDs.
                    // For these, recurse into their children looking for mapped columns.
                    annotatedChildren.add(annotateStructuralField(child, columnId, childrenByParent))
                }
            }

            var builder: Types.GroupBuilder<GroupType> = Types.buildGroup(source.repetition)
            if (source.logicalTypeAnnotation != null) {
                builder = builder.`as`(source.logicalTypeAnnotation)
            }
            for (annotatedChild in annotatedChildren) {
                builder.addField(annotatedChild)
            }
            return builder.id(columnId.toInt()).named(source.name)
        }

        /**
         * Handles Parquet structural wrapper fields (e.g., "list" in arrays, "key_value" in maps)
         * that don't have direct DuckLake column ID mappings. Recursively annotates children
         * using the parent column ID's children map.
         */
        private fun annotateStructuralField(
                field: Type,
                parentColumnId: Long,
                childrenByParent: Map<Long, Map<String, DucklakeColumn>>): Type {
            if (field.isPrimitive) {
                return field // No ID for unmapped primitives inside structural wrappers
            }

            val group = field.asGroupType()
            val children = childrenByParent.getOrDefault(parentColumnId, mapOf())

            val annotatedChildren: MutableList<Type> = ArrayList()
            for (child in group.fields) {
                val childColumn = children[child.name]
                if (childColumn != null) {
                    annotatedChildren.add(annotateType(child, childColumn.columnId, childrenByParent))
                }
                else {
                    annotatedChildren.add(annotateStructuralField(child, parentColumnId, childrenByParent))
                }
            }

            var builder: Types.GroupBuilder<GroupType> = Types.buildGroup(group.repetition)
            if (group.logicalTypeAnnotation != null) {
                builder = builder.`as`(group.logicalTypeAnnotation)
            }
            for (annotatedChild in annotatedChildren) {
                builder.addField(annotatedChild)
            }
            return builder.named(group.name)
        }
    }
}
