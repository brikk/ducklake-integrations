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
import io.trino.spi.type.RowType

/**
 * Builds the per-file struct reshape plans the DuckDB-engine read path needs when a struct column's
 * shape in a data file differs from the current schema (nested `ADD COLUMN s.child` / `DROP COLUMN
 * s.child`). Pure: given the projected columns, the current column tree, and the file-era column
 * tree (both from `catalog.getAllColumnsWithParentage`, ordered by `(column_order, column_id)`), it
 * emits a [StructFieldPlan] tree per struct column whose file shape differs. Matching is by
 * `column_id` (stable across renames), so the resulting `struct_pack` handles add/drop/rename/
 * reorder. An identical struct gets no plan (the builder then projects it plainly).
 *
 * The nested generalization of the top-level resolution in [DuckDbSelectSqlBuilder]; see
 * dev-docs/DESIGN-nested-field-evolution.md.
 */
object NestedFieldReshapePlanner {

    fun buildPlans(
            projectedColumns: List<DucklakeColumnHandle>,
            currentColumns: List<DucklakeColumn>,
            fileColumns: List<DucklakeColumn>): Map<Long, List<StructFieldPlan>> {
        val currentByParent: Map<Long, List<DucklakeColumn>> = childrenByParent(currentColumns)
        val fileByParent: Map<Long, List<DucklakeColumn>> = childrenByParent(fileColumns)
        val fileNameById: Map<Long, String> = fileColumns.associate { it.columnId to it.columnName }

        val plans: MutableMap<Long, List<StructFieldPlan>> = LinkedHashMap()
        for (column in projectedColumns) {
            val type = column.columnType
            if (type is RowType && fileNameById.containsKey(column.columnId)) {
                // Struct present in the file — reshape if its shape drifted. (A struct ADDED after
                // the file is absent from the file tree; the top-level NULL projection handles it.)
                val plan = buildStructPlan(type, column.columnId, currentByParent, fileByParent, fileNameById)
                if (plan != null) {
                    plans[column.columnId] = plan
                }
            }
        }
        return plans
    }

    /**
     * Plan for one struct, or null if its file shape is identical to the current shape (so it can be
     * projected as-is). [structColumnId] keys both the current and file child lists; field i of
     * [currentType] aligns with current child i (same `(column_order, column_id)` ordering).
     */
    private fun buildStructPlan(
            currentType: RowType,
            structColumnId: Long,
            currentByParent: Map<Long, List<DucklakeColumn>>,
            fileByParent: Map<Long, List<DucklakeColumn>>,
            fileNameById: Map<Long, String>): List<StructFieldPlan>? {
        val currentChildren: List<DucklakeColumn> = currentByParent[structColumnId] ?: emptyList()
        val fileChildren: List<DucklakeColumn> = fileByParent[structColumnId] ?: emptyList()
        // Conservative structural diff: a different child-id sequence (add / drop / reorder) forces a
        // reshape on its own; renames and nested drifts are caught in the loop below.
        var needsReshape: Boolean =
                currentChildren.map { it.columnId } != fileChildren.map { it.columnId }

        val fields: MutableList<StructFieldPlan> = ArrayList(currentType.fields.size)
        for (i in currentType.fields.indices) {
            val field = currentType.fields[i]
            val currentName: String = field.name.orElseThrow {
                IllegalStateException("Anonymous struct field in column_id $structColumnId is not supported")
            }
            val childColumnId: Long = currentChildren[i].columnId
            val fileName: String? = fileNameById[childColumnId]
            if (fileName == null || fileName != currentName) {
                needsReshape = true
            }
            var childPlan: List<StructFieldPlan>? = null
            val fieldType = field.type
            if (fieldType is RowType && fileName != null) {
                childPlan = buildStructPlan(fieldType, childColumnId, currentByParent, fileByParent, fileNameById)
                if (childPlan != null) {
                    needsReshape = true
                }
            }
            // initial_default of a subfield ADDED after this file was written (fileName == null):
            // projected in place of NULL for rows that predate `ADD COLUMN s.child … DEFAULT …`.
            fields.add(StructFieldPlan(currentName, fieldType, fileName, childPlan, currentChildren[i].initialDefault))
        }
        return if (needsReshape) fields else null
    }

    private fun childrenByParent(columns: List<DucklakeColumn>): Map<Long, List<DucklakeColumn>> {
        val byParent: MutableMap<Long, MutableList<DucklakeColumn>> = LinkedHashMap()
        for (column in columns) {
            val parent = column.parentColumn ?: continue
            byParent.getOrPut(parent) { ArrayList() }.add(column)
        }
        return byParent
    }
}
