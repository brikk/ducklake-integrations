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
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type

/**
 * Identity-based field mapping for inlined-data reads across nested schema
 * evolution.
 *
 * Inlined rows live in per-schema-version catalog tables and serialize struct
 * values with the field NAMES of that era. Matching era names against the
 * CURRENT type by name is silently wrong for two evolutions:
 *  - RENAME (field keeps its `column_id` but changes name): the era text key
 *    differs from the current name — name-matching drops the value;
 *  - REUSE (drop field `i`, later add a NEW field also named `i`): the era
 *    text HAS an `i`, but it belongs to the dead field — name-matching would
 *    resurrect dropped values.
 *
 * The `ducklake_column` tree gives the truth: nested fields are rows with
 * stable `column_id` identity and per-era names. [InlinedNestedFieldMapper.build]
 * walks the CURRENT tree aligned with the Trino type shape and, for each
 * current field's `column_id`, looks up the NAME that identity had in the
 * inlined table's era — producing per-struct translations for the text parser.
 */
sealed interface InlinedTextFieldMapping {
    /**
     * For a ROW node: [eraNames]`[i]` is the era-text key holding current field
     * i's value (null = the field identity did not exist in that era → NULL);
     * [children]`[i]` is the mapping for nested field types; [defaults]`[i]` is
     * the parsed native value of field i's `initial_default` when the field was
     * ADDED after this era (so `eraNames[i]` is null) and declares a default —
     * projected in place of NULL for rows that predate the field (upstream
     * issue-1135 semantics, nested-field variant). Null when the field existed in
     * the era or declares no default.
     */
    data class Struct(
        val eraNames: List<String?>,
        val children: List<InlinedTextFieldMapping?>,
        val defaults: List<Any?>,
    ) : InlinedTextFieldMapping {
        /** Lowercased era name → current field index. */
        val indexByEraName: Map<String, Int> =
            eraNames.withIndex()
                .filter { it.value != null }
                .associate { (i, name) -> name!!.lowercase() to i }
    }

    /** For a LIST element or MAP value: the mapping of the contained type. */
    data class Container(val child: InlinedTextFieldMapping?) : InlinedTextFieldMapping
}

object InlinedNestedFieldMapper {

    /**
     * Builds mappings for the given top-level columns. [currentTree] and
     * [eraTree] are flat `getAllColumnsWithParentage` results (ordered by
     * `column_order, column_id`) at the query snapshot and at the inlined
     * table's schema-version era respectively. Columns without nested rows, or
     * whose tree shape cannot be aligned with the Trino type, are absent from
     * the result (callers fall back to name matching).
     */
    fun build(
        columns: List<Pair<Long, Type>>,
        currentTree: List<DucklakeColumn>,
        eraTree: List<DucklakeColumn>,
    ): Map<Long, InlinedTextFieldMapping> {
        val currentByParent: Map<Long?, List<DucklakeColumn>> = currentTree.groupBy { it.parentColumn }
        val eraById: Map<Long, DucklakeColumn> = eraTree.associateBy { it.columnId }
        val result = LinkedHashMap<Long, InlinedTextFieldMapping>()
        for ((columnId, type) in columns) {
            if (!containsRow(type)) {
                continue
            }
            mapNode(columnId, type, currentByParent, eraById)?.let { result[columnId] = it }
        }
        return result
    }

    /** True when the type contains a ROW anywhere (the only shape needing identity mapping). */
    fun containsRow(type: Type): Boolean =
        when (type) {
            is RowType -> true
            is ArrayType -> containsRow(type.elementType)
            is MapType -> containsRow(type.keyType) || containsRow(type.valueType)
            else -> false
        }

    private fun mapNode(
        columnId: Long,
        type: Type,
        currentByParent: Map<Long?, List<DucklakeColumn>>,
        eraById: Map<Long, DucklakeColumn>,
    ): InlinedTextFieldMapping? {
        return when (type) {
            is RowType -> {
                val children = currentByParent[columnId] ?: return null
                if (children.size != type.fields.size) {
                    // Tree/type misalignment — refuse the mapping rather than guess.
                    return null
                }
                val eraNames = children.map { eraById[it.columnId]?.columnName }
                // A field ADDED after this inlined era (absent from eraById) projects its
                // initial_default for rows that predate it — the nested-field variant of the
                // top-level ADD COLUMN ... DEFAULT rule. Parsed to the field's native value; an
                // unparseable / typeless default degrades to null (same as the scalar path).
                val defaults =
                    children.mapIndexed { i, child ->
                        val default = child.initialDefault
                        if (default != null && eraById[child.columnId] == null) {
                            runCatching {
                                DucklakePartitionValueParser.parseIdentity(type.fields[i].type, default)
                            }.getOrNull()
                        } else {
                            null
                        }
                    }
                val childMappings =
                    children.mapIndexed { i, child ->
                        mapNode(child.columnId, type.fields[i].type, currentByParent, eraById)
                    }
                InlinedTextFieldMapping.Struct(eraNames, childMappings, defaults)
            }
            is ArrayType -> {
                val element = currentByParent[columnId]?.singleOrNull() ?: return null
                val child = mapNode(element.columnId, type.elementType, currentByParent, eraById) ?: return null
                InlinedTextFieldMapping.Container(child)
            }
            is MapType -> {
                val children = currentByParent[columnId] ?: return null
                if (children.size != 2) {
                    return null
                }
                // key, value in column_order.
                val child = mapNode(children[1].columnId, type.valueType, currentByParent, eraById) ?: return null
                InlinedTextFieldMapping.Container(child)
            }
            else -> null
        }
    }
}
