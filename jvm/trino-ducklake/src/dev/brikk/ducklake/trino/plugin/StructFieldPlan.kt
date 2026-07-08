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

import io.trino.spi.type.Type

/**
 * How to project one subfield of a struct against a SPECIFIC data file whose struct shape differs
 * from the current schema. The nested generalization of the top-level schema-evolution resolution
 * (see [DuckDbSelectSqlBuilder]): for the non-parquet (DuckDB-engine) read path, .db/vortex/lance
 * files store physical field names from write time, so a file written before a nested
 * `ADD COLUMN s.child` / `DROP COLUMN s.child` carries a struct of a different shape. The provider
 * builds a plan by matching the current and file-era column trees by `column_id`; the builder
 * renders it into a `struct_pack` that normalizes the file's struct to the current shape.
 *
 * - [currentName] — the field's name in the current schema; the `struct_pack` key.
 * - [type] — the field's current Trino type; used for `CAST(NULL AS …)` when [fileName] is null.
 * - [fileName] — the field's name in the file, or null if it was ADDED after the file was written
 *   (→ projected as its [initialDefault] when one is declared, else a typed NULL). A field DROPPED
 *   since the file simply has no plan entry, so the `struct_pack` omits it (the file's extra field
 *   is left unread).
 * - [children] — non-null iff this field is itself a struct that needs reshaping; its own plan.
 * - [initialDefault] — the field's `ducklake_column.initial_default` value text, projected for rows
 *   that predate an `ADD COLUMN s.child … DEFAULT …` (only meaningful when [fileName] is null).
 * - [promoted] — true iff this field is PRESENT in the file ([fileName] non-null) but its type
 *   widened since the file was written (`ALTER COLUMN s.child SET DATA TYPE …`). The physical file
 *   holds the OLD type, so the builder CASTs the field to the current [type] inside `struct_pack`.
 *   Distinct from [initialDefault], which is for era-absent fields ([fileName] == null).
 */
class StructFieldPlan(
        val currentName: String,
        val type: Type,
        val fileName: String?,
        val children: List<StructFieldPlan>?,
        val initialDefault: String? = null,
        val promoted: Boolean = false) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StructFieldPlan) return false
        return currentName == other.currentName &&
                type == other.type &&
                fileName == other.fileName &&
                children == other.children &&
                initialDefault == other.initialDefault &&
                promoted == other.promoted
    }

    override fun hashCode(): Int = java.util.Objects.hash(currentName, type, fileName, children, initialDefault, promoted)

    override fun toString(): String =
            "StructFieldPlan[currentName=$currentName, type=$type, fileName=$fileName, " +
                    "children=$children, initialDefault=$initialDefault, promoted=$promoted]"
}
