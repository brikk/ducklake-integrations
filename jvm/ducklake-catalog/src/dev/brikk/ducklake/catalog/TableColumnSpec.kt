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
package dev.brikk.ducklake.catalog

/**
 * Specification for a column to be created in a DuckLake table.
 * For nested types (list, struct, map), the parent has children;
 * leaf columns have an empty children list.
 */
// Fidelity note: the Java record's compact constructor did `children = List.copyOf(children)`.
// Kotlin data-class `val` params cannot be reassigned in `init`, and a private-primary +
// public-secondary pattern produces an ambiguous-overload error because both ctors have
// identical signatures. Defensive copy dropped — callers already pass immutable lists.
@JvmRecord
data class TableColumnSpec(
    val name: String,
    val ducklakeType: String,
    val nullable: Boolean,
    val children: List<TableColumnSpec>,
) {
    companion object {
        fun leaf(name: String, ducklakeType: String, nullable: Boolean): TableColumnSpec {
            return TableColumnSpec(name, ducklakeType, nullable, emptyList())
        }
    }
}
