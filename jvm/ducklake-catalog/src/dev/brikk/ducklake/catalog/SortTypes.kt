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

import java.util.Locale

/**
 * One entry from `ducklake_sort_expression`, joined with the active
 * `ducklake_sort_info` row for its table at a snapshot. [expression]
 * is the raw text from the catalog — DuckLake stores SQL in the writer's
 * dialect. Consumers (e.g. the Trino connector's planner-hint path) decide
 * whether they can interpret it; arbitrary expressions are downgraded to "no
 * sort property" silently rather than misreporting an order.
 */
@JvmRecord
data class DucklakeSortKey(
    val sortKeyIndex: Int,
    val expression: String,
    val dialect: String,
    val direction: DucklakeSortDirection,
    val nullOrder: DucklakeNullOrder,
) {
    init {
        require(sortKeyIndex >= 0) { "sortKeyIndex must be non-negative: $sortKeyIndex" }
    }
}

/**
 * Sort direction encoded in `ducklake_sort_expression.sort_direction`.
 * Upstream writes the literal strings `"ASC"` / `"DESC"`.
 */
enum class DucklakeSortDirection {
    ASC,
    DESC,
    ;

    companion object {
        @JvmStatic
        fun fromCatalog(value: String?): DucklakeSortDirection {
            if (value == null) {
                throw IllegalArgumentException("sort_direction is null")
            }
            return when (value.trim().uppercase(Locale.ROOT)) {
                "ASC" -> ASC
                "DESC" -> DESC
                else -> throw IllegalArgumentException("Unknown sort_direction: $value")
            }
        }
    }
}

/**
 * Null-ordering encoded in `ducklake_sort_expression.null_order`.
 * Upstream writes the literal strings `"NULLS_FIRST"` / `"NULLS_LAST"`.
 */
enum class DucklakeNullOrder {
    NULLS_FIRST,
    NULLS_LAST,
    ;

    companion object {
        @JvmStatic
        fun fromCatalog(value: String?): DucklakeNullOrder {
            if (value == null) {
                throw IllegalArgumentException("null_order is null")
            }
            return when (value.trim().uppercase(Locale.ROOT)) {
                "NULLS_FIRST" -> NULLS_FIRST
                "NULLS_LAST" -> NULLS_LAST
                else -> throw IllegalArgumentException("Unknown null_order: $value")
            }
        }
    }
}
