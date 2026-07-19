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
import dev.brikk.ducklake.catalog.DucklakeNullOrder
import dev.brikk.ducklake.catalog.DucklakeSortDirection
import dev.brikk.ducklake.catalog.DucklakeSortKey
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.LocalProperty
import io.trino.spi.connector.SortOrder
import io.trino.spi.connector.SortingProperty
import java.util.Locale

/**
 * Translates DuckLake's catalog-stored sort spec into Trino
 * [LocalProperty] entries for the planner.
 *
 * Trino interprets a `List<LocalProperty<ColumnHandle>>` as a
 * sequence: the table is sorted by the first property, then within ties by
 * the second, and so on. A *prefix* of the declared sequence is a
 * weaker-but-still-true claim, which the planner can rely on for queries
 * that `ORDER BY` only the leading columns. The inverse is not true:
 * skipping an entry in the middle is a lie. So when we cannot interpret a
 * sort key (unknown dialect, non-column expression, column missing at this
 * snapshot), we stop and return only the safe prefix.
 *
 * Currently only the `duckdb` dialect is honored. DuckLake writers
 * store expression text verbatim in their dialect; without a dialect-aware
 * parser we treat foreign-dialect expressions as opaque and emit no sort
 * property at all rather than misinterpret quoting/identifier rules.
 */
/**
 * One entry of the honored sort prefix: the lowercased top-level column name plus its
 * resolved [SortOrder]. Callers map [lowercaseColumnName] to whatever they need — a
 * [ColumnHandle] on the read side, a page channel index on the write side.
 */
internal data class ResolvedSortColumn(val lowercaseColumnName: String, val order: SortOrder)

class DucklakeSortPropertyMapper private constructor() {
    companion object {
        private const val DUCKDB_DIALECT = "duckdb"

        /**
         * @param sortKeys           ordered as stored in `ducklake_sort_expression`
         * (`ORDER BY sort_key_index`)
         * @param columnHandlesByLowercaseName top-level columns at the active snapshot,
         * keyed by lowercased column name. Used to
         * resolve simple column-reference expressions.
         */
        fun toLocalProperties(
                sortKeys: List<DucklakeSortKey>,
                columnHandlesByLowercaseName: Map<String, out ColumnHandle>): List<LocalProperty<ColumnHandle>> {
            val resolved = resolveHonoredPrefix(sortKeys, columnHandlesByLowercaseName.keys)
            if (resolved.isEmpty()) {
                return ImmutableList.of()
            }
            val properties = ImmutableList.builder<LocalProperty<ColumnHandle>>()
            for (column in resolved) {
                // resolveHonoredPrefix only emits names present in the map, so this is non-null.
                val handle = columnHandlesByLowercaseName.getValue(column.lowercaseColumnName)
                properties.add(SortingProperty(handle, column.order))
            }
            return properties.build()
        }

        /**
         * Shared honored-prefix resolution: the single source of truth for which sort keys
         * both the read side (planner [SortingProperty] hints) and the write side (physically
         * sorting rows) may act on. Returns the *leading resolvable prefix* of [sortKeys],
         * each entry lowercased-column-name + [SortOrder]. Stops (returning only the safe
         * prefix, possibly empty) at the first key that is:
         *
         *   - non-contiguous by `sort_key_index` (guards against a half-applied sort-info
         *     update masquerading surviving keys as an adjacent prefix),
         *   - not the `duckdb` dialect (foreign dialects are opaque without a parser),
         *   - not a simple column reference, or
         *   - naming a column absent from [resolvableLowercaseColumnNames].
         *
         * Read and write MUST agree on this prefix, otherwise a data file could be sorted by a
         * key the planner did not advertise (or vice versa), so both callers route through here.
         */
        internal fun resolveHonoredPrefix(
                sortKeys: List<DucklakeSortKey>,
                resolvableLowercaseColumnNames: Set<String>): List<ResolvedSortColumn> {
            if (sortKeys.isEmpty()) {
                return emptyList()
            }
            val resolved = ArrayList<ResolvedSortColumn>(sortKeys.size)
            // Track the expected next sort_key_index so the safe-prefix guarantee is self-contained
            // rather than relying on the caller's ORDER BY. A gap (e.g. indices 0, 2 with 1 missing
            // due to a partial/half-applied sort-info update) would otherwise make the surviving
            // keys masquerade as an adjacent leading prefix — exactly the "skipping an entry in the
            // middle is a lie" scenario this class guards against. Start at the first key's index
            // (don't assume 0) and break to the safe prefix on any discontinuity.
            var expectedIndex = sortKeys[0].sortKeyIndex
            for (key in sortKeys) {
                if (key.sortKeyIndex != expectedIndex) {
                    break
                }
                if (!DUCKDB_DIALECT.equals(key.dialect, ignoreCase = true)) {
                    break
                }
                val columnName = parseColumnReference(key.expression) ?: break
                val lowercase = columnName.lowercase(Locale.ROOT)
                if (!resolvableLowercaseColumnNames.contains(lowercase)) {
                    // Expression names a column we don't see at this snapshot (likely renamed
                    // / dropped after the sort spec was written) or one not being written by
                    // this sink. Stop here.
                    break
                }
                resolved.add(ResolvedSortColumn(lowercase, toSortOrder(key.direction, key.nullOrder)))
                expectedIndex++
            }
            return resolved
        }

        /**
         * Visible for testing. Returns the unquoted column name when `expression` is
         * a simple identifier (optionally double-quoted, per DuckDB dialect), otherwise
         * `null`.
         */
        internal fun parseColumnReference(expression: String?): String? {
            if (expression == null) {
                return null
            }
            val trimmed = expression.trim()
            if (trimmed.isEmpty()) {
                return null
            }
            if (trimmed[0] == '"') {
                // Quoted identifier. Closing quote must be the last char; "" inside escapes a
                // literal double quote. DuckDB / SQL standard.
                if (trimmed.length < 2 || trimmed[trimmed.length - 1] != '"') {
                    return null
                }
                val out = StringBuilder(trimmed.length - 2)
                var i = 1
                val limit = trimmed.length - 1
                while (i < limit) {
                    val c = trimmed[i]
                    if (c == '"') {
                        if (i + 1 < limit && trimmed[i + 1] == '"') {
                            out.append('"')
                            i += 2
                            continue
                        }
                        // unescaped closing quote before the end — malformed
                        return null
                    }
                    out.append(c)
                    i++
                }
                return if (out.isEmpty()) null else out.toString()
            }
            // Unquoted: standard SQL identifier — letter or underscore followed by
            // letters / digits / underscores. Anything else (parens, dots, operators) means
            // we cannot safely interpret as a single column reference.
            val first = trimmed[0]
            if (!isIdentifierStart(first)) {
                return null
            }
            for (i in 1 until trimmed.length) {
                if (!isIdentifierPart(trimmed[i])) {
                    return null
                }
            }
            return trimmed
        }

        private fun isIdentifierStart(c: Char): Boolean =
            c == '_' || (c in 'A'..'Z') || (c in 'a'..'z')

        private fun isIdentifierPart(c: Char): Boolean =
            isIdentifierStart(c) || (c in '0'..'9')

        internal fun toSortOrder(direction: DucklakeSortDirection, nullOrder: DucklakeNullOrder): SortOrder =
            when (direction) {
                DucklakeSortDirection.ASC -> when (nullOrder) {
                    DucklakeNullOrder.NULLS_FIRST -> SortOrder.ASC_NULLS_FIRST
                    DucklakeNullOrder.NULLS_LAST -> SortOrder.ASC_NULLS_LAST
                }
                DucklakeSortDirection.DESC -> when (nullOrder) {
                    DucklakeNullOrder.NULLS_FIRST -> SortOrder.DESC_NULLS_FIRST
                    DucklakeNullOrder.NULLS_LAST -> SortOrder.DESC_NULLS_LAST
                }
            }
    }
}
