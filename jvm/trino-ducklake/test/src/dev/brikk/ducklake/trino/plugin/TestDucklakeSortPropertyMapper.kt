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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeNullOrder
import dev.brikk.ducklake.catalog.DucklakeSortDirection
import dev.brikk.ducklake.catalog.DucklakeSortKey
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.SortOrder
import io.trino.spi.connector.SortingProperty
import io.trino.spi.type.IntegerType
import io.trino.spi.type.VarcharType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [DucklakeSortPropertyMapper]. The end-to-end planner
 * check (Trino skips the sort operator when the catalog declares the right
 * order) lives in the cross-engine integration test.
 */
class TestDucklakeSortPropertyMapper {
    @Test
    fun testSingleAscNullsLastSortKey() {
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(key(0, "name", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_LAST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_LAST))
    }

    @Test
    fun testAllFourSortOrderCombinations() {
        // ASC + NULLS_FIRST, ASC + NULLS_LAST, DESC + NULLS_FIRST, DESC + NULLS_LAST.
        // Catalog stores literal "ASC"/"DESC" + "NULLS_FIRST"/"NULLS_LAST" strings; this
        // pins the 4-way mapping to Trino's SortOrder enum.
        assertThat(DucklakeSortPropertyMapper.toSortOrder(DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST))
                .isEqualTo(SortOrder.ASC_NULLS_FIRST)
        assertThat(DucklakeSortPropertyMapper.toSortOrder(DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_LAST))
                .isEqualTo(SortOrder.ASC_NULLS_LAST)
        assertThat(DucklakeSortPropertyMapper.toSortOrder(DucklakeSortDirection.DESC, DucklakeNullOrder.NULLS_FIRST))
                .isEqualTo(SortOrder.DESC_NULLS_FIRST)
        assertThat(DucklakeSortPropertyMapper.toSortOrder(DucklakeSortDirection.DESC, DucklakeNullOrder.NULLS_LAST))
                .isEqualTo(SortOrder.DESC_NULLS_LAST)
    }

    @Test
    fun testMultiKeyOrderingPreserved() {
        // Sort spec: event_ts DESC NULLS_FIRST, name ASC NULLS_LAST.
        // Mapper must emit them in the same order — Trino interprets the list as
        // (primary, secondary, ...) and a permutation would silently break the contract.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(
                        key(0, "event_ts", "duckdb", DucklakeSortDirection.DESC, DucklakeNullOrder.NULLS_FIRST),
                        key(1, "name", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_LAST)),
                COLUMNS)
        assertThat(result).containsExactly(
                SortingProperty(COL_TS, SortOrder.DESC_NULLS_FIRST),
                SortingProperty(COL_NAME, SortOrder.ASC_NULLS_LAST))
    }

    @Test
    fun testCaseInsensitiveColumnLookup() {
        // DuckLake catalogs may quote identifiers, preserving case. Trino lowercases column
        // names internally; we match case-insensitively.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(key(0, "NAME", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_FIRST))
    }

    @Test
    fun testQuotedIdentifierStripsQuotes() {
        // DuckDB's BuildSortOrderSQL emits "name" (quoted) for column refs to be safe
        // against reserved words. We accept either form.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(key(0, "\"name\"", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_FIRST))
    }

    @Test
    fun testQuotedIdentifierWithEmbeddedDoubleQuote() {
        // SQL escape: "" inside a quoted identifier represents a literal " character.
        val cols: Map<String, ColumnHandle> = ImmutableMap.of(
                "weird\"name", DucklakeColumnHandle(99L, "weird\"name", VarcharType.VARCHAR, true))
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("\"weird\"\"name\""))
                .isEqualTo("weird\"name")
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(key(0, "\"weird\"\"name\"", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                cols)
        assertThat(result).hasSize(1)
    }

    @Test
    fun testUnknownDialectStopsAtFirstKey() {
        // If a sort key was written in an unsupported dialect, we cannot trust the rest
        // of the spec (different identifier rules / expression syntax). Truncate to the
        // valid prefix — here, the first key, which is duckdb.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(
                        key(0, "name", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST),
                        key(1, "event_ts", "spark", DucklakeSortDirection.DESC, DucklakeNullOrder.NULLS_LAST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_FIRST))
    }

    @Test
    fun testNonColumnExpressionTruncatesToValidPrefix() {
        // Sort by a function call after a simple column ref. We emit only the prefix.
        // Emitting [name ASC, price ASC] (skipping the middle entry) would be a lie —
        // the table is *not* secondarily sorted by price; it's secondarily sorted by
        // lower(name).
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(
                        key(0, "name", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST),
                        key(1, "lower(name)", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST),
                        key(2, "price", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_FIRST))
    }

    @Test
    fun testFirstExpressionUntranslatableYieldsEmpty() {
        // If we can't even parse the leading key, no prefix survives.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(key(0, "lower(name)", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                COLUMNS)
        assertThat(result).isEmpty()
    }

    @Test
    fun testColumnDroppedAtSnapshotTruncates() {
        // Common drift case: ALTER TABLE DROP COLUMN happened after the sort spec was
        // recorded. The dropped column isn't in our column map anymore — bail to the
        // valid prefix.
        val result = DucklakeSortPropertyMapper.toLocalProperties(
                listOf(
                        key(0, "name", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST),
                        key(1, "gone", "duckdb", DucklakeSortDirection.ASC, DucklakeNullOrder.NULLS_FIRST)),
                COLUMNS)
        assertThat(result).containsExactly(SortingProperty(COL_NAME, SortOrder.ASC_NULLS_FIRST))
    }

    @Test
    fun testEmptySortSpecReturnsEmpty() {
        assertThat(DucklakeSortPropertyMapper.toLocalProperties(listOf(), COLUMNS)).isEmpty()
    }

    @Test
    fun testParseColumnReferenceEdgeCases() {
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("simple")).isEqualTo("simple")
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("  with_padding  ")).isEqualTo("with_padding")
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("_underscore_start")).isEqualTo("_underscore_start")
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("col123")).isEqualTo("col123")

        // Rejections:
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("   ")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("123starts_with_digit")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("has space")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("dotted.path")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("call(x)")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("\"unterminated")).isNull()
        assertThat(DucklakeSortPropertyMapper.parseColumnReference("\"\"")).isNull() // empty quoted ident
    }

    companion object {
        private val COL_NAME = DucklakeColumnHandle(1L, "name", VarcharType.VARCHAR, true)
        private val COL_TS = DucklakeColumnHandle(2L, "event_ts", IntegerType.INTEGER, true)
        private val COL_PRICE = DucklakeColumnHandle(3L, "price", IntegerType.INTEGER, true)

        private val COLUMNS: Map<String, ColumnHandle> = ImmutableMap.of(
                "name", COL_NAME,
                "event_ts", COL_TS,
                "price", COL_PRICE)

        private fun key(index: Int, expression: String, dialect: String,
                direction: DucklakeSortDirection, nullOrder: DucklakeNullOrder): DucklakeSortKey {
            return DucklakeSortKey(index, expression, dialect, direction, nullOrder)
        }
    }
}
