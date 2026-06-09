package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.ColumnRangePredicate
import org.apache.doris.connector.api.pushdown.ConnectorAnd
import org.apache.doris.connector.api.pushdown.ConnectorBetween
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall
import org.apache.doris.connector.api.pushdown.ConnectorIn
import org.apache.doris.connector.api.pushdown.ConnectorLike
import org.apache.doris.connector.api.pushdown.ConnectorLiteral
import org.apache.doris.connector.api.pushdown.ConnectorOr
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakePredicateConverter] — the filter → file-stat
 * range mapping that drives applyFilter's pruning. No catalog / FE needed.
 */
internal class DuckLakePredicateConverterTest {

    private val ids = mapOf("id" to 1L, "total" to 2L)
    private val type = DuckLakeTypeMapping.fromDucklakeType("int32")

    private fun col(name: String) = ConnectorColumnRef(name, type)
    private fun lit(v: Any) = ConnectorLiteral(type, v)
    private fun cmp(op: ConnectorComparison.Operator, column: String, v: Any) =
        ConnectorComparison(op, col(column), lit(v))
    private fun like(column: String, pattern: String, op: ConnectorLike.Operator = ConnectorLike.Operator.LIKE) =
        ConnectorLike(op, col(column), lit(pattern))

    private fun convert(expr: ConnectorExpression) =
        DuckLakePredicateConverter.toColumnRangePredicates(expr, ids)

    @Test
    fun equalityBecomesPointRange() {
        assertThat(convert(cmp(ConnectorComparison.Operator.EQ, "id", 5)))
            .containsExactly(ColumnRangePredicate(1L, "5", "5"))
    }

    @Test
    fun inequalitiesBecomeOpenRangesAndStrictBoundsWidenInclusive() {
        assertThat(convert(cmp(ConnectorComparison.Operator.GE, "id", 5)))
            .containsExactly(ColumnRangePredicate(1L, "5", null))
        assertThat(convert(cmp(ConnectorComparison.Operator.GT, "id", 5)))
            .containsExactly(ColumnRangePredicate(1L, "5", null)) // strict > widened to >=
        assertThat(convert(cmp(ConnectorComparison.Operator.LE, "id", 10)))
            .containsExactly(ColumnRangePredicate(1L, null, "10"))
        assertThat(convert(cmp(ConnectorComparison.Operator.LT, "id", 10)))
            .containsExactly(ColumnRangePredicate(1L, null, "10")) // strict < widened to <=
    }

    @Test
    fun betweenBecomesClosedRange() {
        val between = ConnectorBetween(col("total"), lit(5), lit(10))
        assertThat(convert(between)).containsExactly(ColumnRangePredicate(2L, "5", "10"))
    }

    @Test
    fun andExtractsEveryConvertibleConjunct() {
        val and = ConnectorAnd(
            listOf(
                cmp(ConnectorComparison.Operator.GE, "id", 5),
                cmp(ConnectorComparison.Operator.LE, "total", 99),
            ),
        )
        assertThat(convert(and)).containsExactlyInAnyOrder(
            ColumnRangePredicate(1L, "5", null),
            ColumnRangePredicate(2L, null, "99"),
        )
    }

    @Test
    fun skipsNonConvertiblePredicates() {
        // NE can't be a single range.
        assertThat(convert(cmp(ConnectorComparison.Operator.NE, "id", 5))).isEmpty()
        // Unknown column.
        assertThat(convert(cmp(ConnectorComparison.Operator.EQ, "ghost", 5))).isEmpty()
        // Function call — never pushed (no function pushdown by design).
        val fn = ConnectorFunctionCall("lower", type, listOf(col("id")))
        assertThat(convert(fn)).isEmpty()
        // A function buried in an AND is skipped; the sibling comparison still converts.
        val mixed = ConnectorAnd(listOf(fn, cmp(ConnectorComparison.Operator.EQ, "id", 7)))
        assertThat(convert(mixed)).containsExactly(ColumnRangePredicate(1L, "7", "7"))
    }

    @Test
    fun inListBecomesTypedSpanRange() {
        // The span must be computed in the values' TYPED order: 3..10, not the
        // string span "10".."9". A regression to string ordering would yield an
        // inverted/incorrect range here.
        val inList = ConnectorIn(col("id"), listOf(lit(3), lit(10), lit(9)), false)
        assertThat(convert(inList)).containsExactly(ColumnRangePredicate(1L, "3", "10"))
    }

    @Test
    fun orOfSameColumnEqualitiesBecomesSpanRange() {
        val or = ConnectorOr(
            listOf(
                cmp(ConnectorComparison.Operator.EQ, "id", 7),
                cmp(ConnectorComparison.Operator.EQ, "id", 3),
                cmp(ConnectorComparison.Operator.EQ, "id", 5),
            ),
        )
        assertThat(convert(or)).containsExactly(ColumnRangePredicate(1L, "3", "7"))
    }

    @Test
    fun membershipComposesAsAConjunctInsideAnd() {
        // `total >= 5 AND id IN (1, 2)` → a range from each conjunct.
        val and = ConnectorAnd(
            listOf(
                cmp(ConnectorComparison.Operator.GE, "total", 5),
                ConnectorIn(col("id"), listOf(lit(1), lit(2)), false),
            ),
        )
        assertThat(convert(and)).containsExactlyInAnyOrder(
            ColumnRangePredicate(2L, "5", null),
            ColumnRangePredicate(1L, "1", "2"),
        )
    }

    @Test
    fun prefixLikeBecomesCodepointRange() {
        // 'abc%' matches exactly the strings in [abc, abd) — abd = codepoint successor.
        assertThat(convert(like("id", "abc%")))
            .containsExactly(ColumnRangePredicate(1L, "abc", "abd"))
        // single-char prefix: 'a%' → [a, b)
        assertThat(convert(like("id", "a%")))
            .containsExactly(ColumnRangePredicate(1L, "a", "b"))
    }

    @Test
    fun skipsNonPrefixLike() {
        // Leading wildcard — no usable prefix.
        assertThat(convert(like("id", "%abc"))).isEmpty()
        // `_` single-char wildcard inside the prefix.
        assertThat(convert(like("id", "a_c%"))).isEmpty()
        // No trailing `%` — an exact match, left to the BE / equality path.
        assertThat(convert(like("id", "abc"))).isEmpty()
        // Escape char in the prefix — semantics we don't decode.
        assertThat(convert(like("id", "a\\%b%"))).isEmpty()
        // REGEXP is not a prefix range.
        assertThat(convert(like("id", "abc.*", ConnectorLike.Operator.REGEXP))).isEmpty()
        // Unknown column.
        assertThat(convert(like("ghost", "abc%"))).isEmpty()
    }

    @Test
    fun skipsUnsafeMembership() {
        // NOT IN is the unbounded complement — can't be a single range.
        assertThat(convert(ConnectorIn(col("id"), listOf(lit(1), lit(2)), true))).isEmpty()
        // OR across two columns can't collapse to one range per column.
        val mixedColumnOr = ConnectorOr(
            listOf(
                cmp(ConnectorComparison.Operator.EQ, "id", 1),
                cmp(ConnectorComparison.Operator.EQ, "total", 2),
            ),
        )
        assertThat(convert(mixedColumnOr)).isEmpty()
        // OR with a non-equality disjunct (a range) is not membership.
        val rangeOr = ConnectorOr(
            listOf(
                cmp(ConnectorComparison.Operator.EQ, "id", 1),
                cmp(ConnectorComparison.Operator.GT, "id", 9),
            ),
        )
        assertThat(convert(rangeOr)).isEmpty()
    }
}
