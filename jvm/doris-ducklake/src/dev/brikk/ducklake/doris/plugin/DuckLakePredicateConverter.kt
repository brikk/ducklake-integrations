package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.ColumnRangePredicate
import org.apache.doris.connector.api.pushdown.ConnectorAnd
import org.apache.doris.connector.api.pushdown.ConnectorBetween
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorLiteral

/**
 * Converts the parts of a Doris [ConnectorExpression] filter that we can safely
 * use for **file-level statistics pruning** into per-column [ColumnRangePredicate]s.
 *
 * Deliberately conservative — this drives *file elimination*, not row-level
 * enforcement, so the BE always re-evaluates the full predicate. We only need to
 * never drop a file that could contain a matching row:
 * - Only top-level `AND` conjuncts are considered (an `OR` can't collapse to one
 *   range per column without union logic we don't need yet).
 * - Only `col <op> literal` comparisons and `col BETWEEN lo AND hi` are mapped.
 * - **Function calls, LIKE, IN, IS NULL, NOT, OR, and NE are skipped** — they
 *   stay in the remaining filter for the BE. (No function pushdown by design.)
 * - Strict `<`/`>` are widened to inclusive bounds (`ColumnRangePredicate` bounds
 *   are inclusive); widening can only keep extra files, never drop matching ones.
 */
internal object DuckLakePredicateConverter {

    /** One [ColumnRangePredicate] per convertible conjunct; unconvertible parts are dropped. */
    fun toColumnRangePredicates(
        filter: ConnectorExpression,
        columnIdByName: Map<String, Long>,
    ): List<ColumnRangePredicate> {
        val conjuncts = when (filter) {
            is ConnectorAnd -> filter.conjuncts
            else -> listOf(filter)
        }
        return conjuncts.mapNotNull { tryRange(it, columnIdByName) }
    }

    private fun tryRange(expr: ConnectorExpression, ids: Map<String, Long>): ColumnRangePredicate? =
        when (expr) {
            is ConnectorComparison -> fromComparison(expr, ids)
            is ConnectorBetween -> fromBetween(expr, ids)
            else -> null // ConnectorFunctionCall / Like / In / IsNull / Or / Not → skip
        }

    private fun fromComparison(cmp: ConnectorComparison, ids: Map<String, Long>): ColumnRangePredicate? {
        val col = cmp.left as? ConnectorColumnRef ?: return null
        val lit = cmp.right as? ConnectorLiteral ?: return null
        if (lit.isNull) return null
        val columnId = ids[col.columnName] ?: return null
        val v = lit.value?.toString() ?: return null
        return when (cmp.operator) {
            ConnectorComparison.Operator.EQ,
            ConnectorComparison.Operator.EQ_FOR_NULL -> ColumnRangePredicate(columnId, v, v)
            // GT/GE → lower bound only; LT/LE → upper bound only. Strict bounds are
            // widened to inclusive (safe: only keeps extra files, never drops one).
            ConnectorComparison.Operator.GT,
            ConnectorComparison.Operator.GE -> ColumnRangePredicate(columnId, v, null)
            ConnectorComparison.Operator.LT,
            ConnectorComparison.Operator.LE -> ColumnRangePredicate(columnId, null, v)
            ConnectorComparison.Operator.NE -> null // can't be a single range
        }
    }

    private fun fromBetween(b: ConnectorBetween, ids: Map<String, Long>): ColumnRangePredicate? {
        val col = b.value as? ConnectorColumnRef ?: return null
        val lo = (b.lower as? ConnectorLiteral)?.takeUnless { it.isNull }?.value?.toString() ?: return null
        val hi = (b.upper as? ConnectorLiteral)?.takeUnless { it.isNull }?.value?.toString() ?: return null
        val columnId = ids[col.columnName] ?: return null
        return ColumnRangePredicate(columnId, lo, hi)
    }
}
