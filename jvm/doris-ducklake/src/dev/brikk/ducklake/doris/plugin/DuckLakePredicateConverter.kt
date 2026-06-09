package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.ColumnRangePredicate
import org.apache.doris.connector.api.pushdown.ConnectorAnd
import org.apache.doris.connector.api.pushdown.ConnectorBetween
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorIn
import org.apache.doris.connector.api.pushdown.ConnectorLike
import org.apache.doris.connector.api.pushdown.ConnectorLiteral
import org.apache.doris.connector.api.pushdown.ConnectorOr

/**
 * Converts the parts of a Doris [ConnectorExpression] filter that we can safely
 * use for **file-level statistics pruning** into per-column [ColumnRangePredicate]s.
 *
 * Deliberately conservative — this drives *file elimination*, not row-level
 * enforcement, so the BE always re-evaluates the full predicate. We only need to
 * never drop a file that could contain a matching row:
 * - Only top-level `AND` conjuncts are considered.
 * - Mapped: `col <op> literal` comparisons, `col BETWEEN lo AND hi`, **membership**
 *   predicates (`col IN (…)`, `col = a OR col = b …`) collapsed to the inclusive
 *   `[min..max]` span of their values (see [DuckLakeMembership]), and **prefix
 *   `LIKE`** (`col LIKE 'abc%'`) → `[abc, abd)` — which is exactly `col >= 'abc'
 *   AND col < 'abd'`, the same range trust as `>`/`BETWEEN`. A file outside the
 *   range can hold no matching row, so it's safe; a file inside is kept and
 *   re-checked by the BE.
 * - **Function calls, REGEXP, non-prefix / escaped / `_`-wildcard LIKE, IS NULL,
 *   NOT, NOT IN, NE, and mixed-column / range `OR`s are skipped** — they stay in
 *   the remaining filter for the BE. (No function pushdown by design.)
 * - Strict `<`/`>` are widened to inclusive bounds (`ColumnRangePredicate` bounds
 *   are inclusive); widening can only keep extra files, never drop matching ones.
 *
 * String ranges (incl. `LIKE`) assume the column's **codepoint/binary collation**
 * — DuckDB's VARCHAR default. The same assumption already underlies `>`/`<`/
 * `BETWEEN` string pruning here; `LIKE` adds no new exposure.
 */
internal object DuckLakePredicateConverter {

    /** LIKE metacharacters (`%`, `_`) and the escape (`\`) — their presence in a prefix means "not a clean prefix". */
    private const val LIKE_SPECIAL_CHARS = "%_\\"

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
            is ConnectorIn, is ConnectorOr -> fromMembership(expr, ids)
            is ConnectorLike -> fromLike(expr, ids)
            else -> null // ConnectorFunctionCall / IsNull / Not → skip
        }

    /**
     * Prefix `col LIKE 'abc%'` → `[abc, abd)` (`abd` = the prefix's codepoint
     * successor). Only a clean prefix followed by a single trailing `%` qualifies:
     * patterns with `_`, an escape `\`, an interior/leading `%`, or `REGEXP` are
     * skipped (null). If the prefix has no finite successor (all chars `0xFFFF`),
     * the upper bound is left open — still a safe superset.
     */
    private fun fromLike(like: ConnectorLike, ids: Map<String, Long>): ColumnRangePredicate? {
        if (like.operator != ConnectorLike.Operator.LIKE) return null // REGEXP — not a prefix range
        val col = like.value as? ConnectorColumnRef ?: return null
        val pattern = (like.pattern as? ConnectorLiteral)?.takeUnless { it.isNull }?.value as? String ?: return null
        val prefix = prefixOf(pattern) ?: return null
        val columnId = ids[col.columnName] ?: return null
        return ColumnRangePredicate(columnId, prefix, prefixUpperBound(prefix))
    }

    /** The literal prefix of a `'<prefix>%'` pattern, or null unless it's a clean prefix + one trailing `%`. */
    private fun prefixOf(pattern: String): String? {
        // Require: 1+ literal chars (no %, _, or escape \), then exactly one trailing %.
        if (!pattern.endsWith('%')) return null
        val prefix = pattern.dropLast(1)
        if (prefix.isEmpty() || prefix.any { it in LIKE_SPECIAL_CHARS }) return null
        return prefix
    }

    /** Smallest string strictly greater than every string with [prefix] (codepoint successor), or null if none. */
    private fun prefixUpperBound(prefix: String): String? {
        val chars = prefix.toCharArray()
        var i = chars.size - 1
        while (i >= 0 && chars[i] == Char.MAX_VALUE) {
            i--
        }
        if (i < 0) return null // all chars 0xFFFF — no finite successor; leave upper bound open
        val bumped = chars.copyOf(i + 1)
        bumped[i] = bumped[i] + 1
        return String(bumped)
    }

    /** `col IN (…)` or `col = a OR col = b …` → the inclusive `[min..max]` span over its values. */
    private fun fromMembership(expr: ConnectorExpression, ids: Map<String, Long>): ColumnRangePredicate? {
        val (column, values) = DuckLakeMembership.of(expr) ?: return null
        val columnId = ids[column] ?: return null
        return spanRange(columnId, values)
    }

    /**
     * The inclusive `[min..max]` span of [values], compared in the literals' own
     * **typed** order — so `id IN (3, 10, 9)` spans `3..10`, not the string span
     * `"10".."9"`. Returns null (don't prune this conjunct — keeps all files) if the
     * literals aren't one mutually-comparable type.
     */
    private fun spanRange(columnId: Long, values: List<Any>): ColumnRangePredicate? {
        val cls = values.first()::class
        if (values.first() !is Comparable<*> || values.any { it::class != cls }) {
            return null
        }
        @Suppress("UNCHECKED_CAST")
        val byValue = Comparator<Any> { a, b -> (a as Comparable<Any>).compareTo(b) }
        val sorted = values.sortedWith(byValue)
        return ColumnRangePredicate(columnId, sorted.first().toString(), sorted.last().toString())
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
