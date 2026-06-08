package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorIn
import org.apache.doris.connector.api.pushdown.ConnectorLiteral
import org.apache.doris.connector.api.pushdown.ConnectorOr

/**
 * Recognizes **membership** predicates — those constraining a *single* column to
 * a finite set of literal values — and extracts `(columnName, typedValues)`.
 *
 * Three shapes qualify, each meaning "the row's column value is one of these":
 *  - `col = literal`           → `[literal]`
 *  - `col IN (a, b, c)`        → `[a, b, c]`   (`NOT IN` is excluded — its complement is unbounded)
 *  - `col = a OR col = b …`    → `[a, b]`      (every disjunct an equality on the *same* column)
 *
 * Shared by both file-pruning paths in the connector:
 *  - **stats pruning** collapses the set to an inclusive `[min..max]` span
 *    ([DuckLakePredicateConverter]);
 *  - **BUCKET pruning** hashes each value to its bucket ([DuckLakeConnectorMetadata]).
 *
 * Both are deliberate over-approximations — they keep a *superset* of matching
 * files and never drop one that could contain a matching row — so the BE's
 * row-level re-check of the full predicate stays correct.
 */
internal object DuckLakeMembership {

    /** `(columnName, candidate typed values)` if [expr] is a membership predicate, else null. */
    fun of(expr: ConnectorExpression): Pair<String, List<Any>>? = when (expr) {
        is ConnectorComparison -> equality(expr)?.let { (column, value) -> column to listOf(value) }
        is ConnectorIn -> fromIn(expr)
        is ConnectorOr -> fromOr(expr)
        else -> null
    }

    private fun fromIn(inExpr: ConnectorIn): Pair<String, List<Any>>? {
        if (inExpr.isNegated) return null // NOT IN is the unbounded complement — not a finite set
        val column = (inExpr.value as? ConnectorColumnRef)?.columnName ?: return null
        val values = literalValues(inExpr.inList) ?: return null
        return column to values
    }

    private fun fromOr(orExpr: ConnectorOr): Pair<String, List<Any>>? {
        // Every disjunct must be an equality, and all on the same column, for the
        // OR to mean "col ∈ {…}". A range disjunct or a second column → bail (null).
        val facts = orExpr.disjuncts.map { equality(it) ?: return null }
        if (facts.isEmpty()) return null
        val column = facts.map { it.first }.distinct().singleOrNull() ?: return null
        return column to facts.map { it.second }
    }

    /** `col = literal` (or null-safe `<=>`) → `(columnName, typedValue)`, else null. */
    private fun equality(expr: ConnectorExpression): Pair<String, Any>? {
        if (expr !is ConnectorComparison) return null
        if (expr.operator != ConnectorComparison.Operator.EQ &&
            expr.operator != ConnectorComparison.Operator.EQ_FOR_NULL
        ) {
            return null
        }
        val column = (expr.left as? ConnectorColumnRef)?.columnName ?: return null
        val value = (expr.right as? ConnectorLiteral)?.takeUnless { it.isNull }?.value ?: return null
        return column to value
    }

    /** Typed values of a literal list, or null if any element isn't a non-null literal (list never empty). */
    private fun literalValues(exprs: List<ConnectorExpression>): List<Any>? {
        if (exprs.isEmpty()) return null
        return exprs.map { e ->
            (e as? ConnectorLiteral)?.takeUnless { it.isNull }?.value ?: return null
        }
    }
}
