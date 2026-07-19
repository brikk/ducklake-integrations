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
package dev.brikk.duckbridge.trino.plugin

import io.trino.matching.Captures
import io.trino.matching.Pattern
import io.trino.plugin.base.expression.ConnectorExpressionPatterns
import io.trino.plugin.base.expression.ConnectorExpressionRule
import io.trino.plugin.jdbc.QueryParameter
import io.trino.plugin.jdbc.expression.ParameterizedExpression
import io.trino.spi.expression.Call
import java.util.Optional

/**
 * A single base-jdbc [ConnectorExpressionRule] that delegates a whole [Call] conjunct to the ported
 * [DuckBridgeExpressionTranslator], preserving the translator's hard-won semantics verbatim rather
 * than re-deriving them as a pile of `GenericRewrite` string patterns.
 *
 * Why one whole-expression rule instead of `addStandardRules(...)` + per-function rules:
 *
 *  - The translator is a self-contained ConnectorExpression → DuckDB-SQL compiler that already
 *    handles variables, constants, comparisons, arithmetic, AND/OR/NOT/IS NULL/LIKE, CAST, coalesce,
 *    nullif, the `concat → ||` NULL-semantics rewrite, and the full `trino_*` parity catalog with
 *    its per-argument type gates (Tier A/B/C, the `pushdown_timestamp_with_timezone` gate). Every one
 *    of those encodes a correctness edge case discovered in the DuckLake work.
 *  - It emits fully-inlined SQL (literals rendered directly, identifiers quoted) with NO bound
 *    parameters, so the produced [ParameterizedExpression] carries an empty parameter list. That's
 *    intentional and safe: the translator only inlines constants it can render losslessly (integers,
 *    finite doubles, escaped varchar, in-range dates) and refuses everything else, returning null so
 *    the conjunct stays above the scan.
 *  - base-jdbc splits the constraint into conjuncts before calling `convertPredicate`, so returning
 *    [Optional.empty] here leaves exactly that conjunct for Trino while other conjuncts still push —
 *    the same per-conjunct partial-pushdown model the DuckLake translator relied on.
 *
 * The rule matches any [Call]; the top-level predicate Trino hands `applyFilter` is a boolean-typed
 * Call (comparison, AND, IS NULL, LIKE, ...), so matching `Call` covers the pushable shapes. Bare
 * Variable/Constant conjuncts are not independently pushable predicates and are correctly skipped.
 */
class DuckBridgeParityExpressionRule : ConnectorExpressionRule<Call, ParameterizedExpression> {
    override fun getPattern(): Pattern<Call> = PATTERN

    override fun rewrite(
        call: Call,
        captures: Captures,
        context: ConnectorExpressionRule.RewriteContext<ParameterizedExpression>,
    ): Optional<ParameterizedExpression> {
        val sql =
            DuckBridgeExpressionTranslator.translate(call, context.assignments, context.session)
                ?: return Optional.empty()
        return Optional.of(ParameterizedExpression(sql, emptyList<QueryParameter>()))
    }

    private companion object {
        private val PATTERN: Pattern<Call> = ConnectorExpressionPatterns.call()
    }
}
