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

/**
 * Builds the `SELECT ... FROM ... WHERE ...` statement the executors send
 * to DuckDB. Shared between [InProcessDuckDbExecutor] and
 * [QuackDuckDbExecutor] so both engines emit the same SQL — and so the
 * generated SQL is unit-testable without spinning up a DuckDB connection.
 *
 * WHERE is the conjunction of two sources:
 *
 *   - [DuckDbWhereClauseTranslator] on the request's
 *       `pushedPredicate` (range / equality predicates from
 *       `TupleDomain`);
 *   - each string in `pushedExpressions` — function-shape predicates
 *       already translated by [DuckDbExpressionTranslator] at plan time
 *       (e.g. `trino_lower("name") = 'apple'`).
 *
 */
object DuckDbSelectSqlBuilder {
    fun buildSelectSql(
            fullyQualifiedTable: String,
            request: DucklakeDuckDbExecutor.ExecutionRequest): String {
        val sql = StringBuilder("SELECT ")
        if (request.isEmptyProjection()) {
            // COUNT(*) and similar collapse to no projected columns. We still need a
            // SELECT clause that yields one row per file row so deletes can be applied
            // by row position downstream — emit a constant, ignored by the converter.
            sql.append("1")
        }
        else {
            val columns = request.projectedColumns()
            for (i in columns.indices) {
                if (i > 0) {
                    sql.append(", ")
                }
                val name = columns[i].columnName.replace("\"", "\"\"")
                sql.append('"').append(name).append('"')
            }
        }
        sql.append(" FROM ").append(fullyQualifiedTable)

        val domainClause = DuckDbWhereClauseTranslator.toWhereClause(request.pushedPredicate())
        val expressionClauses = request.pushedExpressions()
        val hasDomain = domainClause != null
        val hasExpressions = !expressionClauses.isEmpty()
        if (hasDomain || hasExpressions) {
            sql.append(" WHERE ")
            var first = true
            if (domainClause != null) {
                sql.append(domainClause)
                first = false
            }
            for (clause in expressionClauses) {
                if (!first) {
                    sql.append(" AND ")
                }
                sql.append('(').append(clause).append(')')
                first = false
            }
        }
        return sql.toString()
    }
}
