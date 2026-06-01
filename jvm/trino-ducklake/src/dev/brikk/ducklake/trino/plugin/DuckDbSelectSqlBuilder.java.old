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
package dev.brikk.ducklake.trino.plugin;

import java.util.List;
import java.util.Optional;

/**
 * Builds the {@code SELECT ... FROM ... WHERE ...} statement the executors send
 * to DuckDB. Shared between {@link InProcessDuckDbExecutor} and
 * {@link QuackDuckDbExecutor} so both engines emit the same SQL — and so the
 * generated SQL is unit-testable without spinning up a DuckDB connection.
 *
 * <p>WHERE is the conjunction of two sources:
 * <ul>
 *   <li>{@link DuckDbWhereClauseTranslator} on the request's
 *       {@code pushedPredicate} (range / equality predicates from
 *       {@code TupleDomain});</li>
 *   <li>each string in {@code pushedExpressions} — function-shape predicates
 *       already translated by {@link DuckDbExpressionTranslator} at plan time
 *       (e.g. {@code trino_lower("name") = 'apple'}).</li>
 * </ul>
 */
final class DuckDbSelectSqlBuilder
{
    private DuckDbSelectSqlBuilder() {}

    static String buildSelectSql(
            String fullyQualifiedTable,
            DucklakeDuckDbExecutor.ExecutionRequest request)
    {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (request.isEmptyProjection()) {
            // COUNT(*) and similar collapse to no projected columns. We still need a
            // SELECT clause that yields one row per file row so deletes can be applied
            // by row position downstream — emit a constant, ignored by the converter.
            sql.append("1");
        }
        else {
            var columns = request.projectedColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String name = columns.get(i).columnName().replace("\"", "\"\"");
                sql.append('"').append(name).append('"');
            }
        }
        sql.append(" FROM ").append(fullyQualifiedTable);

        Optional<String> domainClause = DuckDbWhereClauseTranslator.toWhereClause(request.pushedPredicate());
        List<String> expressionClauses = request.pushedExpressions();
        boolean hasDomain = domainClause.isPresent();
        boolean hasExpressions = !expressionClauses.isEmpty();
        if (hasDomain || hasExpressions) {
            sql.append(" WHERE ");
            boolean first = true;
            if (hasDomain) {
                sql.append(domainClause.get());
                first = false;
            }
            for (String clause : expressionClauses) {
                if (!first) {
                    sql.append(" AND ");
                }
                sql.append('(').append(clause).append(')');
                first = false;
            }
        }
        return sql.toString();
    }
}
