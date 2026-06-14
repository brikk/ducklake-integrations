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
            // Schema-evolution name map: column_id -> name in the physical file. Empty => no
            // resolution, project by current name (no-evolution fast path / lance-search PTF).
            val fileNames: Map<Long, String> = request.fileColumnNamesById()
            for (i in columns.indices) {
                if (i > 0) {
                    sql.append(", ")
                }
                appendProjectedColumn(sql, columns[i], fileNames)
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

    /**
     * Append one projected column to the SELECT list, applying schema-evolution resolution:
     *
     *   - [fileNames] empty (no resolution): project the column by its current name.
     *   - column present in [fileNames]: project the file's physical name, aliased to the
     *     current name when they differ (handles RENAME COLUMN — the file kept the old name).
     *   - column absent from a non-empty [fileNames]: the column was added AFTER the file was
     *     written, so it has no physical column — project a typed NULL under the current name
     *     (handles ADD COLUMN; matches the parquet path's missing-column-as-NULL behavior).
     *
     * The projection order always equals the requested column order, so the Arrow→page
     * converter (which maps by position to the requested types) is unaffected.
     */
    private fun appendProjectedColumn(
            sql: StringBuilder,
            column: DucklakeColumnHandle,
            fileNames: Map<Long, String>) {
        val currentName = column.columnName
        if (fileNames.isEmpty()) {
            appendQuoted(sql, currentName)
            return
        }
        val physicalName: String? = fileNames[column.columnId]
        if (physicalName == null) {
            // Added after this file was written — no physical column to read.
            sql.append("CAST(NULL AS ")
                    .append(DuckDbWriterSupport.toDuckDbSqlType(column.columnType, "schema-evolution NULL projection"))
                    .append(") AS ")
            appendQuoted(sql, currentName)
            return
        }
        appendQuoted(sql, physicalName)
        if (physicalName != currentName) {
            sql.append(" AS ")
            appendQuoted(sql, currentName)
        }
    }

    private fun appendQuoted(sql: StringBuilder, identifier: String) {
        sql.append('"').append(identifier.replace("\"", "\"\"")).append('"')
    }
}
