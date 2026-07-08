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
            val structReshapePlans: Map<Long, List<StructFieldPlan>> = request.structReshapePlans()
            val promotedColumnIds: Set<Long> = request.promotedColumnIds()
            for (i in columns.indices) {
                if (i > 0) {
                    sql.append(", ")
                }
                appendProjectedColumn(sql, columns[i], fileNames, structReshapePlans, promotedColumnIds)
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
            fileNames: Map<Long, String>,
            structReshapePlans: Map<Long, List<StructFieldPlan>>,
            promotedColumnIds: Set<Long>) {
        val currentName = column.columnName
        if (fileNames.isEmpty()) {
            appendQuoted(sql, currentName)
            return
        }
        val physicalName: String? = fileNames[column.columnId]
        if (physicalName == null) {
            // Added after this file was written — no physical column to read. Rows
            // predating an ADD COLUMN ... DEFAULT project the initial default
            // (upstream issue 1135 semantics); otherwise NULL. The default is plain
            // value text — render as an escaped string literal and CAST (DuckDB
            // coerces '42' -> INTEGER etc.).
            val initialDefault = column.initialDefault
            if (initialDefault == null) {
                sql.append("CAST(NULL AS ")
            } else {
                sql.append("CAST('").append(initialDefault.replace("'", "''")).append("' AS ")
            }
            sql.append(DuckDbWriterSupport.toDuckDbSqlType(column.columnType, "schema-evolution default projection"))
                    .append(") AS ")
            appendQuoted(sql, currentName)
            return
        }
        val reshape: List<StructFieldPlan>? = structReshapePlans[column.columnId]
        if (reshape != null) {
            // The struct exists in the file but with a different shape — normalize it to the current
            // shape with struct_pack. The CASE guard preserves NULL structs: struct_pack would
            // otherwise turn a NULL struct into a (non-null) struct of NULL fields.
            val rootExpr = quoteIdentifier(physicalName)
            sql.append("CASE WHEN ").append(rootExpr).append(" IS NULL THEN NULL ELSE ")
            appendStructPack(sql, reshape, rootExpr)
            sql.append(" END AS ")
            appendQuoted(sql, currentName)
            return
        }
        if (column.columnId in promotedColumnIds) {
            // The column's type widened since this file was written (ALTER … SET DATA TYPE); the
            // physical file holds the OLD type, so CAST it to the current type. The Arrow→page
            // converter reads by the current (widened) type, so the vector kinds must match.
            sql.append("CAST(")
            appendQuoted(sql, physicalName)
            sql.append(" AS ")
                    .append(DuckDbWriterSupport.toDuckDbSqlType(column.columnType, "type-promotion projection"))
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

    /** `struct_pack(curName := <field expr>, …)` rebuilding the current struct from [parentExpr]. */
    private fun appendStructPack(sql: StringBuilder, fields: List<StructFieldPlan>, parentExpr: String) {
        sql.append("struct_pack(")
        for (i in fields.indices) {
            if (i > 0) {
                sql.append(", ")
            }
            val field = fields[i]
            appendQuoted(sql, field.currentName)
            sql.append(" := ")
            appendFieldExpr(sql, field, parentExpr)
        }
        sql.append(")")
    }

    private fun appendFieldExpr(sql: StringBuilder, field: StructFieldPlan, parentExpr: String) {
        if (field.fileName == null) {
            // Subfield (or wholly-added nested struct) added after the file was written. Project its
            // initial_default (rows predating `ADD COLUMN s.child … DEFAULT …`), else a typed NULL.
            val initialDefault = field.initialDefault
            sql.append("CAST(")
            if (initialDefault == null) {
                sql.append("NULL")
            } else {
                sql.append("'").append(initialDefault.replace("'", "''")).append("'")
            }
            sql.append(" AS ")
                    .append(DuckDbWriterSupport.toDuckDbSqlType(field.type, "nested schema-evolution default projection"))
                    .append(")")
            return
        }
        val childExpr = "(" + parentExpr + ")." + quoteIdentifier(field.fileName)
        if (field.children == null) {
            if (field.promoted) {
                // The field's type widened since the file was written (ALTER COLUMN s.child SET DATA
                // TYPE). The file holds the OLD physical type, so CAST it to the current type — the
                // Arrow→page converter reads by the current (widened) type, so the vectors must match.
                sql.append("CAST(").append(childExpr).append(" AS ")
                        .append(DuckDbWriterSupport.toDuckDbSqlType(field.type, "nested type-promotion projection"))
                        .append(")")
                return
            }
            // Leaf: a scalar, or a nested struct whose shape matches the file (read as-is).
            sql.append(childExpr)
        }
        else {
            // Nested struct that itself needs reshaping — recurse, NULL-guarded like the root.
            sql.append("CASE WHEN ").append(childExpr).append(" IS NULL THEN NULL ELSE ")
            appendStructPack(sql, field.children, childExpr)
            sql.append(" END")
        }
    }

    private fun appendQuoted(sql: StringBuilder, identifier: String) {
        sql.append(quoteIdentifier(identifier))
    }

    private fun quoteIdentifier(identifier: String): String =
            "\"" + identifier.replace("\"", "\"\"") + "\""
}
