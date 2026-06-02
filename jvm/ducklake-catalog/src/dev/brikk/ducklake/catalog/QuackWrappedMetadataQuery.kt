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
package dev.brikk.ducklake.catalog

import org.jooq.DSLContext
import org.jooq.Query
import org.jooq.Record
import org.jooq.RecordMapper
import org.jooq.ResultQuery
import java.util.stream.Collectors

/**
 * [MetadataQuery] for the Quack catalog backend. Renders the caller's
 * jOOQ query to bind-inlined SQL and routes execution through
 * `CALL system.main.quack_query_by_name('<metadata_catalog>', '<inner_sql>')`.
 *
 * From the local DuckDB optimizer's point of view the wrapped statement is a
 * single `TableFunction` call — exactly one `LogicalGet` — so the
 * "Multiple streaming scans or streaming scans + CTAS / insert in the same
 * query are not currently supported" check in
 * `duckdb-quack/src/storage/quack_optimizer.cpp` doesn't fire regardless
 * of how many references to the same `ducklake_*` table appear in the
 * inner SQL. The inner SQL executes server-side as plain DuckDB SQL against
 * base tables, so JOINs / same-table subqueries / UPDATE / DELETE that the
 * attached-catalog path rejects all work.
 *
 * Mirrors the wrapper shape used by upstream's C++
 * `QuackMetadataManager::Query`
 * (in `vendor/ducklake/src/metadata_manager/quack_metadata_manager.cpp`).
 *
 * **Result-type fidelity:** `quack_query_by_name` is a polymorphic
 * table function whose output columns are resolved at JDBC prepare time via a
 * server PREPARE round trip — empirically confirmed: wrapping
 * `SELECT snapshot_id, schema_version FROM ducklake_snapshot WHERE ...`
 * produces a prepared statement that advertises
 * `(snapshot_id BIGINT, schema_version BIGINT)`. jOOQ's reflective
 * `Record.into(Class)` mapping picks the columns up by name into the
 * caller's generated record type without further coercion hints.
 */
internal class QuackWrappedMetadataQuery(metadataCatalogName: String) : MetadataQuery {
    private val metadataCatalogName: String = metadataCatalogName

    override fun <R : Record> fetchOne(dsl: DSLContext, query: ResultQuery<R>): R? {
        val wrapped = wrap(dsl.renderInlined(query))
        val row = dsl.fetchOne(wrapped) ?: return null
        return row.into(query.recordType)
    }

    override fun <R : Record> fetch(dsl: DSLContext, query: ResultQuery<R>): List<R> {
        val wrapped = wrap(dsl.renderInlined(query))
        val recordType: Class<out R> = query.recordType
        return dsl.fetch(wrapped)
                .stream()
                .map { row -> row.into(recordType) }
                .collect(Collectors.toList())
    }

    override fun <T> fetch(dsl: DSLContext, query: ResultQuery<*>, mapper: RecordMapper<in Record, T>): List<T> {
        val fields = query.fields()
        // Multi-table JOIN projections frequently have name collisions across
        // sides (e.g. `file.path` + `delfile.path`). The wrapper's polymorphic
        // output is forced to flat column names — duplicates throw
        // "Binder Error: table quack_query_by_name has duplicate column name".
        // Sidestep it by wrapping the inner SQL in a derived-table SELECT *
        // with positional aliases (c0, c1, …), then coerce back to the
        // original Field<?>[] (coerce is positional, so the mapper's
        // r.get(originalField) still works).
        val aliasedInner = aliasColumnsPositionally(dsl.renderInlined(query), fields.size)
        val wrapped = wrap(aliasedInner)
        return dsl.resultQuery(wrapped).coerce(*fields).fetch().map(mapper)
    }

    override fun execute(dsl: DSLContext, mutation: Query): Int {
        // The Quack server runs the inner DML via DuckDB's SendQuery and the
        // table-function surfaces DuckDB's standard `Count BIGINT` shape — i.e.
        // one row, one column carrying the affected-row count (or 0 for DDL).
        // Read it back and return as int so callers preserve the
        // Query.execute() contract on PG / local DuckDB.
        val wrapped = wrap(dsl.renderInlined(mutation))
        val rows = dsl.fetch(wrapped)
        if (rows.isEmpty()) {
            return 0
        }
        val value = rows[0].get(0)
        return if (value is Number) value.toInt() else 0
    }

    private fun wrap(innerSql: String): String {
        return ("CALL system.main.quack_query_by_name("
                + sqlLiteral(metadataCatalogName)
                + ", "
                + sqlLiteral(innerSql)
                + ")")
    }

    companion object {
        private fun aliasColumnsPositionally(innerSql: String, columnCount: Int): String {
            val aliases = StringBuilder()
            for (i in 0 until columnCount) {
                if (i > 0) {
                    aliases.append(", ")
                }
                aliases.append("c").append(i)
            }
            return "SELECT * FROM ($innerSql) AS _q($aliases)"
        }

        private fun sqlLiteral(value: String): String {
            return "'" + value.replace("'", "''") + "'"
        }
    }
}
