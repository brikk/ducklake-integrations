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

import io.trino.spi.predicate.TupleDomain
import org.apache.arrow.vector.ipc.ArrowReader
import java.io.IOException
import java.sql.SQLException

/**
 * Execution-engine strategy for the DuckDB-format read path. Owns the JDBC /
 * RPC client used to ATTACH a `.db` file and stream rows back as Arrow.
 *
 * Three concrete shapes are planned:
 *
 *   - **In-process** — embedded DuckDB via `jdbc:duckdb:`. Crash takes
 *       the JVM down; fastest steady-state when the file is local.
 *   - **Quack** — out-of-process DuckDB reached over the Quack RPC protocol.
 *       JDBC client is still `duckdb_jdbc` (the quack extension is the
 *       transport); server-side ATTACH is issued via `quack_query_by_name`.
 *       Sidecar crash recoverable independently of the Trino worker.
 *   - **Swanlake** — out-of-process DuckDB reached over Arrow Flight SQL.
 *       JDBC client is Arrow's Flight SQL JDBC driver; server-side ATTACH is
 *       direct SQL. Same resilience story as Quack, different wire protocol.
 *
 * [DuckDbFilePageSource] is engine-agnostic — it instantiates an
 * executor per split, calls [execute], iterates the
 * resulting [ArrowReader], and closes the [ExecutionContext] when
 * done. The executor is responsible for the entire lifecycle of the underlying
 * JDBC / RPC connection, including any DETACH on close.
 *
 * Selection is via the `ducklake.execution-engine` catalog property,
 * resolved by [DucklakeDuckDbExecutorFactory].
 */
interface DucklakeDuckDbExecutor {
    /**
     * Open a connection, ATTACH the file in `request.target()` (server-side
     * for remote engines), and execute the SELECT for the requested projection
     * and pushed predicate. Returns a context whose [ArrowReader] streams
     * the result and whose `close()` releases the connection / statement /
     * allocator / any server-side ATTACH state.
     */
    @Throws(SQLException::class)
    fun execute(request: ExecutionRequest): ExecutionContext

    /**
     * Inputs to [execute]. The executor decides how to
     * translate the predicate to `WHERE`-clause SQL (see
     * [DuckDbWhereClauseTranslator]) and what to name the server-side
     * ATTACH alias.
     *
     * @param target            where the `.db` file is reachable from
     * @param projectedColumns  the columns Trino asked for; empty means
     *                          `COUNT(*)`-style — caller wants only row
     *                          counts, executor should emit a single-column
     *                          result so position counts flow downstream
     * @param pushedPredicate   pre-filtered TupleDomain (non-pushed predicates
     *                          re-apply above the page source)
     * @param pushedExpressions function-shape SQL fragments already translated
     *                          to DuckDB (e.g. `trino_lower("c") = 'a'`);
     *                          AND-ed into the WHERE alongside the
     *                          TupleDomain-derived clause
     * @param duckDbTimeZone    optional DuckDB-acceptable zone identifier the
     *                          executor should pass to `SET TimeZone`
     *                          immediately after attach; carries Trino's session
     *                          `TimeZoneKey` after normalisation through
     *                          [TrinoTimeZoneNormaliser]. Empty when the
     *                          caller has no session (most tests). When present
     *                          the executor MAY fail to apply it (e.g.
     *                          fractional-offset zone that DuckDB rejects) —
     *                          such failure is logged and execution continues
     *                          without a `SET TimeZone`, compromising
     *                          Tier C pushdown correctness for this attach but
     *                          leaving Tier A/B unaffected.
     */
    class ExecutionRequest(
            target: DuckDbAttachTarget,
            projectedColumns: List<DucklakeColumnHandle>,
            pushedPredicate: TupleDomain<DucklakeColumnHandle>,
            pushedExpressions: List<String>?,
            duckDbTimeZone: String?,
            fileColumnNamesById: Map<Long, String>?) {
        private val target: DuckDbAttachTarget = target
        private val projectedColumns: List<DucklakeColumnHandle> = projectedColumns
        private val pushedPredicate: TupleDomain<DucklakeColumnHandle> = pushedPredicate
        private val pushedExpressions: List<String> =
            pushedExpressions?.toList() ?: emptyList()
        private val duckDbTimeZone: String? = duckDbTimeZone
        // Schema-evolution resolution: maps each projected column's catalog column_id to the
        // name it had IN THE PHYSICAL FILE (i.e. at the file's begin_snapshot). A projected
        // column whose id is absent from this map was added AFTER the file was written and is
        // projected as a typed NULL. Empty => no resolution: project columns by their current
        // name (the no-evolution fast path, and the contract for the lance-search PTF path).
        // See DuckDbSelectSqlBuilder. The .db/vortex/lance files store physical column names
        // from write time, so the parquet path's field_id matching has no analogue here —
        // catalog-snapshot name resolution is how renames + added columns are handled.
        private val fileColumnNamesById: Map<Long, String> = fileColumnNamesById ?: emptyMap()

        constructor(
                target: DuckDbAttachTarget,
                projectedColumns: List<DucklakeColumnHandle>,
                pushedPredicate: TupleDomain<DucklakeColumnHandle>)
                : this(target, projectedColumns, pushedPredicate, emptyList(), null, null)

        constructor(
                target: DuckDbAttachTarget,
                projectedColumns: List<DucklakeColumnHandle>,
                pushedPredicate: TupleDomain<DucklakeColumnHandle>,
                pushedExpressions: List<String>)
                : this(target, projectedColumns, pushedPredicate, pushedExpressions, null, null)

        constructor(
                target: DuckDbAttachTarget,
                projectedColumns: List<DucklakeColumnHandle>,
                pushedPredicate: TupleDomain<DucklakeColumnHandle>,
                pushedExpressions: List<String>,
                duckDbTimeZone: String?)
                : this(target, projectedColumns, pushedPredicate, pushedExpressions, duckDbTimeZone, null)

        fun target(): DuckDbAttachTarget = target
        fun projectedColumns(): List<DucklakeColumnHandle> = projectedColumns
        fun pushedPredicate(): TupleDomain<DucklakeColumnHandle> = pushedPredicate
        fun pushedExpressions(): List<String> = pushedExpressions
        fun duckDbTimeZone(): String? = duckDbTimeZone
        fun fileColumnNamesById(): Map<Long, String> = fileColumnNamesById

        fun isEmptyProjection(): Boolean = projectedColumns.isEmpty()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ExecutionRequest) return false
            return target == other.target
                    && projectedColumns == other.projectedColumns
                    && pushedPredicate == other.pushedPredicate
                    && pushedExpressions == other.pushedExpressions
                    && duckDbTimeZone == other.duckDbTimeZone
                    && fileColumnNamesById == other.fileColumnNamesById
        }

        override fun hashCode(): Int {
            return java.util.Objects.hash(target, projectedColumns, pushedPredicate, pushedExpressions, duckDbTimeZone, fileColumnNamesById)
        }

        override fun toString(): String {
            return "ExecutionRequest[target=" + target + ", projectedColumns=" + projectedColumns +
                    ", pushedPredicate=" + pushedPredicate + ", pushedExpressions=" + pushedExpressions +
                    ", duckDbTimeZone=" + duckDbTimeZone + ", fileColumnNamesById=" + fileColumnNamesById + "]"
        }
    }

    /**
     * Per-execution lifecycle handle. The page source iterates the
     * [ArrowReader]; on close, the executor releases all resources
     * (DETACH for engines whose server-side state persists across queries
     * is the executor's call — typically left in place to amortise ATTACH
     * cost across queries against the same file, with explicit DETACH only
     * on cache-row invalidation).
     */
    interface ExecutionContext : AutoCloseable {
        fun arrowReader(): ArrowReader

        fun memoryUsage(): Long

        @Throws(IOException::class)
        override fun close()
    }
}
