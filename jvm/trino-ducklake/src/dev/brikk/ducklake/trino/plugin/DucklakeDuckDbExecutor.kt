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
import java.util.Optional

/**
 * Execution-engine strategy for the DuckDB-format read path. Owns the JDBC /
 * RPC client used to ATTACH a {@code .db} file and stream rows back as Arrow.
 *
 * <p>Three concrete shapes are planned:
 *
 * <ul>
 *   <li><b>In-process</b> — embedded DuckDB via {@code jdbc:duckdb:}. Crash takes
 *       the JVM down; fastest steady-state when the file is local.</li>
 *   <li><b>Quack</b> — out-of-process DuckDB reached over the Quack RPC protocol.
 *       JDBC client is still {@code duckdb_jdbc} (the quack extension is the
 *       transport); server-side ATTACH is issued via {@code quack_query_by_name}.
 *       Sidecar crash recoverable independently of the Trino worker.</li>
 *   <li><b>Swanlake</b> — out-of-process DuckDB reached over Arrow Flight SQL.
 *       JDBC client is Arrow's Flight SQL JDBC driver; server-side ATTACH is
 *       direct SQL. Same resilience story as Quack, different wire protocol.</li>
 * </ul>
 *
 * <p>{@link DuckDbFilePageSource} is engine-agnostic — it instantiates an
 * executor per split, calls {@link #execute(ExecutionRequest)}, iterates the
 * resulting {@link ArrowReader}, and closes the {@link ExecutionContext} when
 * done. The executor is responsible for the entire lifecycle of the underlying
 * JDBC / RPC connection, including any DETACH on close.
 *
 * <p>Selection is via the {@code ducklake.execution-engine} catalog property,
 * resolved by {@link DucklakeDuckDbExecutorFactory}.
 */
interface DucklakeDuckDbExecutor {
    /**
     * Open a connection, ATTACH the file in {@code request.target()} (server-side
     * for remote engines), and execute the SELECT for the requested projection
     * and pushed predicate. Returns a context whose {@link ArrowReader} streams
     * the result and whose {@code close()} releases the connection / statement /
     * allocator / any server-side ATTACH state.
     */
    @Throws(SQLException::class)
    fun execute(request: ExecutionRequest): ExecutionContext

    /**
     * Inputs to {@link #execute(ExecutionRequest)}. The executor decides how to
     * translate the predicate to {@code WHERE}-clause SQL (see
     * {@link DuckDbWhereClauseTranslator}) and what to name the server-side
     * ATTACH alias.
     *
     * @param target            where the {@code .db} file is reachable from
     * @param projectedColumns  the columns Trino asked for; empty means
     *                          {@code COUNT(*)}-style — caller wants only row
     *                          counts, executor should emit a single-column
     *                          result so position counts flow downstream
     * @param pushedPredicate   pre-filtered TupleDomain (non-pushed predicates
     *                          re-apply above the page source)
     * @param pushedExpressions function-shape SQL fragments already translated
     *                          to DuckDB (e.g. {@code trino_lower("c") = 'a'});
     *                          AND-ed into the WHERE alongside the
     *                          TupleDomain-derived clause
     * @param duckDbTimeZone    optional DuckDB-acceptable zone identifier the
     *                          executor should pass to {@code SET TimeZone}
     *                          immediately after attach; carries Trino's session
     *                          {@code TimeZoneKey} after normalisation through
     *                          {@link TrinoTimeZoneNormaliser}. Empty when the
     *                          caller has no session (most tests). When present
     *                          the executor MAY fail to apply it (e.g.
     *                          fractional-offset zone that DuckDB rejects) —
     *                          such failure is logged and execution continues
     *                          without a {@code SET TimeZone}, compromising
     *                          Tier C pushdown correctness for this attach but
     *                          leaving Tier A/B unaffected.
     */
    class ExecutionRequest(
            target: DuckDbAttachTarget,
            projectedColumns: List<DucklakeColumnHandle>,
            pushedPredicate: TupleDomain<DucklakeColumnHandle>,
            pushedExpressions: List<String>?,
            duckDbTimeZone: Optional<String>?) {
        private val target: DuckDbAttachTarget = target
        private val projectedColumns: List<DucklakeColumnHandle> = projectedColumns
        private val pushedPredicate: TupleDomain<DucklakeColumnHandle> = pushedPredicate
        private val pushedExpressions: List<String> =
                if (pushedExpressions == null) java.util.List.of() else java.util.List.copyOf(pushedExpressions)
        private val duckDbTimeZone: Optional<String> =
                if (duckDbTimeZone == null) Optional.empty() else duckDbTimeZone

        constructor(
                target: DuckDbAttachTarget,
                projectedColumns: List<DucklakeColumnHandle>,
                pushedPredicate: TupleDomain<DucklakeColumnHandle>)
                : this(target, projectedColumns, pushedPredicate, java.util.List.of(), Optional.empty())

        constructor(
                target: DuckDbAttachTarget,
                projectedColumns: List<DucklakeColumnHandle>,
                pushedPredicate: TupleDomain<DucklakeColumnHandle>,
                pushedExpressions: List<String>)
                : this(target, projectedColumns, pushedPredicate, pushedExpressions, Optional.empty())

        fun target(): DuckDbAttachTarget = target
        fun projectedColumns(): List<DucklakeColumnHandle> = projectedColumns
        fun pushedPredicate(): TupleDomain<DucklakeColumnHandle> = pushedPredicate
        fun pushedExpressions(): List<String> = pushedExpressions
        fun duckDbTimeZone(): Optional<String> = duckDbTimeZone

        fun isEmptyProjection(): Boolean {
            return projectedColumns.isEmpty()
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ExecutionRequest) return false
            return target == other.target
                    && projectedColumns == other.projectedColumns
                    && pushedPredicate == other.pushedPredicate
                    && pushedExpressions == other.pushedExpressions
                    && duckDbTimeZone == other.duckDbTimeZone
        }

        override fun hashCode(): Int {
            return java.util.Objects.hash(target, projectedColumns, pushedPredicate, pushedExpressions, duckDbTimeZone)
        }

        override fun toString(): String {
            return "ExecutionRequest[target=" + target + ", projectedColumns=" + projectedColumns +
                    ", pushedPredicate=" + pushedPredicate + ", pushedExpressions=" + pushedExpressions +
                    ", duckDbTimeZone=" + duckDbTimeZone + "]"
        }
    }

    /**
     * Per-execution lifecycle handle. The page source iterates the
     * {@link ArrowReader}; on close, the executor releases all resources
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
