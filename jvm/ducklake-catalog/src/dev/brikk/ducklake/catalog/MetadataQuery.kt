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

/**
 * Indirection over the read side of the DuckLake metadata SQL.
 *
 * For PostgreSQL and local-DuckDB backends this is a pass-through to jOOQ's
 * native fetch — [DirectMetadataQuery]. For the remote-DuckDB-over-Quack
 * backend it routes each query through DuckDB's `quack_query_by_name`
 * table function — [QuackWrappedMetadataQuery] — so that the inner SQL
 * executes server-side as a single `LogicalGet` from the local optimizer's
 * point of view, sidestepping `quack_optimizer.cpp`'s rejection of
 * "multiple streaming scans" across attached-catalog tables.
 *
 * The Quack wrapper exists solely because the Quack RPC server is still beta
 * and its planner rejects SQL shapes that PG and local DuckDB handle natively
 * (same-table multi-scan, two-table JOINs against a Quack-attached metadata
 * catalog). Routing through the wrapper preserves [JdbcDucklakeCatalog]'s
 * existing jOOQ DSL — no SQL-shape compromises in the catalog code.
 *
 * **Retire condition:** when [QuackWrappedMetadataQuery] has no
 * remaining callers (i.e. upstream Quack relaxes the relevant restrictions),
 * delete the implementation and collapse this interface back to direct
 * execution. `git grep MetadataQuery` surfaces the call sites.
 *
 * Visibility note: this type is package-private in Java. Kotlin has no
 * package-private; widening to `public` (rather than `internal`) preserves
 * JVM call-compatibility with the existing Java implementations and call
 * sites in the same package.
 */
interface MetadataQuery {
    /**
     * Fetch a single row from `query`, or `null` if no row matches.
     * The `dsl` is the same DSLContext the caller would have invoked
     * [ResultQuery.fetchOne] on directly — it carries the connection
     * (and therefore the active transaction) the query runs on.
     */
    fun <R : Record> fetchOne(dsl: DSLContext, query: ResultQuery<R>): R?

    /**
     * Fetch all rows from `query`. Empty list if no rows match.
     */
    fun <R : Record> fetch(dsl: DSLContext, query: ResultQuery<R>): List<R>

    /**
     * Fetch all rows from `query` via `mapper`, mainly for ad-hoc
     * projections — including multi-table JOINs, which the Quack RPC optimizer
     * also rejects under attached metadata catalogs. On the Quack backend the
     * inner SQL is rendered, wrapped in `quack_query_by_name`, and the
     * raw result is re-coerced against `query.getSelect()` so the
     * mapper's [Record] retains its original jOOQ `Field`
     * metadata — field-typed access (`r.get(table.COLUMN_NAME)`)
     * works the same as on the direct path.
     */
    fun <T> fetch(dsl: DSLContext, query: ResultQuery<*>, mapper: RecordMapper<in Record, T>): List<T>

    /**
     * Execute a mutation (UPDATE / DELETE / INSERT / DDL). Returns the
     * affected-row count when meaningful (UPDATE / DELETE); 0 for DDL or when
     * a wrapped backend can't surface a count.
     *
     * The Quack RPC binder rejects UPDATE / DELETE against tables exposed by
     * an attached metadata catalog (`Binder Error: Can only update base
     * table`). Routing through this method on the Quack backend wraps the
     * inner SQL in `quack_query_by_name` so the mutation executes
     * server-side against the real base table.
     */
    fun execute(dsl: DSLContext, mutation: Query): Int
}
