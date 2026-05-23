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
package dev.brikk.ducklake.catalog;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;

import java.util.List;

/**
 * Indirection over the read side of the DuckLake metadata SQL.
 *
 * <p>For PostgreSQL and local-DuckDB backends this is a pass-through to jOOQ's
 * native fetch — {@link DirectMetadataQuery}. For the remote-DuckDB-over-Quack
 * backend it routes each query through DuckDB's {@code quack_query_by_name}
 * table function — {@link QuackWrappedMetadataQuery} — so that the inner SQL
 * executes server-side as a single {@code LogicalGet} from the local optimizer's
 * point of view, sidestepping {@code quack_optimizer.cpp}'s rejection of
 * "multiple streaming scans" across attached-catalog tables.
 *
 * <p>The Quack wrapper exists solely because the Quack RPC server is still beta
 * and its planner rejects SQL shapes that PG and local DuckDB handle natively
 * (same-table multi-scan, two-table JOINs against a Quack-attached metadata
 * catalog). Routing through the wrapper preserves {@link JdbcDucklakeCatalog}'s
 * existing jOOQ DSL — no SQL-shape compromises in the catalog code.
 *
 * <p><b>Retire condition:</b> when {@link QuackWrappedMetadataQuery} has no
 * remaining callers (i.e. upstream Quack relaxes the relevant restrictions),
 * delete the implementation and collapse this interface back to direct
 * execution. {@code git grep MetadataQuery} surfaces the call sites.
 */
interface MetadataQuery
{
    /**
     * Fetch a single row from {@code query}, or {@code null} if no row matches.
     * The {@code dsl} is the same DSLContext the caller would have invoked
     * {@link ResultQuery#fetchOne()} on directly — it carries the connection
     * (and therefore the active transaction) the query runs on.
     */
    <R extends Record> R fetchOne(DSLContext dsl, ResultQuery<R> query);

    /**
     * Fetch all rows from {@code query}. Empty list if no rows match.
     */
    <R extends Record> List<R> fetch(DSLContext dsl, ResultQuery<R> query);
}
