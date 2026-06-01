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
 * Pass-through [MetadataQuery] for PostgreSQL and local-DuckDB backends.
 * The caller's jOOQ query executes as-is on the supplied DSLContext.
 */
internal class DirectMetadataQuery : MetadataQuery
{
    override fun <R : Record> fetchOne(dsl: DSLContext, query: ResultQuery<R>): R?
    {
        return query.fetchOne()
    }

    override fun <R : Record> fetch(dsl: DSLContext, query: ResultQuery<R>): List<R>
    {
        return query.fetch()
    }

    override fun <T> fetch(dsl: DSLContext, query: ResultQuery<*>, mapper: RecordMapper<in Record, T>): List<T>
    {
        // ResultQuery<?> doesn't carry a usable R for the typed fetch(RecordMapper)
        // overload, but jOOQ's runtime mapper invocation only needs Record — so
        // we route through DSLContext.fetch(ResultQuery) (which returns
        // Result<Record>) and apply the mapper there.
        return dsl.fetch(query).map(mapper)
    }

    override fun execute(dsl: DSLContext, mutation: Query): Int
    {
        return mutation.execute()
    }
}
