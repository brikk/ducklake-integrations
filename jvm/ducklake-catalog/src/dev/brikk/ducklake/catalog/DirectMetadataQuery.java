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
 * Pass-through {@link MetadataQuery} for PostgreSQL and local-DuckDB backends.
 * The caller's jOOQ query executes as-is on the supplied DSLContext.
 */
final class DirectMetadataQuery
        implements MetadataQuery
{
    @Override
    public <R extends Record> R fetchOne(DSLContext dsl, ResultQuery<R> query)
    {
        return query.fetchOne();
    }

    @Override
    public <R extends Record> List<R> fetch(DSLContext dsl, ResultQuery<R> query)
    {
        return query.fetch();
    }
}
