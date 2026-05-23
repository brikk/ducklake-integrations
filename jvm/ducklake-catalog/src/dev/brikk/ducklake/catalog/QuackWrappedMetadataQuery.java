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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * {@link MetadataQuery} for the Quack catalog backend. Renders the caller's
 * jOOQ query to bind-inlined SQL and routes execution through
 * {@code CALL system.main.quack_query_by_name('<metadata_catalog>', '<inner_sql>')}.
 *
 * <p>From the local DuckDB optimizer's point of view the wrapped statement is a
 * single {@code TableFunction} call — exactly one {@code LogicalGet} — so the
 * "Multiple streaming scans or streaming scans + CTAS / insert in the same
 * query are not currently supported" check in
 * {@code duckdb-quack/src/storage/quack_optimizer.cpp} doesn't fire regardless
 * of how many references to the same {@code ducklake_*} table appear in the
 * inner SQL. The inner SQL executes server-side as plain DuckDB SQL against
 * base tables, so JOINs / same-table subqueries / UPDATE / DELETE that the
 * attached-catalog path rejects all work.
 *
 * <p>Mirrors the wrapper shape used by upstream's C++
 * {@code QuackMetadataManager::Query}
 * (in {@code vendor/ducklake/src/metadata_manager/quack_metadata_manager.cpp}).
 *
 * <p><b>Result-type fidelity:</b> {@code quack_query_by_name} is a polymorphic
 * table function whose output columns are resolved at JDBC prepare time via a
 * server PREPARE round trip — empirically confirmed: wrapping
 * {@code SELECT snapshot_id, schema_version FROM ducklake_snapshot WHERE ...}
 * produces a prepared statement that advertises
 * {@code (snapshot_id BIGINT, schema_version BIGINT)}. jOOQ's reflective
 * {@code Record.into(Class)} mapping picks the columns up by name into the
 * caller's generated record type without further coercion hints.
 */
final class QuackWrappedMetadataQuery
        implements MetadataQuery
{
    private final String metadataCatalogName;

    QuackWrappedMetadataQuery(String metadataCatalogName)
    {
        this.metadataCatalogName = requireNonNull(metadataCatalogName, "metadataCatalogName is null");
    }

    @Override
    public <R extends Record> R fetchOne(DSLContext dsl, ResultQuery<R> query)
    {
        String wrapped = wrap(dsl.renderInlined(query));
        Record row = dsl.fetchOne(wrapped);
        if (row == null) {
            return null;
        }
        return row.into(query.getRecordType());
    }

    @Override
    public <R extends Record> List<R> fetch(DSLContext dsl, ResultQuery<R> query)
    {
        String wrapped = wrap(dsl.renderInlined(query));
        Class<? extends R> recordType = query.getRecordType();
        return dsl.fetch(wrapped)
                .stream()
                .map(row -> row.into(recordType))
                .collect(Collectors.toList());
    }

    private String wrap(String innerSql)
    {
        return "CALL system.main.quack_query_by_name("
                + sqlLiteral(metadataCatalogName)
                + ", "
                + sqlLiteral(innerSql)
                + ")";
    }

    private static String sqlLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
