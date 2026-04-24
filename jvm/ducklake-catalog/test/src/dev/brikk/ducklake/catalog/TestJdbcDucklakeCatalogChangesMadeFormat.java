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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the {@code ducklake_snapshot_changes.changes_made} serializer:
 * {@link JdbcDucklakeCatalog#formatChangesMade(List)} (joiner) plus the quoting builders
 * {@link JdbcDucklakeCatalog#changeCreatedTable(String, String)},
 * {@link JdbcDucklakeCatalog#changeCreatedView(String, String)}, and
 * {@link JdbcDucklakeCatalog#changeCreatedSchema(String)}.
 * <p>
 * Upstream DuckDB parses this column via {@code ParseChangesList} in
 * {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}.
 * {@code created_table} / {@code created_view} values are read by
 * {@code DuckLakeUtil::ParseCatalogEntry} which requires the fully qualified
 * {@code "schema"."name"} form; {@code created_schema} is a single quoted value read by
 * {@code DuckLakeUtil::ParseQuotedValue}. Both forms wrap the value in {@code "..."} and escape
 * embedded {@code "} by doubling ({@code ""}).
 */
public class TestJdbcDucklakeCatalogChangesMadeFormat
{
    // ==================== formatChangesMade: plain joiner ====================

    @Test
    public void testEmptyListProducesEmptyString()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of())).isEmpty();
    }

    @Test
    public void testSingleEntryIsPassedThrough()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of("dropped_table:7")))
                .isEqualTo("dropped_table:7");
    }

    @Test
    public void testMultipleEntriesJoinedWithComma()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of(
                JdbcDucklakeCatalog.changeCreatedSchema("sales"),
                JdbcDucklakeCatalog.changeCreatedTable("sales", "orders"),
                "inserted_into_table:7")))
                .isEqualTo("created_schema:\"sales\",created_table:\"sales\".\"orders\",inserted_into_table:7");
    }

    @Test
    public void testNumericIdEntriesStayUnquoted()
    {
        // Spec: `dropped_*`, `altered_*`, `inserted_into_table`, `deleted_from_table` use raw
        // integers (parsed by StringUtil::ToUnsigned upstream), never quoted.
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of(
                "dropped_schema:3",
                "dropped_table:12",
                "altered_table:42",
                "inserted_into_table:100",
                "deleted_from_table:100")))
                .isEqualTo("dropped_schema:3,dropped_table:12,altered_table:42,inserted_into_table:100,deleted_from_table:100");
    }

    // ==================== changeCreatedTable: "schema"."name" ====================

    @Test
    public void testChangeCreatedTableProducesFullyQualifiedQuotedForm()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedTable("sales", "orders"))
                .isEqualTo("created_table:\"sales\".\"orders\"");
    }

    @Test
    public void testChangeCreatedTableQuotesCommaInName()
    {
        // Without quoting, ParseChangesList would see three entries instead of one.
        assertThat(JdbcDucklakeCatalog.changeCreatedTable("sales", "bad,name"))
                .isEqualTo("created_table:\"sales\".\"bad,name\"");
    }

    @Test
    public void testChangeCreatedTableEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedTable("sales", "weird\"name"))
                .isEqualTo("created_table:\"sales\".\"weird\"\"name\"");
    }

    @Test
    public void testChangeCreatedTableQuotesBothSchemaAndTable()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedTable("odd.schema", "odd.table"))
                .isEqualTo("created_table:\"odd.schema\".\"odd.table\"");
    }

    // ==================== changeCreatedView: "schema"."name" ====================

    @Test
    public void testChangeCreatedViewProducesFullyQualifiedQuotedForm()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedView("analytics", "daily_summary"))
                .isEqualTo("created_view:\"analytics\".\"daily_summary\"");
    }

    @Test
    public void testChangeCreatedViewEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedView("a\"b", "c\"d"))
                .isEqualTo("created_view:\"a\"\"b\".\"c\"\"d\"");
    }

    // ==================== changeCreatedSchema: single quoted value ====================

    @Test
    public void testChangeCreatedSchemaWrapsNameInQuotes()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedSchema("sales"))
                .isEqualTo("created_schema:\"sales\"");
    }

    @Test
    public void testChangeCreatedSchemaEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedSchema("weird\"name"))
                .isEqualTo("created_schema:\"weird\"\"name\"");
    }

    @Test
    public void testChangeCreatedSchemaQuotesCommaInName()
    {
        assertThat(JdbcDucklakeCatalog.changeCreatedSchema("bad,name"))
                .isEqualTo("created_schema:\"bad,name\"");
    }
}
