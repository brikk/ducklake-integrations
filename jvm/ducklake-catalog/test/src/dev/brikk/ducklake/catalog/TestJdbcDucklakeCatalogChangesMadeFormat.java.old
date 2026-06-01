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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the {@code ducklake_snapshot_changes.changes_made} serializer:
 * {@link WriteChange#formatChangesMade(List)} (joiner) plus the per-variant
 * {@code toChangesMadeEntry} renderers.
 *
 * <p>Upstream DuckDB parses this column via {@code ParseChangesList} in
 * {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}.
 * {@code created_table} / {@code created_view} values are read by
 * {@code DuckLakeUtil::ParseCatalogEntry} which requires the fully qualified
 * {@code "schema"."name"} form; {@code created_schema} is a single quoted value
 * read by {@code DuckLakeUtil::ParseQuotedValue}. Both forms wrap the value in
 * {@code "..."} and escape embedded {@code "} by doubling ({@code ""}).
 */
public class TestJdbcDucklakeCatalogChangesMadeFormat
{
    // schemaId is carried on CreatedTable / CreatedView for matrix cross-checks
    // but is NOT serialized to the changes_made text — these format tests don't
    // care which value we pass.
    private static final long SCHEMA_ID = 1L;

    // ==================== formatChangesMade: plain joiner ====================

    @Test
    public void testEmptyListProducesEmptyString()
    {
        assertThat(WriteChange.formatChangesMade(List.of())).isEmpty();
    }

    @Test
    public void testSingleEntryIsPassedThrough()
    {
        assertThat(WriteChange.formatChangesMade(List.of(new WriteChange.DroppedTable(7))))
                .isEqualTo("dropped_table:7");
    }

    @Test
    public void testMultipleEntriesJoinedWithComma()
    {
        assertThat(WriteChange.formatChangesMade(List.of(
                new WriteChange.CreatedSchema("sales"),
                new WriteChange.CreatedTable(SCHEMA_ID, "sales", "orders"),
                new WriteChange.InsertedIntoTable(7, Set.of()))))
                .isEqualTo("created_schema:\"sales\",created_table:\"sales\".\"orders\",inserted_into_table:7");
    }

    @Test
    public void testNumericIdEntriesStayUnquoted()
    {
        // Spec: `dropped_*`, `altered_*`, `inserted_into_table`, `deleted_from_table` use raw
        // integers (parsed by StringUtil::ToUnsigned upstream), never quoted.
        assertThat(WriteChange.formatChangesMade(List.of(
                new WriteChange.DroppedSchema(3, "sales"),
                new WriteChange.DroppedTable(12),
                new WriteChange.AlteredTable(42),
                new WriteChange.InsertedIntoTable(100, Set.of()),
                new WriteChange.DeletedFromTable(100, Set.of()))))
                .isEqualTo("dropped_schema:3,dropped_table:12,altered_table:42,inserted_into_table:100,deleted_from_table:100");
    }

    // ==================== CreatedTable: "schema"."name" ====================

    @Test
    public void testCreatedTableProducesFullyQualifiedQuotedForm()
    {
        assertThat(new WriteChange.CreatedTable(SCHEMA_ID, "sales", "orders").toChangesMadeEntry())
                .isEqualTo("created_table:\"sales\".\"orders\"");
    }

    @Test
    public void testCreatedTableQuotesCommaInName()
    {
        // Without quoting, ParseChangesList would see three entries instead of one.
        assertThat(new WriteChange.CreatedTable(SCHEMA_ID, "sales", "bad,name").toChangesMadeEntry())
                .isEqualTo("created_table:\"sales\".\"bad,name\"");
    }

    @Test
    public void testCreatedTableEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(new WriteChange.CreatedTable(SCHEMA_ID, "sales", "weird\"name").toChangesMadeEntry())
                .isEqualTo("created_table:\"sales\".\"weird\"\"name\"");
    }

    @Test
    public void testCreatedTableQuotesBothSchemaAndTable()
    {
        assertThat(new WriteChange.CreatedTable(SCHEMA_ID, "odd.schema", "odd.table").toChangesMadeEntry())
                .isEqualTo("created_table:\"odd.schema\".\"odd.table\"");
    }

    // ==================== CreatedView: "schema"."name" ====================

    @Test
    public void testCreatedViewProducesFullyQualifiedQuotedForm()
    {
        assertThat(new WriteChange.CreatedView(SCHEMA_ID, "analytics", "daily_summary").toChangesMadeEntry())
                .isEqualTo("created_view:\"analytics\".\"daily_summary\"");
    }

    @Test
    public void testCreatedViewEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(new WriteChange.CreatedView(SCHEMA_ID, "a\"b", "c\"d").toChangesMadeEntry())
                .isEqualTo("created_view:\"a\"\"b\".\"c\"\"d\"");
    }

    // ==================== CreatedSchema: single quoted value ====================

    @Test
    public void testCreatedSchemaWrapsNameInQuotes()
    {
        assertThat(new WriteChange.CreatedSchema("sales").toChangesMadeEntry())
                .isEqualTo("created_schema:\"sales\"");
    }

    @Test
    public void testCreatedSchemaEscapesEmbeddedQuoteByDoubling()
    {
        assertThat(new WriteChange.CreatedSchema("weird\"name").toChangesMadeEntry())
                .isEqualTo("created_schema:\"weird\"\"name\"");
    }

    @Test
    public void testCreatedSchemaQuotesCommaInName()
    {
        assertThat(new WriteChange.CreatedSchema("bad,name").toChangesMadeEntry())
                .isEqualTo("created_schema:\"bad,name\"");
    }
}
