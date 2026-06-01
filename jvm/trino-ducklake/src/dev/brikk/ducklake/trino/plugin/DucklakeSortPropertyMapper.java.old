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
package dev.brikk.ducklake.trino.plugin;

import com.google.common.collect.ImmutableList;
import dev.brikk.ducklake.catalog.DucklakeNullOrder;
import dev.brikk.ducklake.catalog.DucklakeSortDirection;
import dev.brikk.ducklake.catalog.DucklakeSortKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Translates DuckLake's catalog-stored sort spec into Trino
 * {@link LocalProperty} entries for the planner.
 *
 * <p>Trino interprets a {@code List<LocalProperty<ColumnHandle>>} as a
 * sequence: the table is sorted by the first property, then within ties by
 * the second, and so on. A <em>prefix</em> of the declared sequence is a
 * weaker-but-still-true claim, which the planner can rely on for queries
 * that {@code ORDER BY} only the leading columns. The inverse is not true:
 * skipping an entry in the middle is a lie. So when we cannot interpret a
 * sort key (unknown dialect, non-column expression, column missing at this
 * snapshot), we stop and return only the safe prefix.
 *
 * <p>Currently only the {@code duckdb} dialect is honored. DuckLake writers
 * store expression text verbatim in their dialect; without a dialect-aware
 * parser we treat foreign-dialect expressions as opaque and emit no sort
 * property at all rather than misinterpret quoting/identifier rules.
 */
public final class DucklakeSortPropertyMapper
{
    private static final String DUCKDB_DIALECT = "duckdb";

    private DucklakeSortPropertyMapper() {}

    /**
     * @param sortKeys           ordered as stored in {@code ducklake_sort_expression}
     *                           ({@code ORDER BY sort_key_index})
     * @param columnHandlesByLowercaseName top-level columns at the active snapshot,
     *                                     keyed by lowercased column name. Used to
     *                                     resolve simple column-reference expressions.
     */
    public static List<LocalProperty<ColumnHandle>> toLocalProperties(
            List<DucklakeSortKey> sortKeys,
            Map<String, ? extends ColumnHandle> columnHandlesByLowercaseName)
    {
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<LocalProperty<ColumnHandle>> properties = ImmutableList.builder();
        for (DucklakeSortKey key : sortKeys) {
            if (!DUCKDB_DIALECT.equalsIgnoreCase(key.dialect())) {
                // Unknown dialect — bail to safe prefix (possibly empty). Better to lose
                // a planner optimization than to misreport order.
                break;
            }
            String columnName = parseColumnReference(key.expression());
            if (columnName == null) {
                break;
            }
            ColumnHandle handle = columnHandlesByLowercaseName.get(columnName.toLowerCase(Locale.ROOT));
            if (handle == null) {
                // Expression names a column we don't see at this snapshot (likely renamed
                // / dropped after the sort spec was written). Stop here.
                break;
            }
            properties.add(new SortingProperty<>(handle, toSortOrder(key.direction(), key.nullOrder())));
        }
        return properties.build();
    }

    /**
     * Visible for testing. Returns the unquoted column name when {@code expression} is
     * a simple identifier (optionally double-quoted, per DuckDB dialect), otherwise
     * {@code null}.
     */
    static String parseColumnReference(String expression)
    {
        if (expression == null) {
            return null;
        }
        String trimmed = expression.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (trimmed.charAt(0) == '"') {
            // Quoted identifier. Closing quote must be the last char; "" inside escapes a
            // literal double quote. DuckDB / SQL standard.
            if (trimmed.length() < 2 || trimmed.charAt(trimmed.length() - 1) != '"') {
                return null;
            }
            StringBuilder out = new StringBuilder(trimmed.length() - 2);
            int i = 1;
            int limit = trimmed.length() - 1;
            while (i < limit) {
                char c = trimmed.charAt(i);
                if (c == '"') {
                    if (i + 1 < limit && trimmed.charAt(i + 1) == '"') {
                        out.append('"');
                        i += 2;
                        continue;
                    }
                    // unescaped closing quote before the end — malformed
                    return null;
                }
                out.append(c);
                i++;
            }
            return out.length() == 0 ? null : out.toString();
        }
        // Unquoted: standard SQL identifier — letter or underscore followed by
        // letters / digits / underscores. Anything else (parens, dots, operators) means
        // we cannot safely interpret as a single column reference.
        char first = trimmed.charAt(0);
        if (!isIdentifierStart(first)) {
            return null;
        }
        for (int i = 1; i < trimmed.length(); i++) {
            if (!isIdentifierPart(trimmed.charAt(i))) {
                return null;
            }
        }
        return trimmed;
    }

    private static boolean isIdentifierStart(char c)
    {
        return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    private static boolean isIdentifierPart(char c)
    {
        return isIdentifierStart(c) || (c >= '0' && c <= '9');
    }

    static SortOrder toSortOrder(DucklakeSortDirection direction, DucklakeNullOrder nullOrder)
    {
        return switch (direction) {
            case ASC -> switch (nullOrder) {
                case NULLS_FIRST -> SortOrder.ASC_NULLS_FIRST;
                case NULLS_LAST -> SortOrder.ASC_NULLS_LAST;
            };
            case DESC -> switch (nullOrder) {
                case NULLS_FIRST -> SortOrder.DESC_NULLS_FIRST;
                case NULLS_LAST -> SortOrder.DESC_NULLS_LAST;
            };
        };
    }
}
