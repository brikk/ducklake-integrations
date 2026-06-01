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

import static java.util.Objects.requireNonNull;

/**
 * One entry from {@code ducklake_sort_expression}, joined with the active
 * {@code ducklake_sort_info} row for its table at a snapshot. {@code expression}
 * is the raw text from the catalog — DuckLake stores SQL in the writer's
 * dialect. Consumers (e.g. the Trino connector's planner-hint path) decide
 * whether they can interpret it; arbitrary expressions are downgraded to "no
 * sort property" silently rather than misreporting an order.
 */
public record DucklakeSortKey(
        int sortKeyIndex,
        String expression,
        String dialect,
        DucklakeSortDirection direction,
        DucklakeNullOrder nullOrder)
{
    public DucklakeSortKey
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(dialect, "dialect is null");
        requireNonNull(direction, "direction is null");
        requireNonNull(nullOrder, "nullOrder is null");
        if (sortKeyIndex < 0) {
            throw new IllegalArgumentException("sortKeyIndex must be non-negative: " + sortKeyIndex);
        }
    }
}
