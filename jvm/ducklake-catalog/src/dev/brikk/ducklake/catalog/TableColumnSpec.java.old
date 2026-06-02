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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Specification for a column to be created in a DuckLake table.
 * For nested types (list, struct, map), the parent has children;
 * leaf columns have an empty children list.
 */
public record TableColumnSpec(
        String name,
        String ducklakeType,
        boolean nullable,
        List<TableColumnSpec> children)
{
    public TableColumnSpec
    {
        requireNonNull(name, "name is null");
        requireNonNull(ducklakeType, "ducklakeType is null");
        requireNonNull(children, "children is null");
        children = List.copyOf(children);
    }

    public static TableColumnSpec leaf(String name, String ducklakeType, boolean nullable)
    {
        return new TableColumnSpec(name, ducklakeType, nullable, List.of());
    }
}
