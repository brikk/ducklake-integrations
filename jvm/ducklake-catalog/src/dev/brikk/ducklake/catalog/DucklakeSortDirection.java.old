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

/**
 * Sort direction encoded in {@code ducklake_sort_expression.sort_direction}.
 * Upstream writes the literal strings {@code "ASC"} / {@code "DESC"}.
 */
public enum DucklakeSortDirection
{
    ASC,
    DESC;

    public static DucklakeSortDirection fromCatalog(String value)
    {
        if (value == null) {
            throw new IllegalArgumentException("sort_direction is null");
        }
        return switch (value.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "ASC" -> ASC;
            case "DESC" -> DESC;
            default -> throw new IllegalArgumentException("Unknown sort_direction: " + value);
        };
    }
}
