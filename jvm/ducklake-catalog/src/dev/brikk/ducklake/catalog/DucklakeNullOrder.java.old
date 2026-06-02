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
 * Null-ordering encoded in {@code ducklake_sort_expression.null_order}.
 * Upstream writes the literal strings {@code "NULLS_FIRST"} / {@code "NULLS_LAST"}.
 */
public enum DucklakeNullOrder
{
    NULLS_FIRST,
    NULLS_LAST;

    public static DucklakeNullOrder fromCatalog(String value)
    {
        if (value == null) {
            throw new IllegalArgumentException("null_order is null");
        }
        return switch (value.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "NULLS_FIRST" -> NULLS_FIRST;
            case "NULLS_LAST" -> NULLS_LAST;
            default -> throw new IllegalArgumentException("Unknown null_order: " + value);
        };
    }
}
