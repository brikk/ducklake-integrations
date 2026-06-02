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
 * Describes a range predicate on a column for data file pruning.
 * Values are string representations — the catalog performs typed comparison
 * (numeric, date, etc.) internally based on the column's type.
 *
 * @param columnId the column to filter on
 * @param minValue the lower bound (inclusive), or null for unbounded
 * @param maxValue the upper bound (inclusive), or null for unbounded
 */
public record ColumnRangePredicate(long columnId, String minValue, String maxValue) {}
