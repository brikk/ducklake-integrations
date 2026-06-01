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

import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * One parquet leaf column chunk, paired with the DuckLake catalog field_id
 * the chunk maps to. Consumed by {@link DucklakeStatsExtractor} to emit
 * one {@code ducklake_file_column_stats} row per leaf.
 *
 * <p>{@code parquetColumnIndex} is the leaf's position in the file's
 * {@code RowGroup.columns} list — depth-first leaf order in the parquet
 * schema. For Trino-written files it advances 0, 1, 2 in step with the
 * leaf walk over Trino types; for {@code add_files}-registered files it
 * tracks the file's own leaf order, including leaves that have no
 * corresponding catalog column (skipped via {@code ignore_extra_columns}).
 */
public record LeafStatsTarget(long fieldId, Type leafType, int parquetColumnIndex)
{
    public LeafStatsTarget
    {
        requireNonNull(leafType, "leafType is null");
    }
}
