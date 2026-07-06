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
package dev.brikk.ducklake.catalog

/**
 * Namespaces this connector's NON-parquet data-file formats when they are persisted to the shared
 * catalog (`ducklake_data_file.file_format` and the table-scoped `data_file_format` setting), so a
 * future upstream DuckLake that introduces a file format of the same bare name (`vortex`, `lance`,
 * `duckdb`, ...) cannot collide with ours in a catalog both engines share. The stored form is
 * `trino/<format>` — the same slash-namespacing convention the connector uses for its view dialect
 * (`trino/brikk`, see `DucklakeMetadata`). `parquet` is left bare: it is the DuckLake spec's own
 * format, read by every engine, and must never be namespaced.
 *
 * All persistence flows through [toStored] (write) / [fromStored] (read), so the connector always
 * works in bare internal names (`vortex`) while the catalog carries the namespaced value. Reads
 * accept BOTH the namespaced form and the legacy bare form written before this change, so existing
 * catalogs keep working with no migration.
 */
internal object CatalogFileFormat {
    private const val NAMESPACE_PREFIX = "trino/"
    private const val PARQUET = "parquet"

    /** Bare internal format → the value to persist. `parquet` stays bare; others become `trino/<fmt>`. */
    fun toStored(format: String): String =
        if (format.equals(PARQUET, ignoreCase = true) || format.startsWith(NAMESPACE_PREFIX)) {
            format
        }
        else {
            NAMESPACE_PREFIX + format
        }

    /** Persisted value → bare internal format. Strips our namespace; legacy bare names pass through. */
    fun fromStored(stored: String?): String? = stored?.removePrefix(NAMESPACE_PREFIX)
}
