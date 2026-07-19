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
 * Persistence adapter for this connector's data-file format in the shared catalog
 * (`ducklake_data_file.file_format` and the table-scoped `data_file_format` setting).
 *
 * Writes are parquet-only now (the experimental duckdb/vortex/lance formats were removed —
 * see PLAN-duckdb-parity-moveout.md §4.2 / brikk/duckbridge), so [toStored] no longer namespaces
 * anything: it persists the bare format as-is.
 *
 * [fromStored] STAYS and still strips the legacy `trino/<format>` namespace prefix that older
 * catalogs may carry for those removed formats, so the fail-loud guard (§7) sees the bare format
 * name (`vortex`, `lance`, `duckdb`) and can name it in the error instead of `trino/vortex`.
 */
internal object CatalogFileFormat {
    private const val NAMESPACE_PREFIX = "trino/"

    /** Bare internal format → the value to persist. Writes are parquet-only; persist as-is. */
    fun toStored(format: String): String = format

    /** Persisted value → bare internal format. Strips the legacy namespace; bare names pass through. */
    fun fromStored(stored: String?): String? = stored?.removePrefix(NAMESPACE_PREFIX)
}
