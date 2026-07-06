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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit coverage of [CatalogFileFormat]: our non-parquet formats persist namespaced (`trino/<fmt>`)
 * so a future upstream format of the same bare name can't collide; `parquet` stays bare; and reads
 * accept BOTH the namespaced form and the legacy bare form (no migration for existing catalogs).
 */
class TestCatalogFileFormat {
    @Test
    fun parquetIsNeverNamespaced() {
        assertThat(CatalogFileFormat.toStored("parquet")).isEqualTo("parquet")
        assertThat(CatalogFileFormat.toStored("PARQUET")).isEqualTo("PARQUET")
        assertThat(CatalogFileFormat.fromStored("parquet")).isEqualTo("parquet")
    }

    @Test
    fun nonParquetFormatsAreNamespacedOnWrite() {
        assertThat(CatalogFileFormat.toStored("duckdb")).isEqualTo("trino/duckdb")
        assertThat(CatalogFileFormat.toStored("vortex")).isEqualTo("trino/vortex")
        assertThat(CatalogFileFormat.toStored("lance")).isEqualTo("trino/lance")
    }

    @Test
    fun namespaceIsNotAppliedTwice() {
        assertThat(CatalogFileFormat.toStored("trino/vortex")).isEqualTo("trino/vortex")
    }

    @Test
    fun readStripsNamespaceAndToleratesLegacyBareNames() {
        // Namespaced (written after this change).
        assertThat(CatalogFileFormat.fromStored("trino/vortex")).isEqualTo("vortex")
        assertThat(CatalogFileFormat.fromStored("trino/lance")).isEqualTo("lance")
        // Legacy bare names (written before this change) — backward compatible, no migration.
        assertThat(CatalogFileFormat.fromStored("vortex")).isEqualTo("vortex")
        assertThat(CatalogFileFormat.fromStored("duckdb")).isEqualTo("duckdb")
        assertThat(CatalogFileFormat.fromStored(null)).isNull()
    }

    @Test
    fun roundTrips() {
        for (format in listOf("parquet", "duckdb", "vortex", "lance")) {
            assertThat(CatalogFileFormat.fromStored(CatalogFileFormat.toStored(format))).isEqualTo(format)
        }
    }
}
