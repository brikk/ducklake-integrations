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
 * Unit coverage of [CatalogFileFormat] after the P6 moveout: writes are parquet-only, so [toStored]
 * is now identity (no namespacing); [fromStored] STAYS and still strips the legacy `trino/<fmt>`
 * namespace so the §7 fail-loud guard sees the bare removed-format name in stale catalogs.
 */
class TestCatalogFileFormat {
    @Test
    fun toStoredIsIdentity() {
        assertThat(CatalogFileFormat.toStored("parquet")).isEqualTo("parquet")
        assertThat(CatalogFileFormat.toStored("PARQUET")).isEqualTo("PARQUET")
    }

    @Test
    fun readStripsLegacyNamespaceAndToleratesBareNames() {
        assertThat(CatalogFileFormat.fromStored("parquet")).isEqualTo("parquet")
        // Legacy namespaced removed-format names (written before P6) decode to the bare name so the
        // guard can name the format in its error.
        assertThat(CatalogFileFormat.fromStored("trino/vortex")).isEqualTo("vortex")
        assertThat(CatalogFileFormat.fromStored("trino/lance")).isEqualTo("lance")
        assertThat(CatalogFileFormat.fromStored("trino/duckdb")).isEqualTo("duckdb")
        // Legacy bare names pass through unchanged.
        assertThat(CatalogFileFormat.fromStored("vortex")).isEqualTo("vortex")
        assertThat(CatalogFileFormat.fromStored("duckdb")).isEqualTo("duckdb")
        assertThat(CatalogFileFormat.fromStored(null)).isNull()
    }
}
