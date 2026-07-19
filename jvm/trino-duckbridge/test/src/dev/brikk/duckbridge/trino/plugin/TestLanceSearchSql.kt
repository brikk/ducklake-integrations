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
package dev.brikk.duckbridge.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [LanceSearchSql] rendering (no DuckDB needed). Ported from the DuckLake
 * `renderExtraArgsSql` coverage; pins the exact call shapes the canary asserts against
 * `duckdb_functions()`.
 */
class TestLanceSearchSql {
    @Test
    fun lanceScanRendersPathOnly() {
        assertThat(LanceSearchSql.lanceScan("/data/x.lance"))
            .isEqualTo("SELECT * FROM __lance_scan('/data/x.lance')")
    }

    @Test
    fun vortexScanRendersReadVortex() {
        assertThat(LanceSearchSql.vortexScan("/data/x.vortex"))
            .isEqualTo("SELECT * FROM read_vortex('/data/x.vortex')")
    }

    @Test
    fun vectorSearchRendersNamedArgs() {
        assertThat(LanceSearchSql.lanceVectorSearch("/d", "emb", listOf(1.0, 0.0, 0.5), 5, true))
            .isEqualTo("SELECT * FROM lance_vector_search('/d', 'emb', [1.0, 0.0, 0.5]::DOUBLE[], k := 5, prefilter := true)")
    }

    @Test
    fun ftsRendersQuery() {
        assertThat(LanceSearchSql.lanceFts("/d", "txt", "hello world", 10, false))
            .isEqualTo("SELECT * FROM lance_fts('/d', 'txt', 'hello world', k := 10, prefilter := false)")
    }

    @Test
    fun hybridSearchOmitsAlphaWhenNull() {
        val sql = LanceSearchSql.lanceHybridSearch("/d", "emb", listOf(0.1, 0.2), "txt", "cat", 7, null, false)
        assertThat(sql).doesNotContain("alpha")
        assertThat(sql).contains("k := 7", "prefilter := false")
    }

    @Test
    fun hybridSearchIncludesAlphaWhenSet() {
        val sql = LanceSearchSql.lanceHybridSearch("/d", "emb", listOf(0.1), "txt", "cat", 7, 0.3, true)
        assertThat(sql).contains("alpha := 0.3")
    }

    @Test
    fun singleQuotesEscapedInArgs() {
        assertThat(LanceSearchSql.lanceScan("/data/o'brien.lance"))
            .isEqualTo("SELECT * FROM __lance_scan('/data/o''brien.lance')")
        assertThat(LanceSearchSql.lanceFts("/d", "c", "it's a test", 1, false))
            .contains("'it''s a test'")
    }
}
