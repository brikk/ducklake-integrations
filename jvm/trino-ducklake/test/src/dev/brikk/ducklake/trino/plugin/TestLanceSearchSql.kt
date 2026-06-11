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
package dev.brikk.ducklake.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins the DuckDB argument tail [LanceSearchSplitProcessor.renderExtraArgsSql] appends after
 * the quoted dataset path inside the `lance_*` call, per handle type — element rendering, named
 * params, quote escaping, and the scan-function dispatch. End-to-end behavior is covered by the
 * `TestDucklakeLance*` integration tests; this keeps the SQL shape unit-testable without DuckDB.
 */
class TestLanceSearchSql {
    @Test
    fun vectorSearchRendersColumnVectorKAndPrefilter() {
        val handle = LanceVectorSearchFunctionHandle(
                listOf("/data/t.lance"), "emb", listOf(1.0, 0.5, -2.25), 5L, false, listOf())
        assertThat(LanceSearchSplitProcessor.scanFunctionFor(handle)).isEqualTo("lance_vector_search")
        assertThat(LanceSearchSplitProcessor.renderExtraArgsSql(handle))
                .isEqualTo(", 'emb', [1.0, 0.5, -2.25]::DOUBLE[], k := 5, prefilter := false")
    }

    @Test
    fun vectorSearchEscapesSingleQuotesInColumnName() {
        val handle = LanceVectorSearchFunctionHandle(
                listOf("/data/t.lance"), "e'mb", listOf(1.0), 1L, true, listOf())
        assertThat(LanceSearchSplitProcessor.renderExtraArgsSql(handle))
                .isEqualTo(", 'e''mb', [1.0]::DOUBLE[], k := 1, prefilter := true")
    }

    @Test
    fun ftsRendersColumnQueryKAndPrefilter() {
        val handle = LanceFtsFunctionHandle(
                listOf("/data/t.lance"), "body", "quick 'n dirty fox", 3L, true, listOf())
        assertThat(LanceSearchSplitProcessor.scanFunctionFor(handle)).isEqualTo("lance_fts")
        assertThat(LanceSearchSplitProcessor.renderExtraArgsSql(handle))
                .isEqualTo(", 'body', 'quick ''n dirty fox', k := 3, prefilter := true")
    }

    @Test
    fun hybridRendersBothHalvesWithOptionalAlpha() {
        val withAlpha = LanceHybridSearchFunctionHandle(
                listOf("/data/t.lance"), "emb", listOf(1.0, 0.0), "body", "fox", 4L, 0.7, false, listOf())
        assertThat(LanceSearchSplitProcessor.scanFunctionFor(withAlpha)).isEqualTo("lance_hybrid_search")
        assertThat(LanceSearchSplitProcessor.renderExtraArgsSql(withAlpha))
                .isEqualTo(", 'emb', [1.0, 0.0]::DOUBLE[], 'body', 'fox', k := 4, alpha := 0.7, prefilter := false")

        val withoutAlpha = LanceHybridSearchFunctionHandle(
                listOf("/data/t.lance"), "emb", listOf(1.0, 0.0), "body", "fox", 4L, null, false, listOf())
        assertThat(LanceSearchSplitProcessor.renderExtraArgsSql(withoutAlpha))
                .`as`("omitted ALPHA must not render — the extension's default blend applies")
                .isEqualTo(", 'emb', [1.0, 0.0]::DOUBLE[], 'body', 'fox', k := 4, prefilter := false")
    }
}
