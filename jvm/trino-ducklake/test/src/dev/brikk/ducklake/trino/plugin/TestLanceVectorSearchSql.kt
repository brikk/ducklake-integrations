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
 * Pins the DuckDB argument tail [LanceVectorSearchSplitProcessor.renderExtraArgsSql] appends
 * after the quoted dataset path inside `lance_vector_search(...)` — element rendering, named
 * params, and column-name quote escaping. The end-to-end behavior is covered by
 * [TestDucklakeLanceVectorSearch]; this keeps the SQL shape unit-testable without DuckDB.
 */
class TestLanceVectorSearchSql {
    @Test
    fun rendersColumnVectorKAndPrefilter() {
        val handle = LanceVectorSearchFunctionHandle(
                listOf("/data/t.lance"), "emb", listOf(1.0, 0.5, -2.25), 5L, false, listOf())
        assertThat(LanceVectorSearchSplitProcessor.renderExtraArgsSql(handle))
                .isEqualTo(", 'emb', [1.0, 0.5, -2.25]::DOUBLE[], k := 5, prefilter := false")
    }

    @Test
    fun escapesSingleQuotesInColumnName() {
        val handle = LanceVectorSearchFunctionHandle(
                listOf("/data/t.lance"), "e'mb", listOf(1.0), 1L, true, listOf())
        assertThat(LanceVectorSearchSplitProcessor.renderExtraArgsSql(handle))
                .isEqualTo(", 'e''mb', [1.0]::DOUBLE[], k := 1, prefilter := true")
    }
}
