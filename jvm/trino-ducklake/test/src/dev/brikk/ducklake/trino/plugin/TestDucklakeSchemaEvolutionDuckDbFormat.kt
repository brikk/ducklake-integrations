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

import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/** Schema evolution over `duckdb` (.db) data files — see [AbstractDucklakeSchemaEvolutionFormatTest]. */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeSchemaEvolutionDuckDbFormat : AbstractDucklakeSchemaEvolutionFormatTest() {
    override fun isolatedCatalogName(): String = "schema-evo-duckdb"

    override fun format(): String = DucklakeSessionProperties.FORMAT_DUCKDB
}
