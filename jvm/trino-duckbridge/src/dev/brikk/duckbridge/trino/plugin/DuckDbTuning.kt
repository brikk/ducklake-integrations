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

import io.airlift.units.DataSize

/**
 * DuckDB engine tuning applied at connection init by the T2 [DuckBridgeExecutor]s. For the
 * in-process engine each setting targets the per-split embedded instance; for Quack the SETs travel
 * via `quack_query_by_name` to the server-side DuckDB (instance-wide there). Ported from the DuckLake
 * connector's `DuckDbTuning`.
 */
@JvmRecord
data class DuckDbTuning(
    val memoryLimit: DataSize?,
    val threads: Int?,
    val tempDirectory: String?,
    val tempDirectoryMaxSize: DataSize?,
    val enableObjectCache: Boolean,
) {
    companion object {
        fun defaults(): DuckDbTuning = DuckDbTuning(null, null, null, null, true)
    }
}
