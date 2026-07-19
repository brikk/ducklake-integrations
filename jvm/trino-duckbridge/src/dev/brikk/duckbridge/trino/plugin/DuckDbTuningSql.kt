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

import java.sql.SQLException
import java.sql.Statement

/**
 * Renders a [DuckDbTuning] as the SET statements DuckDB expects, plus the settings we always apply
 * (`http_keep_alive`, `http_retries`). Executors call [applyDirect] (in-process) or [statements]
 * (Quack, which ships each via the `quack_query_by_name` wrapper). Ported from the DuckLake connector.
 */
object DuckDbTuningSql {
    fun statements(tuning: DuckDbTuning): List<String> {
        val out = mutableListOf<String>()
        // Mandatory: connection reuse + sane retry posture for httpfs paths.
        out.add("SET http_keep_alive = true")
        out.add("SET http_retries = 5")
        out.add("SET enable_object_cache = ${tuning.enableObjectCache}")
        tuning.memoryLimit?.let { v -> out.add("SET memory_limit = '$v'") }
        tuning.threads?.let { v -> out.add("SET threads = $v") }
        tuning.tempDirectory?.let { v -> out.add("SET temp_directory = '${v.replace("'", "''")}'") }
        tuning.tempDirectoryMaxSize?.let { v -> out.add("SET max_temp_directory_size = '$v'") }
        return out
    }

    @Throws(SQLException::class)
    fun applyDirect(stmt: Statement, tuning: DuckDbTuning) {
        for (sql in statements(tuning)) {
            stmt.execute(sql)
        }
    }
}
