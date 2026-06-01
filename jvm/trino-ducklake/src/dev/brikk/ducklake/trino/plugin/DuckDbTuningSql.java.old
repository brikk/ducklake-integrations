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
package dev.brikk.ducklake.trino.plugin;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Renders a {@link DuckDbTuning} as the SET statements DuckDB expects, plus the
 * mandatory settings we always apply ({@code http_keep_alive}, {@code http_retries}).
 * Executors call {@link #applyDirect} (in-process) or {@link #statements} (for
 * Quack, which ships each via the {@code quack_query_by_name} wrapper).
 */
final class DuckDbTuningSql
{
    private DuckDbTuningSql() {}

    static List<String> statements(DuckDbTuning tuning)
    {
        List<String> out = new ArrayList<>();
        // Mandatory: connection reuse + sane retry posture for httpfs paths.
        out.add("SET http_keep_alive = true");
        out.add("SET http_retries = 5");
        out.add("SET enable_object_cache = " + tuning.enableObjectCache());
        tuning.memoryLimit().ifPresent(v -> out.add("SET memory_limit = '" + v + "'"));
        tuning.threads().ifPresent(v -> out.add("SET threads = " + v));
        tuning.tempDirectory().ifPresent(v -> out.add(
                "SET temp_directory = '" + v.replace("'", "''") + "'"));
        tuning.tempDirectoryMaxSize().ifPresent(v -> out.add("SET max_temp_directory_size = '" + v + "'"));
        return out;
    }

    static void applyDirect(Statement stmt, DuckDbTuning tuning) throws SQLException
    {
        for (String sql : statements(tuning)) {
            stmt.execute(sql);
        }
    }
}
