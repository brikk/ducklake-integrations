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

import org.junit.jupiter.api.Assumptions.assumeTrue
import java.sql.DriverManager

/**
 * Network/platform gate shared by the suites that exercise community-extension data file
 * formats: `INSTALL`/`LOAD` of the extension in a throwaway in-memory DuckDB. If the
 * extension can't be installed (offline / unsupported platform — e.g. lance 404s on
 * osx_amd64), the calling test SKIPS rather than fails.
 */
object FormatExtensionAssumptions {
    fun assumeDuckDbExtensionAvailable(extension: String) {
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL $extension")
                    s.execute("LOAD $extension")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "$extension DuckDB extension unavailable (offline / unsupported platform): ${e.message}")
        }
    }
}
