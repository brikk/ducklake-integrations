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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * Cross-engine round trip for the connector's puffin deletion-vector WRITE path (the inverse of
 * `TestDucklakeCrossEnginePuffinDeleteRoundTrip`): with `write_deletion_vectors = true`, Trino's
 * merge sink writes DELETE/UPDATE/MERGE tombstones as a DuckLake `.puffin` deletion-vector file
 * (`DucklakePuffinDeleteWriter`), and we assert both Trino AND DuckDB read back the surviving rows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeTrinoPuffinDeleteWrite : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "xengine-trino-puffin-write"

    private fun puffinSession(): Session = Session.builder(session)
            .setCatalogSessionProperty("ducklake", DucklakeSessionProperties.WRITE_DELETION_VECTORS, "true")
            .build()

    @Test
    @Throws(Exception::class)
    fun trinoWritesPuffinDeletesReadableByBothEngines() {
        val table = "test_schema.trino_puffin_write"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES "
                    + "(1, 'keep'), (2, 'drop'), (3, 'keep'), (4, 'drop'), (5, 'keep')) AS t(id, label)")

            // DELETE under write_deletion_vectors → the merge sink emits a .puffin delete file.
            computeActual(puffinSession(), "DELETE FROM $table WHERE label = 'drop'")

            // The delete file the connector wrote must be format='puffin' with a .puffin path.
            assertThat(puffinDeleteFileCount()).`as`("Trino wrote a puffin deletion-vector delete file").isEqualTo(1L)

            // Trino reads its own puffin deletes back.
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int })
                    .`as`("Trino read of Trino-written puffin deletes").containsExactly(1, 3, 5)

            // DuckDB reads the same puffin deletes from the shared PostgreSQL catalog.
            assertThat(duckdbIds("ducklake_db.test_schema.trino_puffin_write"))
                    .`as`("DuckDB read of Trino-written puffin deletes").containsExactly(1, 3, 5)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Throws(Exception::class)
    private fun duckdbIds(qualified: String): List<Int> {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id FROM $qualified ORDER BY id").use { rs ->
                    val ids = mutableListOf<Int>()
                    while (rs.next()) {
                        ids.add(rs.getInt(1))
                    }
                    return ids
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun puffinDeleteFileCount(): Long {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
            val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
            return CatalogTestSupport.dsl(conn).selectCount()
                    .from(delfile)
                    .where(delfile.FORMAT.equalIgnoreCase("puffin"))
                    .and(delfile.PATH.like("%.puffin"))
                    .and(delfile.END_SNAPSHOT.isNull)
                    .fetchOne(0, Long::class.java) ?: 0L
        }
    }
}
