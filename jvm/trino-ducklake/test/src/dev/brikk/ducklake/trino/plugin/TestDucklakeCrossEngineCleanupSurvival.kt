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
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path

/**
 * Protection test (TODO-uhoh: "cross-engine-cleanup-survival"): stock DuckDB's orphan sweep
 * (`ducklake_delete_orphaned_files`) must NOT delete this connector's non-parquet data files —
 * the `duckdb` (`.db`), `vortex` (`.vortex`), and `lance` (dataset *directory*) formats that stock
 * DuckLake doesn't produce. Upstream's sweep currently only lists/deletes `*.parquet` orphans; this
 * test PINS that assumption so an upstream filter change (e.g. when they add a second official
 * format) can't silently start deleting our data.
 *
 * The test plants ORPHAN files of each format (untracked by the catalog) under the DuckLake data
 * path, then runs stock DuckDB's catalog-wide orphan sweep and asserts our non-parquet orphans
 * survive. A parquet orphan is planted too as a live control: DuckDB is expected to remove it,
 * confirming the sweep actually ran (so non-parquet survival is protection, not a no-op).
 *
 * SAME_THREAD: shared catalog + on-disk data path.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineCleanupSurvival : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "xengine-cleanup-survival"

    private fun plant(path: Path): Path {
        Files.createDirectories(path.parent)
        Files.write(path, byteArrayOf(1, 2, 3, 4))
        return path
    }

    @Test
    @Throws(Exception::class)
    fun duckdbOrphanSweepSparesNonParquetFiles() {
        val table = "test_schema.cleanup_survivor"
        try {
            // A real parquet table so the catalog + data path exist and DuckDB can attach.
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")

            // Plant orphan files (untracked by the catalog) of each of our formats. Put them beside
            // real data — inside the table's data directory — so the sweep definitely scans them.
            val dataDir = getIsolatedCatalog().dataDir
            val tableDir: Path = Files.walk(dataDir).use { w ->
                w.filter { it.toString().endsWith(".parquet") }.findFirst()
                    .orElseThrow { AssertionError("no parquet data file under $dataDir") }
                    .parent
            }
            val parquetOrphan = plant(tableDir.resolve("orphan-${java.util.UUID.randomUUID()}.parquet"))
            val duckdbOrphan = plant(tableDir.resolve("orphan-${java.util.UUID.randomUUID()}.db"))
            val vortexOrphan = plant(tableDir.resolve("orphan-${java.util.UUID.randomUUID()}.vortex"))
            val lanceOrphanDir = tableDir.resolve("orphan-${java.util.UUID.randomUUID()}.lance")
            val lanceMember = plant(lanceOrphanDir.resolve("data").resolve("part-0.lance"))
            plant(lanceOrphanDir.resolve("_versions").resolve("1.manifest"))

            // Stock DuckDB's catalog-wide orphan sweep, no age filter.
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_delete_orphaned_files('ducklake_db', cleanup_all => true)")
                }
            }

            // The protection: our non-parquet orphans must survive DuckDB's *.parquet-only sweep.
            assertThat(Files.exists(duckdbOrphan)).`as`("duckdb (.db) orphan survives stock sweep").isTrue()
            assertThat(Files.exists(vortexOrphan)).`as`("vortex (.vortex) orphan survives stock sweep").isTrue()
            assertThat(Files.exists(lanceMember)).`as`("lance dataset-directory member survives stock sweep").isTrue()

            // Control: DuckDB is expected to have removed the parquet orphan, proving the sweep ran
            // and WOULD have deleted the others were it not for the extension filter.
            assertThat(Files.exists(parquetOrphan)).`as`("stock sweep removed the parquet orphan (sweep active)").isFalse()

            // And the real table still reads through both engines.
            assertThat(computeActual("SELECT id FROM $table").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1)
        }
        finally {
            tryDropTable(table)
        }
    }
}
