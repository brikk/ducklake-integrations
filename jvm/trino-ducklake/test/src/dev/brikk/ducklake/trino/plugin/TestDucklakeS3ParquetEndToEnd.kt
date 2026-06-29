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

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.QueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Full-Trino end-to-end coverage of **parquet data files written to and read from S3** (MinIO),
 * the cell the capability grid never filled: every existing S3 test (`TestDucklakeLanceS3QuackRead`,
 * `TestDucklakeQuackS3InitRace`, `TestDucklakeDuckDbExecutorBackends`) exercises the *executor*
 * (DuckDB/lance/vortex over s3), never Trino's own `ParquetWriter`/`ParquetPageSource` against an
 * S3-resident DuckLake catalog. Parquet over S3 goes through Trino's native S3 filesystem
 * (`trino-filesystem-s3`, `fs.native-s3.enabled`) with no DuckDB involvement, so this is a pure
 * connector + filesystem path: CTAS, INSERT, SELECT, DELETE, UPDATE, MERGE, schema evolution, and
 * time travel, all with data files physically on `s3://`.
 *
 * Topology: a MinIO container reached from the in-JVM Trino over the mapped host port; the DuckLake
 * metadata lives in a PostgreSQL catalog ATTACHed with an `s3://` `DATA_PATH` (so the
 * catalog-stored data path — which wins over the connector property in [DucklakePathResolver] —
 * routes every new data file to the bucket). PG (not a single-file local DuckDB catalog) is used so
 * the concurrent-writer cell is real: a `.db` catalog is single-writer by construction. Parquet
 * over s3 needs no DuckDB executor at all; the one duckdb-format cell uses the parity extension.
 * Skips when Docker/MinIO is unavailable.
 *
 * SAME_THREAD by default; the concurrent-writer test manages its own thread pool.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeS3ParquetEndToEnd : AbstractTestQueryFramework() {

    private var minio: GenericContainer<*>? = null
    private var pgServer: dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer? = null
    private var databaseName: String? = null

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        val endpoint = startMinio()

        // PostgreSQL holds the DuckLake metadata (a real multi-writer catalog, so the
        // concurrent-INSERT cell is meaningful — a single-file local DuckDB catalog is
        // single-writer by construction). The catalog is ATTACHed with the s3 bucket as its
        // DATA_PATH, so the connector writes every new data file straight to MinIO.
        val pg = DucklakeTestCatalogEnvironment.getServer()
        val dbName = "ducklake_s3_parquet_e2e"
        pgServer = pg
        databaseName = dbName
        bootstrapCatalog(pg, dbName)

        val session = testSessionBuilder()
                .setCatalog("ducklake")
                .setSchema("test_schema")
                .build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            runner.installPlugin(DucklakePlugin())
            runner.createCatalog("ducklake", "ducklake", connectorProperties(pg, dbName, endpoint))
            return runner
        }
        catch (e: Throwable) {
            runner.close()
            throw e
        }
    }

    /** Starts MinIO + creates the bucket; returns the host-mapped s3 endpoint. Skips on no Docker. */
    private fun startMinio(): String {
        try {
            val container = GenericContainer("minio/minio:latest")
                    .withEnv("MINIO_ROOT_USER", USER)
                    .withEnv("MINIO_ROOT_PASSWORD", PASSWORD)
                    .withCommand("server", "/data")
                    .withExposedPorts(9000)
                    .waitingFor(Wait.forListeningPort())
            container.start()
            val mb = container.execInContainer("/bin/sh", "-c",
                    "mc alias set local http://localhost:9000 $USER $PASSWORD && mc mb local/$BUCKET")
            check(mb.exitCode == 0) { "bucket creation failed: ${mb.stdout}\n${mb.stderr}" }
            minio = container
            return "http://" + container.host + ":" + container.getMappedPort(9000)
        }
        catch (e: Exception) {
            assumeTrue(false, "MinIO fixture unavailable (docker): ${e.message}")
            throw AssertionError("unreachable")
        }
    }

    /** Pure-metadata bootstrap: a fresh PG database ATTACHed with the s3 DATA_PATH + one schema. */
    private fun bootstrapCatalog(
            pg: dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer, dbName: String) {
        pg.createDatabase(dbName)
        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("INSTALL postgres")
                stmt.execute("LOAD postgres")
                stmt.execute("INSTALL ducklake")
                stmt.execute("LOAD ducklake")
                // No data is written here, so the bootstrap DuckDB needs no s3 credentials.
                stmt.execute("ATTACH '" + pg.getDuckDbAttachUri(dbName) + "' AS lake "
                        + "(DATA_PATH 's3://$BUCKET/data/')")
                stmt.execute("CREATE SCHEMA lake.test_schema")
            }
        }
    }

    private fun connectorProperties(
            pg: dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer,
            dbName: String, endpoint: String): Map<String, String> {
        val props = mutableMapOf(
                "ducklake.catalog.database-url" to pg.getJdbcUrl(dbName),
                "ducklake.catalog.database-user" to pg.getUser(),
                "ducklake.catalog.database-password" to pg.getPassword(),
                "ducklake.data-path" to "s3://$BUCKET/data/",
                "fs.native-s3.enabled" to "true",
                "s3.endpoint" to endpoint,
                "s3.region" to "us-east-1",
                "s3.aws-access-key" to USER,
                "s3.aws-secret-key" to PASSWORD,
                "s3.path-style-access" to "true")
        // For the duckdb-format-over-s3 case the in-process executor LOADs the parity extension;
        // thread the host-side binary path through when the build provides it.
        System.getProperty("ducklake.test.parityExtensionPath")?.takeIf { it.isNotBlank() }?.let {
            props["ducklake.duckdb.parity-extension-path"] = it
        }
        return props
    }

    @AfterAll
    fun stopFixture() {
        minio?.stop()
    }

    /** Recursively lists object keys under the s3 bucket via the in-container `mc`. */
    private fun s3Objects(): List<String> {
        val res = minio!!.execInContainer("/bin/sh", "-c", "mc ls --recursive local/$BUCKET")
        check(res.exitCode == 0) { "mc ls failed: ${res.stdout}\n${res.stderr}" }
        return res.stdout.lines().filter { it.isNotBlank() }
                .map { it.trim().substringAfterLast(' ') }
    }

    /**
     * Proves the table's data files physically live on s3: the `$files` catalog rows are present
     * (DuckLake stores them as relative names under the table's data path) AND matching `.parquet`
     * objects exist in the bucket — there is no local data dir, so a successful read necessarily
     * came from s3.
     */
    private fun assertFilesOnS3(table: String) {
        val names = computeActual("SELECT path FROM \"$table\$files\"").materializedRows
                .map { (it.getField(0) as String).substringAfterLast('/') }
        assertThat(names).`as`("$table has catalog-tracked data files").isNotEmpty
        val objects = s3Objects()
        assertThat(names)
                .`as`("every $table data file is a physical object under s3://$BUCKET/")
                .allSatisfy { name -> assertThat(objects).anySatisfy { assertThat(it).endsWith(name) } }
    }

    @Test
    fun ctasThenReadFromS3() {
        val table = "test_schema.s3_ctas"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), "
                    + "(3, 'gamma')) AS t(id, name)")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")
            assertFilesOnS3("s3_ctas")
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun insertAppendsNewS3Files() {
        val table = "test_schema.s3_insert"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
            computeActual("INSERT INTO $table VALUES (3, 'c')")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            // Two INSERTs => at least two data files, all on s3.
            assertThat(computeScalar("SELECT count(*) FROM \"s3_insert\$files\"") as Long)
                    .isGreaterThanOrEqualTo(2L)
            assertFilesOnS3("s3_insert")
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun deleteRemovesRowsOverS3() {
        val table = "test_schema.s3_delete"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), "
                    + "(3, 'c'), (4, 'd')) AS t(id, name)")
            computeActual("DELETE FROM $table WHERE id IN (2, 4)")
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int })
                    .containsExactly(1, 3)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun updateMutatesRowsOverS3() {
        val table = "test_schema.s3_update"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), "
                    + "(3, 'c')) AS t(id, name)")
            computeActual("UPDATE $table SET name = 'updated' WHERE id = 2")
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("a", "updated", "c")
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun mergeOverS3() {
        val target = "test_schema.s3_merge_t"
        val source = "test_schema.s3_merge_s"
        try {
            computeActual("CREATE TABLE $target AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), "
                    + "(3, 'c')) AS t(id, name)")
            computeActual("CREATE TABLE $source AS SELECT * FROM (VALUES (2, 'B2'), (4, 'D4')) "
                    + "AS t(id, name)")
            computeActual("MERGE INTO $target tgt USING $source src ON tgt.id = src.id "
                    + "WHEN MATCHED THEN UPDATE SET name = src.name "
                    + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.name)")
            assertThat(computeActual("SELECT id, name FROM $target ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String })
                    .containsExactly(1 to "a", 2 to "B2", 3 to "c", 4 to "D4")
        }
        finally {
            tryDrop(target)
            tryDrop(source)
        }
    }

    @Test
    fun schemaEvolutionOverS3() {
        val table = "test_schema.s3_evolve"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            // ADD COLUMN: old s3 files null-fill; new INSERT populates it.
            computeActual("ALTER TABLE $table ADD COLUMN extra INTEGER")
            computeActual("INSERT INTO $table VALUES (3, 'c', 30)")
            assertThat(computeActual("SELECT id, extra FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) })
                    .containsExactly(1 to null, 2 to null, 3 to 30)
            // RENAME COLUMN: read path must alias against files written under the old name.
            computeActual("ALTER TABLE $table RENAME COLUMN name TO label")
            assertThat(computeActual("SELECT label FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("a", "b", "c")
            // DROP COLUMN.
            computeActual("ALTER TABLE $table DROP COLUMN extra")
            assertThat(computeActual("SELECT * FROM $table ORDER BY id").materializedRows.map { it.fieldCount })
                    .allSatisfy { assertThat(it).isEqualTo(2) }
            assertFilesOnS3("s3_evolve")
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun timeTravelOverS3() {
        val table = "test_schema.s3_timetravel"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            val v1 = computeScalar("SELECT max(snapshot_id) FROM \"s3_timetravel\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (2, 'b')")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
            // The pre-INSERT snapshot still sees only its row, read from the original s3 file.
            assertThat(computeScalar("SELECT count(*) FROM $table FOR VERSION AS OF $v1")).isEqualTo(1L)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun partitionedWriteOverS3() {
        val table = "test_schema.s3_partitioned"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) "
                    + "WITH (partitioned_by = ARRAY['region'])")
            computeActual("INSERT INTO $table VALUES (1, 'us'), (2, 'eu'), (3, 'us'), (4, 'apac')")
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'us'")).isEqualTo(2L)
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'eu'").materializedRows
                    .map { it.getField(0) as Int })
                    .containsExactly(2)
            // Partition dirs nest under the s3 table path (key=value/) as real bucket objects.
            assertThat(s3Objects())
                    .`as`("partition directories materialize as s3 objects")
                    .anySatisfy { assertThat(it).contains("region=us") }
                    .anySatisfy { assertThat(it).contains("region=eu") }
        }
        finally {
            tryDrop(table)
        }
    }

    /**
     * The duckdb data-file format over s3: the connector writes a `.db` file to a local temp dir
     * and uploads it to the bucket through the native S3 filesystem; the read path downloads it
     * back (auto → materialize for a small file) and runs through the in-process DuckDB executor
     * with the parity extension. Proves the non-parquet write+read round-trips over s3.
     */
    @Test
    fun duckdbFormatCtasAndReadOverS3() {
        val table = "test_schema.s3_duckdb"
        try {
            computeActual("CREATE TABLE $table WITH (data_file_format = 'duckdb') AS "
                    + "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"s3_duckdb\$files\"") as String)
                    .isEqualTo("duckdb")
            assertThat(s3Objects())
                    .`as`("the duckdb .db data file is a physical object on s3")
                    .anySatisfy { assertThat(it).endsWith(".db") }
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            // Predicate read exercises pushdown into DuckDB over the s3-materialized file.
            assertThat(computeActual("SELECT name FROM $table WHERE id = 2").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("b")
        }
        finally {
            tryDrop(table)
        }
    }

    /**
     * Concurrent writers committing to the same table while data lands on s3 — the s3-transport
     * variant of the catalog conflict matrix (which is otherwise in-JVM with local data). Each of
     * [WRITERS] barrier-aligned threads runs its own `INSERT ... VALUES` (a separate snapshot
     * commit); the connector's snapshot-lineage retry must serialize them so every row survives
     * and no commit is lost. Confirms the write + commit path is correct under contention with
     * real s3 object writes.
     */
    @Test
    fun concurrentInsertsOverS3() {
        val table = "test_schema.s3_concurrent"
        try {
            computeActual("CREATE TABLE $table (writer INTEGER, seq INTEGER)")
            val runner = queryRunner
            val barrier = CyclicBarrier(WRITERS)
            val failures = ConcurrentLinkedQueue<Throwable>()
            val pool = Executors.newFixedThreadPool(WRITERS)
            try {
                val tasks = (0 until WRITERS).map { w ->
                    pool.submit {
                        try {
                            barrier.await(2, TimeUnit.MINUTES)
                            repeat(ROWS_PER_WRITER) { seq ->
                                runner.execute(session, "INSERT INTO $table VALUES ($w, $seq)")
                            }
                        }
                        catch (t: Throwable) {
                            failures.add(t)
                        }
                    }
                }
                tasks.forEach { it.get(5, TimeUnit.MINUTES) }
            }
            finally {
                pool.shutdownNow()
            }
            assertThat(failures).`as`("concurrent s3 INSERTs must all commit via lineage retry").isEmpty()
            assertThat(computeScalar("SELECT count(*) FROM $table"))
                    .isEqualTo((WRITERS * ROWS_PER_WRITER).toLong())
            // Every (writer, seq) pair landed exactly once — no lost or duplicated commit.
            assertThat(computeScalar("SELECT count(DISTINCT (writer, seq)) FROM $table"))
                    .isEqualTo((WRITERS * ROWS_PER_WRITER).toLong())
            assertFilesOnS3("s3_concurrent")
        }
        finally {
            tryDrop(table)
        }
    }

    private fun tryDrop(table: String) {
        try {
            computeActual("DROP TABLE $table")
        }
        catch (ignored: Exception) {
        }
    }

    companion object {
        private const val USER = "s3-parquet-user"
        private const val PASSWORD = "s3-parquet-secret"
        private const val BUCKET = "lake"
        private const val WRITERS = 4
        private const val ROWS_PER_WRITER = 3
    }
}
