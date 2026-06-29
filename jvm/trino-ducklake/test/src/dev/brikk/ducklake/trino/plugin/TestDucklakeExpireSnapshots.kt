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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.stream.Collectors

/**
 * End-to-end coverage of `expire_snapshots` + `cleanup_old_files` — the F6 catalog-driven reclaim
 * pair (see dev-docs/DESIGN-maintenance.md). expire_snapshots deletes old (catalog-wide) snapshots
 * and SCHEDULES the now-dead data/delete files; cleanup_old_files physically deletes scheduled
 * files past the grace period. Two-phase deletion: expiry never unlinks a file directly.
 *
 * Self-contained: a PostgreSQL metadata catalog ATTACHed with a LOCAL data path the test owns, so
 * it can count physical files on disk. `ducklake.maintenance.min-retention` is set to `0s` so the
 * test can clean up freshly-scheduled files (the floor is exercised separately by the config test);
 * expiry of fresh snapshots uses the explicit `snapshot_ids` path, which is not floored.
 *
 * SAME_THREAD: writes to one shared catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeExpireSnapshots : AbstractTestQueryFramework() {

    private var pgServer: dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer? = null
    private lateinit var dataDir: Path

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        val pg = DucklakeTestCatalogEnvironment.getServer()
        pgServer = pg
        val dbName = "ducklake_expire_snapshots_e2e"
        dataDir = Files.createTempDirectory("ducklake-expire-")
        pg.createDatabase(dbName)
        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("INSTALL postgres")
                stmt.execute("LOAD postgres")
                stmt.execute("INSTALL ducklake")
                stmt.execute("LOAD ducklake")
                stmt.execute("ATTACH '" + pg.getDuckDbAttachUri(dbName) + "' AS lake "
                        + "(DATA_PATH '" + dataDir.toAbsolutePath() + "/')")
                stmt.execute("CREATE SCHEMA lake.test_schema")
            }
        }
        val session = testSessionBuilder().setCatalog("ducklake").setSchema("test_schema").build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            runner.installPlugin(DucklakePlugin())
            runner.createCatalog("ducklake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to pg.getJdbcUrl(dbName),
                    "ducklake.catalog.database-user" to pg.getUser(),
                    "ducklake.catalog.database-password" to pg.getPassword(),
                    "ducklake.data-path" to (dataDir.toAbsolutePath().toString() + "/"),
                    "ducklake.maintenance.min-retention" to "0s",
                    "fs.hadoop.enabled" to "true"))
            return runner
        }
        catch (e: Throwable) {
            runner.close()
            throw e
        }
    }

    @AfterAll
    fun cleanup() {
        if (::dataDir.isInitialized && Files.exists(dataDir)) {
            Files.walk(dataDir).use { w ->
                w.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }

    /**
     * Physical parquet count under a SPECIFIC table's data directory — scoped per table because
     * cleanup_old_files is catalog-wide and the suite shares one catalog/data dir, so a global
     * count would be contaminated by sibling tests. The table dir is the parent of any live file
     * (from $files); it also contains the table's end-snapshotted (dead-but-not-cleaned) files.
     */
    private fun tableParquetCount(table: String): Int {
        val unqualified = table.substringAfter('.')
        val liveName: String = (computeActual("SELECT path FROM \"$unqualified\$files\" LIMIT 1")
                .materializedRows.firstOrNull()?.getField(0) as String?)
                ?.substringAfterLast('/')
                ?: return 0
        val tableDir: Path = Files.walk(dataDir).use { w ->
            w.filter { it.fileName?.toString() == liveName }.findFirst().orElse(null)?.parent
        } ?: return 0
        Files.list(tableDir).use { s ->
            return s.filter { it.toString().endsWith(".parquet") }.collect(Collectors.toList()).size
        }
    }

    private fun snapshotIds(table: String): List<Long> {
        val unqualified = table.substringAfter('.')
        return computeActual("SELECT snapshot_id FROM \"$unqualified\$snapshots\" ORDER BY snapshot_id")
                .materializedRows.map { (it.getField(0) as Number).toLong() }
    }

    private fun sqlArray(ids: Collection<Long>): String = "ARRAY[" + ids.joinToString(",") + "]"

    /** The full reclaim cycle: TRUNCATE end-snapshots files, expire-all-but-latest kills them, cleanup unlinks. */
    @Test
    fun expireThenCleanupReclaimsDeadFiles() {
        val table = "test_schema.expire_reclaim"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            computeActual("INSERT INTO $table VALUES (3, 'c')")
            computeActual("TRUNCATE TABLE $table")            // end-snapshots every data file
            computeActual("INSERT INTO $table VALUES (4, 'd')") // the only file live at the latest snapshot
            val filesBefore = tableParquetCount(table)
            assertThat(filesBefore).`as`("end-snapshotted + live files coexist on disk").isGreaterThanOrEqualTo(3)

            val all = snapshotIds(table)
            val latest = all.max()
            val toExpire = all.filter { it != latest }

            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(toExpire)}, dry_run => false)")
            // Files are scheduled but not yet unlinked.
            assertThat(tableParquetCount(table)).`as`("expiry only schedules; nothing physically deleted yet")
                    .isEqualTo(filesBefore)

            computeActual("CALL system.cleanup_old_files(retention_threshold => '0s', dry_run => false)")
            assertThat(tableParquetCount(table)).`as`("cleanup unlinks the dead files").isLessThan(filesBefore)

            // The latest snapshot is intact and reads correctly — no live file was removed.
            assertThat(computeActual("SELECT id, name FROM $table").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String })
                    .containsExactly(4 to "d")
        }
        finally {
            tryDrop(table)
        }
    }

    /** Expiry must never touch a file a SURVIVING snapshot still needs. */
    @Test
    fun survivingSnapshotStaysReadableAndItsFilesRemain() {
        val table = "test_schema.expire_survivor"
        try {
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")
            computeActual("INSERT INTO $table VALUES (2)")
            val keep = computeScalar("SELECT max(snapshot_id) FROM \"expire_survivor\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (3)")  // advances the latest past `keep`
            val filesBefore = tableParquetCount(table)

            // Expire only the snapshots strictly before `keep`; `keep` and the latest survive. All
            // data files are still live (end_snapshot NULL), so NOTHING should be scheduled.
            val older = snapshotIds(table).filter { it < keep }
            assertThat(older).isNotEmpty
            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(older)}, dry_run => false)")
            computeActual("CALL system.cleanup_old_files(retention_threshold => '0s', dry_run => false)")

            assertThat(tableParquetCount(table)).`as`("no live file removed").isEqualTo(filesBefore)
            // Time travel to the surviving snapshot still reads its data.
            assertThat(computeActual("SELECT id FROM $table FOR VERSION AS OF $keep ORDER BY id")
                    .materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun dryRunExpiresNothing() {
        val table = "test_schema.expire_dryrun"
        try {
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")
            computeActual("INSERT INTO $table VALUES (2)")
            val before = computeScalar("SELECT count(*) FROM \"expire_dryrun\$snapshots\"") as Long
            val all = snapshotIds(table)
            val toExpire = all.filter { it != all.max() }

            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(toExpire)}, dry_run => true)")

            assertThat(computeScalar("SELECT count(*) FROM \"expire_dryrun\$snapshots\"") as Long)
                    .`as`("dry_run leaves all snapshots in place").isEqualTo(before)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun neverExpiresLatestSnapshot() {
        val table = "test_schema.expire_latest"
        try {
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")
            computeActual("INSERT INTO $table VALUES (2)")
            val before = computeScalar("SELECT count(*) FROM \"expire_latest\$snapshots\"") as Long
            val latest = computeScalar("SELECT max(snapshot_id) FROM \"expire_latest\$snapshots\"") as Long

            // Explicitly asking to expire the latest is silently excluded — it's never expirable.
            computeActual("CALL system.expire_snapshots(snapshot_ids => ARRAY[$latest], dry_run => false)")

            assertThat(computeScalar("SELECT count(*) FROM \"expire_latest\$snapshots\"") as Long)
                    .`as`("latest snapshot is protected").isEqualTo(before)
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun cleanupIsNoOpWhenNothingScheduled() {
        val table = "test_schema.cleanup_noop"
        try {
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")
            val before = tableParquetCount(table)
            computeActual("CALL system.cleanup_old_files(retention_threshold => '0s', dry_run => false)")
            assertThat(tableParquetCount(table)).`as`("no scheduled files → nothing deleted").isEqualTo(before)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(1L)
        }
        finally {
            tryDrop(table)
        }
    }

    /**
     * cleanup_old_files must resolve ROOT-relative scheduled paths (the form DuckLake writes — the
     * schedule table has no table_id) against the catalog data_path root, not a per-table dir. We
     * simulate a DuckDB-scheduled row by inserting a `path_is_relative = true` entry directly, with
     * a matching physical file under the data root, and assert Trino deletes it and drains the row.
     */
    @Test
    fun cleanupResolvesRootRelativeScheduledPaths() {
        val relative = "sched_rel_dir/ghost-${java.util.UUID.randomUUID()}.parquet"
        val physical = dataDir.resolve(relative)
        Files.createDirectories(physical.parent)
        Files.write(physical, byteArrayOf(9, 9, 9))
        val fakeId = 987654321L

        DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
            conn.prepareStatement("INSERT INTO ducklake_files_scheduled_for_deletion "
                    + "(data_file_id, path, path_is_relative, schedule_start) "
                    + "VALUES (?, ?, true, (NOW() - INTERVAL '1 day'))").use { ps ->
                ps.setLong(1, fakeId)
                ps.setString(2, relative)
                ps.executeUpdate()
            }
        }
        assertThat(Files.exists(physical)).isTrue()

        computeActual("CALL system.cleanup_old_files(retention_threshold => '0s', dry_run => false)")

        assertThat(Files.exists(physical)).`as`("root-relative scheduled file resolved + deleted").isFalse()
        // The schedule row was drained.
        DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
            conn.prepareStatement("SELECT count(*) FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = ?").use { ps ->
                ps.setLong(1, fakeId)
                ps.executeQuery().use { rs ->
                    rs.next()
                    assertThat(rs.getLong(1)).`as`("schedule row removed after deletion").isEqualTo(0L)
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun maxSnapshot(): Long =
            DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                    pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot").use { rs ->
                        rs.next(); rs.getLong(1)
                    }
                }
            }

    @Throws(Exception::class)
    private fun allSnapshotIds(): List<Long> =
            DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                    pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id").use { rs ->
                        val ids = mutableListOf<Long>()
                        while (rs.next()) {
                            ids.add(rs.getLong(1))
                        }
                        ids
                    }
                }
            }

    @Throws(Exception::class)
    private fun tableMetadataRowCount(tableName: String): Long =
            DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                    pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
                conn.prepareStatement("SELECT count(*) FROM ducklake_table WHERE table_name = ?").use { ps ->
                    ps.setString(1, tableName)
                    ps.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
                }
            }

    /**
     * Expiring all snapshots in a dropped table's lifetime GCs its dangling metadata rows (the
     * table_id-keyed cleanup), while a live table's metadata is untouched.
     */
    @Test
    @Throws(Exception::class)
    fun deadDroppedTableMetadataIsGarbageCollected() {
        computeActual("CREATE TABLE test_schema.gc_target AS SELECT 1 AS id")
        computeActual("DROP TABLE test_schema.gc_target")
        // Advance the latest past the drop so gc_target's whole lifetime is expirable.
        computeActual("CREATE TABLE test_schema.gc_keeper AS SELECT 1 AS id")
        try {
            assertThat(tableMetadataRowCount("gc_target")).`as`("dropped table row present before expiry").isEqualTo(1L)

            // Expire every snapshot except the latest — gc_target's [begin,end) then has no survivor.
            val latest = maxSnapshot()
            val toExpire = allSnapshotIds().filter { it != latest }
            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(toExpire)}, dry_run => false)")

            assertThat(tableMetadataRowCount("gc_target")).`as`("dead dropped-table metadata GC'd").isEqualTo(0L)
            assertThat(tableMetadataRowCount("gc_keeper")).`as`("live table metadata untouched").isGreaterThanOrEqualTo(1L)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.gc_keeper")).isEqualTo(1L)
        }
        finally {
            tryDrop("test_schema.gc_keeper")
        }
    }

    private fun tryDrop(table: String) {
        try {
            computeActual("DROP TABLE $table")
        }
        catch (ignored: Exception) {
        }
    }

    @Throws(Exception::class)
    private fun rowCount(sql: String, vararg args: String): Long =
            DriverManager.getConnection(pgServer!!.getJdbcUrl("ducklake_expire_snapshots_e2e"),
                    pgServer!!.getUser(), pgServer!!.getPassword()).use { conn ->
                conn.prepareStatement(sql).use { ps ->
                    args.forEachIndexed { i, a -> ps.setString(i + 1, a) }
                    ps.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
                }
            }

    /**
     * Expiring a dropped VIEW's whole lifetime GCs its dangling `ducklake_view` rows, while a live
     * view is untouched. Companion to deadDroppedTableMetadataIsGarbageCollected for the
     * schema/view/macro sweep added with the compaction work.
     */
    @Test
    @Throws(Exception::class)
    fun deadDroppedViewMetadataIsGarbageCollected() {
        computeActual("CREATE TABLE test_schema.view_src AS SELECT 1 AS id")
        computeActual("CREATE VIEW test_schema.gc_view AS SELECT id FROM test_schema.view_src")
        computeActual("DROP VIEW test_schema.gc_view")
        computeActual("CREATE VIEW test_schema.keep_view AS SELECT id FROM test_schema.view_src")
        try {
            assertThat(rowCount("SELECT count(*) FROM ducklake_view WHERE view_name = ?", "gc_view"))
                    .`as`("dropped view row present before expiry").isEqualTo(1L)

            val latest = maxSnapshot()
            val toExpire = allSnapshotIds().filter { it != latest }
            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(toExpire)}, dry_run => false)")

            assertThat(rowCount("SELECT count(*) FROM ducklake_view WHERE view_name = ?", "gc_view"))
                    .`as`("dead dropped-view metadata GC'd").isEqualTo(0L)
            assertThat(rowCount("SELECT count(*) FROM ducklake_view WHERE view_name = ?", "keep_view"))
                    .`as`("live view metadata untouched").isGreaterThanOrEqualTo(1L)
            assertThat(computeActual("SELECT id FROM test_schema.keep_view").materializedRows
                    .map { it.getField(0) as Int }).containsExactly(1)
        }
        finally {
            try { computeActual("DROP VIEW test_schema.keep_view") } catch (ignored: Exception) {}
            tryDrop("test_schema.view_src")
        }
    }

    /** Expiring a dropped (empty) SCHEMA's whole lifetime GCs its dangling `ducklake_schema` row. */
    @Test
    @Throws(Exception::class)
    fun deadDroppedSchemaMetadataIsGarbageCollected() {
        computeActual("CREATE SCHEMA gc_schema")
        computeActual("DROP SCHEMA gc_schema")
        // Advance the latest past the drop so gc_schema's whole lifetime is expirable.
        computeActual("CREATE TABLE test_schema.schema_gc_keeper AS SELECT 1 AS id")
        try {
            assertThat(rowCount("SELECT count(*) FROM ducklake_schema WHERE schema_name = ?", "gc_schema"))
                    .`as`("dropped schema row present before expiry").isEqualTo(1L)

            val latest = maxSnapshot()
            val toExpire = allSnapshotIds().filter { it != latest }
            computeActual("CALL system.expire_snapshots(snapshot_ids => ${sqlArray(toExpire)}, dry_run => false)")

            assertThat(rowCount("SELECT count(*) FROM ducklake_schema WHERE schema_name = ?", "gc_schema"))
                    .`as`("dead dropped-schema metadata GC'd").isEqualTo(0L)
            assertThat(rowCount("SELECT count(*) FROM ducklake_schema WHERE schema_name = ?", "test_schema"))
                    .`as`("live schema untouched").isGreaterThanOrEqualTo(1L)
        }
        finally {
            tryDrop("test_schema.schema_gc_keeper")
        }
    }
}
