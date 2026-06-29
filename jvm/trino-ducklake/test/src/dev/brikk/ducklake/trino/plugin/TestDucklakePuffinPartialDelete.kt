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
import org.roaringbitmap.RoaringBitmap
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator
import java.util.zip.CRC32

/**
 * End-to-end proof that the connector snapshot-filters a CONSOLIDATED ("partial") PUFFIN delete
 * file on read — the change that retired the last partial-file read gate (DESIGN-maintenance.md
 * § 6). A real PFA1 puffin container (`Magic Blob1 Blob2 Footer`) is written next to a
 * Trino-produced data file, each blob tagged with a `ducklake-snapshot-id`, and a
 * `ducklake_delete_file` row (format='puffin', partial_max) is injected. Reads at different
 * snapshots must apply only the deletions whose blob snapshot id is `<= read snapshot`.
 *
 * Self-contained (own DistributedQueryRunner + local data dir + PostgreSQL catalog) so the puffin
 * file can be written to the real on-disk data path and the catalog row injected directly.
 * SAME_THREAD: one shared catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePuffinPartialDelete : AbstractTestQueryFramework() {

    private var pgServer: dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer? = null
    private lateinit var dataDir: Path
    private val dbName = "ducklake_puffin_partial_e2e"

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        val pg = DucklakeTestCatalogEnvironment.getServer()
        pgServer = pg
        dataDir = Files.createTempDirectory("ducklake-puffin-partial-")
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

    private fun catalogConn() = DriverManager.getConnection(
            pgServer!!.getJdbcUrl(dbName), pgServer!!.getUser(), pgServer!!.getPassword())

    @Test
    @Throws(Exception::class)
    fun consolidatedPuffinDeleteIsSnapshotFilteredPerBlob() {
        val table = "test_schema.puffin_partial"
        try {
            // A single 3-row file: VALUES preserve order so id == file-local position (0,1,2).
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (0), (1), (2)) AS t(id)")
            // Two more snapshots to time-travel to. Each lands in its OWN file, leaving the
            // 3-row file's positions untouched.
            computeActual("INSERT INTO $table VALUES (10)")
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"puffin_partial\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (11)")
            val s3 = computeScalar("SELECT max(snapshot_id) FROM \"puffin_partial\$snapshots\"") as Long

            // Locate the 3-row data file (the CTAS file) + the table id.
            val (dataFileId, dataFilePath) = threeRowDataFile()

            // Build a consolidated puffin delete vector over that file:
            //   blob @ snapshot s2 deletes position 1 (id=1)
            //   blob @ snapshot s3 deletes positions 1,2 (id=1, id=2)   [cumulative]
            val puffinPath = dataDir.resolve("test_schema").resolve("puffin_partial")
                    .resolve("dv-partial-${java.util.UUID.randomUUID()}.puffin")
            Files.createDirectories(puffinPath.parent)
            val puffinBytes = buildPuffinContainer(listOf(
                    s2 to longArrayOf(1L),
                    s3 to longArrayOf(1L, 2L)))
            Files.write(puffinPath, puffinBytes)

            // Inject the catalog delete-file row: active from s2, partial_max = s3.
            injectPuffinDeleteFile(dataFileId, puffinPath.toAbsolutePath().toString(),
                    beginSnapshot = s2, partialMax = s3, deleteCount = 2L, fileSize = puffinBytes.size.toLong())

            // Read AS OF s2: only the s2 blob applies (s3 > s2 excluded). File pos 1 (id=1) gone;
            // the s2 INSERT (10) is visible, the s3 INSERT (11) is not.
            assertThat(idsAt(table, s2)).containsExactly(0, 2, 10)

            // Read at the latest (s3): partial_max not > s3 → both blobs apply. Positions 1,2
            // (id 1,2) gone; both inserts visible.
            assertThat(idsLatest(table)).containsExactly(0, 10, 11)
        }
        finally {
            try { computeActual("DROP TABLE $table") } catch (ignored: Exception) {}
        }
    }

    private fun idsAt(table: String, snapshot: Long): List<Int> =
            computeActual("SELECT id FROM $table FOR VERSION AS OF $snapshot ORDER BY id")
                    .materializedRows.map { it.getField(0) as Int }

    private fun idsLatest(table: String): List<Int> =
            computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int }

    /** (data_file_id, path) of the 3-row CTAS file. */
    @Throws(Exception::class)
    private fun threeRowDataFile(): Pair<Long, String> {
        catalogConn().use { conn ->
            conn.prepareStatement(
                    "SELECT data_file_id, path FROM ducklake_data_file WHERE record_count = 3 AND end_snapshot IS NULL").use { ps ->
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "no 3-row data file found" }
                    return rs.getLong(1) to rs.getString(2)
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun injectPuffinDeleteFile(
            dataFileId: Long, absolutePath: String, beginSnapshot: Long, partialMax: Long,
            deleteCount: Long, fileSize: Long) {
        catalogConn().use { conn ->
            val tableId: Long = conn.prepareStatement(
                    "SELECT table_id FROM ducklake_data_file WHERE data_file_id = ?").use { ps ->
                ps.setLong(1, dataFileId)
                ps.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
            }
            val nextId: Long = conn.prepareStatement(
                    "SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file").use { ps ->
                ps.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
            }
            conn.prepareStatement(
                    "INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, data_file_id, "
                            + "path, path_is_relative, format, delete_count, file_size_bytes, footer_size, partial_max) "
                            + "VALUES (?, ?, ?, ?, ?, false, 'puffin', ?, ?, 0, ?)").use { ps ->
                ps.setLong(1, nextId)
                ps.setLong(2, tableId)
                ps.setLong(3, beginSnapshot)
                ps.setLong(4, dataFileId)
                ps.setString(5, absolutePath)
                ps.setLong(6, deleteCount)
                ps.setLong(7, fileSize)
                ps.setLong(8, partialMax)
                assertThat(ps.executeUpdate()).isEqualTo(1)
            }
        }
    }

    companion object {
        private val DELETION_VECTOR_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39.toByte(), 0x64.toByte())
        private val PUFFIN_MAGIC = byteArrayOf('P'.code.toByte(), 'F'.code.toByte(), 'A'.code.toByte(), '1'.code.toByte())

        /** Build a PFA1 container `Magic Blob1..BlobN Footer`, each blob tagged with a snapshot id. */
        private fun buildPuffinContainer(blobs: List<Pair<Long, LongArray>>): ByteArray {
            val out = ByteArrayOutputStream()
            out.write(PUFFIN_MAGIC)
            data class Info(val snapshotId: Long, val offset: Int, val length: Int)
            val infos = mutableListOf<Info>()
            var offset = PUFFIN_MAGIC.size
            for ((snap, positions) in blobs) {
                val blobBytes = buildBlob(positions)
                infos.add(Info(snap, offset, blobBytes.size))
                out.write(blobBytes)
                offset += blobBytes.size
            }
            val json = buildString {
                append("{\"blobs\":[")
                infos.forEachIndexed { i, info ->
                    if (i > 0) append(",")
                    append("{\"type\":\"deletion-vector-v1\",\"offset\":").append(info.offset)
                    append(",\"length\":").append(info.length)
                    append(",\"properties\":{\"ducklake-snapshot-id\":\"").append(info.snapshotId).append("\"}}")
                }
                append("]}")
            }
            val payload = json.toByteArray(Charsets.UTF_8)
            out.write(PUFFIN_MAGIC)
            out.write(payload)
            out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(payload.size).array())
            out.write(ByteArray(4)) // flags = 0
            out.write(PUFFIN_MAGIC)
            return out.toByteArray()
        }

        /** Single deletion-vector-v1 blob, byte-exact to DuckLakeDeletionVectorData::ToBlob. */
        private fun buildBlob(positions: LongArray): ByteArray {
            val byHigh = LinkedHashMap<Int, RoaringBitmap>()
            for (p in positions) {
                val high = (p shr 32).toInt()
                byHigh.getOrPut(high) { RoaringBitmap() }.add((p and 0xFFFFFFFFL).toInt())
            }
            val serialized = LinkedHashMap<Int, ByteArray>()
            for ((k, rb) in byHigh) {
                rb.runOptimize()
                val b = ByteBuffer.allocate(rb.serializedSizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
                rb.serialize(b)
                serialized[k] = b.array()
            }
            var bitmapsTotal = 0
            for (s in serialized.values) {
                bitmapsTotal += 4 + s.size
            }
            val checksummedLength = 4 + 8 + bitmapsTotal
            val total = 4 + checksummedLength + 4
            val buf = ByteBuffer.allocate(total)
            buf.order(ByteOrder.BIG_ENDIAN).putInt(checksummedLength)
            val checksummedStart = buf.position()
            buf.put(DELETION_VECTOR_MAGIC)
            buf.order(ByteOrder.LITTLE_ENDIAN).putLong(serialized.size.toLong())
            for ((k, v) in serialized) {
                buf.putInt(k)
                buf.put(v)
            }
            val crc = CRC32()
            crc.update(buf.array(), checksummedStart, buf.position() - checksummedStart)
            buf.order(ByteOrder.BIG_ENDIAN).putInt(crc.value.toInt())
            return buf.array()
        }
    }
}
