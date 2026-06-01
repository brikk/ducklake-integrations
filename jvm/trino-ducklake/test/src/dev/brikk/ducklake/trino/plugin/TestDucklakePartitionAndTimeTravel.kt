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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.READ_SNAPSHOT_TIMESTAMP
import io.trino.Session
import io.trino.spi.type.TimeZoneKey
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Snapshot-aware read suite. Two related concerns:
 *
 *
 *  - **Partitioned reads** — full scan, partition predicates, partition pruning,
 *    per-partition aggregation/rollup, and the temporal/daily partition transforms
 *    (year/month/day) that have their own pruning logic.
 *  - **Time travel** — `FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`
 *    (with literal vs. `timestamptz` semantics), and the
 *    `read_snapshot_timestamp` session property. Includes session-time-zone
 *    independence assertions and precise error messages on bad versions.
 *
 */
open class TestDucklakePartitionAndTimeTravel : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-partition-time-travel"
    }

    // ==================== Partitioned tables ====================

    @Test
    fun testPartitionedTableFullScan() {
        assertQuery("SELECT count(*) FROM partitioned_table", "VALUES 5")
    }

    @Test
    fun testPartitionedTableWithPredicate() {
        val result = computeActual("SELECT * FROM partitioned_table WHERE region = 'US'")
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testPartitionedTableWithNonMatchingPredicate() {
        assertQueryReturnsEmptyResult("SELECT * FROM partitioned_table WHERE region = 'NONEXISTENT'")
    }

    @Test
    fun testPartitionedTableDistinctPartitions() {
        val result = computeActual(
            "SELECT DISTINCT region FROM partitioned_table ORDER BY region")
        assertThat(result.rowCount).isEqualTo(3)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("APAC")
        assertThat(result.materializedRows[1].getField(0)).isEqualTo("EU")
        assertThat(result.materializedRows[2].getField(0)).isEqualTo("US")
    }

    @Test
    fun testPartitionedTableAggregationPerPartition() {
        assertQuery(
            "SELECT region, sum(amount), count(*) FROM partitioned_table GROUP BY region ORDER BY region",
            "VALUES ('APAC', 300.0, 1), ('EU', 400.0, 2), ('US', 300.0, 2)")
    }

    @Test
    fun testPartitionedTableRollup() {
        assertQuery(
            "SELECT region, sum(amount) FROM partitioned_table " +
                    "GROUP BY ROLLUP(region) ORDER BY region NULLS LAST",
            "VALUES ('APAC', 300.0), ('EU', 400.0), ('US', 300.0), (NULL, 1000.0)")
    }

    @Test
    fun testTemporalPartitionedTableRead() {
        assertQuery("SELECT count(*) FROM temporal_partitioned_table", "VALUES 6")
    }

    @Test
    fun testTemporalPartitionedTableWithDatePredicate() {
        val result = computeActual(
            "SELECT count(*) FROM temporal_partitioned_table WHERE event_date = DATE '2023-06-10'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1L)
    }

    @Test
    fun testTemporalPartitionedTableWithYearPredicate() {
        val result = computeActual(
            "SELECT count(*) FROM temporal_partitioned_table WHERE year(event_date) = 2023")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(4L)
    }

    @Test
    fun testTemporalPartitionedTableWithMonthPredicate() {
        val result = computeActual(
            "SELECT count(*) FROM temporal_partitioned_table WHERE month(event_date) = 1")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testDailyPartitionedTableRead() {
        assertQuery("SELECT count(*) FROM daily_partitioned_table", "VALUES 5")
    }

    @Test
    fun testDailyPartitionedTableByExactDate() {
        val result = computeActual(
            "SELECT count(*) FROM daily_partitioned_table WHERE event_date = DATE '2023-06-15'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testDailyPartitionedTableDateRange() {
        val result = computeActual(
            "SELECT count(*) FROM daily_partitioned_table " +
                    "WHERE event_date >= DATE '2023-06-15' AND event_date <= DATE '2023-06-20'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    // ==================== Time travel ====================

    @Test
    fun testForVersionAsOfReadsHistoricalSnapshot() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 4")
        assertQuery(
            "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + historicalSnapshot.snapshotId,
            "VALUES 2")
        assertQuery(
            "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF CAST(" + historicalSnapshot.snapshotId + " AS INTEGER)",
            "VALUES 2")
    }

    @Test
    fun testForTimestampAsOfReadsHistoricalSnapshot() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        assertQuery(
            "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF from_iso8601_timestamp('" + historicalSnapshot.snapshotTime + "')",
            "VALUES 2")
    }

    @Test
    fun testForTimestampAsOfTimestampLiteralWithSessionTimeZone() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        val utcSession = sessionWithTimeZone("UTC")
        val tokyoSession = sessionWithTimeZone("Asia/Tokyo")

        val utcTimestampLiteral = toTimestampLiteral(historicalSnapshot.snapshotTime, "UTC")
        val tokyoTimestampLiteral = toTimestampLiteral(historicalSnapshot.snapshotTime, "Asia/Tokyo")

        assertQuery(
            utcSession,
            "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '" + utcTimestampLiteral + "'",
            "VALUES 2")
        assertQuery(
            tokyoSession,
            "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '" + tokyoTimestampLiteral + "'",
            "VALUES 2")
    }

    @Test
    fun testForTimestampAsOfTimestampWithTimeZoneIsSessionIndependent() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        val utcSession = sessionWithTimeZone("UTC")
        val tokyoSession = sessionWithTimeZone("Asia/Tokyo")

        val asOfInstant = historicalSnapshot.snapshotTime.toString()
        val sql = "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF from_iso8601_timestamp('" + asOfInstant + "')"
        assertQuery(utcSession, sql, "VALUES 2")
        assertQuery(tokyoSession, sql, "VALUES 2")
    }

    @Test
    fun testSessionReadSnapshotTimestampIsSessionIndependent() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        val utcSession = Session.builder(sessionWithTimeZone("UTC"))
            .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_TIMESTAMP, historicalSnapshot.snapshotTime.toString())
            .build()
        val tokyoSession = Session.builder(sessionWithTimeZone("Asia/Tokyo"))
            .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_TIMESTAMP, historicalSnapshot.snapshotTime.toString())
            .build()

        assertQuery(utcSession, "SELECT count(*) FROM schema_evolution_table", "VALUES 2")
        assertQuery(tokyoSession, "SELECT count(*) FROM schema_evolution_table", "VALUES 2")
    }

    @Test
    fun testVersionedReadsReturnPreciseErrors() {
        val historicalSnapshot = getSchemaEvolutionHistoricalSnapshot()
        assertQueryFails(
            "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF 'not-a-snapshot-id'",
            "Unsupported type for table version: varchar.*")
        assertQueryFails(
            "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + (historicalSnapshot.snapshotId + 10_000_000),
            "DuckLake snapshot ID does not exist: .*")
        assertQueryFails(
            "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00 UTC'",
            "No DuckLake snapshot exists at or before timestamp: .*")
    }

    private fun getSchemaEvolutionHistoricalSnapshot(): HistoricalSnapshot {
        // Find the snapshot where schema_evolution_table had exactly 2 rows by scanning
        // snapshots in descending order using FOR VERSION AS OF through the query runner.
        // This uses the same catalog as the test (including isolated catalogs).
        val snapshots = computeActual(
            "SELECT snapshot_id, snapshot_time FROM \"schema_evolution_table\$snapshots\" ORDER BY snapshot_id DESC")

        for (row in snapshots.materializedRows) {
            val snapshotId = (row.getField(0) as Number).toLong()
            val count = (computeScalar(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF $snapshotId") as Number).toLong()
            if (count == 2L) {
                // snapshot_time is a timestamp with time zone; extract as Instant
                val snapshotTime = (row.getField(1) as ZonedDateTime).toInstant()
                return HistoricalSnapshot(snapshotId, snapshotTime)
            }
        }
        throw AssertionError("No historical snapshot found with 2 rows for schema_evolution_table")
    }

    private fun sessionWithTimeZone(zoneId: String): Session {
        return Session.builder(session)
            .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId))
            .build()
    }

    @JvmRecord
    private data class HistoricalSnapshot(val snapshotId: Long, val snapshotTime: Instant)

    companion object {
        private val TIMESTAMP_LITERAL_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")

        private fun toTimestampLiteral(instant: Instant, zoneId: String): String {
            return TIMESTAMP_LITERAL_FORMATTER.withZone(ZoneId.of(zoneId)).format(instant)
        }
    }
}
