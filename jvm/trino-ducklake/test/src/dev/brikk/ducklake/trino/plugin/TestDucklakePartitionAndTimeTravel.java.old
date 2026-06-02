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

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Snapshot-aware read suite. Two related concerns:
 *
 * <ul>
 *   <li><b>Partitioned reads</b> — full scan, partition predicates, partition pruning,
 *       per-partition aggregation/rollup, and the temporal/daily partition transforms
 *       (year/month/day) that have their own pruning logic.</li>
 *   <li><b>Time travel</b> — {@code FOR VERSION AS OF}, {@code FOR TIMESTAMP AS OF}
 *       (with literal vs. {@code timestamptz} semantics), and the
 *       {@code read_snapshot_timestamp} session property. Includes session-time-zone
 *       independence assertions and precise error messages on bad versions.</li>
 * </ul>
 */
public class TestDucklakePartitionAndTimeTravel
        extends AbstractDucklakeIntegrationTest
{
    private static final DateTimeFormatter TIMESTAMP_LITERAL_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    @Override
    protected String isolatedCatalogName()
    {
        return "integration-partition-time-travel";
    }

    // ==================== Partitioned tables ====================

    @Test
    public void testPartitionedTableFullScan()
    {
        assertQuery("SELECT count(*) FROM partitioned_table", "VALUES 5");
    }

    @Test
    public void testPartitionedTableWithPredicate()
    {
        MaterializedResult result = computeActual("SELECT * FROM partitioned_table WHERE region = 'US'");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testPartitionedTableWithNonMatchingPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM partitioned_table WHERE region = 'NONEXISTENT'");
    }

    @Test
    public void testPartitionedTableDistinctPartitions()
    {
        MaterializedResult result = computeActual(
                "SELECT DISTINCT region FROM partitioned_table ORDER BY region");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("APAC");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("EU");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("US");
    }

    @Test
    public void testPartitionedTableAggregationPerPartition()
    {
        assertQuery(
                "SELECT region, sum(amount), count(*) FROM partitioned_table GROUP BY region ORDER BY region",
                "VALUES ('APAC', 300.0, 1), ('EU', 400.0, 2), ('US', 300.0, 2)");
    }

    @Test
    public void testPartitionedTableRollup()
    {
        assertQuery(
                "SELECT region, sum(amount) FROM partitioned_table " +
                        "GROUP BY ROLLUP(region) ORDER BY region NULLS LAST",
                "VALUES ('APAC', 300.0), ('EU', 400.0), ('US', 300.0), (NULL, 1000.0)");
    }

    @Test
    public void testTemporalPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM temporal_partitioned_table", "VALUES 6");
    }

    @Test
    public void testTemporalPartitionedTableWithDatePredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE event_date = DATE '2023-06-10'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
    }

    @Test
    public void testTemporalPartitionedTableWithYearPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE year(event_date) = 2023");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);
    }

    @Test
    public void testTemporalPartitionedTableWithMonthPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE month(event_date) = 1");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testDailyPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM daily_partitioned_table", "VALUES 5");
    }

    @Test
    public void testDailyPartitionedTableByExactDate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM daily_partitioned_table WHERE event_date = DATE '2023-06-15'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testDailyPartitionedTableDateRange()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM daily_partitioned_table " +
                        "WHERE event_date >= DATE '2023-06-15' AND event_date <= DATE '2023-06-20'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    // ==================== Time travel ====================

    @Test
    public void testForVersionAsOfReadsHistoricalSnapshot()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 4");
        assertQuery(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + historicalSnapshot.snapshotId(),
                "VALUES 2");
        assertQuery(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF CAST(" + historicalSnapshot.snapshotId() + " AS INTEGER)",
                "VALUES 2");
    }

    @Test
    public void testForTimestampAsOfReadsHistoricalSnapshot()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        assertQuery(
                "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF from_iso8601_timestamp('" + historicalSnapshot.snapshotTime() + "')",
                "VALUES 2");
    }

    @Test
    public void testForTimestampAsOfTimestampLiteralWithSessionTimeZone()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        Session utcSession = sessionWithTimeZone("UTC");
        Session tokyoSession = sessionWithTimeZone("Asia/Tokyo");

        String utcTimestampLiteral = toTimestampLiteral(historicalSnapshot.snapshotTime(), "UTC");
        String tokyoTimestampLiteral = toTimestampLiteral(historicalSnapshot.snapshotTime(), "Asia/Tokyo");

        assertQuery(
                utcSession,
                "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '" + utcTimestampLiteral + "'",
                "VALUES 2");
        assertQuery(
                tokyoSession,
                "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '" + tokyoTimestampLiteral + "'",
                "VALUES 2");
    }

    @Test
    public void testForTimestampAsOfTimestampWithTimeZoneIsSessionIndependent()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        Session utcSession = sessionWithTimeZone("UTC");
        Session tokyoSession = sessionWithTimeZone("Asia/Tokyo");

        String asOfInstant = historicalSnapshot.snapshotTime().toString();
        String sql = "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF from_iso8601_timestamp('" + asOfInstant + "')";
        assertQuery(utcSession, sql, "VALUES 2");
        assertQuery(tokyoSession, sql, "VALUES 2");
    }

    @Test
    public void testSessionReadSnapshotTimestampIsSessionIndependent()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        Session utcSession = Session.builder(sessionWithTimeZone("UTC"))
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_TIMESTAMP, historicalSnapshot.snapshotTime().toString())
                .build();
        Session tokyoSession = Session.builder(sessionWithTimeZone("Asia/Tokyo"))
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_TIMESTAMP, historicalSnapshot.snapshotTime().toString())
                .build();

        assertQuery(utcSession, "SELECT count(*) FROM schema_evolution_table", "VALUES 2");
        assertQuery(tokyoSession, "SELECT count(*) FROM schema_evolution_table", "VALUES 2");
    }

    @Test
    public void testVersionedReadsReturnPreciseErrors()
            throws Exception
    {
        HistoricalSnapshot historicalSnapshot = getSchemaEvolutionHistoricalSnapshot();
        assertQueryFails(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF 'not-a-snapshot-id'",
                "Unsupported type for table version: varchar.*");
        assertQueryFails(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + (historicalSnapshot.snapshotId() + 10_000_000),
                "DuckLake snapshot ID does not exist: .*");
        assertQueryFails(
                "SELECT count(*) FROM schema_evolution_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00 UTC'",
                "No DuckLake snapshot exists at or before timestamp: .*");
    }

    private HistoricalSnapshot getSchemaEvolutionHistoricalSnapshot()
    {
        // Find the snapshot where schema_evolution_table had exactly 2 rows by scanning
        // snapshots in descending order using FOR VERSION AS OF through the query runner.
        // This uses the same catalog as the test (including isolated catalogs).
        MaterializedResult snapshots = computeActual(
                "SELECT snapshot_id, snapshot_time FROM \"schema_evolution_table$snapshots\" ORDER BY snapshot_id DESC");

        for (MaterializedRow row : snapshots.getMaterializedRows()) {
            long snapshotId = ((Number) row.getField(0)).longValue();
            long count = ((Number) computeScalar(
                    "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + snapshotId)).longValue();
            if (count == 2) {
                // snapshot_time is a timestamp with time zone; extract as Instant
                Instant snapshotTime = ((java.time.ZonedDateTime) row.getField(1)).toInstant();
                return new HistoricalSnapshot(snapshotId, snapshotTime);
            }
        }
        throw new AssertionError("No historical snapshot found with 2 rows for schema_evolution_table");
    }

    private Session sessionWithTimeZone(String zoneId)
    {
        return Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId))
                .build();
    }

    private static String toTimestampLiteral(Instant instant, String zoneId)
    {
        return TIMESTAMP_LITERAL_FORMATTER.withZone(ZoneId.of(zoneId)).format(instant);
    }

    private record HistoricalSnapshot(long snapshotId, Instant snapshotTime) {}
}
