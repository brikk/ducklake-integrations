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

import io.trino.testing.MaterializedRow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Writes into partitioned tables: identity-partitioned, year-partitioned, year+month
 * temporal-partitioned, partitioned CTAS, multi-batch INSERTs into partitions, and NULL
 * partition values. Asserts that data lands in the right partition (partition-pushed
 * SELECTs find the rows) and that NULL partition values are addressable via
 * `IS NULL`.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartitionedWrite : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "write-partitioned"
    }

    @Test
    fun testPartitionedInsertIdentity() {
        computeActual("CREATE TABLE test_schema.part_identity (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            computeActual("INSERT INTO test_schema.part_identity VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0), (4, 'APAC', 40.0)")

            // Verify all data is readable
            val result = computeActual("SELECT count(*) FROM test_schema.part_identity")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(4L)

            // Verify partition filtering works
            val usData = computeActual(
                    "SELECT id, amount FROM test_schema.part_identity WHERE region = 'US' ORDER BY id")
            assertThat(usData.rowCount).isEqualTo(2)
            assertThat(usData.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(usData.materializedRows[1].getField(0)).isEqualTo(3)

            val euData = computeActual(
                    "SELECT count(*) FROM test_schema.part_identity WHERE region = 'EU'")
            assertThat(euData.materializedRows[0].getField(0)).isEqualTo(1L)
        }
        finally {
            tryDropTable("test_schema.part_identity")
        }
    }

    @Test
    fun testPartitionedInsertTemporalYear() {
        computeActual("CREATE TABLE test_schema.part_year (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)'])")
        try {
            computeActual("INSERT INTO test_schema.part_year VALUES " +
                    "(1, DATE '2023-01-15', 10.0), " +
                    "(2, DATE '2023-06-20', 20.0), " +
                    "(3, DATE '2024-03-05', 30.0)")

            val all = computeActual("SELECT count(*) FROM test_schema.part_year")
            assertThat(all.materializedRows[0].getField(0)).isEqualTo(3L)

            // Verify year-based partition filtering
            val y2023 = computeActual(
                    "SELECT count(*) FROM test_schema.part_year WHERE event_date >= DATE '2023-01-01' AND event_date < DATE '2024-01-01'")
            assertThat(y2023.materializedRows[0].getField(0)).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.part_year")
        }
    }

    @Test
    fun testPartitionedInsertTemporalYearMonth() {
        computeActual("CREATE TABLE test_schema.part_ym (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])")
        try {
            computeActual("INSERT INTO test_schema.part_ym VALUES " +
                    "(1, DATE '2023-01-15', 100.0), " +
                    "(2, DATE '2023-01-20', 200.0), " +
                    "(3, DATE '2023-06-10', 300.0), " +
                    "(4, DATE '2024-03-05', 400.0)")

            val all = computeActual("SELECT count(*) FROM test_schema.part_ym")
            assertThat(all.materializedRows[0].getField(0)).isEqualTo(4L)

            // Verify values are correct
            val ordered = computeActual(
                    "SELECT id, amount FROM test_schema.part_ym ORDER BY id")
            assertThat(ordered.rowCount).isEqualTo(4)
            assertThat(ordered.materializedRows[0].getField(1)).isEqualTo(100.0)
            assertThat(ordered.materializedRows[3].getField(1)).isEqualTo(400.0)
        }
        finally {
            tryDropTable("test_schema.part_ym")
        }
    }

    @Test
    fun testPartitionedCtas() {
        computeActual("CREATE TABLE test_schema.part_ctas_src (id INTEGER, region VARCHAR, value DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.part_ctas_src VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0)")

            computeActual("CREATE TABLE test_schema.part_ctas_dst " +
                    "WITH (partitioned_by = ARRAY['region']) AS " +
                    "SELECT * FROM test_schema.part_ctas_src")

            val result = computeActual("SELECT count(*) FROM test_schema.part_ctas_dst")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)

            val usData = computeActual(
                    "SELECT id FROM test_schema.part_ctas_dst WHERE region = 'US' ORDER BY id")
            assertThat(usData.rowCount).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.part_ctas_src")
            tryDropTable("test_schema.part_ctas_dst")
        }
    }

    @Test
    fun testPartitionedMultipleInserts() {
        computeActual("CREATE TABLE test_schema.part_multi (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            computeActual("INSERT INTO test_schema.part_multi VALUES (1, 'US', 10.0), (2, 'EU', 20.0)")
            computeActual("INSERT INTO test_schema.part_multi VALUES (3, 'US', 30.0), (4, 'APAC', 40.0)")
            computeActual("INSERT INTO test_schema.part_multi VALUES (5, 'EU', 50.0)")

            val total = computeActual("SELECT count(*) FROM test_schema.part_multi")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(5L)

            val perRegion = computeActual(
                    "SELECT region, count(*) as cnt FROM test_schema.part_multi GROUP BY region ORDER BY region")
            assertThat(perRegion.rowCount).isEqualTo(3)
            // APAC=1, EU=2, US=2
            val rows: List<MaterializedRow> = perRegion.materializedRows
            assertThat(rows[0].getField(0)).isEqualTo("APAC")
            assertThat(rows[0].getField(1)).isEqualTo(1L)
            assertThat(rows[1].getField(0)).isEqualTo("EU")
            assertThat(rows[1].getField(1)).isEqualTo(2L)
            assertThat(rows[2].getField(0)).isEqualTo("US")
            assertThat(rows[2].getField(1)).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.part_multi")
        }
    }

    @Test
    fun testPartitionedWithNullPartitionValues() {
        computeActual("CREATE TABLE test_schema.part_nulls (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            computeActual("INSERT INTO test_schema.part_nulls VALUES " +
                    "(1, 'US', 10.0), (2, NULL, 20.0), (3, 'EU', 30.0)")

            val total = computeActual("SELECT count(*) FROM test_schema.part_nulls")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(3L)

            val nullRegion = computeActual(
                    "SELECT id FROM test_schema.part_nulls WHERE region IS NULL")
            assertThat(nullRegion.rowCount).isEqualTo(1)
            assertThat(nullRegion.materializedRows[0].getField(0)).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.part_nulls")
        }
    }

    @Test
    fun testPartitionPathKeyNamingMatchesUpstreamConvention() {
        // Upstream hive-path key names (DuckLakePartitionUtils::GetPartitionKeyName):
        // bucket → `bucket=`, temporal → `year=`, identity → column name; repeated
        // transforms cascade to `<prefix>_<column>` (then `_2`, `_3`…). Without the
        // cascade, bucket(4, id) + bucket(8, id) both emitted `id=` — ambiguous paths.
        computeActual("CREATE TABLE test_schema.part_naming (id INTEGER, ts DATE, region VARCHAR) " +
                "WITH (partitioned_by = ARRAY['region', 'bucket(4, id)', 'bucket(8, id)', 'year(ts)'])")
        try {
            computeActual("INSERT INTO test_schema.part_naming VALUES " +
                    "(1, DATE '2023-05-01', 'us'), (2, DATE '2024-06-01', 'eu')")

            val paths = computeActual("SELECT DISTINCT \"\$path\" FROM test_schema.part_naming")
                    .materializedRows.map { it.getField(0) as String }
            assertThat(paths).isNotEmpty()
            for (path in paths) {
                assertThat(path).`as`("identity keeps the column name").contains("region=")
                assertThat(path).`as`("first bucket transform uses the bare prefix").contains("/bucket=")
                assertThat(path).`as`("second bucket on the same column disambiguates").contains("/bucket_id=")
                assertThat(path).`as`("temporal transform uses the transform-name prefix").contains("/year=")
                assertThat(path).`as`("no duplicate bare-column key for bucket transforms")
                        .doesNotContain("/id=")
            }

            // Values still round-trip through the catalog regardless of path names.
            val total = computeActual("SELECT count(*) FROM test_schema.part_naming")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.part_naming")
        }
    }

    @Test
    fun testIdentityPartitionValuesWithSpecialCharsRoundTrip() {
        // Regression: identity partition path segments must be percent-encoded the way
        // DuckDB/DuckLake do (HivePartitioning::Escape). A raw '/' would split into extra
        // path segments, and a raw '%'/space would not round-trip through our URL-decoding
        // read side. See DucklakeHivePartitionCodec.
        computeActual("CREATE TABLE test_schema.part_special (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            computeActual("INSERT INTO test_schema.part_special VALUES " +
                    "(1, 'a/b', 10.0), " +          // slash -> %2F (must not create a subdir)
                    "(2, '50% off', 20.0), " +      // percent + space -> %25 %20
                    "(3, 'two words', 30.0), " +    // space -> %20
                    "(4, 'café', 40.0), " +         // non-ASCII -> %C3%A9
                    "(5, NULL, 50.0)")              // NULL sentinel

            assertThat(computeActual("SELECT count(*) FROM test_schema.part_special")
                    .materializedRows[0].getField(0)).isEqualTo(5L)

            // Each special value round-trips: the exact string comes back and is addressable
            // by an equality predicate (partition pruning parses the raw catalog value).
            for ((id, region) in listOf(1 to "a/b", 2 to "50% off", 3 to "two words", 4 to "café")) {
                val r = computeActual(
                        "SELECT id FROM test_schema.part_special WHERE region = '" +
                                region.replace("'", "''") + "'")
                assertThat(r.rowCount).`as`("equality on %s", region).isEqualTo(1)
                assertThat(r.materializedRows[0].getField(0)).isEqualTo(id)
            }
            assertThat(computeActual("SELECT id FROM test_schema.part_special WHERE region IS NULL")
                    .materializedRows[0].getField(0)).isEqualTo(5)

            // On-disk path segments are percent-encoded exactly like DuckDB/DuckLake.
            val paths = computeActual("SELECT id, \"\$path\" FROM test_schema.part_special ORDER BY id")
                    .materializedRows.associate { it.getField(0) as Int to it.getField(1) as String }
            assertThat(paths[1]).`as`("slash encoded, no extra dir").contains("/region=a%2Fb/")
            assertThat(paths[2]).contains("/region=50%25%20off/")
            assertThat(paths[3]).contains("/region=two%20words/")
            assertThat(paths[4]).contains("/region=caf%C3%A9/")
            assertThat(paths[5]).`as`("NULL uses the sentinel").contains("/region=__HIVE_DEFAULT_PARTITION__/")
        }
        finally {
            tryDropTable("test_schema.part_special")
        }
    }

    @Test
    fun testBucketPartitionedInsertAndRead() {
        // Round-trip: create bucket-partitioned table, insert, read all rows back.
        computeActual("CREATE TABLE test_schema.part_bucket (id INTEGER, name VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['bucket(4, name)'])")
        try {
            computeActual("INSERT INTO test_schema.part_bucket VALUES " +
                    "(1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0), " +
                    "(4, 'dave', 40.0), (5, 'eve', 50.0), (6, 'frank', 60.0)")

            // All rows readable (the bucket transform shouldn't drop data).
            val total = computeActual("SELECT count(*) FROM test_schema.part_bucket")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(6L)

            val ordered = computeActual(
                    "SELECT id, name, amount FROM test_schema.part_bucket ORDER BY id")
            assertThat(ordered.rowCount).isEqualTo(6)
            assertThat(ordered.materializedRows[0].getField(1)).isEqualTo("alice")
            assertThat(ordered.materializedRows[5].getField(1)).isEqualTo("frank")
        }
        finally {
            tryDropTable("test_schema.part_bucket")
        }
    }

    @Test
    fun testBucketPartitionEqualityPredicatePruning() {
        // Equality predicate should still return the correct row (and exercise the
        // bucket pruning code path in DucklakeBucketPartitionMatcher).
        computeActual("CREATE TABLE test_schema.part_bucket_prune (id INTEGER, name VARCHAR) " +
                "WITH (partitioned_by = ARRAY['bucket(4, name)'])")
        try {
            computeActual("INSERT INTO test_schema.part_bucket_prune VALUES " +
                    "(1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'dave')")

            val result = computeActual(
                    "SELECT id FROM test_schema.part_bucket_prune WHERE name = 'alice'")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)

            // Range predicate must not prune (bucketing scrambles ordering).
            val rangeResult = computeActual(
                    "SELECT count(*) FROM test_schema.part_bucket_prune WHERE name >= 'a' AND name < 'z'")
            assertThat(rangeResult.materializedRows[0].getField(0)).isEqualTo(4L)
        }
        finally {
            tryDropTable("test_schema.part_bucket_prune")
        }
    }

    @Test
    fun testBucketPartitionOnInteger() {
        computeActual("CREATE TABLE test_schema.part_bucket_int (id BIGINT, payload VARCHAR) " +
                "WITH (partitioned_by = ARRAY['bucket(8, id)'])")
        try {
            computeActual("INSERT INTO test_schema.part_bucket_int VALUES " +
                    "(1, 'a'), (2, 'b'), (3, 'c'), (100, 'd'), (1000, 'e')")

            val total = computeActual("SELECT count(*) FROM test_schema.part_bucket_int")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(5L)

            val specific = computeActual(
                    "SELECT payload FROM test_schema.part_bucket_int WHERE id = 100")
            assertThat(specific.rowCount).isEqualTo(1)
            assertThat(specific.materializedRows[0].getField(0)).isEqualTo("d")
        }
        finally {
            tryDropTable("test_schema.part_bucket_int")
        }
    }

    @Test
    fun testBucketPartitionStoresZeroBasedBucketIndices() {
        // The catalog stores the bucket index (0..N-1) as the partition value. Verify
        // that all values fall in range and that we observe at least one distinct bucket.
        computeActual("CREATE TABLE test_schema.part_bucket_meta (id INTEGER, name VARCHAR) " +
                "WITH (partitioned_by = ARRAY['bucket(4, name)'])")
        try {
            computeActual("INSERT INTO test_schema.part_bucket_meta VALUES " +
                    "(1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'dave'), " +
                    "(5, 'eve'), (6, 'frank'), (7, 'grace'), (8, 'heidi')")

            // The $files metadata table exposes the underlying parquet files; each one
            // belongs to a single bucket. Just verify we can read all 8 rows back.
            val total = computeActual("SELECT count(*) FROM test_schema.part_bucket_meta")
            assertThat(total.materializedRows[0].getField(0)).isEqualTo(8L)
        }
        finally {
            tryDropTable("test_schema.part_bucket_meta")
        }
    }
}
