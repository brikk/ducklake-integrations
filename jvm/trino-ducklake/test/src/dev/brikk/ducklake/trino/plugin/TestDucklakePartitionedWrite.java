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

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes into partitioned tables: identity-partitioned, year-partitioned, year+month
 * temporal-partitioned, partitioned CTAS, multi-batch INSERTs into partitions, and NULL
 * partition values. Asserts that data lands in the right partition (partition-pushed
 * SELECTs find the rows) and that NULL partition values are addressable via
 * {@code IS NULL}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakePartitionedWrite
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "write-partitioned";
    }

    @Test
    public void testPartitionedInsertIdentity()
    {
        computeActual("CREATE TABLE test_schema.part_identity (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_identity VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0), (4, 'APAC', 40.0)");

            // Verify all data is readable
            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.part_identity");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);

            // Verify partition filtering works
            MaterializedResult usData = computeActual(
                    "SELECT id, amount FROM test_schema.part_identity WHERE region = 'US' ORDER BY id");
            assertThat(usData.getRowCount()).isEqualTo(2);
            assertThat(usData.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(usData.getMaterializedRows().get(1).getField(0)).isEqualTo(3);

            MaterializedResult euData = computeActual(
                    "SELECT count(*) FROM test_schema.part_identity WHERE region = 'EU'");
            assertThat(euData.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
        }
        finally {
            tryDropTable("test_schema.part_identity");
        }
    }

    @Test
    public void testPartitionedInsertTemporalYear()
    {
        computeActual("CREATE TABLE test_schema.part_year (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)'])");
        try {
            computeActual("INSERT INTO test_schema.part_year VALUES " +
                    "(1, DATE '2023-01-15', 10.0), " +
                    "(2, DATE '2023-06-20', 20.0), " +
                    "(3, DATE '2024-03-05', 30.0)");

            MaterializedResult all = computeActual("SELECT count(*) FROM test_schema.part_year");
            assertThat(all.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            // Verify year-based partition filtering
            MaterializedResult y2023 = computeActual(
                    "SELECT count(*) FROM test_schema.part_year WHERE event_date >= DATE '2023-01-01' AND event_date < DATE '2024-01-01'");
            assertThat(y2023.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.part_year");
        }
    }

    @Test
    public void testPartitionedInsertTemporalYearMonth()
    {
        computeActual("CREATE TABLE test_schema.part_ym (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])");
        try {
            computeActual("INSERT INTO test_schema.part_ym VALUES " +
                    "(1, DATE '2023-01-15', 100.0), " +
                    "(2, DATE '2023-01-20', 200.0), " +
                    "(3, DATE '2023-06-10', 300.0), " +
                    "(4, DATE '2024-03-05', 400.0)");

            MaterializedResult all = computeActual("SELECT count(*) FROM test_schema.part_ym");
            assertThat(all.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);

            // Verify values are correct
            MaterializedResult ordered = computeActual(
                    "SELECT id, amount FROM test_schema.part_ym ORDER BY id");
            assertThat(ordered.getRowCount()).isEqualTo(4);
            assertThat(ordered.getMaterializedRows().get(0).getField(1)).isEqualTo(100.0);
            assertThat(ordered.getMaterializedRows().get(3).getField(1)).isEqualTo(400.0);
        }
        finally {
            tryDropTable("test_schema.part_ym");
        }
    }

    @Test
    public void testPartitionedCtas()
    {
        computeActual("CREATE TABLE test_schema.part_ctas_src (id INTEGER, region VARCHAR, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.part_ctas_src VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0)");

            computeActual("CREATE TABLE test_schema.part_ctas_dst " +
                    "WITH (partitioned_by = ARRAY['region']) AS " +
                    "SELECT * FROM test_schema.part_ctas_src");

            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.part_ctas_dst");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            MaterializedResult usData = computeActual(
                    "SELECT id FROM test_schema.part_ctas_dst WHERE region = 'US' ORDER BY id");
            assertThat(usData.getRowCount()).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.part_ctas_src");
            tryDropTable("test_schema.part_ctas_dst");
        }
    }

    @Test
    public void testPartitionedMultipleInserts()
    {
        computeActual("CREATE TABLE test_schema.part_multi (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_multi VALUES (1, 'US', 10.0), (2, 'EU', 20.0)");
            computeActual("INSERT INTO test_schema.part_multi VALUES (3, 'US', 30.0), (4, 'APAC', 40.0)");
            computeActual("INSERT INTO test_schema.part_multi VALUES (5, 'EU', 50.0)");

            MaterializedResult total = computeActual("SELECT count(*) FROM test_schema.part_multi");
            assertThat(total.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            MaterializedResult perRegion = computeActual(
                    "SELECT region, count(*) as cnt FROM test_schema.part_multi GROUP BY region ORDER BY region");
            assertThat(perRegion.getRowCount()).isEqualTo(3);
            // APAC=1, EU=2, US=2
            List<MaterializedRow> rows = perRegion.getMaterializedRows();
            assertThat(rows.get(0).getField(0)).isEqualTo("APAC");
            assertThat(rows.get(0).getField(1)).isEqualTo(1L);
            assertThat(rows.get(1).getField(0)).isEqualTo("EU");
            assertThat(rows.get(1).getField(1)).isEqualTo(2L);
            assertThat(rows.get(2).getField(0)).isEqualTo("US");
            assertThat(rows.get(2).getField(1)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.part_multi");
        }
    }

    @Test
    public void testPartitionedWithNullPartitionValues()
    {
        computeActual("CREATE TABLE test_schema.part_nulls (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_nulls VALUES " +
                    "(1, 'US', 10.0), (2, NULL, 20.0), (3, 'EU', 30.0)");

            MaterializedResult total = computeActual("SELECT count(*) FROM test_schema.part_nulls");
            assertThat(total.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            MaterializedResult nullRegion = computeActual(
                    "SELECT id FROM test_schema.part_nulls WHERE region IS NULL");
            assertThat(nullRegion.getRowCount()).isEqualTo(1);
            assertThat(nullRegion.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.part_nulls");
        }
    }
}
