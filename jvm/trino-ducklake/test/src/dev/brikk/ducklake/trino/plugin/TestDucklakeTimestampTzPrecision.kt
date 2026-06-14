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

/**
 * `TIMESTAMP WITH TIME ZONE` precision on non-parquet writes. DuckLake's timestamptz is
 * microsecond-only (DuckDB has a single `TIMESTAMPTZ` = micros), so any declared precision is
 * stored as micros and reads back as `TIMESTAMP(6) WITH TIME ZONE`.
 *
 * Regression for the short-precision CTAS crash: a `CREATE TABLE AS SELECT` of a
 * short-precision tstz (precision ≤ 3 — including Trino's *default* `TIMESTAMP WITH TIME ZONE`,
 * which is precision 3) used to hand the Arrow writer a precision-6 (long / Fixed12) column
 * type while the CTAS streamed the source's short (LongArray) blocks — a `LongArrayBlock
 * cannot be cast to Fixed12Block` in the stats accumulator (vortex/lance) or an opaque "Failed
 * to close writer" (duckdb). beginCreateTable now gives the write handle the originally-declared
 * tstz type so the writer's short/long branch matches the blocks; the value still widens to
 * micros on read. INSERT into a declared tstz column already worked (engine coerces to micros).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeTimestampTzPrecision : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "tstz-precision"

    @Test
    fun ctasShortAndLongPrecisionTstzRoundTripOnDuckDb() = assertCtasRoundTrips(DucklakeSessionProperties.FORMAT_DUCKDB)

    @Test
    fun ctasShortAndLongPrecisionTstzRoundTripOnVortex() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
        assertCtasRoundTrips(DucklakeSessionProperties.FORMAT_VORTEX)
    }

    @Test
    fun ctasShortAndLongPrecisionTstzRoundTripOnLance() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        assertCtasRoundTrips(DucklakeSessionProperties.FORMAT_LANCE)
    }

    /**
     * CTAS each of precision 0 / 3 / 6 (short, short, long) — the short ones are the regressors —
     * and confirm the instant round-trips and the column reads back as micros (precision 6).
     */
    private fun assertCtasRoundTrips(format: String) {
        for (precision in listOf(0, 3, 6)) {
            val table = "tstz_ctas_${format}_$precision"
            try {
                computeActual("CREATE TABLE $table WITH (data_file_format = '$format') AS " +
                        "SELECT CAST(TIMESTAMP '2023-06-15 12:00:00.123456 UTC' AS TIMESTAMP($precision) WITH TIME ZONE) AS ts")

                // The catalog stores micros, so the column reads back at precision 6.
                val declaredType = computeActual("SHOW COLUMNS FROM $table").materializedRows
                        .first { it.getField(0) == "ts" }.getField(1) as String
                assertThat(declaredType).isEqualTo("timestamp(6) with time zone")

                // The instant is preserved (truncated to the declared precision before micros widening).
                assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(1L)
                assertThat(computeScalar(
                        "SELECT count(*) FROM $table WHERE ts > TIMESTAMP '2000-01-01 00:00:00 UTC'"))
                        .isEqualTo(1L)
            }
            finally {
                tryDropTable(table)
            }
        }
    }
}
