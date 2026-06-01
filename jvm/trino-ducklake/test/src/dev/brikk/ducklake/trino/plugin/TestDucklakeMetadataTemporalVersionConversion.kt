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

import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableVersion
import io.trino.spi.connector.PointerType
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.TimeZoneKey.getTimeZoneKey
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.testing.TestingConnectorSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.reflect.Method
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

class TestDucklakeMetadataTemporalVersionConversion {
    @Test
    fun testDateVersionUsesSessionTimeZone() {
        val dateVersion = ConnectorTableVersion(
                PointerType.TEMPORAL,
                DATE,
                LocalDate.of(2024, 6, 15).toEpochDay())

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), dateVersion))
                .isEqualTo(Instant.parse("2024-06-15T00:00:00Z"))
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), dateVersion))
                .isEqualTo(Instant.parse("2024-06-14T15:00:00Z"))
    }

    @Test
    fun testTimestampVersionUsesSessionTimeZone() {
        val timestamp = LocalDateTime.of(2024, 6, 15, 8, 30, 45, 123_456_000)
        val epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + (timestamp.nano / 1_000)
        val timestampVersion = ConnectorTableVersion(PointerType.TEMPORAL, TIMESTAMP_MICROS, epochMicros)

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), timestampVersion))
                .isEqualTo(Instant.parse("2024-06-15T08:30:45.123456Z"))
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), timestampVersion))
                .isEqualTo(Instant.parse("2024-06-14T23:30:45.123456Z"))
    }

    @Test
    fun testTimestampWithTimeZoneVersionIgnoresSessionTimeZone() {
        val instant = Instant.parse("2024-06-15T08:30:45.123Z")
        val packedTimestamp = packDateTimeWithZone(instant.toEpochMilli(), getTimeZoneKey("Europe/Paris"))
        val timestampWithTimeZoneVersion = ConnectorTableVersion(
                PointerType.TEMPORAL,
                TIMESTAMP_TZ_MILLIS,
                packedTimestamp)

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), timestampWithTimeZoneVersion))
                .isEqualTo(instant)
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), timestampWithTimeZoneVersion))
                .isEqualTo(instant)
    }

    companion object {
        private val GET_SNAPSHOT_TIMESTAMP_FROM_VERSION: Method = getSnapshotTimestampMethod()

        private fun sessionWithTimeZone(timeZoneId: String): ConnectorSession {
            return TestingConnectorSession.builder()
                    .setTimeZoneKey(getTimeZoneKey(timeZoneId))
                    .build()
        }

        private fun getSnapshotTimestampFromVersion(session: ConnectorSession, version: ConnectorTableVersion): Instant {
            try {
                return GET_SNAPSHOT_TIMESTAMP_FROM_VERSION.invoke(null, session, version) as Instant
            }
            catch (e: ReflectiveOperationException) {
                throw RuntimeException("Unable to invoke DucklakeMetadata#getSnapshotTimestampFromVersion", e)
            }
        }

        private fun getSnapshotTimestampMethod(): Method {
            try {
                val method = DucklakeMetadata::class.java.getDeclaredMethod(
                        "getSnapshotTimestampFromVersion",
                        ConnectorSession::class.java,
                        ConnectorTableVersion::class.java)
                method.isAccessible = true
                return method
            }
            catch (e: ReflectiveOperationException) {
                throw RuntimeException("Unable to access DucklakeMetadata#getSnapshotTimestampFromVersion", e)
            }
        }
    }
}
