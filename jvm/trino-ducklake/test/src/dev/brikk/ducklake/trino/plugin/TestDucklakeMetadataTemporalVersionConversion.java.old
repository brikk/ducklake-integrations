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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeMetadataTemporalVersionConversion
{
    private static final Method GET_SNAPSHOT_TIMESTAMP_FROM_VERSION = getSnapshotTimestampMethod();

    @Test
    public void testDateVersionUsesSessionTimeZone()
    {
        ConnectorTableVersion dateVersion = new ConnectorTableVersion(
                PointerType.TEMPORAL,
                DATE,
                LocalDate.of(2024, 6, 15).toEpochDay());

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), dateVersion))
                .isEqualTo(Instant.parse("2024-06-15T00:00:00Z"));
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), dateVersion))
                .isEqualTo(Instant.parse("2024-06-14T15:00:00Z"));
    }

    @Test
    public void testTimestampVersionUsesSessionTimeZone()
    {
        LocalDateTime timestamp = LocalDateTime.of(2024, 6, 15, 8, 30, 45, 123_456_000);
        long epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + (timestamp.getNano() / 1_000);
        ConnectorTableVersion timestampVersion = new ConnectorTableVersion(PointerType.TEMPORAL, TIMESTAMP_MICROS, epochMicros);

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), timestampVersion))
                .isEqualTo(Instant.parse("2024-06-15T08:30:45.123456Z"));
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), timestampVersion))
                .isEqualTo(Instant.parse("2024-06-14T23:30:45.123456Z"));
    }

    @Test
    public void testTimestampWithTimeZoneVersionIgnoresSessionTimeZone()
    {
        Instant instant = Instant.parse("2024-06-15T08:30:45.123Z");
        long packedTimestamp = packDateTimeWithZone(instant.toEpochMilli(), getTimeZoneKey("Europe/Paris"));
        ConnectorTableVersion timestampWithTimeZoneVersion = new ConnectorTableVersion(
                PointerType.TEMPORAL,
                TIMESTAMP_TZ_MILLIS,
                packedTimestamp);

        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("UTC"), timestampWithTimeZoneVersion))
                .isEqualTo(instant);
        assertThat(getSnapshotTimestampFromVersion(sessionWithTimeZone("Asia/Tokyo"), timestampWithTimeZoneVersion))
                .isEqualTo(instant);
    }

    private static ConnectorSession sessionWithTimeZone(String timeZoneId)
    {
        return TestingConnectorSession.builder()
                .setTimeZoneKey(getTimeZoneKey(timeZoneId))
                .build();
    }

    private static Instant getSnapshotTimestampFromVersion(ConnectorSession session, ConnectorTableVersion version)
    {
        try {
            return (Instant) GET_SNAPSHOT_TIMESTAMP_FROM_VERSION.invoke(null, session, version);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to invoke DucklakeMetadata#getSnapshotTimestampFromVersion", e);
        }
    }

    private static Method getSnapshotTimestampMethod()
    {
        try {
            Method method = DucklakeMetadata.class.getDeclaredMethod(
                    "getSnapshotTimestampFromVersion",
                    ConnectorSession.class,
                    ConnectorTableVersion.class);
            method.setAccessible(true);
            return method;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to access DucklakeMetadata#getSnapshotTimestampFromVersion", e);
        }
    }
}
