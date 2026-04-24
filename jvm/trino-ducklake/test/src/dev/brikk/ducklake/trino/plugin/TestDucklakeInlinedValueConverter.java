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

import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeInlinedValueConverter
{
    @Test
    public void testConvertTimestampMicrosFromString()
    {
        Object converted = DucklakeInlinedValueConverter.convertJdbcValue(
                "2024-01-01 12:34:56.123456",
                TIMESTAMP_MICROS);

        long expectedMicros = LocalDateTime.parse("2024-01-01T12:34:56.123456")
                .toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123_456;
        assertThat(converted).isEqualTo(expectedMicros);
    }

    @Test
    public void testConvertTimestampWithTimeZoneMicrosFromOffsetString()
    {
        Object converted = DucklakeInlinedValueConverter.convertJdbcValue(
                "2024-01-01 00:00:00.123456-06",
                TIMESTAMP_TZ_MICROS);

        assertThat(converted).isInstanceOf(LongTimestampWithTimeZone.class);
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) converted;

        Instant expectedInstant = OffsetDateTime.parse("2024-01-01T00:00:00.123456-06:00").toInstant();
        assertThat(timestamp.getEpochMillis()).isEqualTo(expectedInstant.toEpochMilli());
        assertThat(timestamp.getPicosOfMilli()).isEqualTo((expectedInstant.getNano() % 1_000_000) * 1_000);
        assertThat(timestamp.getTimeZoneKey()).isEqualTo(UTC_KEY.getKey());
    }

    @Test
    public void testConvertShortTimestampWithTimeZoneUsesUtcZoneKey()
    {
        TimestampWithTimeZoneType shortTimestampWithTimeZone = TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
        Object converted = DucklakeInlinedValueConverter.convertJdbcValue(
                "2024-01-01 00:00:00.123-06:00",
                shortTimestampWithTimeZone);

        assertThat(converted).isInstanceOf(Long.class);
        long packed = (long) converted;
        Instant expectedInstant = OffsetDateTime.parse("2024-01-01T00:00:00.123-06:00").toInstant();
        assertThat(unpackMillisUtc(packed)).isEqualTo(expectedInstant.toEpochMilli());
        assertThat(unpackZoneKey(packed)).isEqualTo(UTC_KEY);
    }
}
