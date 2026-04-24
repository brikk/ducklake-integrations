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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDucklakeCatalogSnapshotTimeParsing
{
    private static final Method PARSE_SNAPSHOT_TIME = getParseSnapshotTimeMethod();

    @Test
    public void testParsesOffsetWithoutMinutes()
    {
        Instant parsed = parseSnapshotTime("2024-01-01 00:00:00.123456-06");
        assertThat(parsed).isEqualTo(OffsetDateTime.parse("2024-01-01T00:00:00.123456-06:00").toInstant());
    }

    @Test
    public void testParsesOffsetWithoutColon()
    {
        Instant parsed = parseSnapshotTime("2024-01-01 00:00:00.123456-0600");
        assertThat(parsed).isEqualTo(OffsetDateTime.parse("2024-01-01T00:00:00.123456-06:00").toInstant());
    }

    @Test
    public void testParsesIsoStyleOffset()
    {
        Instant parsed = parseSnapshotTime("2024-01-01 00:00:00.123456-06:00");
        assertThat(parsed).isEqualTo(OffsetDateTime.parse("2024-01-01T00:00:00.123456-06:00").toInstant());
    }

    private static Instant parseSnapshotTime(String value)
    {
        try {
            return (Instant) PARSE_SNAPSHOT_TIME.invoke(null, value);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to invoke JdbcDucklakeCatalog#parseSnapshotTime", e);
        }
    }

    private static Method getParseSnapshotTimeMethod()
    {
        try {
            Method method = JdbcDucklakeCatalog.class.getDeclaredMethod("parseSnapshotTime", String.class);
            method.setAccessible(true);
            return method;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to access JdbcDucklakeCatalog#parseSnapshotTime", e);
        }
    }
}
