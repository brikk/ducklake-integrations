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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class DucklakeSessionProperties
{
    public static final String READ_SNAPSHOT_ID = "read_snapshot_id";
    public static final String READ_SNAPSHOT_TIMESTAMP = "read_snapshot_timestamp";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DucklakeSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                longProperty(
                        READ_SNAPSHOT_ID,
                        "Optional DuckLake snapshot ID to pin reads in this session",
                        null,
                        value -> {
                            if (value <= 0) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, READ_SNAPSHOT_ID + " must be greater than 0");
                            }
                        },
                        false),
                stringProperty(
                        READ_SNAPSHOT_TIMESTAMP,
                        "Optional DuckLake snapshot timestamp (ISO-8601 instant) to pin reads in this session",
                        null,
                        value -> parseSnapshotTimestamp(value),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static OptionalLong getReadSnapshotId(ConnectorSession session)
    {
        Long snapshotId = session.getProperty(READ_SNAPSHOT_ID, Long.class);
        return snapshotId == null ? OptionalLong.empty() : OptionalLong.of(snapshotId);
    }

    public static Optional<Instant> getReadSnapshotTimestamp(ConnectorSession session)
    {
        String timestamp = session.getProperty(READ_SNAPSHOT_TIMESTAMP, String.class);
        if (timestamp == null) {
            return Optional.empty();
        }
        return Optional.of(parseSnapshotTimestamp(timestamp));
    }

    private static Instant parseSnapshotTimestamp(String value)
    {
        try {
            return Instant.parse(value);
        }
        catch (DateTimeParseException e) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, READ_SNAPSHOT_TIMESTAMP + " must be an ISO-8601 instant (example: 2024-01-01T00:00:00Z)");
        }
    }
}
