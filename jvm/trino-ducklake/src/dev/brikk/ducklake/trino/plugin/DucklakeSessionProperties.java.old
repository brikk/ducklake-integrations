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
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class DucklakeSessionProperties
{
    public static final String READ_SNAPSHOT_ID = "read_snapshot_id";
    public static final String READ_SNAPSHOT_TIMESTAMP = "read_snapshot_timestamp";
    public static final String DATA_FILE_FORMAT = "data_file_format";

    public static final String FORMAT_PARQUET = "parquet";
    public static final String FORMAT_DUCKDB = "duckdb";

    public static final String DUCKDB_WRITER_MODE = "duckdb_writer_mode";

    public static final String WRITER_MODE_APPENDER = "appender";
    public static final String WRITER_MODE_ARROW_STREAM = "arrow_stream";

    public static final String DUCKDB_READ_MODE = "duckdb_read_mode";

    public static final String READ_MODE_MATERIALIZE = "materialize";
    public static final String READ_MODE_HTTPFS = "httpfs";
    public static final String READ_MODE_AUTO = "auto";

    /**
     * Step 4 chunk 3: enable Tier C pushdown — date/time functions on
     * {@code TIMESTAMP WITH TIME ZONE} arguments. Off by default while the
     * cross-engine semantic corpus burns in; flip after staging confirms no
     * regressions. The translator gates WTZ-arg entries on this property; when
     * off, predicates over WTZ columns stay above the scan (Trino re-evaluates,
     * correct but slower). When on, the predicates push to DuckDB which
     * interprets the WTZ instants using the session {@code TimeZone} that chunk
     * 2's plumbing set on attach.
     *
     * <p>Correctness is bounded by chunk 2's {@code SET TimeZone} success: if
     * that failed (rare — only for fractional bare-offset session zones DuckDB
     * can't normalise), Tier C results may diverge from Trino's reference. The
     * one-shot WARN logged by both executors is the breadcrumb. See
     * dev-docs/archive/REPORT-datetime-tz-handling.md.
     */
    public static final String PUSHDOWN_TIMESTAMP_WITH_TIMEZONE = "pushdown_timestamp_with_timezone";

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
                        false),
                stringProperty(
                        DATA_FILE_FORMAT,
                        "Data file format for new writes in this session: 'parquet' or 'duckdb'. " +
                                "When unset (default), writes inherit the format of the most recent existing data file " +
                                "in the table (or fall back to 'parquet' for an empty table). An explicit set on this " +
                                "property overrides that inheritance.",
                        null,
                        DucklakeSessionProperties::validateDataFileFormat,
                        false),
                stringProperty(
                        DUCKDB_WRITER_MODE,
                        "Writer implementation for the duckdb format: 'arrow_stream' (default — Page → Arrow → INSERT FROM registered stream, columnar) or 'appender' (JDBC Appender, per-cell JNI; kept for comparison). No effect when data_file_format is 'parquet'.",
                        WRITER_MODE_ARROW_STREAM,
                        DucklakeSessionProperties::validateDuckDbWriterMode,
                        false),
                stringProperty(
                        DUCKDB_READ_MODE,
                        "Read strategy for duckdb-format data files: 'httpfs' (default; DuckDB streams blocks from S3 directly via the httpfs extension and maintains its own warm cache across queries), 'materialize' (download whole .db file to local tmp then ATTACH; useful when many small reads dominate), or 'auto' (picks per-file based on ducklake.duckdb.auto-httpfs-threshold). No effect when data_file_format is 'parquet'.",
                        READ_MODE_HTTPFS,
                        DucklakeSessionProperties::validateDuckDbReadMode,
                        false),
                booleanProperty(
                        PUSHDOWN_TIMESTAMP_WITH_TIMEZONE,
                        "Enable pushdown of date/time predicates over TIMESTAMP WITH TIME ZONE columns " +
                                "to DuckDB on the duckdb-format read path. Off by default while the cross-engine " +
                                "semantic corpus burns in. When on, requires successful SET TimeZone on attach " +
                                "(automatic for all named IANA zones and integer-hour offsets; fractional bare " +
                                "offsets like '+05:30' get a one-shot WARN and the pushdown silently degrades " +
                                "to Trino-side evaluation for that attach). See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                        false,
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

    public static Optional<String> getDataFileFormat(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(DATA_FILE_FORMAT, String.class));
    }

    private static void validateDataFileFormat(String value)
    {
        // Validator only fires on explicit SET — null (unset) never reaches this method.
        if (!FORMAT_PARQUET.equalsIgnoreCase(value) && !FORMAT_DUCKDB.equalsIgnoreCase(value)) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    DATA_FILE_FORMAT + " must be one of: '" + FORMAT_PARQUET + "', '" + FORMAT_DUCKDB + "'");
        }
    }

    public static String getDuckDbWriterMode(ConnectorSession session)
    {
        return session.getProperty(DUCKDB_WRITER_MODE, String.class);
    }

    private static void validateDuckDbWriterMode(String value)
    {
        if (!WRITER_MODE_APPENDER.equalsIgnoreCase(value) && !WRITER_MODE_ARROW_STREAM.equalsIgnoreCase(value)) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    DUCKDB_WRITER_MODE + " must be one of: '" + WRITER_MODE_APPENDER + "', '" + WRITER_MODE_ARROW_STREAM + "'");
        }
    }

    public static String getDuckDbReadMode(ConnectorSession session)
    {
        return session.getProperty(DUCKDB_READ_MODE, String.class);
    }

    /**
     * @return {@code true} iff the session has opted into Tier C pushdown
     *         (date/time predicates over TIMESTAMP WITH TIME ZONE columns).
     *         Default {@code false}. A {@code null} session (the test overload of
     *         {@code translateConjuncts} that doesn't have one) reads as off,
     *         which keeps the safer "don't push WTZ" behaviour for unit tests
     *         that synthesize calls without a session.
     */
    public static boolean isPushdownTimestampWithTimeZone(ConnectorSession session)
    {
        if (session == null) {
            return false;
        }
        Boolean v = session.getProperty(PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, Boolean.class);
        return v != null && v;
    }

    private static void validateDuckDbReadMode(String value)
    {
        if (!READ_MODE_MATERIALIZE.equalsIgnoreCase(value)
                && !READ_MODE_HTTPFS.equalsIgnoreCase(value)
                && !READ_MODE_AUTO.equalsIgnoreCase(value)) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    DUCKDB_READ_MODE + " must be one of: '" + READ_MODE_MATERIALIZE + "', '" + READ_MODE_HTTPFS + "', '" + READ_MODE_AUTO + "'");
        }
    }
}
