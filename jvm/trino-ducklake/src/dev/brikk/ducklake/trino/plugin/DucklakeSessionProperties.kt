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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.session.PropertyMetadata
import io.trino.spi.session.PropertyMetadata.booleanProperty
import io.trino.spi.session.PropertyMetadata.longProperty
import io.trino.spi.session.PropertyMetadata.stringProperty
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.OptionalLong

public open class DucklakeSessionProperties @Inject constructor() {
    private val sessionProperties: List<PropertyMetadata<*>>

    init {
        sessionProperties = ImmutableList.of(
                longProperty(
                        READ_SNAPSHOT_ID,
                        "Optional DuckLake snapshot ID to pin reads in this session",
                        null,
                        { value ->
                            if (value <= 0) {
                                throw TrinoException(INVALID_SESSION_PROPERTY, READ_SNAPSHOT_ID + " must be greater than 0")
                            }
                        },
                        false),
                stringProperty(
                        READ_SNAPSHOT_TIMESTAMP,
                        "Optional DuckLake snapshot timestamp (ISO-8601 instant) to pin reads in this session",
                        null,
                        { value -> parseSnapshotTimestamp(value); Unit },
                        false),
                stringProperty(
                        DATA_FILE_FORMAT,
                        "Data file format for new writes in this session: 'parquet' or 'duckdb'. " +
                                "When unset (default), writes inherit the format of the most recent existing data file " +
                                "in the table (or fall back to 'parquet' for an empty table). An explicit set on this " +
                                "property overrides that inheritance.",
                        null,
                        { value -> validateDataFileFormat(value) },
                        false),
                stringProperty(
                        DUCKDB_WRITER_MODE,
                        "Writer implementation for the duckdb format: 'arrow_stream' (default — Page → Arrow → INSERT FROM registered stream, columnar) or 'appender' (JDBC Appender, per-cell JNI; kept for comparison). No effect when data_file_format is 'parquet'.",
                        WRITER_MODE_ARROW_STREAM,
                        { value -> validateDuckDbWriterMode(value) },
                        false),
                stringProperty(
                        DUCKDB_READ_MODE,
                        "Read strategy for duckdb-format data files: 'httpfs' (default; DuckDB streams blocks from S3 directly via the httpfs extension and maintains its own warm cache across queries), 'materialize' (download whole .db file to local tmp then ATTACH; useful when many small reads dominate), or 'auto' (picks per-file based on ducklake.duckdb.auto-httpfs-threshold). No effect when data_file_format is 'parquet'.",
                        READ_MODE_HTTPFS,
                        { value -> validateDuckDbReadMode(value) },
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
                        false))
    }

    public open fun getSessionProperties(): List<PropertyMetadata<*>> {
        return sessionProperties
    }

    public companion object {
        public const val READ_SNAPSHOT_ID: String = "read_snapshot_id"
        public const val READ_SNAPSHOT_TIMESTAMP: String = "read_snapshot_timestamp"
        public const val DATA_FILE_FORMAT: String = "data_file_format"

        public const val FORMAT_PARQUET: String = "parquet"
        public const val FORMAT_DUCKDB: String = "duckdb"

        public const val DUCKDB_WRITER_MODE: String = "duckdb_writer_mode"

        public const val WRITER_MODE_APPENDER: String = "appender"
        public const val WRITER_MODE_ARROW_STREAM: String = "arrow_stream"

        public const val DUCKDB_READ_MODE: String = "duckdb_read_mode"

        public const val READ_MODE_MATERIALIZE: String = "materialize"
        public const val READ_MODE_HTTPFS: String = "httpfs"
        public const val READ_MODE_AUTO: String = "auto"

        /**
         * Step 4 chunk 3: enable Tier C pushdown — date/time functions on
         * `TIMESTAMP WITH TIME ZONE` arguments. Off by default while the
         * cross-engine semantic corpus burns in; flip after staging confirms no
         * regressions. The translator gates WTZ-arg entries on this property; when
         * off, predicates over WTZ columns stay above the scan (Trino re-evaluates,
         * correct but slower). When on, the predicates push to DuckDB which
         * interprets the WTZ instants using the session `TimeZone` that chunk
         * 2's plumbing set on attach.
         *
         *
         * Correctness is bounded by chunk 2's `SET TimeZone` success: if
         * that failed (rare — only for fractional bare-offset session zones DuckDB
         * can't normalise), Tier C results may diverge from Trino's reference. The
         * one-shot WARN logged by both executors is the breadcrumb. See
         * dev-docs/archive/REPORT-datetime-tz-handling.md.
         */
        public const val PUSHDOWN_TIMESTAMP_WITH_TIMEZONE: String = "pushdown_timestamp_with_timezone"

        @JvmStatic
        public fun getReadSnapshotId(session: ConnectorSession): OptionalLong {
            val snapshotId = session.getProperty(READ_SNAPSHOT_ID, Long::class.javaObjectType)
            return if (snapshotId == null) OptionalLong.empty() else OptionalLong.of(snapshotId)
        }

        @JvmStatic
        public fun getReadSnapshotTimestamp(session: ConnectorSession): Optional<Instant> {
            val timestamp = session.getProperty(READ_SNAPSHOT_TIMESTAMP, String::class.java)
            if (timestamp == null) {
                return Optional.empty()
            }
            // The stored value was already validated by parseSnapshotTimestamp as the SET-time
            // stringProperty validator, so parse directly here — Instant.parse is deterministic
            // and cannot fail on the same string twice. Re-wrapping in the validator's try/catch
            // was dead defensiveness that also nominally moved the failure surface from SET time
            // to query-execution time.
            return Optional.of(Instant.parse(timestamp))
        }

        @JvmStatic
        private fun parseSnapshotTimestamp(value: String): Instant {
            try {
                return Instant.parse(value)
            }
            catch (e: DateTimeParseException) {
                throw TrinoException(INVALID_SESSION_PROPERTY, READ_SNAPSHOT_TIMESTAMP + " must be an ISO-8601 instant (example: 2024-01-01T00:00:00Z)")
            }
        }

        @JvmStatic
        public fun getDataFileFormat(session: ConnectorSession): Optional<String> {
            return Optional.ofNullable(session.getProperty(DATA_FILE_FORMAT, String::class.java))
        }

        @JvmStatic
        private fun validateDataFileFormat(value: String) {
            // Validator only fires on explicit SET — null (unset) never reaches this method.
            if (!FORMAT_PARQUET.equals(value, ignoreCase = true) && !FORMAT_DUCKDB.equals(value, ignoreCase = true)) {
                throw TrinoException(
                        INVALID_SESSION_PROPERTY,
                        DATA_FILE_FORMAT + " must be one of: '" + FORMAT_PARQUET + "', '" + FORMAT_DUCKDB + "'")
            }
        }

        @JvmStatic
        public fun getDuckDbWriterMode(session: ConnectorSession): String {
            return session.getProperty(DUCKDB_WRITER_MODE, String::class.java)
        }

        @JvmStatic
        private fun validateDuckDbWriterMode(value: String) {
            if (!WRITER_MODE_APPENDER.equals(value, ignoreCase = true) && !WRITER_MODE_ARROW_STREAM.equals(value, ignoreCase = true)) {
                throw TrinoException(
                        INVALID_SESSION_PROPERTY,
                        DUCKDB_WRITER_MODE + " must be one of: '" + WRITER_MODE_APPENDER + "', '" + WRITER_MODE_ARROW_STREAM + "'")
            }
        }

        @JvmStatic
        public fun getDuckDbReadMode(session: ConnectorSession): String {
            return session.getProperty(DUCKDB_READ_MODE, String::class.java)
        }

        /**
         * @return `true` iff the session has opted into Tier C pushdown
         * (date/time predicates over TIMESTAMP WITH TIME ZONE columns).
         * Default `false`. A `null` session (the test overload of
         * `translateConjuncts` that doesn't have one) reads as off,
         * which keeps the safer "don't push WTZ" behaviour for unit tests
         * that synthesize calls without a session.
         */
        @JvmStatic
        public fun isPushdownTimestampWithTimeZone(session: ConnectorSession?): Boolean {
            if (session == null) {
                return false
            }
            val v = session.getProperty(PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, Boolean::class.javaObjectType)
            return v != null && v
        }

        @JvmStatic
        private fun validateDuckDbReadMode(value: String) {
            if (!READ_MODE_MATERIALIZE.equals(value, ignoreCase = true)
                    && !READ_MODE_HTTPFS.equals(value, ignoreCase = true)
                    && !READ_MODE_AUTO.equals(value, ignoreCase = true)) {
                throw TrinoException(
                        INVALID_SESSION_PROPERTY,
                        DUCKDB_READ_MODE + " must be one of: '" + READ_MODE_MATERIALIZE + "', '" + READ_MODE_HTTPFS + "', '" + READ_MODE_AUTO + "'")
            }
        }
    }
}
