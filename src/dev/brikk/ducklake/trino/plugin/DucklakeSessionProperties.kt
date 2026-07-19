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

open class DucklakeSessionProperties @Inject constructor() {
    private val sessionProperties: List<PropertyMetadata<*>> = ImmutableList.of(
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
                    "Data file format for new writes in this session. Only 'parquet' is supported. " +
                            "When unset (default), writes inherit the format of the most recent existing data file " +
                            "in the table (or fall back to 'parquet' for an empty table).",
                    null,
                    { value -> validateDataFileFormat(value) },
                    false),
            booleanProperty(
                    WRITE_DELETION_VECTORS,
                    "Write DELETE/UPDATE/MERGE tombstones as DuckLake puffin deletion-vector (.puffin) " +
                            "files (a Roaring bitmap of file-local positions) instead of parquet " +
                            "(file_path, pos) delete files. Off by default. Both shapes are read by Trino " +
                            "AND DuckDB; this mirrors DuckDB's write_deletion_vectors table option.",
                    false,
                    false),
            booleanProperty(
                    WRITE_ROW_LINEAGE,
                    "Preserve row lineage on UPDATE/MERGE: rewritten rows carry their ORIGINAL DuckLake " +
                            "rowid in an embedded _ducklake_internal_row_id parquet column (field-id " +
                            "2147483540), so change feeds (Trino AND DuckDB) pair the rewrite into " +
                            "update_preimage/update_postimage instead of delete+insert, and rowids stay " +
                            "stable across updates. Parquet data files only. ON by default — DuckDB " +
                            "preserves lineage unconditionally, so this is the cross-engine-faithful " +
                            "behavior; set false for the legacy fresh-rowid delete+insert shape.",
                    true,
                    false))

    open fun getSessionProperties(): List<PropertyMetadata<*>> = sessionProperties

    companion object {
        const val READ_SNAPSHOT_ID: String = "read_snapshot_id"
        const val READ_SNAPSHOT_TIMESTAMP: String = "read_snapshot_timestamp"
        const val DATA_FILE_FORMAT: String = "data_file_format"

        const val FORMAT_PARQUET: String = "parquet"

        /**
         * Accepted `data_file_format` values (lowercase) — single source of truth for the
         * validators. Parquet only: the experimental duckdb/vortex/lance data-file formats were
         * removed (see PLAN-duckdb-parity-moveout.md §4.2 / brikk/duckbridge).
         */
        val SUPPORTED_DATA_FILE_FORMATS: Set<String> = setOf(FORMAT_PARQUET)

        const val WRITE_DELETION_VECTORS: String = "write_deletion_vectors"

        /** @return true iff this session writes tombstones as puffin deletion-vector files. Default false. */
        fun isWriteDeletionVectors(session: ConnectorSession): Boolean {
            val v = session.getProperty(WRITE_DELETION_VECTORS, Boolean::class.javaObjectType)
            return v != null && v
        }

        const val WRITE_ROW_LINEAGE: String = "write_row_lineage"

        /** @return true iff UPDATE/MERGE rewrites embed the original rowid (lineage). Default false. */
        fun isWriteRowLineage(session: ConnectorSession): Boolean {
            val v = session.getProperty(WRITE_ROW_LINEAGE, Boolean::class.javaObjectType)
            return v != null && v
        }

        fun getReadSnapshotId(session: ConnectorSession): OptionalLong =
            session.getProperty(READ_SNAPSHOT_ID, Long::class.javaObjectType)
                ?.let { OptionalLong.of(it) }
                ?: OptionalLong.empty()

        fun getReadSnapshotTimestamp(session: ConnectorSession): Optional<Instant> {
            val timestamp = session.getProperty(READ_SNAPSHOT_TIMESTAMP, String::class.java) ?: return Optional.empty()
            // The stored value was already validated by parseSnapshotTimestamp as the SET-time
            // stringProperty validator, so parse directly here — Instant.parse is deterministic
            // and cannot fail on the same string twice. Re-wrapping in the validator's try/catch
            // was dead defensiveness that also nominally moved the failure surface from SET time
            // to query-execution time.
            return Optional.of(Instant.parse(timestamp))
        }

        private fun parseSnapshotTimestamp(value: String): Instant {
            try {
                return Instant.parse(value)
            }
            catch (e: DateTimeParseException) {
                throw TrinoException(INVALID_SESSION_PROPERTY,
                    "$READ_SNAPSHOT_TIMESTAMP must be an ISO-8601 instant (example: 2024-01-01T00:00:00Z)"
                )
            }
        }

        fun getDataFileFormat(session: ConnectorSession): Optional<String> {
            return Optional.ofNullable(session.getProperty(DATA_FILE_FORMAT, String::class.java))
        }

        private fun validateDataFileFormat(value: String) {
            // Validator only fires on explicit SET — null (unset) never reaches this method.
            if (value.lowercase() !in SUPPORTED_DATA_FILE_FORMATS) {
                throw TrinoException(
                        INVALID_SESSION_PROPERTY,
                    "$DATA_FILE_FORMAT must be one of: ${SUPPORTED_DATA_FILE_FORMATS.joinToString(", ") { "'$it'" }}"
                )
            }
        }

    }
}
