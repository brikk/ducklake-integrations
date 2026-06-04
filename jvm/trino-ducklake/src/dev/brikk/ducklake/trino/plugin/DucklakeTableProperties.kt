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
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.PartitionFieldSpec
import dev.brikk.ducklake.catalog.TableLocationSpec
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_DUCKDB
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_PARQUET
import io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY
import io.trino.spi.TrinoException
import io.trino.spi.session.PropertyMetadata
import io.trino.spi.session.PropertyMetadata.stringProperty
import io.trino.spi.type.ArrayType
import io.trino.spi.type.VarcharType.VARCHAR
import java.util.Locale
import java.util.Optional
import java.util.OptionalInt
import java.util.regex.Pattern

open class DucklakeTableProperties @Inject constructor() {
    val tableProperties: List<PropertyMetadata<*>>

    init {
        @Suppress("UNCHECKED_CAST")
        tableProperties = ImmutableList.of(
                PropertyMetadata(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns with optional transforms, e.g. ARRAY['region', 'year(event_date)']",
                        ArrayType(VARCHAR),
                        List::class.java as Class<List<*>>,
                        ImmutableList.of<Any>(),
                        false,
                        { value -> value as List<*> },
                        { value -> value }),
                stringProperty(
                        DATA_FILE_FORMAT_PROPERTY,
                        "Data file format for this table's CTAS payload: 'parquet' (default) or 'duckdb'. Overrides the session-level data_file_format for this CREATE only.",
                        null,
                        { value -> validateDataFileFormat(value) },
                        false),
                stringProperty(
                        LOCATION_PROPERTY,
                        "Storage path for this table's data files. Absolute (s3://bucket/foo/, file:///abs/path/) lands as-is; "
                                + "relative (e.g. 'special_dir/') resolves under the schema's data path.",
                        null,
                        { value -> validateLocation(value) },
                        false))
    }

    companion object {
        const val PARTITIONED_BY_PROPERTY: String = "partitioned_by"
        const val DATA_FILE_FORMAT_PROPERTY: String = "data_file_format"
        const val LOCATION_PROPERTY: String = "location"

        // year(col) / month(col) / day(col) / hour(col). Case-insensitive to match
        // BUCKET_PATTERN (SQL function names are conventionally case-insensitive, and
        // group(1) is upper-cased before the enum lookup anyway) — otherwise YEAR(d)
        // would silently fall through to an identity partition on a column literally
        // named "YEAR(d)". The column group rejects parens/commas ([^(),]+) so malformed
        // transforms like year(foo(bar)) or day(a,b) don't get swallowed into a bogus
        // column name; they fall through to identity and fail loudly as "column not found".
        private val TRANSFORM_PATTERN: Pattern = Pattern.compile("(year|month|day|hour)\\(([^(),]+)\\)", Pattern.CASE_INSENSITIVE)
        // bucket(N, col) — N positive integer, col is the source column name. Spaces tolerated.
        private val BUCKET_PATTERN: Pattern = Pattern.compile("bucket\\(\\s*(\\d+)\\s*,\\s*(.+?)\\s*\\)", Pattern.CASE_INSENSITIVE)
        // URI scheme prefix: <scheme>:// — e.g. s3://, gs://, file://, abfss://, hdfs://, gcs://
        private val URI_SCHEME_PATTERN: Pattern = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+\\-.]*://")

        private fun validateDataFileFormat(value: String?) {
            if (value == null) {
                return
            }
            if (!FORMAT_PARQUET.equals(value, ignoreCase = true) && !FORMAT_DUCKDB.equals(value, ignoreCase = true)) {
                throw TrinoException(
                        INVALID_TABLE_PROPERTY,
                        "$DATA_FILE_FORMAT_PROPERTY must be one of: '$FORMAT_PARQUET', '$FORMAT_DUCKDB'")
            }
        }

        /**
         * Returns the table property value if set, otherwise null. Callers should
         * fall back to [DucklakeSessionProperties.getDataFileFormat] when this
         * returns null.
         */
        @JvmStatic
        fun getDataFileFormat(tableProperties: Map<String, Any?>): String? {
            return tableProperties[DATA_FILE_FORMAT_PROPERTY] as String?
        }

        /**
         * Returns a normalized [TableLocationSpec] when the user supplied
         * `location`, otherwise empty (caller should use the catalog's default
         * relative `<tableName>/` path). Normalization:
         *
         *  * trailing slash appended if missing (DuckLake convention)
         *  * `path_is_relative=false` when the value starts with a URI scheme
         * (e.g. `s3://`, `gs://`, `file://`, `abfss://`),
         * otherwise relative
         *
         * Validation (rejected as `INVALID_TABLE_PROPERTY`) happens in
         * [validateLocation].
         */
        @JvmStatic
        fun getLocation(tableProperties: Map<String, Any?>): Optional<TableLocationSpec> {
            val raw = tableProperties[LOCATION_PROPERTY] as String?
            if (raw.isNullOrBlank()) {
                return Optional.empty()
            }
            val trimmed = raw.trim()
            val isAbsolute = URI_SCHEME_PATTERN.matcher(trimmed).find()
            val normalized = if (trimmed.endsWith("/")) trimmed else "$trimmed/"
            return Optional.of(TableLocationSpec(normalized, !isAbsolute))
        }

        private fun validateLocation(value: String?) {
            if (value == null) {
                return
            }
            val trimmed = value.trim()
            if (trimmed.isEmpty()) {
                throw TrinoException(INVALID_TABLE_PROPERTY, "$LOCATION_PROPERTY must not be blank")
            }
            // Reject path traversal — split on both '/' and '\' to catch Windows-style attempts.
            for (segment in trimmed.split("[/\\\\]".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                if (segment == "..") {
                    throw TrinoException(
                            INVALID_TABLE_PROPERTY,
                            "$LOCATION_PROPERTY must not contain '..' path-traversal segments: $value")
                }
            }
        }

        @JvmStatic
        @Suppress("UNCHECKED_CAST")
        fun getPartitionFields(tableProperties: Map<String, Any?>): List<PartitionFieldSpec> {
            val partitionBy = tableProperties[PARTITIONED_BY_PROPERTY] as List<String>?
            if (partitionBy.isNullOrEmpty()) {
                return emptyList()
            }
            return partitionBy.map { parsePartitionField(it) }
        }

        private fun parsePartitionField(entry: String): PartitionFieldSpec {
            val trimmed = entry.trim()

            val bucketMatcher = BUCKET_PATTERN.matcher(trimmed)
            if (bucketMatcher.matches()) {
                val arity: Int
                try {
                    arity = bucketMatcher.group(1).toInt()
                }
                catch (e: NumberFormatException) {
                    throw TrinoException(INVALID_TABLE_PROPERTY, "Invalid bucket arity: ${bucketMatcher.group(1)}")
                }
                if (arity <= 0) {
                    throw TrinoException(INVALID_TABLE_PROPERTY, "bucket(N, col) requires a positive arity, got $arity")
                }
                val columnName = bucketMatcher.group(2).trim()
                return PartitionFieldSpec(columnName, DucklakePartitionTransform.BUCKET, OptionalInt.of(arity))
            }

            val matcher = TRANSFORM_PATTERN.matcher(trimmed)
            if (matcher.matches()) {
                val transformName = matcher.group(1).uppercase(Locale.ENGLISH)
                val columnName = matcher.group(2).trim()
                val transform: DucklakePartitionTransform
                try {
                    transform = DucklakePartitionTransform.valueOf(transformName)
                }
                catch (e: IllegalArgumentException) {
                    throw TrinoException(INVALID_TABLE_PROPERTY, "Unknown partition transform: ${matcher.group(1)}")
                }
                return PartitionFieldSpec(columnName, transform)
            }

            // No transform — identity partition
            return PartitionFieldSpec(trimmed, DucklakePartitionTransform.IDENTITY)
        }
    }
}
