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
import dev.brikk.ducklake.catalog.DucklakePartitionTransform;
import dev.brikk.ducklake.catalog.PartitionFieldSpec;
import dev.brikk.ducklake.catalog.TableLocationSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_PARQUET;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DucklakeTableProperties
{
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String DATA_FILE_FORMAT_PROPERTY = "data_file_format";
    public static final String LOCATION_PROPERTY = "location";

    private static final Pattern TRANSFORM_PATTERN = Pattern.compile("(year|month|day|hour)\\((.+)\\)");
    // bucket(N, col) — N positive integer, col is the source column name. Spaces tolerated.
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\(\\s*(\\d+)\\s*,\\s*(.+?)\\s*\\)", Pattern.CASE_INSENSITIVE);
    // URI scheme prefix: <scheme>:// — e.g. s3://, gs://, file://, abfss://, hdfs://, gcs://
    private static final Pattern URI_SCHEME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+\\-.]*://");

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DucklakeTableProperties()
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns with optional transforms, e.g. ARRAY['region', 'year(event_date)']",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                stringProperty(
                        DATA_FILE_FORMAT_PROPERTY,
                        "Data file format for this table's CTAS payload: 'parquet' (default) or 'duckdb'. Overrides the session-level data_file_format for this CREATE only.",
                        null,
                        DucklakeTableProperties::validateDataFileFormat,
                        false),
                stringProperty(
                        LOCATION_PROPERTY,
                        "Storage path for this table's data files. Absolute (s3://bucket/foo/, file:///abs/path/) lands as-is; "
                                + "relative (e.g. 'special_dir/') resolves under the schema's data path.",
                        null,
                        DucklakeTableProperties::validateLocation,
                        false));
    }

    private static void validateDataFileFormat(String value)
    {
        if (value == null) {
            return;
        }
        if (!FORMAT_PARQUET.equalsIgnoreCase(value) && !FORMAT_DUCKDB.equalsIgnoreCase(value)) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    DATA_FILE_FORMAT_PROPERTY + " must be one of: '" + FORMAT_PARQUET + "', '" + FORMAT_DUCKDB + "'");
        }
    }

    /**
     * Returns the table property value if set, otherwise null. Callers should
     * fall back to {@link DucklakeSessionProperties#getDataFileFormat} when this
     * returns null.
     */
    public static String getDataFileFormat(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(DATA_FILE_FORMAT_PROPERTY);
    }

    /**
     * Returns a normalized {@link TableLocationSpec} when the user supplied
     * {@code location}, otherwise empty (caller should use the catalog's default
     * relative {@code <tableName>/} path). Normalization:
     * <ul>
     *   <li>trailing slash appended if missing (DuckLake convention)</li>
     *   <li>{@code path_is_relative=false} when the value starts with a URI scheme
     *       (e.g. {@code s3://}, {@code gs://}, {@code file://}, {@code abfss://}),
     *       otherwise relative</li>
     * </ul>
     * Validation (rejected as {@code INVALID_TABLE_PROPERTY}) happens in
     * {@link #validateLocation}.
     */
    public static Optional<TableLocationSpec> getLocation(Map<String, Object> tableProperties)
    {
        String raw = (String) tableProperties.get(LOCATION_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        String trimmed = raw.trim();
        boolean isAbsolute = URI_SCHEME_PATTERN.matcher(trimmed).find();
        String normalized = trimmed.endsWith("/") ? trimmed : trimmed + "/";
        return Optional.of(new TableLocationSpec(normalized, !isAbsolute));
    }

    private static void validateLocation(String value)
    {
        if (value == null) {
            return;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, LOCATION_PROPERTY + " must not be blank");
        }
        // Reject path traversal — split on both '/' and '\' to catch Windows-style attempts.
        for (String segment : trimmed.split("[/\\\\]")) {
            if (segment.equals("..")) {
                throw new TrinoException(
                        INVALID_TABLE_PROPERTY,
                        LOCATION_PROPERTY + " must not contain '..' path-traversal segments: " + value);
            }
        }
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<PartitionFieldSpec> getPartitionFields(Map<String, Object> tableProperties)
    {
        List<String> partitionBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        if (partitionBy == null || partitionBy.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<PartitionFieldSpec> fields = ImmutableList.builder();
        for (String entry : partitionBy) {
            fields.add(parsePartitionField(entry));
        }
        return fields.build();
    }

    private static PartitionFieldSpec parsePartitionField(String entry)
    {
        String trimmed = entry.trim();

        Matcher bucketMatcher = BUCKET_PATTERN.matcher(trimmed);
        if (bucketMatcher.matches()) {
            int arity;
            try {
                arity = Integer.parseInt(bucketMatcher.group(1));
            }
            catch (NumberFormatException e) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid bucket arity: " + bucketMatcher.group(1));
            }
            if (arity <= 0) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "bucket(N, col) requires a positive arity, got " + arity);
            }
            String columnName = bucketMatcher.group(2).trim();
            return new PartitionFieldSpec(columnName, DucklakePartitionTransform.BUCKET, OptionalInt.of(arity));
        }

        Matcher matcher = TRANSFORM_PATTERN.matcher(trimmed);
        if (matcher.matches()) {
            String transformName = matcher.group(1).toUpperCase(java.util.Locale.ENGLISH);
            String columnName = matcher.group(2).trim();
            DucklakePartitionTransform transform;
            try {
                transform = DucklakePartitionTransform.valueOf(transformName);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Unknown partition transform: " + matcher.group(1));
            }
            return new PartitionFieldSpec(columnName, transform);
        }

        // No transform — identity partition
        return new PartitionFieldSpec(trimmed, DucklakePartitionTransform.IDENTITY);
    }
}
