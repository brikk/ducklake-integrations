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
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DucklakeTableProperties
{
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private static final Pattern TRANSFORM_PATTERN = Pattern.compile("(year|month|day|hour)\\((.+)\\)");

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
                        value -> value));
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
        Matcher matcher = TRANSFORM_PATTERN.matcher(entry.trim());
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
        return new PartitionFieldSpec(entry.trim(), DucklakePartitionTransform.IDENTITY);
    }
}
