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

import java.util.Locale;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum DucklakePartitionTransform
{
    IDENTITY,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    BUCKET;

    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);

    public boolean isIdentity()
    {
        return this == IDENTITY;
    }

    public boolean isTemporal()
    {
        return this == YEAR || this == MONTH || this == DAY || this == HOUR;
    }

    public boolean isBucket()
    {
        return this == BUCKET;
    }

    public static DucklakePartitionTransform fromString(String value)
    {
        return valueOf(value.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Parse the catalog-stored transform string from {@code ducklake_partition_column.transform}.
     * Returns the transform kind plus, for {@code bucket(N)}, the arity {@code N}.
     * Simple kinds ({@code identity}, {@code year}, {@code month}, {@code day}, {@code hour})
     * return an empty arity.
     */
    public static ParsedTransform parseCatalogTransform(String value)
    {
        Matcher matcher = BUCKET_PATTERN.matcher(value.trim());
        if (matcher.matches()) {
            int arity = Integer.parseInt(matcher.group(1));
            return new ParsedTransform(BUCKET, OptionalInt.of(arity));
        }
        return new ParsedTransform(fromString(value), OptionalInt.empty());
    }

    /**
     * Produce the catalog string form for {@code ducklake_partition_column.transform}.
     * {@code identity}/{@code year}/{@code month}/{@code day}/{@code hour} are lowercase
     * enum names; {@code BUCKET} is {@code bucket(N)} with {@code N} the field's arity.
     */
    public String toCatalogString(OptionalInt arity)
    {
        if (this == BUCKET) {
            if (arity.isEmpty()) {
                throw new IllegalArgumentException("BUCKET transform requires an arity");
            }
            return "bucket(" + arity.getAsInt() + ")";
        }
        return name().toLowerCase(Locale.ENGLISH);
    }

    public record ParsedTransform(DucklakePartitionTransform transform, OptionalInt arity) {}
}
