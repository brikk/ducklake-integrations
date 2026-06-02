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

import io.airlift.slice.Slices
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType

import java.time.LocalDate

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * Parses the string-encoded partition values stored in
 * {@code ducklake_file_partition_value.partition_value} into Trino native values.
 * Shared by the split manager's partition-pruning path (compares the parsed value
 * against the query's pushdown domain) and the page source provider's
 * partition-column projection (constants the parsed value across the file).
 *
 * <p>Encoding contract is the DuckLake 1.0 calendar form for temporals
 * (handled by {@code DucklakeTemporalPartitionMatcher}); this helper covers
 * identity-transform value parsing only.
 */
object DucklakePartitionValueParser
{
    fun parseIdentity(type: Type, value: String): Any
    {
        if (type.equals(VARCHAR) || type is VarcharType) {
            return Slices.utf8Slice(value)
        }
        if (type.equals(BIGINT)) {
            return java.lang.Long.parseLong(value)
        }
        if (type.equals(INTEGER)) {
            return Integer.parseInt(value).toLong()
        }
        if (type.equals(SMALLINT)) {
            return java.lang.Short.parseShort(value).toLong()
        }
        if (type.equals(TINYINT)) {
            return java.lang.Byte.parseByte(value).toLong()
        }
        if (type.equals(DOUBLE)) {
            return java.lang.Double.parseDouble(value)
        }
        if (type.equals(REAL)) {
            return java.lang.Float.floatToIntBits(java.lang.Float.parseFloat(value)).toLong()
        }
        if (type.equals(DATE)) {
            return LocalDate.parse(value).toEpochDay()
        }
        if (type.equals(BOOLEAN)) {
            // TODO(review:after id=lowtail-parseboolean-1-0-collision): Boolean.parseBoolean silently returns false for '1' and '0'
            return java.lang.Boolean.parseBoolean(value)
        }
        throw IllegalArgumentException("Unsupported partition value type: " + type)
    }
}
