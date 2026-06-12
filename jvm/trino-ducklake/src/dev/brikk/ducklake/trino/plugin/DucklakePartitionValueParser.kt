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
 * `ducklake_file_partition_value.partition_value` into Trino native values.
 * Shared by the split manager's partition-pruning path (compares the parsed value
 * against the query's pushdown domain) and the page source provider's
 * partition-column projection (constants the parsed value across the file).
 *
 * Encoding contract is the DuckLake 1.0 calendar form for temporals
 * (handled by `DucklakeTemporalPartitionMatcher`); this helper covers
 * identity-transform value parsing only.
 */
object DucklakePartitionValueParser
{
    fun parseIdentity(type: Type, value: String): Any
    {
        return when {
            type == VARCHAR || type is VarcharType -> Slices.utf8Slice(value)
            type == BIGINT -> java.lang.Long.parseLong(value)
            type == INTEGER -> Integer.parseInt(value).toLong()
            type == SMALLINT -> java.lang.Short.parseShort(value).toLong()
            type == TINYINT -> java.lang.Byte.parseByte(value).toLong()
            type == DOUBLE -> java.lang.Double.parseDouble(value)
            type == REAL -> java.lang.Float.floatToIntBits(java.lang.Float.parseFloat(value)).toLong()
            type == DATE -> LocalDate.parse(value).toEpochDay()
            type == BOOLEAN -> parseBoolean(value)
            else -> throw IllegalArgumentException("Unsupported partition value type: $type")
        }
    }

    // Boolean.parseBoolean silently maps everything that isn't "true" to false, so an
    // externally-written catalog that encodes boolean partitions as "1"/"0" (the form
    // DuckLake's C++ extension may store, and the form JdbcDucklakeCatalog/DucklakeStatTypes
    // already accept) would parse "1" as false — wrong pruning and a wrong projected
    // constant. Mirror the catalog's encoding and throw on anything else so the callers'
    // catch(RuntimeException) blocks fall back safely (don't-prune / NULL) instead of
    // silently producing the wrong value.
    private fun parseBoolean(value: String): Boolean {
        return when {
            value.equals("true", ignoreCase = true) || value == "1" -> true
            value.equals("false", ignoreCase = true) || value == "0" -> false
            else -> throw IllegalArgumentException("Invalid boolean partition value: $value")
        }
    }
}
