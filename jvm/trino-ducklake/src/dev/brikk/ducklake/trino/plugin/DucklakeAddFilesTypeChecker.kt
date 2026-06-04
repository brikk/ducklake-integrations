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

import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.CharType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType

/**
 * Verifies that a parquet column's logical type is compatible with the target
 * DuckLake table column type. Implements the same widening rules as upstream's
 * `DuckLakeParquetTypeChecker` (signed-int / unsigned-int widening,
 * float→double widening, timestamp-precision compatibility, decimal
 * precision/scale fit, exact-match otherwise).
 *
 *
 * Operates on Trino's [Type] hierarchy on both sides: the source side
 * is derived from the parquet schema by Trino's existing
 * [io.trino.parquet.ParquetTypeUtils] machinery, and the target side
 * comes from the DuckLake catalog's stored column type.
 */
class DucklakeAddFilesTypeChecker private constructor() {
    companion object {
        /**
         * @throws DucklakeAddFilesException when source is not assignable to target.
         */
        fun checkCompatible(target: Type, source: Type, columnPath: String, fileName: String, tableName: String) {
            if (isCompatible(target, source)) {
                return
            }
            throw DucklakeAddFilesException(
                "Failed to map column \"$columnPath\" from file \"$fileName\" to table \"$tableName\": " +
                    "expected ${describe(target)}, found ${describe(source)}")
        }

        private fun isCompatible(target: Type, source: Type): Boolean {
            if (target == source) {
                return true
            }
            if (target is TinyintType
                    || target is SmallintType
                    || target is IntegerType
                    || target is BigintType) {
                return isSignedIntCompatible(target, source)
            }
            if (target is RealType) {
                return source is RealType
            }
            if (target is DoubleType) {
                return source is RealType || source is DoubleType
            }
            if (target is TimestampType && source is TimestampType) {
                // Upstream accepts widening between timestamp precisions; require equal precision
                // for strict round-trip, but accept smaller-precision source.
                return source.precision <= target.precision
            }
            if (target is TimestampWithTimeZoneType && source is TimestampWithTimeZoneType) {
                return source.precision <= target.precision
            }
            if (target is TimeType && source is TimeType) {
                return source.precision <= target.precision
            }
            if (target is TimeWithTimeZoneType && source is TimeWithTimeZoneType) {
                return source.precision <= target.precision
            }
            if (target is DecimalType && source is DecimalType) {
                return source.precision <= target.precision
                        && source.scale <= target.scale
            }
            if (target is VarcharType && source is VarcharType) {
                // Trino VARCHAR(N) — unbounded target accepts any bounded source.
                return target.isUnbounded || (!source.isUnbounded && source.boundedLength <= target.boundedLength)
            }
            if (target is CharType && source is CharType) {
                return source.length <= target.length
            }
            // BLOB / UUID / BOOLEAN / DATE / nested types: exact-match handled by .equals() at top.
            return false
        }

        private fun isSignedIntCompatible(target: Type, source: Type): Boolean {
            val targetWidth = signedIntWidth(target)
            val sourceWidth = signedIntWidth(source)
            return sourceWidth in 1..targetWidth
        }

        private fun signedIntWidth(type: Type): Int = when (type) {
            is TinyintType -> 1
            is SmallintType -> 2
            is IntegerType -> 4
            is BigintType -> 8
            else -> 0
        }

        private fun describe(type: Type): String {
            if (type is ArrayType) {
                return "array(" + describe(type.elementType) + ")"
            }
            if (type is MapType) {
                return "map(" + describe(type.keyType) + "," + describe(type.valueType) + ")"
            }
            if (type is RowType) {
                val sb = StringBuilder("row(")
                var first = true
                for (f in type.fields) {
                    if (!first) {
                        sb.append(",")
                    }
                    first = false
                    f.name.ifPresent { n -> sb.append(n).append(" ") }
                    sb.append(describe(f.type))
                }
                sb.append(")")
                return sb.toString()
            }
            if (type is BooleanType) {
                return "boolean"
            }
            if (type is VarbinaryType) {
                return "varbinary"
            }
            if (type is UuidType) {
                return "uuid"
            }
            if (type is DateType) {
                return "date"
            }
            return type.displayName
        }
    }
}
