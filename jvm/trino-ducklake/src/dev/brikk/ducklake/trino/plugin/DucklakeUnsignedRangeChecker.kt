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
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.type.DecimalType
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType
import io.trino.spi.type.SmallintType

/**
 * Enforces DuckLake unsigned integer type ranges on the write path.
 *
 * DuckLake stores true unsigned types (uint8, uint16, uint32, uint64) but Trino has no
 * native unsigned integer types. The connector widens unsigned types to the next signed
 * Trino type on read (uint8 → SMALLINT, uint16 → INTEGER, uint32 → BIGINT, uint64 →
 * DECIMAL(20, 0)); without this checker, a write of e.g. SMALLINT 300 into a uint8 column
 * would silently wrap to 44 inside DuckDB because Parquet carries only the physical
 * primitive type, not the logical unsigned bound. See
 * dev-docs/COMPARE-pg_ducklake.md B5.
 *
 * Built once per page sink and short-circuits to a no-op when no column in the table
 * is unsigned (the overwhelmingly common case). When unsigned columns are present, only
 * those channels are scanned — other channels are untouched.
 */
public class DucklakeUnsignedRangeChecker private constructor(private val checks: List<Check>) {
    public fun isNoOp(): Boolean {
        return checks.isEmpty()
    }

    /**
     * Validates a page against all unsigned-column ranges. Throws on the first out-of-range
     * value. Must be called before the page is handed to the Parquet writer — once the
     * writer has consumed the page, the offending value has already been encoded and a
     * throw would leave a half-written row group behind.
     */
    public fun validate(page: Page) {
        if (checks.isEmpty()) {
            return
        }
        for (check in checks) {
            check.validate(page.getBlock(check.channel))
        }
    }

    internal sealed class Check(
            internal val channel: Int,
            internal val column: DucklakeColumnHandle,
            internal val ducklakeType: String) {
        internal abstract fun validate(block: Block)

        internal fun overflow(value: Any, maxLiteral: String): TrinoException =
            TrinoException(
                NUMERIC_VALUE_OUT_OF_RANGE,
                "Value $value for column \"${column.columnName}\" is out of range for DuckLake $ducklakeType (allowed: 0..$maxLiteral)")
    }

    private class SmallintUnsignedCheck(
            channel: Int,
            column: DucklakeColumnHandle,
            ducklakeType: String,
            private val max: Long) : Check(channel, column, ducklakeType) {
        override fun validate(block: Block) {
            val positions = block.getPositionCount()
            var i = 0
            while (i < positions) {
                if (block.isNull(i)) {
                    i++
                    continue
                }
                val value: Short = SmallintType.SMALLINT.getShort(block, i)
                if (value < 0 || value > max) {
                    throw overflow(value, java.lang.Long.toString(max))
                }
                i++
            }
        }
    }

    private class IntegerUnsignedCheck(
            channel: Int,
            column: DucklakeColumnHandle,
            ducklakeType: String,
            private val max: Long) : Check(channel, column, ducklakeType) {
        override fun validate(block: Block) {
            val positions = block.getPositionCount()
            var i = 0
            while (i < positions) {
                if (block.isNull(i)) {
                    i++
                    continue
                }
                val value: Int = IntegerType.INTEGER.getInt(block, i)
                if (value < 0 || value > max) {
                    throw overflow(value, java.lang.Long.toString(max))
                }
                i++
            }
        }
    }

    private class BigintUnsignedCheck(
            channel: Int,
            column: DucklakeColumnHandle,
            ducklakeType: String,
            private val max: Long) : Check(channel, column, ducklakeType) {
        override fun validate(block: Block) {
            val positions = block.getPositionCount()
            var i = 0
            while (i < positions) {
                if (block.isNull(i)) {
                    i++
                    continue
                }
                val value: Long = io.trino.spi.type.BigintType.BIGINT.getLong(block, i)
                if (value < 0 || value > max) {
                    throw overflow(value, java.lang.Long.toString(max))
                }
                i++
            }
        }
    }

    private class Uint64Check(channel: Int, column: DucklakeColumnHandle) : Check(channel, column, "uint64") {
        private val type: DecimalType = column.columnType as DecimalType

        override fun validate(block: Block) {
            val positions = block.getPositionCount()
            var i = 0
            while (i < positions) {
                if (block.isNull(i)) {
                    i++
                    continue
                }
                val value: Int128 = type.getObject(block, i) as Int128
                if (value.getHigh() != 0L) {
                    throw overflow(value.toBigInteger(), MAX_LITERAL)
                }
                i++
            }
        }

        public companion object {
            // 2^64 - 1. Int128 range for a valid uint64 is: high == 0, any low. Values with
            // high != 0 are either negative (high = -1, i.e. signed interpretation < 0) or
            // exceed 2^64 - 1 (high >= 1). Checking high is a single long compare regardless
            // of whether the caller wrote a decimal > 18446744073709551615 or a negative.
            private const val MAX_LITERAL: String = "18446744073709551615"
        }
    }

    public companion object {
        private val NO_OP: DucklakeUnsignedRangeChecker = DucklakeUnsignedRangeChecker(emptyList())

        private const val UINT8_MAX: Long = (1L shl 8) - 1
        private const val UINT16_MAX: Long = (1L shl 16) - 1
        private const val UINT32_MAX: Long = (1L shl 32) - 1

        /**
         * Build a checker for the given page channel layout. Channels whose DuckLake type is
         * not an unsigned integer are skipped.
         *
         * @param columns the column handles in channel order (position i of any appended Page
         *                is the value for columns.get(i))
         * @param catalogColumnTypeById map from columnId to the raw DuckLake type string
         *                              (e.g. "uint8") — typically derived from
         *                              `DucklakeWritableTableHandle.allCatalogColumns()`
         */
        @JvmStatic
        public fun build(
                columns: List<DucklakeColumnHandle>,
                catalogColumnTypeById: Map<Long, String>): DucklakeUnsignedRangeChecker {
            val builder: ImmutableList.Builder<Check> = ImmutableList.builder()
            var channel = 0
            while (channel < columns.size) {
                val column: DucklakeColumnHandle = columns.get(channel)
                val ducklakeType: String? = catalogColumnTypeById.get(column.columnId)
                if (ducklakeType == null) {
                    channel++
                    continue
                }
                val check: Check? = checkerFor(channel, column, ducklakeType)
                if (check != null) {
                    builder.add(check)
                }
                channel++
            }
            val checks: List<Check> = builder.build()
            return if (checks.isEmpty()) NO_OP else DucklakeUnsignedRangeChecker(checks)
        }

        @JvmStatic
        public fun build(
                columns: List<DucklakeColumnHandle>,
                allCatalogColumns: List<DucklakeColumn>): DucklakeUnsignedRangeChecker {
            return build(columns, allCatalogColumns.stream()
                    .collect(java.util.stream.Collectors.toMap({ c: DucklakeColumn -> c.columnId }, { c: DucklakeColumn -> c.columnType }, { a, _ -> a })))
        }

        private fun checkerFor(channel: Int, column: DucklakeColumnHandle, ducklakeType: String): Check? {
            // DuckLake type strings are lowercase per upstream ducklake_types.cpp. Defensive
            // normalization in case a JDBC driver up-cases the column.
            val normalized = ducklakeType.lowercase(java.util.Locale.ROOT)
            return when (normalized) {
                "uint8" -> SmallintUnsignedCheck(channel, column, "uint8", UINT8_MAX)
                "uint16" -> IntegerUnsignedCheck(channel, column, "uint16", UINT16_MAX)
                "uint32" -> BigintUnsignedCheck(channel, column, "uint32", UINT32_MAX)
                "uint64" -> {
                    // uint64 read maps to DECIMAL(20, 0); anything else means the table schema
                    // was mutated out from under us and we should skip rather than throw —
                    // this is defensive and shouldn't trigger in practice.
                    if (column.columnType is DecimalType && !(column.columnType as DecimalType).isShort()) {
                        Uint64Check(channel, column)
                    }
                    else {
                        null
                    }
                }
                else -> null
            }
        }
    }
}
