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

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import io.airlift.slice.Slice
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType
import java.lang.Float.intBitsToFloat
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Single-pass, format-agnostic per-column statistics accumulator for connector-written data
 * files. Any writer that streams Trino [Page]s can feed each page through [add] as it flows to
 * the underlying file (e.g. the vortex writer, which has no queryable table to run `MIN/MAX` SQL
 * against), then call [build] once to get the [DucklakeFileColumnStats] for the catalog — no
 * read-back, no scratch table.
 *
 * **Correctness contract (this is the silent-bug-prone part):**
 * - min/max are tracked in NATIVE form and compared with engine semantics. For VARCHAR the value
 *   is the Trino [Slice], whose `compareTo` is UNSIGNED byte (UTF-8) order — matching DuckDB's
 *   default binary collation and Trino's own ordering, so the catalog min/max agree across engines.
 * - NaN REAL/DOUBLE values never enter min/max (DuckLake stat strings can't represent NaN; the
 *   shared formatter drops them too).
 * - VARBINARY and UUID get null/value counts only — no min/max — matching the existing duckdb /
 *   parquet stat extractors.
 * - The winning native value is formatted to the catalog string EXACTLY ONCE, via the shared
 *   [DuckDbWriterSupport.formatStatValue], so the strings are identical to the duckdb writer's.
 *
 * Not thread-safe: [add] is called from the single writer thread that produces pages.
 */
internal class DucklakeColumnStatsAccumulator(columns: List<DucklakeColumnHandle>) {
    private val columns: List<DucklakeColumnHandle> = columns.toList()
    private val types: List<Type> = this.columns.map { it.columnType }
    private val valueCounts: LongArray = LongArray(this.columns.size)
    private val mins: Array<Comparable<Any>?> = arrayOfNulls(this.columns.size)
    private val maxs: Array<Comparable<Any>?> = arrayOfNulls(this.columns.size)
    private var totalRows: Long = 0

    fun add(page: Page) {
        val positionCount = page.positionCount
        if (positionCount == 0) {
            return
        }
        for (channel in types.indices) {
            val block: Block = page.getBlock(channel)
            val type: Type = types[channel]
            val trackMinMax: Boolean = supportsMinMax(type)
            for (position in 0 until positionCount) {
                if (block.isNull(position)) {
                    continue
                }
                valueCounts[channel]++
                if (!trackMinMax) {
                    continue
                }
                val value: Comparable<Any> = extractComparable(type, block, position)
                    ?: continue // NaN — excluded from min/max
                val currentMin = mins[channel]
                if (currentMin == null || value.compareTo(currentMin) < 0) {
                    mins[channel] = value
                }
                val currentMax = maxs[channel]
                if (currentMax == null || value.compareTo(currentMax) > 0) {
                    maxs[channel] = value
                }
            }
        }
        totalRows += positionCount
    }

    fun build(): List<DucklakeFileColumnStats> {
        val result = ArrayList<DucklakeFileColumnStats>(columns.size)
        for (i in columns.indices) {
            val col = columns[i]
            val nullCount = maxOf(0L, totalRows - valueCounts[i])
            result.add(DucklakeFileColumnStats(
                    col.columnId,
                    0L, // column_size_bytes — not tracked here; safe at 0 (matches duckdb writer)
                    valueCounts[i],
                    nullCount,
                    formatWinner(col.columnType, mins[i]),
                    formatWinner(col.columnType, maxs[i]),
                    false))
        }
        return result
    }

    private fun formatWinner(type: Type, winner: Comparable<Any>?): String? {
        if (winner == null) {
            return null
        }
        // Slices carry the comparison (UTF-8 byte order); format from their string form.
        val forFormat: Any = if (winner is Slice) winner.toStringUtf8() else winner
        return DuckDbWriterSupport.formatStatValue(type, forFormat).orElse(null)
    }

    companion object {
        private fun supportsMinMax(type: Type): Boolean =
            // Arrays (e.g. lance embedding columns) get null/value counts only — there is no
            // meaningful catalog min/max for a vector (extractComparable would return null per
            // position anyway; this just skips the wasted walk).
            !(type == VARBINARY || type == UuidType.UUID || type is io.trino.spi.type.ArrayType)

        /**
         * Native, [Comparable] representation of one cell, in a shape [DuckDbWriterSupport
         * .formatStatValue] accepts (Slice for VARCHAR — converted to String at format time).
         * Returns null for NaN floating-point (excluded from min/max) and for types without a
         * meaningful order here.
         */
        @Suppress("UNCHECKED_CAST")
        private fun extractComparable(type: Type, block: Block, position: Int): Comparable<Any>? {
            val value: Comparable<*>? = when {
                type == BOOLEAN -> BOOLEAN.getBoolean(block, position)
                type == TINYINT -> TINYINT.getByte(block, position).toLong()
                type == SMALLINT -> SMALLINT.getShort(block, position).toLong()
                type == INTEGER -> INTEGER.getInt(block, position).toLong()
                type == BIGINT -> BIGINT.getLong(block, position)
                type == REAL -> {
                    val f = intBitsToFloat(REAL.getInt(block, position))
                    if (f.isNaN()) null else f
                }
                type == DOUBLE -> {
                    val d = DOUBLE.getDouble(block, position)
                    if (d.isNaN()) null else d
                }
                type == DATE -> LocalDate.ofEpochDay(DATE.getInt(block, position).toLong())
                type is DecimalType -> if (type.isShort) {
                    BigDecimal.valueOf(type.getLong(block, position), type.scale)
                }
                else {
                    val unscaled = type.getObject(block, position) as Int128
                    BigDecimal(BigInteger(unscaled.toBigEndianBytes()), type.scale)
                }
                type is TimestampType -> if (type.isShort) {
                    microsToLocalDateTime(type.getLong(block, position))
                }
                else {
                    val ts = type.getObject(block, position) as LongTimestamp
                    microsToLocalDateTime(ts.epochMicros).plusNanos((ts.picosOfMicro / 1_000).toLong())
                }
                type is TimestampWithTimeZoneType -> {
                    val instant: Instant = if (type.isShort) {
                        Instant.ofEpochMilli(unpackMillisUtc(type.getLong(block, position)))
                    }
                    else {
                        val ts = type.getObject(block, position) as LongTimestampWithTimeZone
                        Instant.ofEpochMilli(ts.epochMillis).plusNanos((ts.picosOfMilli / 1_000).toLong())
                    }
                    OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
                }
                type is VarcharType -> type.getSlice(block, position) // Slice.compareTo == unsigned UTF-8 bytes
                else -> null
            }
            return value as Comparable<Any>?
        }

        private fun microsToLocalDateTime(epochMicros: Long): LocalDateTime {
            val epochSecond = floorDiv(epochMicros, 1_000_000L)
            val nanoOfSecond = floorMod(epochMicros, 1_000_000L).toInt() * 1_000
            return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC)
        }
    }
}
