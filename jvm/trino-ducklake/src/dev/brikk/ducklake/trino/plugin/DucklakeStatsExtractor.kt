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
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeStatTypes
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.RealType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import org.apache.parquet.format.FileMetaData
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Optional

/**
 * Aggregates per-row-group parquet statistics into one [DucklakeFileColumnStats]
 * row per parquet leaf, keyed by the DuckLake catalog field_id at that leaf
 * (top-level column_id for flat columns; child column_id for nested struct /
 * array / map leaves).
 *
 * Caller is responsible for projecting the leaf list — see
 * [DucklakeStatsLeafProjector] (write path) and
 * [DucklakeAddFilesNameMapper] (add_files path). Both produce a
 * [LeafStatsTarget] list in the same order parquet writes leaves into
 * `RowGroup.columns`.
 */
public object DucklakeStatsExtractor {
    public fun extractStats(
            fileMetaData: FileMetaData,
            leafTargets: List<LeafStatsTarget>): List<DucklakeFileColumnStats> {
        val result: ImmutableList.Builder<DucklakeFileColumnStats> = ImmutableList.builder()

        for (target in leafTargets) {
            val parquetColumnIndex = target.parquetColumnIndex
            val type: Type = target.leafType
            val numeric = isNumericTrinoType(type)

            var totalCompressedSize: Long = 0
            var totalValueCount: Long = 0
            var totalNullCount: Long = 0
            val containsNan = false
            var minValue: Optional<String> = Optional.empty()
            var maxValue: Optional<String> = Optional.empty()
            var hasStats = false

            for (rowGroup in fileMetaData.getRow_groups()) {
                if (parquetColumnIndex >= rowGroup.getColumns().size) {
                    continue
                }
                val columnMeta = rowGroup.getColumns().get(parquetColumnIndex).getMeta_data()
                totalCompressedSize += columnMeta.getTotal_compressed_size()

                var groupNullCount: Long = 0
                if (columnMeta.isSetStatistics()) {
                    val stats = columnMeta.getStatistics()
                    if (stats.isSetNull_count()) {
                        groupNullCount = stats.getNull_count()
                    }

                    if (stats.isSetMin_value() && stats.isSetMax_value()) {
                        hasStats = true
                        val groupMin = convertStatValue(stats.getMin_value(), type, columnMeta.getType())
                        val groupMax = convertStatValue(stats.getMax_value(), type, columnMeta.getType())

                        if (groupMin.isPresent) {
                            minValue = if (minValue.isEmpty) groupMin else Optional.of(DucklakeStatTypes.min(minValue.get(), groupMin.get(), numeric))
                        }
                        if (groupMax.isPresent) {
                            maxValue = if (maxValue.isEmpty) groupMax else Optional.of(DucklakeStatTypes.max(maxValue.get(), groupMax.get(), numeric))
                        }
                    }
                }

                totalNullCount += groupNullCount
                // value_count is the NON-NULL value count. Parquet num_values counts all values
                // (nulls included), but the catalog's value_count must hold the non-null count so
                // that value_count + null_count == row count. This matches the DuckDB writer path
                // (DuckDbFileWriter derives value_count from COUNT(col) and null_count from
                // totalCount - value_count) and the DuckLake spec (ducklake_transaction.cpp:
                // value_count = num_values - null_count). Emitting the raw num_values here would
                // over-count rows for any column containing nulls, inflating the consumer's
                // totalCount past the data-file row count and tripping the stats-suppression guard
                // (and skewing nullsFraction) in DucklakeMetadata.getTableStatistics.
                totalValueCount += (columnMeta.getNum_values() - groupNullCount)
            }

            result.add(DucklakeFileColumnStats(
                    target.fieldId,
                    totalCompressedSize,
                    totalValueCount,
                    totalNullCount,
                    if (hasStats) minValue else Optional.empty(),
                    if (hasStats) maxValue else Optional.empty(),
                    containsNan))
        }

        return result.build()
    }

    internal fun convertStatValue(value: ByteArray?, type: Type): Optional<String> {
        return convertStatValue(value, type, null)
    }

    internal fun convertStatValue(value: ByteArray?, type: Type, physicalType: org.apache.parquet.format.Type?): Optional<String> {
        if (value == null || value.size == 0) {
            return Optional.empty()
        }

        try {
            if (type is BooleanType) {
                return Optional.of(if (value[0].toInt() != 0) "true" else "false")
            }
            if (type is TinyintType || type is SmallintType || type is IntegerType) {
                val intVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt()
                return Optional.of(intVal.toString())
            }
            if (type is BigintType) {
                val longVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong()
                return Optional.of(longVal.toString())
            }
            if (type is RealType) {
                val floatVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getFloat()
                if (floatVal.isNaN()) {
                    return Optional.empty()
                }
                return Optional.of(floatVal.toString())
            }
            if (type is DoubleType) {
                val doubleVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble()
                if (doubleVal.isNaN()) {
                    return Optional.empty()
                }
                return Optional.of(doubleVal.toString())
            }
            if (type is VarcharType) {
                return Optional.of(String(value, Charsets.UTF_8))
            }
            if (type is VarbinaryType || type is UuidType) {
                return Optional.empty()
            }
            if (type is DateType) {
                val daysSinceEpoch = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt()
                return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch.toLong()).toString())
            }
            if (type is TimestampType) {
                val micros = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong()
                val dateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L),
                        ZoneOffset.UTC)
                return Optional.of(dateTime.toString())
            }
            if (type is TimestampWithTimeZoneType) {
                val micros = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong()
                val instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L)
                return Optional.of(instant.toString())
            }
            if (type is DecimalType) {
                val decimalType: DecimalType = type
                val unscaled = decodeDecimalUnscaled(value, physicalType)
                val decimal = BigDecimal(unscaled, decimalType.getScale())
                return Optional.of(decimal.toPlainString())
            }

            return Optional.empty()
        }
        catch (e: RuntimeException) {
            return Optional.empty()
        }
    }

    /**
     * Parquet stores DECIMAL statistics in different byte orders depending on the
     * physical type: INT32/INT64-backed decimals are little-endian two's-complement
     * (matching the primitive layout), while FIXED_LEN_BYTE_ARRAY / BINARY decimals
     * are big-endian two's-complement. Decoding the short INT32/INT64 forms as
     * big-endian (`new BigInteger(byte[])`) silently corrupts min/max for
     * low-precision decimals.
     */
    private fun decodeDecimalUnscaled(value: ByteArray, physicalType: org.apache.parquet.format.Type?): BigInteger {
        if (physicalType == org.apache.parquet.format.Type.INT32) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong())
        }
        if (physicalType == org.apache.parquet.format.Type.INT64) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong())
        }
        // FIXED_LEN_BYTE_ARRAY / BINARY (and the unknown/test path): big-endian two's complement.
        return BigInteger(value)
    }

    private fun isNumericTrinoType(type: Type): Boolean {
        return type is TinyintType
                || type is SmallintType
                || type is IntegerType
                || type is BigintType
                || type is RealType
                || type is DoubleType
                || type is DecimalType
    }
}
