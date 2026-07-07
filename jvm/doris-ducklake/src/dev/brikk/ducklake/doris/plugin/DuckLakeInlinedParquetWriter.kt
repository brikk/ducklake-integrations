package dev.brikk.ducklake.doris.plugin

import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Locale

import dev.brikk.ducklake.catalog.DucklakeColumn

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

/**
 * Synthesizes a temporary Parquet file from DuckLake **inlined** rows (rows the
 * catalog keeps in `ducklake_inlined_data_*` tables, not in data files) so the
 * Doris BE — which only reads files — can scan them like any other data file.
 *
 * **COMPOSE/DEV ONLY — NOT production-viable.** The BE opens this file by path,
 * so it only works when the FE and BE share a filesystem at the same path (a
 * compose bind mount). In a distributed FE/BE cluster the BE can't see a file
 * the FE wrote locally. The real fix is a shared-storage or SPI payload channel
 * — see the friction log entry "No way to hand a small FE-built payload to the
 * BE without shared storage" and `TODO-read.md` (PRODUCTION SOLUTION REQUIRED).
 * The caller (`DuckLakeScanPlanProvider`) gates this to local-fs warehouses.
 *
 * The file carries `field_id` in its Parquet metadata (`== column_id`, the
 * DuckLake invariant) via the low-level `Types...id(...)` builder, so the BE's
 * field-id column matching resolves it exactly like a real DuckLake file
 * (and doesn't demand a `name_mapping`, which the 4.1.0 BE requires for an
 * id-less file). Written with the parquet example `SimpleGroup` writer, which
 * is FE-supplied at runtime (`output/fe/lib`) and avoids Hadoop's
 * `UserGroupInformation` (broken on JDK 25) by using [LocalOutputFile].
 *
 * **Stage 1 scope: SCALAR columns only, local filesystem.** Nested
 * (list/struct/map) inlined columns are out of scope (the caller keeps the
 * guard for them). Value conversion mirrors trino's
 * `DucklakeInlinedValueConverter` scalar cases: jOOQ returns either a typed
 * value or a DuckDB text form depending on backend/type, so each converter
 * accepts both.
 */
internal object DuckLakeInlinedParquetWriter {

    private const val MICROS_PER_SECOND = 1_000_000L
    private const val NANOS_PER_MICRO = 1_000L

    /**
     * Write [rows] (as returned by `DucklakeCatalog.readInlinedData`, in
     * [columns] order — already snapshot-filtered by the catalog) to the local
     * filesystem [target] as Parquet with per-column `field_id == column_id`.
     * [columns] must be top-level scalar columns. Returns the row count.
     */
    fun write(target: java.nio.file.Path, columns: List<DucklakeColumn>, rows: List<List<Any?>>): Long {
        val schema = buildMessageType(columns)
        val conf = Configuration()
        GroupWriteSupport.setSchema(schema, conf)
        parquetWriter(target, schema, conf).use { writer ->
            for (row in rows) {
                val group = SimpleGroup(schema)
                for (i in columns.indices) {
                    appendValue(group, i, columns[i].columnType, row[i])
                }
                writer.write(group)
            }
        }
        return rows.size.toLong()
    }

    private fun parquetWriter(
        target: java.nio.file.Path,
        schema: MessageType,
        conf: Configuration,
    ): ParquetWriter<Group> =
        ExampleParquetWriter.builder(LocalOutputFile(target))
            .withType(schema)
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.ZSTD)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()

    // ---- parquet MessageType with field_id == column_id ----

    private fun buildMessageType(columns: List<DucklakeColumn>): MessageType {
        val fields = columns.map { col -> parquetField(col) }
        return MessageType("ducklake_inlined", fields)
    }

    private fun parquetField(col: DucklakeColumn): Type {
        val t = col.columnType.trim().lowercase(Locale.ROOT)
        val id = col.columnId.toInt()
        // All inlined columns are OPTIONAL (DuckLake stores nulls inline).
        DECIMAL.matchEntire(t)?.let { m ->
            val precision = m.groupValues[1].toInt()
            val scale = m.groupValues[2].toInt()
            return Types.optional(PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.decimalType(scale, precision))
                .id(id).named(col.columnName)
        }
        val builder = when (t) {
            "boolean" -> Types.optional(PrimitiveTypeName.BOOLEAN)
            "int8", "int16", "int32", "uint8", "uint16" -> Types.optional(PrimitiveTypeName.INT32)
            "int64", "uint32" -> Types.optional(PrimitiveTypeName.INT64)
            "float32" -> Types.optional(PrimitiveTypeName.FLOAT)
            "float64" -> Types.optional(PrimitiveTypeName.DOUBLE)
            "date" -> Types.optional(PrimitiveTypeName.INT32)
                .`as`(LogicalTypeAnnotation.dateType())
            // DuckLake temporals are micros (ns clamps to micros on read).
            // timestamptz is written NAIVE (isAdjustedToUtc=false) to match the
            // DATETIMEV2 read mapping: the stored micros are already UTC, and the
            // BE reads DateTimeV2<=>DateTimeV2 (an isAdjustedToUtc file would trip
            // the "DateTimeV2 => TimeStampTz" BE gap). See DuckLakeTypeMapping.
            "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns", "timestamptz" ->
                Types.optional(PrimitiveTypeName.INT64)
                    .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            "varchar" -> Types.optional(PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.stringType())
            "blob" -> Types.optional(PrimitiveTypeName.BINARY)
            else -> throw IllegalArgumentException(
                "inlined-data write: unsupported scalar DuckLake type '${col.columnType}'",
            )
        }
        return builder.id(id).named(col.columnName)
    }

    // ---- value append (jOOQ value | DuckDB text form) ----

    private fun appendValue(group: Group, index: Int, ducklakeType: String, value: Any?) {
        if (value == null) {
            return // absent = null for an OPTIONAL column
        }
        val t = ducklakeType.trim().lowercase(Locale.ROOT)
        DECIMAL.matchEntire(t)?.let { m ->
            group.add(index, Binary.fromConstantByteArray(decimalUnscaledBytes(value, m.groupValues[2].toInt())))
            return
        }
        when (t) {
            "boolean" -> group.add(index, toBoolean(value))
            "int8", "int16", "int32", "uint8", "uint16" -> group.add(index, toLong(value).toInt())
            "int64", "uint32" -> group.add(index, toLong(value))
            "float32" -> group.add(index, toFloat(value))
            "float64" -> group.add(index, toDouble(value))
            "date" -> group.add(index, toEpochDay(value).toInt())
            "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns" ->
                group.add(index, toEpochMicros(value))
            "timestamptz" -> group.add(index, toEpochMicrosTz(value))
            "varchar" -> group.add(index, Binary.fromString(toStringValue(value)))
            "blob" -> group.add(index, Binary.fromConstantByteArray(toBytes(value)))
            else -> throw IllegalArgumentException(
                "inlined-data write: unsupported scalar DuckLake type '$ducklakeType'",
            )
        }
    }

    private fun toBoolean(value: Any): Boolean = when (value) {
        is Boolean -> value
        is Number -> value.toInt() != 0
        else -> toStringValue(value).toBooleanStrict()
    }

    private fun toLong(value: Any): Long =
        if (value is Number) value.toLong() else toStringValue(value).trim().toLong()

    private fun toFloat(value: Any): Float =
        if (value is Number) value.toFloat() else normalizeNonFinite(toStringValue(value)).toFloat()

    private fun toDouble(value: Any): Double =
        if (value is Number) value.toDouble() else normalizeNonFinite(toStringValue(value)).toDouble()

    private fun toEpochDay(value: Any): Long = when (value) {
        is LocalDate -> value.toEpochDay()
        is java.sql.Date -> value.toLocalDate().toEpochDay()
        is Number -> value.toLong()
        else -> LocalDate.parse(toStringValue(value).trim()).toEpochDay()
    }

    private fun toEpochMicros(value: Any): Long = when (value) {
        is Number -> value.toLong()
        is java.sql.Timestamp -> instantToMicros(value.toInstant())
        is LocalDateTime -> localDateTimeToMicros(value)
        else -> localDateTimeToMicros(parseLocalDateTime(toStringValue(value)))
    }

    private fun toEpochMicrosTz(value: Any): Long = when (value) {
        is Number -> value.toLong()
        is java.sql.Timestamp -> instantToMicros(value.toInstant())
        is OffsetDateTime -> instantToMicros(value.toInstant())
        is Instant -> instantToMicros(value)
        is LocalDateTime -> instantToMicros(value.toInstant(ZoneOffset.UTC))
        else -> instantToMicros(parseInstant(toStringValue(value)))
    }

    private fun toBytes(value: Any): ByteArray = when (value) {
        is ByteArray -> value
        else -> decodeBlobText(toStringValue(value))
    }

    private fun decimalUnscaledBytes(value: Any, scale: Int): ByteArray {
        val decimal = when (value) {
            is BigDecimal -> value
            is Number -> BigDecimal.valueOf(value.toDouble())
            else -> BigDecimal(toStringValue(value).trim())
        }.setScale(scale, RoundingMode.UNNECESSARY)
        return decimal.unscaledValue().toByteArray()
    }

    // ---- helpers ----

    private fun instantToMicros(instant: Instant): Long =
        instant.epochSecond * MICROS_PER_SECOND + instant.nano / NANOS_PER_MICRO

    private fun localDateTimeToMicros(dt: LocalDateTime): Long =
        dt.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND + dt.nano / NANOS_PER_MICRO

    private fun parseLocalDateTime(text: String): LocalDateTime =
        LocalDateTime.parse(text.trim().replace(' ', 'T'))

    private fun parseInstant(text: String): Instant {
        val s = text.trim()
        return runCatching { OffsetDateTime.parse(s.replace(' ', 'T')).toInstant() }
            .getOrElse { parseLocalDateTime(s).toInstant(ZoneOffset.UTC) }
    }

    private fun normalizeNonFinite(text: String): String = when (text.trim().lowercase(Locale.ROOT)) {
        "inf", "+inf", "infinity", "+infinity" -> "Infinity"
        "-inf", "-infinity" -> "-Infinity"
        "nan", "-nan" -> "NaN"
        else -> text.trim()
    }

    private fun toStringValue(value: Any): String = when (value) {
        is ByteArray -> String(value, Charsets.UTF_8)
        else -> value.toString()
    }

    /**
     * Decode DuckDB's `Blob::ToString` text form: printable ASCII verbatim
     * (except escapes), everything else `\xNN`. Mirrors trino's
     * `DucklakeInlinedValueConverter.decodeBlobText`.
     */
    private fun decodeBlobText(text: String): ByteArray {
        val out = ArrayList<Byte>(text.length)
        var i = 0
        while (i < text.length) {
            if (isHexEscapeAt(text, i)) {
                out.add(text.substring(i + 2, i + 4).toInt(HEX_RADIX).toByte())
                i += 4
            } else {
                out.add(text[i].code.toByte())
                i += 1
            }
        }
        return out.toByteArray()
    }

    /** True when a `\xNN` escape starts at [i]. */
    private fun isHexEscapeAt(text: String, i: Int): Boolean {
        if (i + 3 >= text.length) {
            return false
        }
        return text[i] == '\\' && text[i + 1] == 'x'
    }

    private val DECIMAL = Regex("""decimal\((\d+),\s*(\d+)\)""")
    private const val HEX_RADIX = 16
}
