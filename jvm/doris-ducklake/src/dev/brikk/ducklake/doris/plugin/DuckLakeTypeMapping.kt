package dev.brikk.ducklake.doris.plugin

import java.util.regex.Pattern

import org.apache.doris.connector.api.ConnectorType

/**
 * Maps DuckLake catalog type strings (as they appear in `ducklake_column.column_type`)
 * to Doris [ConnectorType]. Mirrors the Trino-side `DucklakeTypeConverter` but
 * targets `ConnectorType` factories (`of`, `arrayOf`, `mapOf`, `structOf`) instead of
 * Trino's `Type` hierarchy.
 *
 * Unsupported / degraded mappings are marked `DEGRADED` in comments and mirror the
 * choices the Trino plugin makes — see `DucklakeTypeConverter` and the cross-engine
 * compatibility notes in `dev-docs/archive/COMPARE-pg_ducklake.md`.
 */
internal object DuckLakeTypeMapping {

    private val DECIMAL_PATTERN: Pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)")

    // Microsecond precision is DuckLake's native temporal resolution.
    private const val MICROS_SCALE = 6

    // Bounded-length defaults — Doris ConnectorType demands a length for VARBINARY/CHAR.
    private const val VARBINARY_DEFAULT_LEN = 65535
    private const val UUID_BYTE_LEN = 16

    /**
     * Convert a DuckLake type string to a Doris [ConnectorType].
     *
     * @throws IllegalArgumentException if the type string cannot be parsed
     */
    fun fromDucklakeType(ducklakeType: String?): ConnectorType {
        requireNotNull(ducklakeType) { "ducklakeType is null" }
        val normalized = ducklakeType.trim().lowercase()

        if (normalized.startsWith("list<") && normalized.endsWith(">")) {
            val inner = extractTypeArguments(normalized, "list").trim()
            return ConnectorType.arrayOf(fromDucklakeType(inner))
        }
        if (normalized.startsWith("struct<") && normalized.endsWith(">")) {
            val body = extractTypeArguments(normalized, "struct")
            val parts = splitTopLevelCommas(body)
            val names = ArrayList<String>(parts.size)
            val types = ArrayList<ConnectorType>(parts.size)
            for (field in parts) {
                val colon = field.indexOf(':')
                require(colon >= 0) { "Invalid struct field (missing ':'): $field" }
                names.add(field.substring(0, colon).trim())
                types.add(fromDucklakeType(field.substring(colon + 1).trim()))
            }
            return ConnectorType.structOf(names, types)
        }
        if (normalized.startsWith("map<") && normalized.endsWith(">")) {
            val body = extractTypeArguments(normalized, "map")
            val parts = splitTopLevelCommas(body)
            require(parts.size == 2) {
                "Invalid map type (need 2 args, got ${parts.size}): $ducklakeType"
            }
            return ConnectorType.mapOf(
                fromDucklakeType(parts[0].trim()),
                fromDucklakeType(parts[1].trim()),
            )
        }

        return when (normalized) {
            "boolean" -> ConnectorType.of("BOOLEAN")

            "int8" -> ConnectorType.of("TINYINT")
            "int16" -> ConnectorType.of("SMALLINT")
            "int32" -> ConnectorType.of("INT")
            "int64" -> ConnectorType.of("BIGINT")

            // Unsigned integers: promote to the next signed type that holds the full range.
            // uint64 needs DECIMALV3(20,0); uint128 has no numeric fit, degrade to STRING.
            "uint8" -> ConnectorType.of("SMALLINT")
            "uint16" -> ConnectorType.of("INT")
            "uint32" -> ConnectorType.of("BIGINT")
            "uint64" -> ConnectorType.of("DECIMALV3", 20, 0)

            "int128" -> ConnectorType.of("DECIMALV3", 38, 0)
            "uint128" -> ConnectorType.of("STRING")

            "float32" -> ConnectorType.of("FLOAT")
            "float64" -> ConnectorType.of("DOUBLE")

            "date" -> ConnectorType.of("DATEV2")
            // DuckLake stores timestamps at microsecond precision.
            "timestamp" -> ConnectorType.of("DATETIMEV2", MICROS_SCALE, 0)
            // DEGRADED (BE-gated, documented): the ideal mapping is TIMESTAMPTZ
            // (zone-aware), and the FE now accepts it — but the 4.1.0 BE parquet
            // reader can't convert a TIMESTAMP_MICROS(isAdjustedToUtc) column
            // into a TimeStampTz slot ("Unsupported type change: DateTimeV2 =>
            // TimeStampTz"), so even a real DuckLake timestamptz FILE is
            // unreadable as TIMESTAMPTZ on this BE. DuckLake stores timestamptz
            // as a pure UTC instant and the parquet is UTC-micros, so reading it
            // as naive DATETIMEV2(6) yields the correct UTC wall-clock values
            // (typed as zone-naive). This is the readable-vs-unreadable
            // tradeoff; revisit to TIMESTAMPTZ when the BE supports the
            // conversion. Tracked in the friction log + TODO-read.
            "timestamptz" -> ConnectorType.of("DATETIMEV2", MICROS_SCALE, 0)
            "timestamp_s" -> ConnectorType.of("DATETIMEV2", 0, 0)
            "timestamp_ms" -> ConnectorType.of("DATETIMEV2", 3, 0)
            // DEGRADED (documented-lossy): Doris DATETIMEV2 caps scale at 6, so nanos
            // clamp to micros — the same widen-to-micros trino applies to every DuckLake
            // temporal (TestDucklakeTimestampTzPrecision). Scale 9 would be rejected (or
            // mis-handled) by the FE's datetimev2 validation.
            "timestamp_ns" -> ConnectorType.of("DATETIMEV2", MICROS_SCALE, 0)

            // DEGRADED: Doris has no first-class TIME type — read as STRING. Iceberg's
            // type mapping returns UNSUPPORTED here; STRING is more usable for v1.
            "time" -> ConnectorType.of("STRING")
            "timetz" -> ConnectorType.of("STRING")

            "varchar" -> ConnectorType.of("STRING")
            "blob" -> ConnectorType.of("VARBINARY", VARBINARY_DEFAULT_LEN, 0)
            "uuid" -> ConnectorType.of("VARBINARY", UUID_BYTE_LEN, 0)

            // DEGRADED placeholders — mirror Trino plugin's choices. No JSON/variant/interval
            // first-class support on the Doris side yet; data is round-trippable as text.
            "json",
            "variant",
            "interval",
            -> ConnectorType.of("STRING")

            // DEGRADED: geometry types stored as raw bytes, no spatial functions.
            "geometry",
            "point",
            "linestring",
            "linestring_z",
            "linestring z",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
            -> ConnectorType.of("VARBINARY", VARBINARY_DEFAULT_LEN, 0)

            else -> {
                val decimal = DECIMAL_PATTERN.matcher(normalized)
                if (decimal.matches()) {
                    val precision = decimal.group(1).toInt()
                    val scale = decimal.group(2).toInt()
                    return ConnectorType.of("DECIMALV3", precision, scale)
                }
                throw IllegalArgumentException("Unsupported DuckLake type: $ducklakeType")
            }
        }
    }

    private fun extractTypeArguments(typeString: String, prefix: String): String =
        typeString.substring(prefix.length + 1, typeString.length - 1)

    private fun splitTopLevelCommas(input: String): List<String> {
        val parts = ArrayList<String>()
        var depth = 0
        var start = 0
        for (i in input.indices) {
            val c = input[i]
            if (c == '<') {
                depth++
            } else if (c == '>') {
                depth--
            } else if (c == ',' && depth == 0) {
                parts.add(input.substring(start, i).trim())
                start = i + 1
            }
        }
        parts.add(input.substring(start).trim())
        return parts
    }
}
