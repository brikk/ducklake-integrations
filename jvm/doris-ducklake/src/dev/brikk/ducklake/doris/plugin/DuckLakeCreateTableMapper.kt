package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.ConnectorType

/**
 * Maps a Doris [ConnectorType] (a CREATE TABLE column type) to the DuckLake catalog
 * type string stored in `ducklake_column.column_type`. The write-side inverse of
 * [DuckLakeTypeMapping] (which maps DuckLake → Doris for reads); the two round-trip on
 * the non-degraded core (`toDucklakeType(fromDucklakeType(t)) == t`).
 *
 * Conservative by design: only scalar types with an unambiguous DuckLake form map.
 * Everything else — nested array/map/struct, HLL / BITMAP / QUANTILE_STATE / AGG_STATE,
 * JSON / VARIANT, IPV4 / IPV6, … — **throws**. A wrong type silently corrupts the table
 * definition, so we never guess (same philosophy as [DuckLakeIcebergSchema]); unsupported
 * types are a clean DDL error, not a mis-typed column.
 */
internal object DuckLakeCreateTableMapper {

    // The 1:1 scalar mappings (Doris type name → DuckLake type). Parametric types
    // (decimal, datetime) are handled separately since they read precision/scale.
    private val SCALAR = mapOf(
        "BOOLEAN" to "boolean",
        "TINYINT" to "int8",
        "SMALLINT" to "int16",
        "INT" to "int32", "INTEGER" to "int32",
        "BIGINT" to "int64",
        "LARGEINT" to "int128",
        "FLOAT" to "float32",
        "DOUBLE" to "float64",
        "DATE" to "date", "DATEV2" to "date",
        "TIMESTAMPTZ" to "timestamptz", "TIMESTAMPTZV2" to "timestamptz",
        "CHAR" to "varchar", "VARCHAR" to "varchar", "STRING" to "varchar", "TEXT" to "varchar",
        "VARBINARY" to "blob", "BINARY" to "blob",
    )

    fun toDucklakeType(type: ConnectorType): String {
        val name = type.typeName.trim().uppercase()
        SCALAR[name]?.let { return it }
        return when (name) {
            "DECIMAL", "DECIMALV2", "DECIMALV3" -> "decimal(${type.precision},${type.scale})"
            // DuckLakeTypeMapping carries the DATETIMEV2 fractional-second resolution in the
            // PRECISION slot (`of("DATETIMEV2", micros=6, 0)`), so we read precision, not scale
            // (s=0, ms=3, micros=6 native, ns=9). This keeps read/write symmetric.
            "DATETIME", "DATETIMEV2" -> datetimeType(type.precision)
            else -> throw IllegalArgumentException(
                "unsupported Doris type for DuckLake CREATE TABLE v1: ${type.typeName} ($name)",
            )
        }
    }

    private fun datetimeType(precision: Int): String = when (precision) {
        0 -> "timestamp_s"
        3 -> "timestamp_ms"
        9 -> "timestamp_ns"
        else -> "timestamp" // micros (precision 6) — DuckLake's native resolution
    }
}
