package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeColumn
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types

/**
 * Builds an `org.apache.iceberg.Schema` from DuckLake columns, with iceberg
 * **field_id == DuckLake column_id**. That equality is the invariant DuckLake
 * read-back relies on (Parquet `field_id == ducklake_column.column_id`), and it's
 * what makes a BE-written file resolvable by DuckLake.
 *
 * Serialized to `schema_json` for the write sink ([DuckLakeWritePlanProvider]),
 * mirroring Doris's native `IcebergTableSink` (`SchemaParser.toJson(table.schema())`)
 * — but built from DuckLake metadata since we have no iceberg `Table`.
 *
 * **Scalar types only (v1 INSERT).** Nested (struct/list/map) and types without an
 * unambiguous iceberg mapping (uint64/int128/uint128, time, json/variant, geometry)
 * **throw** — never a silent mis-type, since a wrong schema corrupts the written
 * file. Cross-engine type *fidelity* (does DuckLake read each mapped type back
 * identically) is validated by the live-BE smoke, not here.
 */
internal object DuckLakeIcebergSchema {

    private val DECIMAL = Regex("""decimal\((\d+),\s*(\d+)\)""")

    /** Top-level columns → iceberg `Schema` (field_id = column_id; required iff not nullable). */
    fun of(columns: List<DucklakeColumn>): Schema {
        val fields = columns
            .filter { it.parentColumn == null }
            .sortedBy { it.columnOrder }
            .map { column ->
                val type = icebergType(column.columnType)
                val id = column.columnId.toInt()
                if (column.nullsAllowed) {
                    Types.NestedField.optional(id, column.columnName, type)
                } else {
                    Types.NestedField.required(id, column.columnName, type)
                }
            }
        return Schema(fields)
    }

    private fun icebergType(ducklakeType: String): Type {
        val type = ducklakeType.trim().lowercase()
        require(!isNested(type)) { "nested type not supported for DuckLake write v1: $ducklakeType" }
        return when (type) {
            "boolean" -> Types.BooleanType.get()
            // iceberg has only int(32)/long(64); narrower signed + unsigned-that-fit promote.
            "int8", "int16", "int32", "uint8", "uint16" -> Types.IntegerType.get()
            "int64", "uint32" -> Types.LongType.get()
            "float32" -> Types.FloatType.get()
            "float64" -> Types.DoubleType.get()
            "date" -> Types.DateType.get()
            "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns" -> Types.TimestampType.withoutZone()
            "timestamptz" -> Types.TimestampType.withZone()
            "varchar" -> Types.StringType.get()
            "uuid" -> Types.UUIDType.get()
            "blob" -> Types.BinaryType.get()
            else -> decimalOrThrow(type, ducklakeType)
        }
    }

    private fun isNested(type: String): Boolean =
        type.startsWith("list<") || type.startsWith("struct<") || type.startsWith("map<")

    private fun decimalOrThrow(type: String, original: String): Type {
        val match = DECIMAL.matchEntire(type)
            ?: throw IllegalArgumentException("type not supported for DuckLake write v1: $original")
        return Types.DecimalType.of(match.groupValues[1].toInt(), match.groupValues[2].toInt())
    }
}
