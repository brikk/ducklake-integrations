package dev.brikk.ducklake.doris.plugin

import java.util.Base64
import java.util.Locale

import org.apache.doris.thrift.TColumnType
import org.apache.doris.thrift.TFileScanRangeParams
import org.apache.doris.thrift.TPrimitiveType
import org.apache.doris.thrift.schema.external.TField
import org.apache.doris.thrift.schema.external.TFieldPtr
import org.apache.doris.thrift.schema.external.TSchema
import org.apache.doris.thrift.schema.external.TStructField
import org.apache.thrift.TDeserializer
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol

/**
 * Builds the native-reader **schema dictionary** (`current_schema_id` +
 * `history_schema_info`) that makes the Doris BE match file↔table columns
 * **by field id** across schema evolution (renamed / reordered columns),
 * instead of falling back to NAME matching — which silently reads NULL for a
 * renamed column. Faithful DuckLake analogue of the iceberg connector's
 * `IcebergSchemaUtils` (`fe-connector-iceberg`); see the friction log and
 * `TODO-read.md` (`GAP_NAME_MAPPING`).
 *
 * **Why this works for DuckLake unchanged from iceberg:** DuckLake writes
 * `field_id` into its Parquet file metadata (verified: `parquet_schema(...)`
 * shows `field_id = column_id`), and DuckLake's `column_id` IS the stable
 * field id. So the BE's `iceberg_reader.cpp` field-id path
 * (`by_parquet_field_id`) matches the file's embedded ids to this dictionary's
 * ids by equality. Like iceberg, we emit exactly ONE schema entry
 * (`current_schema_id = -1`) keyed off the requested columns.
 *
 * **`name_mapping` (the `add_files` / legacy-file fallback):** files
 * registered via `ducklake_add_data_files` (or otherwise written with
 * different column names / no embedded ids) carry a per-file
 * `ducklake_data_file.mapping_id` → `ducklake_name_mapping` row that maps
 * `target_field_id → source_name`. Carrying those alternate names on each
 * [TField.name_mapping] lets the BE's `by_parquet_field_id_with_name_mapping`
 * fallback bind a file column by name when it has no embedded field id.
 *
 * **v1 scope: SCALAR top-level columns only.** The dictionary is built ONLY
 * from scalar columns. Nested columns (list/struct/map) are deliberately
 * OMITTED — they read by name today (which works for un-renamed nested
 * columns), and emitting a nested-type [TField] without its full subfield
 * tree crashes the BE: `by_parquet_field_id` recurses into a container field
 * expecting children (`be/src/format/table/table_format_reader.cpp:383,422`)
 * and SIGABRTs the BE when they're absent. Per-subfield field-id evolution
 * (the correct nested dictionary) is a later item; the catalog's `getNameMaps`
 * itself returns only `parent_column IS NULL` top-level entries. A dictionary
 * covering a subset of columns is safe: the BE only field-id-matches the
 * columns present and name-matches the rest.
 */
internal object DuckLakeSchemaDictionary {

    // -1 == "latest"/current schema sentinel, mirroring iceberg's single-entry dict.
    private const val CURRENT_SCHEMA_ID: Long = -1L

    /**
     * Encode the base64 schema dictionary for the requested [columns], carrying
     * the union of the [nameMaps]' alternate source-names per field id. The
     * dictionary's top-level [TField] names are the lowercased Doris slot names
     * (BE's `StructNode` looks children up by that lowercase name). Returns
     * `null` when there are no columns to describe (count-only scans etc.).
     */
    fun encode(
        columns: List<DuckLakeColumnHandle>,
        nameMaps: Collection<Map<Long, String>>,
    ): String? {
        // Scalar columns only — see class doc (nested TField without a subfield
        // tree SIGABRTs the BE's by_parquet_field_id recursion).
        val scalarColumns = columns.filter { isScalar(it.columnType.typeName) }
        if (scalarColumns.isEmpty()) {
            return null
        }
        val alternateNamesByFieldId = safeAlternateNames(nameMaps)

        val root = TStructField()
        for (col in scalarColumns) {
            root.addToFields(fieldPtr(buildField(col, alternateNamesByFieldId[col.columnId])))
        }
        val schema = TSchema()
            .setSchemaId(CURRENT_SCHEMA_ID)
            .setRootField(root)
        val serializer = TSerializer(TBinaryProtocol.Factory())
        // Carry the dict on a throwaway TFileScanRangeParams (the same struct
        // the BE reads current_schema_id / history_schema_info off), mirroring
        // IcebergSchemaUtils' transport shape.
        val carrier = TFileScanRangeParams()
            .setCurrentSchemaId(CURRENT_SCHEMA_ID)
        carrier.addToHistorySchemaInfo(schema)
        return Base64.getEncoder().encodeToString(serializer.serialize(carrier))
    }

    /**
     * Decode [encoded] and copy `current_schema_id` + `history_schema_info`
     * onto the real scan [params]. Fails loud on a decode error — the prop is
     * produced by [encode], so a failure is a real bug, and silently dropping
     * it would re-introduce silent wrong-rows on renamed-column reads.
     */
    fun apply(params: TFileScanRangeParams, encoded: String?) {
        if (encoded.isNullOrEmpty()) {
            return
        }
        val carrier = TFileScanRangeParams()
        TDeserializer(TBinaryProtocol.Factory()).deserialize(carrier, Base64.getDecoder().decode(encoded))
        if (carrier.isSetCurrentSchemaId) {
            params.currentSchemaId = carrier.currentSchemaId
        }
        if (carrier.isSetHistorySchemaInfo) {
            params.historySchemaInfo = carrier.historySchemaInfo
        }
    }

    /**
     * Per-field-id `name_mapping` alternate source-names for the id-less-file
     * fallback, **conflict-filtered**. Each [nameMaps] entry is one file's
     * `mapping_id` row: `target_field_id → source_name`. Because the dictionary
     * is table-level (one for all files in the scan) while each file carries
     * its own mapping, a physical name that maps to DIFFERENT field ids across
     * files (e.g. `col2` → field-id 2 in old files, → field-id 4 in a re-added
     * column) is AMBIGUOUS: attaching it to the newer field id would make the
     * BE bind an old file's physical `col2` onto the new column (silent wrong
     * rows — the `add_files.test` DROP+re-ADD case). Such a name is dropped
     * from the alternates entirely; the field then binds only by embedded
     * field id or by its exact table name, which is correct (old rows read
     * NULL for the new column). A name that maps to exactly ONE field id
     * across all files is safe and kept.
     */
    private fun safeAlternateNames(nameMaps: Collection<Map<Long, String>>): Map<Long, List<String>> {
        // source-name (lowercased) -> the set of distinct field ids it maps to.
        val fieldIdsByName = HashMap<String, MutableSet<Long>>()
        for (map in nameMaps) {
            for ((fieldId, sourceName) in map) {
                fieldIdsByName.getOrPut(sourceName.lowercase(Locale.ROOT)) { HashSet() }.add(fieldId)
            }
        }
        val result = LinkedHashMap<Long, MutableList<String>>()
        for (map in nameMaps) {
            for ((fieldId, sourceName) in map) {
                val ambiguous = (fieldIdsByName[sourceName.lowercase(Locale.ROOT)]?.size ?: 0) > 1
                if (ambiguous) {
                    continue
                }
                result.getOrPut(fieldId) { ArrayList() }.let {
                    if (sourceName !in it) it.add(sourceName)
                }
            }
        }
        return result
    }

    private fun fieldPtr(field: TField): TFieldPtr = TFieldPtr().setFieldPtr(field)

    /**
     * A scalar top-level [TField]: field id (DuckLake `column_id`), the
     * lowercased Doris slot name, and any `name_mapping` alternate names for
     * the id-less-file fallback. `type.type` is a nested-vs-scalar
     * discriminator only — the BE never inspects the specific scalar tag on
     * the field-id path — so a STRING placeholder suffices (only scalars reach
     * here; see [encode]).
     */
    private fun buildField(col: DuckLakeColumnHandle, alternateNames: List<String>?): TField {
        val field = TField()
            .setId(col.columnId.toInt())
            .setName(col.columnName.lowercase(Locale.ROOT))
            .setIsOptional(true)
            .setType(TColumnType().setType(TPrimitiveType.STRING))
        if (!alternateNames.isNullOrEmpty()) {
            field.nameMapping = ArrayList(alternateNames)
        }
        return field
    }

    private fun isScalar(typeName: String): Boolean =
        when (typeName.uppercase(Locale.ROOT)) {
            "ARRAY", "MAP", "STRUCT" -> false
            else -> true
        }
}
