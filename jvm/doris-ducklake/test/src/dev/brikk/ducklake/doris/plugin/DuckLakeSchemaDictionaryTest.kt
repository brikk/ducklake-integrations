package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.thrift.TFileScanRangeParams

import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Pins the field-id schema dictionary the connector emits so the Doris BE
 * matches file↔table columns by field id (renamed/reordered columns read
 * correctly) with a per-file `name_mapping` fallback. Round-trips through the
 * same encode→decode path the two scan SPI methods use.
 */
internal class DuckLakeSchemaDictionaryTest {

    private fun handle(id: Long, name: String, type: String = "INT") =
        DuckLakeColumnHandle(id, name, ConnectorType.of(type), 0)

    private fun decode(encoded: String?): TFileScanRangeParams {
        val params = TFileScanRangeParams()
        DuckLakeSchemaDictionary.apply(params, encoded)
        return params
    }

    @Test
    fun emitsFieldIdsAndLowercasedSlotNamesForRequestedColumns() {
        val encoded = DuckLakeSchemaDictionary.encode(
            listOf(handle(1, "Id"), handle(2, "Name", "STRING")),
            emptyList(),
        )
        val params = decode(encoded)

        assertThat(params.currentSchemaId).isEqualTo(-1L)
        assertThat(params.historySchemaInfo).hasSize(1)
        val fields = params.historySchemaInfo[0].rootField.fields
        assertThat(fields).hasSize(2)
        assertThat(fields.map { it.fieldPtr.id }).containsExactly(1, 2)
        // Names are lowercased to byte-match the BE scan-slot names.
        assertThat(fields.map { it.fieldPtr.name }).containsExactly("id", "name")
        // No name_mapping when there are no add_files name maps.
        assertThat(fields.all { !it.fieldPtr.isSetNameMapping }).isTrue()
    }

    @Test
    fun carriesNameMappingAlternateNamesPerFieldId() {
        // add_files registered a file whose column 1 is physically named "old_id".
        val nameMaps = listOf(mapOf(1L to "old_id", 2L to "old_name"))
        val encoded = DuckLakeSchemaDictionary.encode(
            listOf(handle(1, "id"), handle(2, "name", "STRING")),
            nameMaps,
        )
        val fields = decode(encoded).historySchemaInfo[0].rootField.fields

        assertThat(fields[0].fieldPtr.nameMapping).containsExactly("old_id")
        assertThat(fields[1].fieldPtr.nameMapping).containsExactly("old_name")
    }

    @Test
    fun unionsAlternateNamesAcrossMultipleFileMappingsDistinctly() {
        // Two files with different historical names for the same field id 1.
        val nameMaps = listOf(mapOf(1L to "name_a"), mapOf(1L to "name_b"), mapOf(1L to "name_a"))
        val encoded = DuckLakeSchemaDictionary.encode(listOf(handle(1, "id")), nameMaps)
        val field = decode(encoded).historySchemaInfo[0].rootField.fields[0]

        assertThat(field.fieldPtr.nameMapping).containsExactly("name_a", "name_b")
    }

    @Test
    fun onlyDescribesRequestedColumnsInOrder() {
        // Projection pushdown requests a subset, out of catalog order.
        val encoded = DuckLakeSchemaDictionary.encode(
            listOf(handle(3, "c"), handle(1, "a")),
            emptyList(),
        )
        val fields = decode(encoded).historySchemaInfo[0].rootField.fields

        assertThat(fields.map { it.fieldPtr.id }).containsExactly(3, 1)
    }

    @Test
    fun encodesNullForNoColumns() {
        assertThat(DuckLakeSchemaDictionary.encode(emptyList(), emptyList())).isNull()
    }

    @Test
    fun excludesNestedColumnsToAvoidBeCrash() {
        // A nested TField without its full subfield tree SIGABRTs the BE's
        // by_parquet_field_id recursion — so struct/list/map columns are
        // omitted from the dictionary (they read by name). Only the scalar
        // survives here.
        val encoded = DuckLakeSchemaDictionary.encode(
            listOf(
                handle(1, "id"),
                DuckLakeColumnHandle(2, "tags", ConnectorType.arrayOf(ConnectorType.of("STRING")), 1),
                DuckLakeColumnHandle(3, "props", ConnectorType.of("STRUCT"), 2),
            ),
            emptyList(),
        )
        val fields = decode(encoded).historySchemaInfo[0].rootField.fields
        assertThat(fields.map { it.fieldPtr.id }).containsExactly(1)
    }

    @Test
    fun encodesNullWhenAllColumnsAreNested() {
        val encoded = DuckLakeSchemaDictionary.encode(
            listOf(DuckLakeColumnHandle(1, "tags", ConnectorType.arrayOf(ConnectorType.of("INT")), 0)),
            emptyList(),
        )
        assertThat(encoded).isNull()
    }

    @Test
    fun applyIsNoOpForNullOrEmpty() {
        val params = TFileScanRangeParams()
        DuckLakeSchemaDictionary.apply(params, null)
        DuckLakeSchemaDictionary.apply(params, "")
        assertThat(params.isSetCurrentSchemaId).isFalse()
        assertThat(params.isSetHistorySchemaInfo).isFalse()
    }
}
