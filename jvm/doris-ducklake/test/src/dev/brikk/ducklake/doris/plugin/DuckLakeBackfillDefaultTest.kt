package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeColumn

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeConnectorMetadata.backfillDefaultValue] /
 * [DuckLakeConnectorMetadata.isBackfillDefaultSafe] — the mapping of DuckLake's
 * `ducklake_column.initial_default` onto a Doris `ConnectorColumn.defaultValue`, which the FE
 * backfills onto rows in data files written before the column existed
 * (`ALTER TABLE ADD COLUMN b INT DEFAULT 42` → old rows read 42; corpus `issues/issue_1135`).
 *
 * Deliberately conservative (mirrors the write-side stat decode): surface a default only for scalar
 * types whose DuckLake value is a Doris-castable literal, and skip complex/binary/spatial types —
 * the FE parses + casts `getDefaultValueSql()` EAGERLY at scan-plan time, so an uncastable value
 * would break the whole table scan, not just the column. A skipped default degrades to NULL on old
 * rows (today's behavior; not a new correctness bug). Live backfill is a compose-smoke item.
 */
internal class DuckLakeBackfillDefaultTest {

    private fun column(type: String, initialDefault: String?): DucklakeColumn =
        DucklakeColumn(
            columnId = 1L,
            beginSnapshot = 1L,
            endSnapshot = null,
            tableId = 1L,
            columnOrder = 0L,
            columnName = "b",
            columnType = type,
            nullsAllowed = true,
            parentColumn = null,
            initialDefault = initialDefault,
        )

    @Test
    fun surfacesRawDefaultForScalarTypes() {
        // The issue_1135 case + representative scalars across the DuckLake vocabulary. The raw
        // value is passed through untouched; Doris's getDefaultValueSql() quotes/casts per column type.
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("int32", "42"))).isEqualTo("42")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("int64", "-7"))).isEqualTo("-7")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("boolean", "true"))).isEqualTo("true")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("float64", "3.14"))).isEqualTo("3.14")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("decimal(10,2)", "1.50"))).isEqualTo("1.50")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("varchar", "hi 'there'"))).isEqualTo("hi 'there'")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("date", "2024-01-15"))).isEqualTo("2024-01-15")
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("timestamp", "2024-01-15 12:00:00"))).isEqualTo("2024-01-15 12:00:00")
    }

    @Test
    fun nullInitialDefaultYieldsNoDefault() {
        // The common case: a column present since table creation has SQL NULL initial_default.
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("int32", null))).isNull()
    }

    @Test
    fun skipsComplexTypes() {
        // list/struct/map have no castable scalar literal — surface nothing (old rows read NULL).
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("list<int32>", "[1, 2]"))).isNull()
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("struct<a:int32>", "{a: 1}"))).isNull()
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("map<varchar,int32>", "{x: 1}"))).isNull()
    }

    @Test
    fun skipsBinaryAndSpatialTypes() {
        // blob/uuid/geometry/geography → Doris VARBINARY/bytes; DuckDB's rendering isn't castable.
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("blob", "\\x00"))).isNull()
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("uuid", "00000000-0000-0000-0000-000000000000"))).isNull()
        assertThat(DuckLakeConnectorMetadata.backfillDefaultValue(column("geometry", "POINT(0 0)"))).isNull()
    }

    @Test
    fun isBackfillDefaultSafeIsCaseAndWhitespaceInsensitive() {
        assertThat(DuckLakeConnectorMetadata.isBackfillDefaultSafe("  INT32 ")).isTrue()
        assertThat(DuckLakeConnectorMetadata.isBackfillDefaultSafe("STRUCT<a:INT32>")).isFalse()
        assertThat(DuckLakeConnectorMetadata.isBackfillDefaultSafe("BLOB")).isFalse()
    }
}
