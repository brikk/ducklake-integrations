package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Temporal-type fidelity audit: a REAL DuckDB writer creates one column of every
 * DuckLake temporal type, and we assert the [ConnectorType] the doris FE metadata
 * surfaces for each. Ports the FE-visible half of trino-ducklake's temporal
 * findings — `dev-docs/archive/REPORT-datetime-tz-handling.md` (Q1: TIMESTAMPTZ
 * storage is instant-only; TIMESTAMP is wall-clock) and
 * `TestDucklakeTimestampTzPrecision.kt` (DuckLake timestamptz is micros-only) —
 * onto `DuckLakeConnectorMetadata.getTableSchema` / `getColumnHandles`.
 *
 * Why this exists next to [DuckLakeTypeMappingTest]: that test feeds the mapper
 * hand-written type strings; this one audits the strings DuckLake ACTUALLY writes
 * to `ducklake_column.column_type` (probed 2026-07-05: `date`, `time`, `timetz`,
 * `timestamp`, `timestamp_s`, `timestamp_ms`, `timestamp_ns`, `timestamptz`,
 * `interval`, and bare `list`/`struct`/`map` parent rows with child rows that
 * `JdbcDucklakeCatalog.resolveColumnType` reconstructs). A mapper that is right
 * about invented strings but wrong about real catalog rows would pass the unit
 * test and still break `DESC` in the FE.
 */
internal class DuckLakeTemporalTypeAuditTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeAuditFixtureSeeder.seed(
                server,
                "temporalaudit",
                listOf(
                    "CREATE SCHEMA dl.audit",
                    // Every temporal type DuckLake accepts, in one table. DuckDB has no
                    // parameterized TIMESTAMP(n) — precision variants are the distinct
                    // TIMESTAMP_S / TIMESTAMP_MS / TIMESTAMP_NS types.
                    "CREATE TABLE dl.audit.temporal_all (" +
                        "c_date DATE, c_time TIME, c_timetz TIMETZ, " +
                        "c_ts TIMESTAMP, c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, c_ts_ns TIMESTAMP_NS, " +
                        "c_tstz TIMESTAMP WITH TIME ZONE, c_interval INTERVAL)",
                    // Temporal types nested inside LIST/STRUCT/MAP: the catalog stores a
                    // bare `list`/`struct`/`map` parent row plus child rows, so these
                    // exercise the type-string reconstruction path end to end.
                    "CREATE TABLE dl.audit.temporal_nested (" +
                        "instants TIMESTAMPTZ[], " +
                        "evt STRUCT(occurred TIMESTAMPTZ, wall TIMESTAMP), " +
                        "by_day MAP(DATE, TIMESTAMPTZ))",
                ),
            )
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    private inline fun <R> withMetadata(block: (ConnectorMetadata) -> R): R {
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        return DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L))
            .use { connector -> block(connector.getMetadata(null)) }
    }

    private fun schemaTypes(md: ConnectorMetadata, table: String): Map<String, ConnectorType> {
        val handle = md.getTableHandle(null, "audit", table).orFail("expected audit.$table handle")
        return md.getTableSchema(null, handle).columns.associate { it.name to it.type }
    }

    @Test
    fun timestamptzDegradesToNaiveDatetimeUntilTheBeSupportsTimestamptz() {
        withMetadata { md ->
            val types = schemaTypes(md, "temporal_all")
            val tstz = types.getValue("c_tstz")

            // IDEAL: TIMESTAMPTZ (zone-aware) — DuckLake stores it as a pure UTC
            // instant, so degrading to naive would be the trino report's "silent
            // portability bomb". BUT the 4.1.0 BE parquet reader can't convert a
            // TIMESTAMP_MICROS(isAdjustedToUtc) column into a TimeStampTz slot
            // ("Unsupported type change: DateTimeV2 => TimeStampTz") — even a real
            // DuckLake timestamptz FILE is unreadable typed as TIMESTAMPTZ. So we
            // map to DATETIMEV2(6): the stored value IS UTC micros, so the wall
            // clock is correct; only the zone-aware TYPING is lost. Flip back to
            // TIMESTAMPTZ when the BE supports the conversion (friction log +
            // TODO-read). This pins the DELIBERATE degradation.
            assertThat(tstz.typeName).isEqualTo("DATETIMEV2")
            // DuckLake timestamptz is micros-only → scale 6.
            assertThat(tstz.precision).isEqualTo(6)
        }
    }

    @Test
    fun wallClockTimestampVariantsCarryTheirStorageScale() {
        withMetadata { md ->
            val types = schemaTypes(md, "temporal_all")

            // REPORT-datetime-tz-handling Q1 companion data point: TIMESTAMP (no TZ)
            // is wall-clock and session-zone invariant — naive DATETIMEV2 is the
            // faithful mapping, with the scale of the DuckLake storage variant.
            assertThat(types.getValue("c_ts").typeName).isEqualTo("DATETIMEV2")
            assertThat(types.getValue("c_ts").precision).isEqualTo(6)
            assertThat(types.getValue("c_ts_s").precision).isEqualTo(0)
            assertThat(types.getValue("c_ts_ms").precision).isEqualTo(3)

            // DATE is TZ-invariant in DuckLake (same report) — DATEV2 is exact.
            assertThat(types.getValue("c_date").typeName).isEqualTo("DATEV2")
        }
    }

    @Test
    fun timestampNanosSurfacesAsDatetime() {
        withMetadata { md ->
            // Name-level mapping; the (clamped-to-6) scale is audited in the
            // clamp test below.
            assertThat(schemaTypes(md, "temporal_all").getValue("c_ts_ns").typeName)
                .isEqualTo("DATETIMEV2")
        }
    }

    @Test
    fun timestampNanosMustClampToDorisMaxDatetimeScale() {
        withMetadata { md ->
            val tsNs = schemaTypes(md, "temporal_all").getValue("c_ts_ns")
            assertThat(tsNs.typeName).isEqualTo("DATETIMEV2")
            // Doris datetimev2 scale range is 0..6; nanos must clamp (lossy, documented).
            assertThat(tsNs.precision).isEqualTo(6)
        }
    }

    @Test
    fun timeAndIntervalDegradeToStringByDesign() {
        withMetadata { md ->
            val types = schemaTypes(md, "temporal_all")

            // Documented-lossy mappings (DuckLakeTypeMapping marks them DEGRADED):
            // Doris has no first-class TIME/TIMETZ/INTERVAL, so the metadata surfaces
            // STRING rather than failing the whole table. Pinned here so an accidental
            // future change (e.g. TIME -> BIGINT micros) is a conscious decision, not
            // a drive-by: trino keeps TIME as TIME(6) natively, so any doris upgrade
            // should revisit all three together.
            assertThat(types.getValue("c_time").typeName).isEqualTo("STRING")
            assertThat(types.getValue("c_timetz").typeName).isEqualTo("STRING")
            assertThat(types.getValue("c_interval").typeName).isEqualTo("STRING")
        }
    }

    @Test
    fun nestedTimestamptzReconstructsConsistentlyWithTopLevel() {
        withMetadata { md ->
            val types = schemaTypes(md, "temporal_nested")

            // The catalog stores nested columns as a bare `list`/`struct`/`map`
            // parent row + child rows; JdbcDucklakeCatalog.resolveColumnType
            // reconstructs the composite string the mapper parses. Nested
            // timestamptz must degrade to naive DATETIMEV2 exactly like the
            // top-level case (BE-gated; see DuckLakeTypeMapping) — the point is
            // CONSISTENCY at every level, not silent divergence.
            val instants = types.getValue("instants")
            assertThat(instants.typeName).isEqualTo("ARRAY")
            assertThat(instants.children[0].typeName).isEqualTo("DATETIMEV2")
            assertThat(instants.children[0].precision).isEqualTo(6)

            val evt = types.getValue("evt")
            assertThat(evt.typeName).isEqualTo("STRUCT")
            assertThat(evt.fieldNames).containsExactly("occurred", "wall")
            // occurred (timestamptz) + wall (timestamp) both surface DATETIMEV2 now.
            assertThat(evt.children.map { it.typeName })
                .containsExactly("DATETIMEV2", "DATETIMEV2")

            val byDay = types.getValue("by_day")
            assertThat(byDay.typeName).isEqualTo("MAP")
            assertThat(byDay.children.map { it.typeName })
                .containsExactly("DATEV2", "DATETIMEV2")
        }
    }

    @Test
    fun columnHandleTypesAgreeWithTableSchemaTypes() {
        withMetadata { md ->
            val handle = md.getTableHandle(null, "audit", "temporal_all")
                .orFail("expected audit.temporal_all handle")
            val schemaByName = md.getTableSchema(null, handle).columns.associate { it.name to it.type }
            val handles = md.getColumnHandles(null, handle)

            // getTableSchema drives DESC; getColumnHandles drives planning. Both run
            // the mapping independently, so drift between them would give the planner
            // a different temporal type than the one shown to the user.
            assertThat(handles.keys).containsExactlyElementsOf(schemaByName.keys)
            for ((name, columnHandle) in handles) {
                val handleType = (columnHandle as DuckLakeColumnHandle).columnType
                assertThat(handleType.typeName)
                    .`as`("column handle type for %s", name)
                    .isEqualTo(schemaByName.getValue(name).typeName)
                assertThat(handleType.precision)
                    .`as`("column handle precision for %s", name)
                    .isEqualTo(schemaByName.getValue(name).precision)
            }
        }
    }
}
