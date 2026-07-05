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
 * Nested and wide-integer type audit against a REAL DuckDB-written catalog:
 * LIST/STRUCT/MAP shapes (including deep composition) plus DuckDB's wide/unsigned
 * integers (HUGEINT/UHUGEINT/UTINYINT/UINTEGER/UBIGINT) and byte-ish scalars
 * (BLOB/UUID/JSON), read back through `DuckLakeConnectorMetadata.getTableSchema`.
 *
 * Ports the FE-visible slice of trino-ducklake's
 * `TestDucklakeCrossEngineTypeAudit.kt` (the `list<T>` read audit and
 * HUGEINT/UHUGEINT widening-on-read sections). Values are BE-side; what the FE
 * owns is the type shape, and the sharp edge is that DuckLake persists nested
 * columns as a bare `list`/`struct`/`map` parent row plus child rows (probed
 * 2026-07-05) — `JdbcDucklakeCatalog.resolveColumnType` must reconstruct the
 * composite string before [DuckLakeTypeMapping] ever sees it. The pure-string
 * [DuckLakeTypeMappingTest] cannot catch a reconstruction bug; this audit can.
 */
internal class DuckLakeNestedTypeAuditTest {

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
                "nestedaudit",
                listOf(
                    "CREATE SCHEMA dl.audit",
                    "CREATE TABLE dl.audit.nested_shapes (" +
                        "xs INTEGER[], " +
                        "names VARCHAR[], " +
                        "point STRUCT(id INTEGER, name VARCHAR), " +
                        "counts MAP(VARCHAR, BIGINT), " +
                        "tagged STRUCT(tag VARCHAR, score DOUBLE)[], " +
                        "deep MAP(VARCHAR, STRUCT(xs INTEGER[], m MAP(VARCHAR, DATE))))",
                    "CREATE TABLE dl.audit.wide_scalars (" +
                        "c_huge HUGEINT, c_uhuge UHUGEINT, " +
                        "c_utiny UTINYINT, c_usmall USMALLINT, c_uint UINTEGER, c_ubig UBIGINT, " +
                        "c_dec DECIMAL(18,4), c_blob BLOB, c_uuid UUID, c_json JSON)",
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
    fun listShapesReconstructFromChildRows() {
        withMetadata { md ->
            val types = schemaTypes(md, "nested_shapes")

            val xs = types.getValue("xs")
            assertThat(xs.typeName).isEqualTo("ARRAY")
            assertThat(xs.children).hasSize(1)
            assertThat(xs.children[0].typeName).isEqualTo("INT")

            val names = types.getValue("names")
            assertThat(names.typeName).isEqualTo("ARRAY")
            assertThat(names.children[0].typeName).isEqualTo("STRING")
        }
    }

    @Test
    fun structAndMapShapesKeepFieldNamesAndKeyValueOrder() {
        withMetadata { md ->
            val types = schemaTypes(md, "nested_shapes")

            // Field NAMES matter as much as field types: the BE resolves subfield
            // access by name through the FE-provided shape (trino's nested-field
            // tests lean on the same property).
            val point = types.getValue("point")
            assertThat(point.typeName).isEqualTo("STRUCT")
            assertThat(point.fieldNames).containsExactly("id", "name")
            assertThat(point.children.map { it.typeName }).containsExactly("INT", "STRING")

            // MAP children are positional: key first, value second. A swap here
            // type-checks in many cases (both children exist) but corrupts reads.
            val counts = types.getValue("counts")
            assertThat(counts.typeName).isEqualTo("MAP")
            assertThat(counts.children.map { it.typeName }).containsExactly("STRING", "BIGINT")
        }
    }

    @Test
    fun deepCompositionSurvivesReconstruction() {
        withMetadata { md ->
            val types = schemaTypes(md, "nested_shapes")

            // list<struct<...>> — the composite the trino list-audit found most
            // fragile on the inlined path; at FE level the shape must nest cleanly.
            val tagged = types.getValue("tagged")
            assertThat(tagged.typeName).isEqualTo("ARRAY")
            val element = tagged.children[0]
            assertThat(element.typeName).isEqualTo("STRUCT")
            assertThat(element.fieldNames).containsExactly("tag", "score")
            assertThat(element.children.map { it.typeName }).containsExactly("STRING", "DOUBLE")

            // map<varchar, struct<list<int32>, map<varchar,date>>> — three levels;
            // reconstruction is recursive, so this is the regression canary for
            // depth-related bugs (off-by-one child ordering, dropped grandchildren).
            val deep = types.getValue("deep")
            assertThat(deep.typeName).isEqualTo("MAP")
            assertThat(deep.children[0].typeName).isEqualTo("STRING")
            val deepValue = deep.children[1]
            assertThat(deepValue.typeName).isEqualTo("STRUCT")
            assertThat(deepValue.fieldNames).containsExactly("xs", "m")
            assertThat(deepValue.children[0].typeName).isEqualTo("ARRAY")
            assertThat(deepValue.children[0].children[0].typeName).isEqualTo("INT")
            assertThat(deepValue.children[1].typeName).isEqualTo("MAP")
            assertThat(deepValue.children[1].children.map { it.typeName })
                .containsExactly("STRING", "DATEV2")
        }
    }

    @Test
    fun wideAndUnsignedIntegersWidenWithoutOverflow() {
        withMetadata { md ->
            val types = schemaTypes(md, "wide_scalars")

            // The trino audit reads HUGEINT back as DECIMAL(38,0); doris mirrors it.
            val huge = types.getValue("c_huge")
            assertThat(huge.typeName).isEqualTo("DECIMALV3")
            assertThat(huge.precision).isEqualTo(38)
            assertThat(huge.scale).isEqualTo(0)

            // Unsigned types must widen to the next signed type that holds the FULL
            // range — mapping uint32 to INT would silently wrap values > 2^31.
            assertThat(types.getValue("c_utiny").typeName).isEqualTo("SMALLINT")
            assertThat(types.getValue("c_usmall").typeName).isEqualTo("INT")
            assertThat(types.getValue("c_uint").typeName).isEqualTo("BIGINT")
            val ubig = types.getValue("c_ubig")
            assertThat(ubig.typeName).isEqualTo("DECIMALV3")
            assertThat(ubig.precision).isEqualTo(20)

            // uint128 exceeds DECIMALV3(38) — documented-lossy degrade to STRING
            // (DuckLakeTypeMapping marks it DEGRADED). Pinned so it stays a choice.
            assertThat(types.getValue("c_uhuge").typeName).isEqualTo("STRING")
        }
    }

    @Test
    fun byteishScalarsMapToBoundedBinaryOrText() {
        withMetadata { md ->
            val types = schemaTypes(md, "wide_scalars")

            val dec = types.getValue("c_dec")
            assertThat(dec.typeName).isEqualTo("DECIMALV3")
            assertThat(dec.precision).isEqualTo(18)
            assertThat(dec.scale).isEqualTo(4)

            assertThat(types.getValue("c_blob").typeName).isEqualTo("VARBINARY")
            val uuid = types.getValue("c_uuid")
            assertThat(uuid.typeName).isEqualTo("VARBINARY")
            assertThat(uuid.precision).isEqualTo(16)

            // JSON is a documented-lossy STRING (no doris-side JSON pushdown yet).
            assertThat(types.getValue("c_json").typeName).isEqualTo("STRING")
        }
    }
}
