package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Unicode-identifier audit for the FE metadata surface: schema/table/column names
 * with CJK, emoji (including multi-code-point ZWJ sequences), spaces, embedded
 * quotes, mixed case, and NFC/NFD-distinct forms, written by a REAL DuckDB writer
 * and read back through `listDatabaseNames` / `listTableNames` / `getTableHandle`
 * / `getTableSchema` / `getColumnHandles`.
 *
 * Ports the identifier-level slice of trino-ducklake's unicode work —
 * `dev-docs/archive/REPORT-string-unicode-audit.md` (BINARY identity: engines
 * compare code points, no normalization/case folding) and the corpus of
 * `TestDucklakeUnicodeStringRoundTrip.kt`. Cell VALUES are BE-side (the BE reads
 * the parquet), so value fidelity is out of scope here; what the FE owns is that
 * names survive byte-for-byte, because a single mangled identifier makes the
 * table unreachable no matter how correct the data underneath is.
 */
internal class DuckLakeUnicodeIdentifierAuditTest {

    companion object {
        // NFC/NFD pair: "café" precomposed (U+00E9, 4 code points) vs decomposed
        // (e + U+0301, 5 code points). Escapes, not literals, so no editor/tooling
        // normalization pass can silently collapse the two into one.
        private const val CAFE_NFC = "caf\u00E9"
        private const val CAFE_NFD = "cafe\u0301"

        // Family emoji: 5 code points joined by ZWJ — the REPORT's grapheme-cluster
        // stress case, here as an identifier.
        private const val ZWJ_FAMILY = "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67"

        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeAuditFixtureSeeder.seed(
                server,
                "unicodeaudit",
                listOf(
                    "CREATE SCHEMA dl.\"数据库\"",
                    "CREATE TABLE dl.\"数据库\".\"订单 🦆\" (" +
                        "\"标识\" INTEGER, " +
                        "\"col with space\" VARCHAR, " +
                        "\"MixedCase\" INTEGER, " +
                        "\"quo\"\"te\" VARCHAR)",
                    "CREATE TABLE dl.\"数据库\".norm_probe (" +
                        "\"$CAFE_NFC\" INTEGER, " +
                        "\"$CAFE_NFD\" VARCHAR, " +
                        "\"$ZWJ_FAMILY\" INTEGER)",
                    "CREATE SCHEMA dl.\"schema with space\"",
                    "CREATE TABLE dl.\"schema with space\".plain (id INTEGER)",
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

    @Test
    fun listsUnicodeSchemasAndTables() {
        withMetadata { md ->
            assertThat(md.listDatabaseNames(null)).contains("数据库", "schema with space")
            assertThat(md.databaseExists(null, "数据库")).isTrue()
            // BINARY identity (per the REPORT): a lookup by a differently-cased or
            // ascii-folded name must NOT accidentally match.
            assertThat(md.databaseExists(null, "数据")).isFalse()

            assertThat(md.listTableNames(null, "数据库"))
                .containsExactlyInAnyOrder("订单 🦆", "norm_probe")
            assertThat(md.listTableNames(null, "schema with space"))
                .containsExactly("plain")
        }
    }

    @Test
    fun unicodeColumnIdentifiersRoundTripExactly() {
        withMetadata { md ->
            val handle = md.getTableHandle(null, "数据库", "订单 🦆")
                .orFail("expected 数据库.订单 🦆 handle")

            val schema = md.getTableSchema(null, handle)
            assertThat(schema.tableName).isEqualTo("订单 🦆")
            // Exact, ordered, byte-for-byte: CJK, embedded space, preserved mixed
            // case (no case folding at the metadata layer), and the embedded quote
            // unescaped from the DDL's "" back to a single ".
            assertThat(schema.columns)
                .extracting<String>(ConnectorColumn::getName)
                .containsExactly("标识", "col with space", "MixedCase", "quo\"te")

            // getColumnHandles keys must agree — it feeds planner name lookups.
            assertThat(md.getColumnHandles(null, handle).keys)
                .containsExactly("标识", "col with space", "MixedCase", "quo\"te")
        }
    }

    @Test
    fun normalizationSensitiveIdentifiersStayBinaryDistinct() {
        withMetadata { md ->
            val handle = md.getTableHandle(null, "数据库", "norm_probe")
                .orFail("expected 数据库.norm_probe handle")
            val names = md.getTableSchema(null, handle).columns.map { it.name }

            // REPORT-string-unicode-audit: both engines use BINARY (code-point)
            // identity — precomposed and decomposed "café" are DIFFERENT names and
            // both must survive as distinct columns. An NFC-normalizing layer
            // anywhere in catalog → metadata would collapse them (or drop one).
            assertThat(names).containsExactly(CAFE_NFC, CAFE_NFD, ZWJ_FAMILY)
            assertThat(CAFE_NFC).isNotEqualTo(CAFE_NFD)
            // The ZWJ sequence must keep all 5 code points; a grapheme-aware
            // truncation (the REPORT's `reverse` divergence class) would shorten it.
            assertThat(names[2].codePointCount(0, names[2].length)).isEqualTo(5)

            // The distinct columns stay individually addressable by exact name.
            val handles = md.getColumnHandles(null, handle)
            assertThat(handles.keys).containsExactly(CAFE_NFC, CAFE_NFD, ZWJ_FAMILY)
            assertThat((handles.getValue(CAFE_NFC) as DuckLakeColumnHandle).columnType.typeName)
                .isEqualTo("INT")
            assertThat((handles.getValue(CAFE_NFD) as DuckLakeColumnHandle).columnType.typeName)
                .isEqualTo("STRING")
        }
    }

    @Test
    fun metadataDdlRoundTripsUnicodeIdentifiers() {
        withMetadata { md ->
            // The reverse direction: doris-side DDL writes unicode identifiers into
            // the catalog, and the read path lists them back. This is the pair to
            // the DuckDB-written fixtures above — both writers must land on the same
            // byte-for-byte identifier story or cross-engine listings diverge.
            md.createDatabase(null, "événements 夏", emptyMap())
            assertThat(md.listDatabaseNames(null)).contains("événements 夏")

            val request = ConnectorCreateTableRequest.builder()
                .dbName("événements 夏")
                .tableName("mesures ☀")
                .columns(
                    listOf(
                        ConnectorColumn("clé", ConnectorType.of("INT"), "", false, null),
                        ConnectorColumn("värde 数", ConnectorType.of("STRING"), "", true, null),
                    ),
                )
                .build()
            md.createTable(null, request)

            assertThat(md.listTableNames(null, "événements 夏")).containsExactly("mesures ☀")
            val handle = md.getTableHandle(null, "événements 夏", "mesures ☀")
                .orFail("expected événements 夏.mesures ☀ handle")
            assertThat(md.getTableSchema(null, handle).columns)
                .extracting<String>(ConnectorColumn::getName)
                .containsExactly("clé", "värde 数")

            // Cleanup through the same surface (drop is metadata-only), proving
            // unicode names stay addressable for the full lifecycle.
            md.dropTable(null, handle)
            md.dropDatabase(null, "événements 夏", false)
            assertThat(md.listDatabaseNames(null)).doesNotContain("événements 夏")
        }
    }
}
