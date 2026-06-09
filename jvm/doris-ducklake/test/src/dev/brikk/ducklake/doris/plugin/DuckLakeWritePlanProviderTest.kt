package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.handle.ConnectorTransaction
import org.apache.doris.connector.api.handle.ConnectorWriteHandle
import org.apache.doris.thrift.TDataSinkType
import org.apache.doris.thrift.TFileCompressType
import org.apache.doris.thrift.TFileFormatType
import org.apache.doris.thrift.TFileType
import org.apache.iceberg.PartitionSpecParser
import org.apache.iceberg.SchemaParser
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Optional

/**
 * FE-side coverage of [DuckLakeWritePlanProvider] — it assembles the
 * `TIcebergTableSink` the BE writes through. Per the connector-SPI migration's own
 * tests, sink construction is FE-unit-testable headless; the BE-defined formats
 * (`output_path`/`hadoop_config`) are validated by the compose smoke. The
 * `schema_json` is checked via iceberg's own `SchemaParser` (independent oracle).
 */
internal class DuckLakeWritePlanProviderTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "writeplan")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    private fun openCatalog(): JdbcDucklakeCatalog {
        val config = DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = isolated.jdbcUrl()
            catalogDatabaseUser = isolated.user()
            catalogDatabasePassword = isolated.password()
            dataPath = isolated.dataDir().toAbsolutePath().toString()
        }
        return JdbcDucklakeCatalog(config)
    }

    private inline fun <R> withCatalog(block: (JdbcDucklakeCatalog) -> R): R {
        val catalog = openCatalog()
        return try {
            block(catalog)
        } finally {
            catalog.close()
        }
    }

    private fun provider(catalog: JdbcDucklakeCatalog): DuckLakeWritePlanProvider {
        val warehouse = isolated.dataDir().toAbsolutePath().toString()
        return DuckLakeWritePlanProvider(
            catalog,
            DuckLakePathResolver(catalog, warehouse),
            DorisTestIdiomKit.isolatedProperties(isolated),
        )
    }

    private fun handleFor(catalog: JdbcDucklakeCatalog, schema: String, table: String): ConnectorTableHandle =
        DuckLakeConnectorMetadata(catalog).getTableHandle(null, schema, table)
            .orFail("expected $schema.$table handle")

    private fun ordersHandle(catalog: JdbcDucklakeCatalog): ConnectorTableHandle =
        handleFor(catalog, "sales", "orders")

    /** Drive planWrite for [schema].[table] with a fresh bound transaction; return the sink. */
    private fun sinkFor(catalog: JdbcDucklakeCatalog, schema: String, table: String) =
        provider(catalog)
            .planWrite(FakeSession(DuckLakeConnectorTransaction(99L, catalog)), FakeWriteHandle(handleFor(catalog, schema, table), overwrite = false))
            .dataSink.icebergTableSink

    @Test
    @Throws(Exception::class)
    fun planWriteBuildsIcebergSinkForOrders() {
        withCatalog { catalog ->
            val tableHandle = ordersHandle(catalog)
            val transaction = DuckLakeConnectorTransaction(7L, catalog)
            val session = FakeSession(transaction)

            val plan = provider(catalog).planWrite(session, FakeWriteHandle(tableHandle, overwrite = false))

            val dataSink = plan.dataSink
            assertThat(dataSink.type).isEqualTo(TDataSinkType.ICEBERG_TABLE_SINK)
            val sink = dataSink.icebergTableSink
            assertThat(sink.dbName).isEqualTo("sales")
            assertThat(sink.tbName).isEqualTo("orders")
            assertThat(sink.fileFormat).isEqualTo(TFileFormatType.FORMAT_PARQUET)
            assertThat(sink.compressionType).isEqualTo(TFileCompressType.ZSTD) // BE rejects UNKNOWN
            assertThat(sink.isOverwrite).isFalse()
            // output path is the resolved table data dir (under the warehouse); local FS in tests.
            assertThat(sink.outputPath).startsWith(isolated.dataDir().toAbsolutePath().toString())
            assertThat(sink.fileType).isEqualTo(TFileType.FILE_LOCAL)
            // schema_json is a well-formed iceberg schema (iceberg's own parser is the oracle)
            // carrying the table's columns.
            val schema = SchemaParser.fromJson(sink.schemaJson)
            assertThat(schema.columns().map { it.name() }).contains("id", "total")
            // orders is unpartitioned → no partition spec on the sink.
            assertThat(sink.isSetPartitionSpecId).isFalse()
            assertThat(sink.partitionSpecsJson).isNull()
        }
    }

    @Test
    @Throws(Exception::class)
    fun planWriteSetsIdentityPartitionSpecForByRegion() {
        withCatalog { catalog ->
            val sink = sinkFor(catalog, "sales", "by_region") // PARTITIONED BY (region)

            assertThat(sink.isSetPartitionSpecId).isTrue()
            assertThat(sink.partitionSpecId).isEqualTo(DuckLakeIcebergPartitionSpec.SINK_SPEC_ID)
            // partition_specs_json is keyed by spec id and parses (iceberg's own oracle) to
            // a single IDENTITY field on `region`.
            val json = sink.partitionSpecsJson.getValue(DuckLakeIcebergPartitionSpec.SINK_SPEC_ID)
            val spec = PartitionSpecParser.fromJson(SchemaParser.fromJson(sink.schemaJson), json)
            assertThat(spec.fields()).singleElement()
                .satisfies({ assertThat(it.transform().toString()).isEqualTo("identity") })
        }
    }

    @Test
    @Throws(Exception::class)
    fun planWriteSetsBucketPartitionSpecForByNameBucket() {
        withCatalog { catalog ->
            val sink = sinkFor(catalog, "sales", "by_name_bucket") // PARTITIONED BY (bucket(4, name))

            assertThat(sink.partitionSpecId).isEqualTo(DuckLakeIcebergPartitionSpec.SINK_SPEC_ID)
            val json = sink.partitionSpecsJson.getValue(DuckLakeIcebergPartitionSpec.SINK_SPEC_ID)
            val spec = PartitionSpecParser.fromJson(SchemaParser.fromJson(sink.schemaJson), json)
            // bucket[4] is the murmur3 % 4 transform the BE must match DuckLake on (smoke-validated).
            assertThat(spec.fields().single().transform().toString()).isEqualTo("bucket[4]")
        }
    }

    @Test
    @Throws(Exception::class)
    fun planWriteRequiresABoundTransaction() {
        withCatalog { catalog ->
            val tableHandle = ordersHandle(catalog)
            assertThatThrownBy {
                provider(catalog).planWrite(FakeSession(null), FakeWriteHandle(tableHandle, overwrite = false))
            }.isInstanceOf(DorisConnectorException::class.java)
        }
    }

    /** Minimal session that just carries (optionally) the engine-opened transaction. */
    private class FakeSession(private var transaction: ConnectorTransaction?) : ConnectorSession {
        override fun getQueryId(): String = "test-query"
        override fun getUser(): String = "test"
        override fun getTimeZone(): String = "UTC"
        override fun getLocale(): String = "en_US"
        override fun getCatalogId(): Long = 1L
        override fun getCatalogName(): String = "dl"
        override fun <T> getProperty(name: String, type: Class<T>): T =
            throw UnsupportedOperationException("getProperty not used in this test session")
        override fun getCatalogProperties(): Map<String, String> = emptyMap()
        override fun getCurrentTransaction(): Optional<ConnectorTransaction> = Optional.ofNullable(transaction)
        override fun setCurrentTransaction(txn: ConnectorTransaction?) {
            transaction = txn
        }
    }

    private class FakeWriteHandle(
        private val table: ConnectorTableHandle,
        private val overwrite: Boolean,
    ) : ConnectorWriteHandle {
        override fun getTableHandle(): ConnectorTableHandle = table
        override fun getColumns(): List<ConnectorColumn> = emptyList() // provider uses the catalog schema
        override fun isOverwrite(): Boolean = overwrite
        override fun getWriteContext(): Map<String, String> = emptyMap()
    }
}
