package dev.brikk.ducklake.doris.plugin

import java.util.Optional
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext
import org.apache.doris.connector.api.scan.ConnectorScanRangeType
import org.apache.doris.connector.api.scan.ConnectorScanRequest
import org.apache.doris.thrift.TFileFormatType
import org.apache.doris.thrift.TFileRangeDesc
import org.apache.doris.thrift.TFileScanRangeParams
import org.apache.doris.thrift.TTableFormatFileDesc
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

/**
 * Step 3 of the roadmap: scan-plan provider returning one range per active
 * data file. Goes through the public SPI surface
 * ([DuckLakeConnectorProvider.create] -> [org.apache.doris.connector.api.Connector.getScanPlanProvider])
 * so regressions in the connector-side wiring show up here.
 */
internal class DuckLakeScanPlanProviderTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "scanplan")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun emitsOneRangePerActiveDataFile() {
        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        val ctx = FakeConnectorContext("dl", 1L)
        val provider = DuckLakeConnectorProvider()
        provider.create(properties, ctx).use { connector ->
            val metadata = connector.getMetadata(null)
            val handle = metadata.getTableHandle(null, "sales", "orders")
                .orElseThrow { AssertionError("expected sales.orders handle") }

            val plan = connector.getScanPlanProvider()
            assertThat(plan).isNotNull()
            assertThat(plan.scanRangeType).isEqualTo(ConnectorScanRangeType.FILE_SCAN)

            val req = ConnectorScanRequest.builder()
                .table(handle)
                .columns(listOf())
                .build()
            val ranges = plan.planScan(req)

            // Bootstrap inserts rows into sales.orders. DuckLake's writer may
            // coalesce inserts into a single data file (no compaction needed) —
            // assert "at least one" rather than a tight count so this stays
            // robust against writer-side behaviour changes.
            assertThat(ranges).isNotEmpty()

            for (range in ranges) {
                assertThat(range.rangeType).isEqualTo(ConnectorScanRangeType.FILE_SCAN)
                assertThat(range.fileFormat).isEqualTo("parquet")
                assertThat(range.start).isZero()
                assertThat(range.fileSize).isPositive()
                assertThat(range.length).isEqualTo(range.fileSize)
                assertThat(range.partitionValues).isEmpty()
                assertThat(range.deleteFiles).isEmpty()

                val path = range.path
                    .orElseThrow { AssertionError("scan range path missing") }
                // Paths in DuckLake start as schema-and-table-relative; the path
                // resolver joins them against the configured warehouse / data dir.
                assertThat(path).startsWith(isolated.dataDir().toAbsolutePath().toString())
                assertThat(path).endsWith(".parquet")
            }

            // Empty table emits no ranges.
            val customers = metadata.getTableHandle(null, "sales", "customers")
                .orElseThrow { AssertionError("expected sales.customers handle") }
            val emptyRanges = plan.planScan(
                ConnectorScanRequest.builder().table(customers).columns(listOf()).build(),
            )
            assertThat(emptyRanges).isEmpty()
        }
    }

    @Test
    @Throws(Exception::class)
    fun emitsPositionDeleteFileForFileBasedDeletePath() {
        // sales.returns_file is seeded with INSERT + DELETE under
        // DATA_INLINING_ROW_LIMIT = 0, so DuckLake writes the deletion as a
        // position-delete parquet plus a ducklake_delete_file row (NOT as
        // catalog-DB inline-delete rows). The catalog's LEFT JOIN in
        // getDataFiles inlines the delete-file path on the DucklakeDataFile;
        // Step 7 surfaces that as a populated iceberg_params.delete_files
        // entry on the scan range's wire descriptor.
        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val handle = metadata.getTableHandle(null, "sales", "returns_file")
                    .orElseThrow { AssertionError("expected sales.returns_file handle") }

                val plan = connector.getScanPlanProvider()
                val ranges = plan.planScan(
                    ConnectorScanRequest.builder().table(handle).columns(listOf()).build(),
                )

                // returns_file has one INSERT batch → one active data file. That
                // one data file carries one active position-delete file.
                assertThat(ranges).hasSize(1)
                val range = ranges[0]

                // Inspect the wire descriptor: that's where the BE reads delete
                // files from (table_format_type="iceberg" → iceberg_params).
                val formatDesc = TTableFormatFileDesc()
                val rangeDesc = TFileRangeDesc()
                range.populateRangeParams(formatDesc, rangeDesc)

                assertThat(formatDesc.isSetIcebergParams).isTrue()
                val iceberg = formatDesc.icebergParams
                assertThat(iceberg.deleteFiles).hasSize(1)

                val delete = iceberg.deleteFiles[0]
                // Path is resolved against the warehouse data dir (delete files
                // live under the same table data path as data files in DuckLake).
                assertThat(delete.path)
                    .startsWith(isolated.dataDir().toAbsolutePath().toString())
                    .endsWith(".parquet")
                assertThat(delete.fileFormat).isEqualTo(TFileFormatType.FORMAT_PARQUET)
                assertThat(delete.content).isEqualTo(1) // POSITION_DELETE
            }
    }

    @Disabled(
        "Step 7.5: inline deletes from ducklake_inlined_delete_<tableId> " +
            "are not yet surfaced to Doris. Two things needed before this " +
            "can pass: (1) fixture work — DuckDB-JDBC 1.5.2 doesn't honour " +
            "data_inlining_row_limit for DELETEs, so the bootstrap must " +
            "INSERT directly into ducklake_inlined_delete_<tableId> over a " +
            "PG JDBC connection to simulate the state DuckDB-Python " +
            "produces by default; (2) connector work — DuckLakeScanPlanProvider " +
            "should consume DucklakeCatalog#getInlinedDeletes(tableId, " +
            "snapshotId) and synthesise a parquet delete file under a " +
            "scratch location, then thread it through DuckLakeScanRange " +
            "the same way file-based deletes are threaded today. " +
            "See ducklake-doris-friction.md (2026-05-19) for the full " +
            "writeup and Step 7.5 in ducklake-doris-todo.md.",
    )
    @Test
    @Throws(Exception::class)
    fun emitsDeletesForInlineDeletePath() {
        // sales.returns_inline is currently seeded with INSERTs only —
        // there's no inline-delete row in the catalog DB yet (see fixture
        // TODO in the @Disabled message above). Once that's wired, the
        // assertion below should hold.
        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val handle = metadata.getTableHandle(null, "sales", "returns_inline")
                    .orElseThrow { AssertionError("expected sales.returns_inline handle") }

                val plan = connector.getScanPlanProvider()
                val ranges = plan.planScan(
                    ConnectorScanRequest.builder().table(handle).columns(listOf()).build(),
                )

                assertThat(ranges).hasSize(1)
                val range = ranges[0]

                val formatDesc = TTableFormatFileDesc()
                val rangeDesc = TFileRangeDesc()
                range.populateRangeParams(formatDesc, rangeDesc)

                // Step 7.5 contract (currently NOT satisfied — that's why this
                // test is @Disabled): the inlined-delete row in
                // ducklake_inlined_delete_<tableId> must surface as at least one
                // TIcebergDeleteFileDesc entry on iceberg_params.delete_files.
                assertThat(formatDesc.icebergParams.deleteFiles).isNotEmpty()
            }
    }

    @Test
    @Throws(Exception::class)
    fun scanPlanProviderIsCached() {
        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val first = connector.getScanPlanProvider()
                val second = connector.getScanPlanProvider()
                assertThat(second).isSameAs(first)
            }
    }

    @Test
    @Throws(Exception::class)
    fun scanNodePropertiesCarryStorageCredentialsUnderLocationPrefix() {
        // Catalog properties include the s3.* credentials a SELECT * needs the BE
        // to reach MinIO. Plus a Doris-injected engine property that must NOT
        // leak through to the BE (storage credentials only).
        val properties = LinkedHashMap<String, String>()
        properties["type"] = "ducklake"
        properties[DuckLakeConnectorProperties.METADATA_URL] = isolated.jdbcUrl()
        properties[DuckLakeConnectorProperties.METADATA_USER] = isolated.user()
        properties[DuckLakeConnectorProperties.METADATA_PASSWORD] = isolated.password()
        properties[DuckLakeConnectorProperties.STORAGE_WAREHOUSE] =
            isolated.dataDir().toAbsolutePath().toString()
        properties["s3.endpoint"] = "http://trino-ducklake-minio:9000"
        properties["s3.access_key"] = "minioadmin"
        properties["s3.secret_key"] = "minioadmin"
        properties["s3.region"] = "us-east-1"
        properties["use_path_style"] = "true"
        // Doris injects this on every CREATE CATALOG; must be silently dropped
        // because it carries no meaning on the BE.
        properties["enable.mapping.varbinary"] = "false"

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val plan = connector.getScanPlanProvider()

                val stub = DuckLakeTableHandle("sales", "orders", 0L, 0L, 0L)
                val nodeProps = plan.getScanNodeProperties(
                    null, stub, listOf(), Optional.empty(),
                )

                // Storage keys are forwarded under the prefix; file_format_type
                // is forwarded as a top-level key (PluginDrivenScanNode reads
                // it directly to dispatch the BE reader). Nothing else escapes.
                assertThat(nodeProps)
                    .containsEntry("file_format_type", "parquet")
                    .containsEntry("ducklake.location.s3.endpoint", "http://trino-ducklake-minio:9000")
                    .containsEntry("ducklake.location.s3.access_key", "minioadmin")
                    .containsEntry("ducklake.location.s3.secret_key", "minioadmin")
                    .containsEntry("ducklake.location.s3.region", "us-east-1")
                    .containsEntry("ducklake.location.use_path_style", "true")
                assertThat(nodeProps).allSatisfy { k, _ ->
                    assertThat(k).matches(
                        "^(file_format_type|" + DuckLakeScanPlanProvider.PROP_LOCATION_PREFIX
                            .replace(".", "\\.") + ".+)$",
                    )
                }
                // No leakage of plugin-private or engine-injected keys.
                assertThat(nodeProps).doesNotContainKeys(
                    "ducklake.location.metadata.url",
                    "ducklake.location.metadata.user",
                    "ducklake.location.metadata.password",
                    "ducklake.location.storage.warehouse",
                    "ducklake.location.type",
                    "ducklake.location.enable.mapping.varbinary",
                )

                // populateScanLevelParams strips the prefix and ALSO emits the
                // canonical AWS_* aliases the BE's s3_util.cpp expects verbatim.
                val params = TFileScanRangeParams()
                plan.populateScanLevelParams(params, nodeProps)
                assertThat(params.isSetProperties).isTrue()
                assertThat(params.properties)
                    // FE-style keys preserved for any consumer that prefers them.
                    .containsEntry("s3.endpoint", "http://trino-ducklake-minio:9000")
                    .containsEntry("s3.access_key", "minioadmin")
                    .containsEntry("s3.secret_key", "minioadmin")
                    .containsEntry("s3.region", "us-east-1")
                    .containsEntry("use_path_style", "true")
                    // BE-canonical aliases for the iceberg+S3 reader path.
                    .containsEntry("AWS_ENDPOINT", "http://trino-ducklake-minio:9000")
                    .containsEntry("AWS_ACCESS_KEY", "minioadmin")
                    .containsEntry("AWS_SECRET_KEY", "minioadmin")
                    .containsEntry("AWS_REGION", "us-east-1")
                assertThat(params.properties).allSatisfy { k, _ ->
                    assertThat(k).doesNotStartWith(DuckLakeScanPlanProvider.PROP_LOCATION_PREFIX)
                }
            }
    }

    @Test
    fun canonicalAwsAliasMapping() {
        // Pin the FE→BE key aliasing surface so a typo in the switch can't
        // silently drop credentials on the BE side at runtime.
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.access_key"))
            .isEqualTo("AWS_ACCESS_KEY")
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.secret_key"))
            .isEqualTo("AWS_SECRET_KEY")
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.endpoint"))
            .isEqualTo("AWS_ENDPOINT")
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.region"))
            .isEqualTo("AWS_REGION")
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.session_token"))
            .isEqualTo("AWS_TOKEN")
        // Path-style and unrelated keys have no AWS_ alias.
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("use_path_style")).isNull()
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.path-style-access")).isNull()
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("metadata.url")).isNull()
    }

    @Test
    @Throws(Exception::class)
    fun populateScanLevelParamsNoopOnEmptyMap() {
        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val plan = connector.getScanPlanProvider()

                val params = TFileScanRangeParams()
                plan.populateScanLevelParams(params, null)
                plan.populateScanLevelParams(params, HashMap())
                // Empty input leaves the params struct untouched — no spurious
                // setProperties(empty) call to confuse downstream serialization.
                assertThat(params.isSetProperties).isFalse()
            }
    }

    @Test
    fun isStoragePropertyRecognisesS3Aliases() {
        // Pin the predicate's recognition surface so deploys against
        // Iceberg-style configs (s3.*) and SDK-style configs (AWS_*) both
        // flow through unchanged.
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.endpoint")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.access_key")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.path-style-access")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("AWS_ACCESS_KEY")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("aws.region")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("fs.s3a.impl")).isTrue()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("use_path_style")).isTrue()

        // Plugin / engine keys stay out.
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("metadata.url")).isFalse()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("metadata.user")).isFalse()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("storage.warehouse")).isFalse()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("type")).isFalse()
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("enable.mapping.varbinary")).isFalse()
    }
}
