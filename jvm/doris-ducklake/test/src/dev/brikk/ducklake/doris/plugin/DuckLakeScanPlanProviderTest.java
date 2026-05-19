package dev.brikk.ducklake.doris.plugin;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Step 3 of the roadmap: scan-plan provider returning one range per active
 * data file. Goes through the public SPI surface
 * ({@link DuckLakeConnectorProvider#create} → {@link Connector#getScanPlanProvider})
 * so regressions in the connector-side wiring show up here.
 */
class DuckLakeScanPlanProviderTest {

    private static TestingDucklakePostgreSqlCatalogServer server;
    private static DuckLakeTestCatalogBootstrap.IsolatedCatalog isolated;

    @BeforeAll
    static void setUp() throws Exception {
        server = new TestingDucklakePostgreSqlCatalogServer();
        isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "scanplan");
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void emitsOneRangePerActiveDataFile() throws Exception {
        Map<String, String> properties = Map.of(
                "type", "ducklake",
                DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl(),
                DuckLakeConnectorProperties.METADATA_USER, isolated.user(),
                DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password(),
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                isolated.dataDir().toAbsolutePath().toString());

        ConnectorContext ctx = new FakeConnectorContext("dl", 1L);
        DuckLakeConnectorProvider provider = new DuckLakeConnectorProvider();
        try (Connector connector = provider.create(properties, ctx)) {
            ConnectorMetadata metadata = connector.getMetadata(null);
            ConnectorTableHandle handle = metadata.getTableHandle(null, "sales", "orders")
                    .orElseThrow(() -> new AssertionError("expected sales.orders handle"));

            ConnectorScanPlanProvider plan = connector.getScanPlanProvider();
            assertThat(plan).isNotNull();
            assertThat(plan.getScanRangeType()).isEqualTo(ConnectorScanRangeType.FILE_SCAN);

            ConnectorScanRequest req = ConnectorScanRequest.builder()
                    .table(handle)
                    .columns(List.of())
                    .build();
            List<ConnectorScanRange> ranges = plan.planScan(req);

            // Bootstrap inserts rows into sales.orders. DuckLake's writer may
            // coalesce inserts into a single data file (no compaction needed) —
            // assert "at least one" rather than a tight count so this stays
            // robust against writer-side behaviour changes.
            assertThat(ranges).isNotEmpty();

            for (ConnectorScanRange range : ranges) {
                assertThat(range.getRangeType()).isEqualTo(ConnectorScanRangeType.FILE_SCAN);
                assertThat(range.getFileFormat()).isEqualTo("parquet");
                assertThat(range.getStart()).isZero();
                assertThat(range.getFileSize()).isPositive();
                assertThat(range.getLength()).isEqualTo(range.getFileSize());
                assertThat(range.getPartitionValues()).isEmpty();
                assertThat(range.getDeleteFiles()).isEmpty();

                String path = range.getPath()
                        .orElseThrow(() -> new AssertionError("scan range path missing"));
                // Paths in DuckLake start as schema-and-table-relative; the path
                // resolver joins them against the configured warehouse / data dir.
                assertThat(path).startsWith(isolated.dataDir().toAbsolutePath().toString());
                assertThat(path).endsWith(".parquet");
            }

            // Empty table emits no ranges.
            ConnectorTableHandle customers = metadata.getTableHandle(null, "sales", "customers")
                    .orElseThrow(() -> new AssertionError("expected sales.customers handle"));
            List<ConnectorScanRange> emptyRanges = plan.planScan(
                    ConnectorScanRequest.builder().table(customers).columns(List.of()).build());
            assertThat(emptyRanges).isEmpty();
        }
    }

    @Test
    void scanPlanProviderIsCached() throws Exception {
        Map<String, String> properties = Map.of(
                "type", "ducklake",
                DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl(),
                DuckLakeConnectorProperties.METADATA_USER, isolated.user(),
                DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password(),
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                isolated.dataDir().toAbsolutePath().toString());

        try (Connector connector = new DuckLakeConnectorProvider()
                .create(properties, new FakeConnectorContext("dl", 1L))) {
            ConnectorScanPlanProvider first = connector.getScanPlanProvider();
            ConnectorScanPlanProvider second = connector.getScanPlanProvider();
            assertThat(second).isSameAs(first);
        }
    }

    @Test
    void scanNodePropertiesCarryStorageCredentialsUnderLocationPrefix() throws Exception {
        // Catalog properties include the s3.* credentials a SELECT * needs the BE
        // to reach MinIO. Plus a Doris-injected engine property that must NOT
        // leak through to the BE (storage credentials only).
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "ducklake");
        properties.put(DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl());
        properties.put(DuckLakeConnectorProperties.METADATA_USER, isolated.user());
        properties.put(DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password());
        properties.put(DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                isolated.dataDir().toAbsolutePath().toString());
        properties.put("s3.endpoint", "http://trino-ducklake-minio:9000");
        properties.put("s3.access_key", "minioadmin");
        properties.put("s3.secret_key", "minioadmin");
        properties.put("s3.region", "us-east-1");
        properties.put("use_path_style", "true");
        // Doris injects this on every CREATE CATALOG; must be silently dropped
        // because it carries no meaning on the BE.
        properties.put("enable.mapping.varbinary", "false");

        try (Connector connector = new DuckLakeConnectorProvider()
                .create(properties, new FakeConnectorContext("dl", 1L))) {
            ConnectorScanPlanProvider plan = connector.getScanPlanProvider();

            ConnectorTableHandle stub = new DuckLakeTableHandle("sales", "orders", 0L, 0L, 0L);
            Map<String, String> nodeProps = plan.getScanNodeProperties(
                    null, stub, List.of(), Optional.empty());

            // Storage keys are forwarded under the prefix; file_format_type
            // is forwarded as a top-level key (PluginDrivenScanNode reads
            // it directly to dispatch the BE reader). Nothing else escapes.
            assertThat(nodeProps)
                    .containsEntry("file_format_type", "parquet")
                    .containsEntry("ducklake.location.s3.endpoint", "http://trino-ducklake-minio:9000")
                    .containsEntry("ducklake.location.s3.access_key", "minioadmin")
                    .containsEntry("ducklake.location.s3.secret_key", "minioadmin")
                    .containsEntry("ducklake.location.s3.region", "us-east-1")
                    .containsEntry("ducklake.location.use_path_style", "true");
            assertThat(nodeProps).allSatisfy((k, v) -> assertThat(k).matches(
                    "^(file_format_type|" + DuckLakeScanPlanProvider.PROP_LOCATION_PREFIX
                            .replace(".", "\\.") + ".+)$"));
            // No leakage of plugin-private or engine-injected keys.
            assertThat(nodeProps).doesNotContainKeys(
                    "ducklake.location.metadata.url",
                    "ducklake.location.metadata.user",
                    "ducklake.location.metadata.password",
                    "ducklake.location.storage.warehouse",
                    "ducklake.location.type",
                    "ducklake.location.enable.mapping.varbinary");

            // populateScanLevelParams strips the prefix and ALSO emits the
            // canonical AWS_* aliases the BE's s3_util.cpp expects verbatim.
            TFileScanRangeParams params = new TFileScanRangeParams();
            plan.populateScanLevelParams(params, nodeProps);
            assertThat(params.isSetProperties()).isTrue();
            assertThat(params.getProperties())
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
                    .containsEntry("AWS_REGION", "us-east-1");
            assertThat(params.getProperties()).allSatisfy((k, v) ->
                    assertThat(k).doesNotStartWith(DuckLakeScanPlanProvider.PROP_LOCATION_PREFIX));
        }
    }

    @Test
    void canonicalAwsAliasMapping() {
        // Pin the FE→BE key aliasing surface so a typo in the switch can't
        // silently drop credentials on the BE side at runtime.
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.access_key"))
                .isEqualTo("AWS_ACCESS_KEY");
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.secret_key"))
                .isEqualTo("AWS_SECRET_KEY");
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.endpoint"))
                .isEqualTo("AWS_ENDPOINT");
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.region"))
                .isEqualTo("AWS_REGION");
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.session_token"))
                .isEqualTo("AWS_TOKEN");
        // Path-style and unrelated keys have no AWS_ alias.
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("use_path_style")).isNull();
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("s3.path-style-access")).isNull();
        assertThat(DuckLakeScanPlanProvider.canonicalAwsAlias("metadata.url")).isNull();
    }

    @Test
    void populateScanLevelParamsNoopOnEmptyMap() throws Exception {
        Map<String, String> properties = Map.of(
                "type", "ducklake",
                DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl(),
                DuckLakeConnectorProperties.METADATA_USER, isolated.user(),
                DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password(),
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                isolated.dataDir().toAbsolutePath().toString());

        try (Connector connector = new DuckLakeConnectorProvider()
                .create(properties, new FakeConnectorContext("dl", 1L))) {
            ConnectorScanPlanProvider plan = connector.getScanPlanProvider();

            TFileScanRangeParams params = new TFileScanRangeParams();
            plan.populateScanLevelParams(params, null);
            plan.populateScanLevelParams(params, new HashMap<>());
            // Empty input leaves the params struct untouched — no spurious
            // setProperties(empty) call to confuse downstream serialization.
            assertThat(params.isSetProperties()).isFalse();
        }
    }

    @Test
    void isStoragePropertyRecognisesS3Aliases() {
        // Pin the predicate's recognition surface so deploys against
        // Iceberg-style configs (s3.*) and SDK-style configs (AWS_*) both
        // flow through unchanged.
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.endpoint")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.access_key")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("s3.path-style-access")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("AWS_ACCESS_KEY")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("aws.region")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("fs.s3a.impl")).isTrue();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("use_path_style")).isTrue();

        // Plugin / engine keys stay out.
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("metadata.url")).isFalse();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("metadata.user")).isFalse();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("storage.warehouse")).isFalse();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("type")).isFalse();
        assertThat(DuckLakeScanPlanProvider.isStorageProperty("enable.mapping.varbinary")).isFalse();
    }
}
