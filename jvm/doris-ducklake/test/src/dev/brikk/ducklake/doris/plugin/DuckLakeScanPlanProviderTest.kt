package dev.brikk.ducklake.doris.plugin

import java.util.Optional
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint
import org.apache.doris.connector.api.pushdown.ConnectorIn
import org.apache.doris.connector.api.pushdown.ConnectorLike
import org.apache.doris.connector.api.pushdown.ConnectorLiteral
import org.apache.doris.connector.api.pushdown.ConnectorOr
import org.apache.doris.connector.api.scan.ConnectorScanRangeType
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
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)

        val ctx = FakeConnectorContext("dl", 1L)
        val provider = DuckLakeConnectorProvider()
        provider.create(properties, ctx).use { connector ->
            val metadata = connector.getMetadata(null)
            val handle = metadata.getTableHandle(null, "sales", "orders")
                .orFail("expected sales.orders handle")

            val plan = connector.getScanPlanProvider()
            assertThat(plan).isNotNull()
            assertThat(plan.scanRangeType).isEqualTo(ConnectorScanRangeType.FILE_SCAN)

            val ranges = plan.planScan(null, handle, listOf(), Optional.empty())

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
                    .orFail("scan range path missing")
                // Paths in DuckLake start as schema-and-table-relative; the path
                // resolver joins them against the configured warehouse / data dir.
                assertThat(path).startsWith(isolated.dataDir().toAbsolutePath().toString())
                assertThat(path).endsWith(".parquet")
            }

            // Empty table emits no ranges.
            val customers = metadata.getTableHandle(null, "sales", "customers")
                .orFail("expected sales.customers handle")
            val emptyRanges = plan.planScan(null, customers, listOf(), Optional.empty())
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
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val handle = metadata.getTableHandle(null, "sales", "returns_file")
                    .orFail("expected sales.returns_file handle")

                val plan = connector.getScanPlanProvider()
                val ranges = plan.planScan(null, handle, listOf(), Optional.empty())

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
            "writeup and Step 7.5 in dev-docs/TODO-read.md.",
    )
    @Test
    @Throws(Exception::class)
    fun emitsDeletesForInlineDeletePath() {
        // sales.returns_inline is currently seeded with INSERTs only —
        // there's no inline-delete row in the catalog DB yet (see fixture
        // TODO in the @Disabled message above). Once that's wired, the
        // assertion below should hold.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)

        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val handle = metadata.getTableHandle(null, "sales", "returns_inline")
                    .orFail("expected sales.returns_inline handle")

                val plan = connector.getScanPlanProvider()
                val ranges = plan.planScan(null, handle, listOf(), Optional.empty())

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
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)

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
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
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
    @Throws(Exception::class)
    fun prunesFilesByPartitionEqualityFilter() {
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                // Full scan sees both partitions' files ('us' + 'eu').
                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(2)

                // region = 'us' → applyFilter prunes the 'eu' file(s).
                val region = metadata.getColumnHandles(null, handle)["region"] as DuckLakeColumnHandle
                val filter = ConnectorComparison(
                    ConnectorComparison.Operator.EQ,
                    ConnectorColumnRef("region", region.columnType),
                    ConnectorLiteral(region.columnType, "us"),
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                assertThat(applied.isPresent).isTrue()
                val prunedHandle = applied.get().handle.asDuckLakeHandle<DuckLakeTableHandle>()
                assertThat(prunedHandle.prunedFileIds).isNotNull()

                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                // The 'us' file survives; the 'eu' file(s) are pruned away.
                assertThat(prunedRanges).isNotEmpty()
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByNumericRangeFilter() {
        // Complements the partition-equality test: a numeric open range (GE) on a
        // non-partition column, exercising findDataFileIdsInRange's typed comparison.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                // by_region keeps ≥2 files (one per partition; CHECKPOINT can't
                // compact across partitions, unlike single-file `orders`). Its
                // non-partition `id` column gives a clean numeric range to prune.
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                // Two partition files: 'us' (ids 1, 2) and 'eu' (ids 3, 4).
                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(2)

                // id >= 3 → the 'us' file is pruned by its id stats (max id 2 < 3).
                val id = metadata.getColumnHandles(null, handle)["id"] as DuckLakeColumnHandle
                val filter = ConnectorComparison(
                    ConnectorComparison.Operator.GE,
                    ConnectorColumnRef("id", id.columnType),
                    ConnectorLiteral(id.columnType, 3),
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push id >= 3")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).isNotEmpty()
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByBucketEqualityFilter() {
        // End-to-end BUCKET pruning. bucketPrune is intersected with stats pruning,
        // so a wrong murmur3 hash would compute the wrong target bucket, the
        // intersection would go empty, and isNotEmpty() below would fail — i.e. this
        // can't silently keep the wrong file. (Hash itself is pinned in
        // DuckLakeBucketTransformTest.)
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_name_bucket")
                    .orFail("expected sales.by_name_bucket handle")

                // alice/bob/charlie hash to buckets 1/2/3 → three files.
                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(3)

                // name = 'alice' → only bucket 1's file survives.
                val name = metadata.getColumnHandles(null, handle)["name"] as DuckLakeColumnHandle
                val filter = ConnectorComparison(
                    ConnectorComparison.Operator.EQ,
                    ConnectorColumnRef("name", name.columnType),
                    ConnectorLiteral(name.columnType, "alice"),
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push name = 'alice'")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).isNotEmpty()
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByBucketInListFilter() {
        // IN-list over a bucket column: `name IN ('alice','bob')` keeps buckets
        // {1,2} and prunes charlie's bucket-3 file. Exercises the membership →
        // {bucket(v)} set path that single-equality pruning can't cover.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_name_bucket")
                    .orFail("expected sales.by_name_bucket handle")

                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(3)

                val name = metadata.getColumnHandles(null, handle)["name"] as DuckLakeColumnHandle
                val filter = ConnectorIn(
                    ConnectorColumnRef("name", name.columnType),
                    listOf(
                        ConnectorLiteral(name.columnType, "alice"),
                        ConnectorLiteral(name.columnType, "bob"),
                    ),
                    false,
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push name IN ('alice','bob')")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                // alice + bob survive (2 files); charlie's bucket-3 file is pruned.
                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).hasSize(2)
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByOrOfEqualitiesFilter() {
        // `name = 'alice' OR name = 'bob'` is membership on a single column and must
        // prune identically to the IN-list above — proving the OR → IN normalization.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_name_bucket")
                    .orFail("expected sales.by_name_bucket handle")

                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(3)

                val name = metadata.getColumnHandles(null, handle)["name"] as DuckLakeColumnHandle
                fun eq(value: String) = ConnectorComparison(
                    ConnectorComparison.Operator.EQ,
                    ConnectorColumnRef("name", name.columnType),
                    ConnectorLiteral(name.columnType, value),
                )
                val filter = ConnectorOr(listOf(eq("alice"), eq("bob")))
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push name = 'alice' OR name = 'bob'")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).hasSize(2)
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByNumericInListFilter() {
        // IN-list through the STATS path (not bucket): `id IN (3, 4)` spans 3..4, so
        // by_region's 'us' file (ids 1,2) is pruned by its id stats while 'eu' (3,4)
        // survives. Covers the typed-span pruning independent of partitioning.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(2)

                val id = metadata.getColumnHandles(null, handle)["id"] as DuckLakeColumnHandle
                val filter = ConnectorIn(
                    ConnectorColumnRef("id", id.columnType),
                    listOf(ConnectorLiteral(id.columnType, 3), ConnectorLiteral(id.columnType, 4)),
                    false,
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push id IN (3, 4)")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).isNotEmpty()
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
            }
    }

    @Test
    @Throws(Exception::class)
    fun prunesFilesByPrefixLikeFilter() {
        // `name LIKE 'a%'` → stats range [a, b): alice's file survives, bob/charlie
        // (both > 'b') are pruned by their name stats. LIKE feeds only the stats
        // path (not bucket — a prefix scatters across buckets), so this isolates the
        // prefix → range conversion end-to-end.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_name_bucket")
                    .orFail("expected sales.by_name_bucket handle")

                val allRanges = plan.planScan(null, handle, listOf(), java.util.Optional.empty())
                assertThat(allRanges.size).isGreaterThanOrEqualTo(3)

                val name = metadata.getColumnHandles(null, handle)["name"] as DuckLakeColumnHandle
                val filter = ConnectorLike(
                    ConnectorLike.Operator.LIKE,
                    ConnectorColumnRef("name", name.columnType),
                    ConnectorLiteral(name.columnType, "a%"),
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push name LIKE 'a%'")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()

                // Only alice's file matches the 'a' prefix.
                val prunedRanges = plan.planScan(null, prunedHandle, listOf(), java.util.Optional.empty())
                assertThat(prunedRanges).hasSize(1)
                assertThat(prunedRanges.size).isLessThan(allRanges.size)
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
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)

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


/**
 * P6 read-side features on [DuckLakeScanPlanProvider], split from
 * [DuckLakeScanPlanProviderTest] only to keep each class under the detekt
 * LargeClass budget (same fixture, same SPI surface):
 *
 *  - COUNT(*) pushdown (7-arg `planScan` with `countPushdown=true`): a clean
 *    table collapses to ONE range carrying the exact metadata count (the
 *    emission shape copied from `IcebergScanPlanProvider.planCountPushdown`);
 *    ANY doubt (delete files, filters, pruned file sets) must emit only
 *    normal ranges so the -1 sentinel survives and BE counts by reading
 *    (`PluginDrivenScanNode.resolvePushDownRowCount`).
 *  - partition-bearing ranges: partitioned tables flag
 *    `isPartitionBearing=true` (+ identity values keyed by lowercased column
 *    name), unpartitioned tables keep the pre-P6 `false`/empty shape.
 */
internal class DuckLakeScanPlanCountAndPartitionTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "scanplanagg")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownCollapsesCleanTableToSingleCountRange() {
        // COUNT(*) pushdown on a clean table (no deletes, no filter, no
        // pruning): the scan collapses to ONE range carrying the exact total
        // (2+1 = 3 rows seeded on sales.orders) — the emission shape copied
        // from IcebergScanPlanProvider.planCountPushdown. The BE-side carrier
        // is iceberg's table_level_row_count on the format descriptor.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "orders")
                    .orFail("expected sales.orders handle")

                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.empty(), -1L, null, true,
                )
                assertThat(ranges).hasSize(1)
                val range = ranges[0]
                assertThat(range.pushDownRowCount).isEqualTo(3L)

                // The count range must ALSO carry the total on the thrift
                // descriptor — that is what BE's count reader actually serves.
                val formatDesc = TTableFormatFileDesc()
                val rangeDesc = TFileRangeDesc()
                range.populateRangeParams(formatDesc, rangeDesc)
                assertThat(formatDesc.isSetTableLevelRowCount).isTrue()
                assertThat(formatDesc.tableLevelRowCount).isEqualTo(3L)
            }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownOnEmptyTableEmitsNoRanges() {
        // Iceberg parity: an empty table under count pushdown yields ZERO
        // ranges, so BE sums nothing and COUNT(*) returns 0.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "customers")
                    .orFail("expected sales.customers handle")

                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.empty(), -1L, null, true,
                )
                assertThat(ranges).isEmpty()
            }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownServedForPartitionedTableAndCarriesPartitionShape() {
        // A partitioned but otherwise clean table still serves the metadata
        // count — and the single collapsed range keeps the partition-bearing
        // shape every normal range of the table carries (iceberg's count range
        // goes through the same buildRange as data ranges).
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.empty(), -1L, null, true,
                )
                assertThat(ranges).hasSize(1)
                val range = ranges[0]
                assertThat(range.pushDownRowCount).isEqualTo(4L)
                assertThat(range.isPartitionBearing).isTrue()
            }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownRefusedWhenTableHasDeleteFiles() {
        // sales.returns_file carries a position-delete file, so the file
        // metadata count (4 inserted rows) over-counts the 3 live rows. The
        // provider must emit NO count range — every range keeps the -1
        // sentinel so PluginDrivenScanNode.resolvePushDownRowCount stays -1
        // and BE counts by reading (deletes applied).
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "returns_file")
                    .orFail("expected sales.returns_file handle")

                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.empty(), -1L, null, true,
                )
                // Normal scan shape survives: one range per data file, with the
                // position delete still attached.
                assertThat(ranges).hasSize(1)
                for (range in ranges) {
                    assertThat(range.pushDownRowCount).isEqualTo(-1L)
                    val formatDesc = TTableFormatFileDesc()
                    range.populateRangeParams(formatDesc, TFileRangeDesc())
                    // The -1 sentinel must not leak into the thrift either
                    // (unset == thrift default -1; keeps parity bytes stable).
                    assertThat(formatDesc.isSetTableLevelRowCount).isFalse()
                    assertThat(formatDesc.icebergParams.deleteFiles).hasSize(1)
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownRefusedWhenRemainingFilterPresent() {
        // A remaining (non-pushed) filter expression means the COUNT(*) is
        // over a filtered row set — a whole-table metadata count would be
        // wrong. Normal ranges only.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "orders")
                    .orFail("expected sales.orders handle")

                val id = metadata.getColumnHandles(null, handle)["id"] as DuckLakeColumnHandle
                val remaining = ConnectorComparison(
                    ConnectorComparison.Operator.EQ,
                    ConnectorColumnRef("id", id.columnType),
                    ConnectorLiteral(id.columnType, 1),
                )

                val normalRanges = plan.planScan(null, handle, listOf(), Optional.empty())
                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.of(remaining), -1L, null, true,
                )
                assertThat(ranges).hasSameSizeAs(normalRanges)
                for (range in ranges) {
                    assertThat(range.pushDownRowCount).isEqualTo(-1L)
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun countPushdownRefusedWhenHandleCarriesPrunedFileSet() {
        // applyFilter leaves pushedFilter + prunedFileIds on the handle: the
        // engine-visible remaining filter may be empty, but the scan is no
        // longer whole-table — a metadata count would answer the wrong query.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                val region = metadata.getColumnHandles(null, handle)["region"] as DuckLakeColumnHandle
                val filter = ConnectorComparison(
                    ConnectorComparison.Operator.EQ,
                    ConnectorColumnRef("region", region.columnType),
                    ConnectorLiteral(region.columnType, "us"),
                )
                val applied = metadata.applyFilter(null, handle, ConnectorFilterConstraint(filter))
                    .orFail("expected applyFilter to push region = 'us'")
                val prunedHandle = applied.handle.asDuckLakeHandle<DuckLakeTableHandle>()
                assertThat(prunedHandle.prunedFileIds).isNotNull()

                val ranges = plan.planScan(
                    null, prunedHandle, listOf(), Optional.empty(), -1L, null, true,
                )
                assertThat(ranges).isNotEmpty()
                for (range in ranges) {
                    assertThat(range.pushDownRowCount).isEqualTo(-1L)
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun countSentinelNeverLeaksWhenCountPushdownIsOff() {
        // countPushdown=false must behave exactly like the 4-arg planScan:
        // same range count, every range on the -1 sentinel. A stray
        // precomputed count here would let a non-COUNT query read a collapsed
        // single-range plan.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "orders")
                    .orFail("expected sales.orders handle")

                val normalRanges = plan.planScan(null, handle, listOf(), Optional.empty())
                val ranges = plan.planScan(
                    null, handle, listOf(), Optional.empty(), -1L, null, false,
                )
                assertThat(ranges).hasSameSizeAs(normalRanges)
                for (range in ranges) {
                    assertThat(range.pushDownRowCount).isEqualTo(-1L)
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun partitionedTableRangesArePartitionBearingWithIdentityValues() {
        // Identity-partitioned table: every range flags partition-bearing (so
        // PluginDrivenSplit emits an EMPTY partition-values list instead of
        // null → no Hive key=value path parsing) and carries the file's
        // identity values keyed by lowercased column name (iceberg's
        // getIdentityPartitionInfoMap keying).
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_region")
                    .orFail("expected sales.by_region handle")

                val ranges = plan.planScan(null, handle, listOf(), Optional.empty())
                assertThat(ranges.size).isGreaterThanOrEqualTo(2)

                val seenRegions = HashSet<String>()
                for (range in ranges) {
                    assertThat(range.isPartitionBearing).isTrue()
                    assertThat(range.partitionValues).containsOnlyKeys("region")
                    seenRegions.add(range.partitionValues.getValue("region"))

                    // FE-plan-level only: partition values must NOT surface as
                    // columns_from_path on the wire (BE reads partition columns
                    // from the parquet body; we emit no path_partition_keys).
                    val rangeDesc = TFileRangeDesc()
                    range.populateRangeParams(TTableFormatFileDesc(), rangeDesc)
                    assertThat(rangeDesc.isSetColumnsFromPath).isFalse()
                    assertThat(rangeDesc.isSetColumnsFromPathKeys).isFalse()
                }
                // Both partitions' files show their own value.
                assertThat(seenRegions).containsExactlyInAnyOrder("us", "eu")
            }
    }

    @Test
    @Throws(Exception::class)
    fun bucketPartitionedRangesArePartitionBearingWithoutRawValues() {
        // BUCKET-transformed partitions: the stored value is the derived
        // bucket number, NOT the column value, so it must never be presented
        // as a raw column value (iceberg surfaces identity transforms only).
        // The ranges still flag partition-bearing — the table HAS an active
        // spec — with an empty values map.
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "by_name_bucket")
                    .orFail("expected sales.by_name_bucket handle")

                val ranges = plan.planScan(null, handle, listOf(), Optional.empty())
                assertThat(ranges.size).isGreaterThanOrEqualTo(3)
                for (range in ranges) {
                    assertThat(range.isPartitionBearing).isTrue()
                    assertThat(range.partitionValues).isEmpty()
                }
            }
    }

    @Test
    @Throws(Exception::class)
    fun unpartitionedTableRangesStayNonPartitionBearing() {
        // No active spec → the pre-P6 behaviour is preserved exactly:
        // isPartitionBearing=false and empty values (PluginDrivenSplit keeps
        // its legacy empty→null collapse for these).
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider()
            .create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                val metadata = connector.getMetadata(null)
                val plan = connector.getScanPlanProvider()
                val handle = metadata.getTableHandle(null, "sales", "orders")
                    .orFail("expected sales.orders handle")

                val ranges = plan.planScan(null, handle, listOf(), Optional.empty())
                assertThat(ranges).isNotEmpty()
                for (range in ranges) {
                    assertThat(range.isPartitionBearing).isFalse()
                    assertThat(range.partitionValues).isEmpty()
                }
            }
    }
}
