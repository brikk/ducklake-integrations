/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.airlift.slice.Slices
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.DateType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RowType
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.Optional

class TestDucklakePartitionPruning {

    companion object {
        private lateinit var catalog: DucklakeCatalog
        private lateinit var config: DucklakeConfig
        private var snapshotId: Long = 0
        private lateinit var partitionedTable: DucklakeTable
        private lateinit var temporalPartitionedTable: DucklakeTable
        private lateinit var dailyPartitionedTable: DucklakeTable
        private lateinit var timestampPartitionedTable: DucklakeTable

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            config = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
            snapshotId = catalog.currentSnapshotId

            val schema = catalog.listSchemas(snapshotId).stream()
                .filter { it.schemaName == "test_schema" }
                .findFirst().orElseThrow()
            partitionedTable = catalog.listTables(schema.schemaId, snapshotId).stream()
                .filter { it.tableName == "partitioned_table" }
                .findFirst().orElseThrow()
            temporalPartitionedTable = catalog.listTables(schema.schemaId, snapshotId).stream()
                .filter { it.tableName == "temporal_partitioned_table" }
                .findFirst().orElseThrow()
            dailyPartitionedTable = catalog.listTables(schema.schemaId, snapshotId).stream()
                .filter { it.tableName == "daily_partitioned_table" }
                .findFirst().orElseThrow()
            timestampPartitionedTable = catalog.listTables(schema.schemaId, snapshotId).stream()
                .filter { it.tableName == "timestamp_partitioned_table" }
                .findFirst().orElseThrow()
        }

        @JvmStatic
        @Throws(Exception::class)
        private fun getSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle): List<DucklakeSplit> {
            splitManager.getSplits(
                null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue()
            ).use { splitSource ->
                val splits = ImmutableList.builder<DucklakeSplit>()
                while (!splitSource.isFinished) {
                    for (split in splitSource.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get()) {
                        splits.add(split as DucklakeSplit)
                    }
                }
                return splits.build()
            }
        }
    }

    @Test
    fun testApplyFilterClassifiesEnforcedPredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)

        // Apply filter with region = 'US'
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(regionColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))
            )
        )

        val result: Optional<io.trino.spi.connector.ConstraintApplicationResult<ConnectorTableHandle>> =
            metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // Region predicate should be enforced (partition column with identity transform)
        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(regionColumn)

        // Remaining filter should NOT include region (it's enforced)
        val remaining: TupleDomain<ColumnHandle> = result.get().remainingFilter
        assertThat(remaining.isAll).isTrue()
    }

    @Test
    fun testApplyFilterNonPartitionColumnIsUnenforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val amountColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "amount" }
            .findFirst().orElseThrow().columnId
        val amountColumn = DucklakeColumnHandle(amountColumnId, "amount", DOUBLE, true)

        // Apply filter with amount > 100
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    amountColumn as ColumnHandle, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(DOUBLE, 100.0)),
                        false
                    )
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // Amount should be unenforced (not a partition column)
        assertThat(newHandle.enforcedPredicate.isAll).isTrue()
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()

        // Remaining filter should include amount
        assertThat(result.get().remainingFilter.isAll).isFalse()
    }

    @Test
    fun testApplyFilterAllConstraintReturnsEmpty() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val result = metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue())

        assertThat(result).isEmpty
    }

    @Test
    fun testApplyFilterNoneConstraintProducesUnsatisfiableHandle() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val result = metadata.applyFilter(SESSION, tableHandle, Constraint(TupleDomain.none()))

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle
        assertThat(newHandle.enforcedPredicate.isNone).isTrue()
    }

    @Test
    fun testApplyFilterIsIdempotentForSamePredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(regionColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))
            )
        )

        val filteredHandle = metadata.applyFilter(SESSION, tableHandle, constraint)
            .orElseThrow()
            .handle as DucklakeTableHandle

        val secondApply = metadata.applyFilter(SESSION, filteredHandle, constraint)
        assertThat(secondApply).isEmpty
    }

    @Test
    fun testApplyFilterIgnoresComplexTypePredicate() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val nestedTable = catalog.listSchemas(snapshotId).stream()
            .filter { it.schemaName == "test_schema" }
            .findFirst()
            .flatMap { schema ->
                catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { it.tableName == "nested_table" }
                    .findFirst()
            }
            .orElseThrow()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "nested_table", nestedTable.tableId, snapshotId
        )

        val metadataColumnId = catalog.getTableColumns(nestedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "metadata" }
            .findFirst().orElseThrow().columnId
        val metadataType = RowType.from(
            listOf(
                RowType.Field(Optional.of("key"), VARCHAR),
                RowType.Field(Optional.of("value"), VARCHAR)
            )
        )
        val metadataColumn = DucklakeColumnHandle(metadataColumnId, "metadata", metadataType, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(metadataColumn as ColumnHandle, Domain.onlyNull(metadataType))
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        // Complex types are not pushed down into Ducklake predicates.
        assertThat(result).isEmpty
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerPrunesByPartitionValue() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val regionColumnId = catalog.getTableColumns(partitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "region" }
            .findFirst().orElseThrow().columnId
        val regionColumn = DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true)

        // Create table handle with enforced predicate for region = 'EU'
        val tableHandle = DucklakeTableHandle(
            "test_schema", "partitioned_table",
            partitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    regionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("EU"))
                )
            )
        )

        // Get all splits
        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId, snapshotId
            )
        )

        // Get pruned splits
        val prunedSplits = getSplits(splitManager, tableHandle)

        // Pruned splits should be fewer than all splits
        assertThat(prunedSplits.size).isLessThan(allSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    fun testNonPartitionedTableAllPredicatesUnenforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        // Use simple_table which is NOT partitioned
        val simpleTable = catalog.listSchemas(snapshotId).stream()
            .filter { it.schemaName == "test_schema" }
            .findFirst()
            .flatMap { schema ->
                catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { it.tableName == "simple_table" }
                    .findFirst()
            }
            .orElseThrow()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "simple_table", simpleTable.tableId, snapshotId
        )

        val nameColumnId = catalog.getTableColumns(simpleTable.tableId, snapshotId).stream()
            .filter { it.columnName == "name" }
            .findFirst().orElseThrow().columnId
        val nameColumn = DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true)

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(nameColumn as ColumnHandle, Domain.singleValue(VARCHAR, Slices.utf8Slice("Product A")))
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // No partition specs -> all predicates should be unenforced
        assertThat(newHandle.enforcedPredicate.isAll).isTrue()
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
    }

    @Test
    fun testTemporalPartitionSpecHasYearAndMonthTransforms() {
        val specs = catalog.getPartitionSpecs(temporalPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(2)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
    }

    @Test
    fun testTemporalPartitionFileValuesPresent() {
        val values = catalog.getFilePartitionValues(temporalPartitionedTable.tableId, snapshotId)

        assertThat(values).hasSize(3)
        // Each file should have 2 partition values (year + month)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(2)
        }
    }

    @Test
    fun testApplyFilterClassifiesTemporalPartitionAsEnforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Apply filter with event_date = DATE '2023-06-10'
        val daysSinceEpoch = LocalDate.of(2023, 6, 10).toEpochDay()
        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn as ColumnHandle,
                    Domain.singleValue(DateType.DATE, daysSinceEpoch)
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // event_date has temporal partition transforms (year, month) -> should be enforced
        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(eventDateColumn)

        // Temporal transform pruning is partial; engine must still verify the original predicate.
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
        assertThat(result.get().remainingFilter.isAll).isFalse()
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerPrunesByTemporalPartitionValue() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Predicate: event_date in January 2023 (days 19358..19388)
        val jan1 = LocalDate.of(2023, 1, 1).toEpochDay()
        val jan31 = LocalDate.of(2023, 1, 31).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, jan1, true, jan31, true)
                        ),
                        false
                    )
                )
            )
        )

        // Get all splits (no predicate)
        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId, snapshotId
            )
        )

        // Get pruned splits (only Jan 2023)
        val prunedSplits = getSplits(splitManager, tableHandle)

        // Should have 3 files total (Jan 2023, Jun 2023, Mar 2024) but only 1 after pruning
        assertThat(allSplits.size).isGreaterThan(prunedSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerEpochStrictPrunesCalendarCatalogTooAggressively() {
        val epochStrictConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("epoch")
            .setTemporalPartitionEncodingReadLeniency(false)
        val splitManager = DucklakeSplitManager(
            catalog, epochStrictConfig, DucklakePathResolver(catalog, epochStrictConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)
        assertThat(prunedSplits).isEmpty()
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerEpochLenientReadsCalendarCatalogSafely() {
        val epochLenientConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("epoch")
            .setTemporalPartitionEncodingReadLeniency(true)
        val splitManager = DucklakeSplitManager(
            catalog, epochLenientConfig, DucklakePathResolver(catalog, epochLenientConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testSplitManagerCalendarStrictAndLenientMatchForCalendarCatalog() {
        val calendarStrictConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("calendar")
            .setTemporalPartitionEncodingReadLeniency(false)
        val calendarLenientConfig = DucklakeTestCatalogEnvironment.createDucklakeConfig()
            .setTemporalPartitionEncoding("calendar")
            .setTemporalPartitionEncodingReadLeniency(true)

        val strictSplitManager = DucklakeSplitManager(
            catalog, calendarStrictConfig, DucklakePathResolver(catalog, calendarStrictConfig), NoopSplitAffinityProvider()
        )
        val lenientSplitManager = DucklakeSplitManager(
            catalog, calendarLenientConfig, DucklakePathResolver(catalog, calendarLenientConfig), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()
        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val strictSplits = getSplits(strictSplitManager, tableHandle)
        val lenientSplits = getSplits(lenientSplitManager, tableHandle)

        assertThat(strictSplits).hasSize(1)
        assertThat(lenientSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testTemporalPruningByYearOnly() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Predicate: event_date in all of 2024
        val year2024Start = LocalDate.of(2024, 1, 1).toEpochDay()
        val year2024End = LocalDate.of(2024, 12, 31).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "temporal_partitioned_table",
            temporalPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, year2024Start, true, year2024End, true)
                        ),
                        false
                    )
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // 2023 files should be pruned, only 2024 files remain
        assertThat(allSplits.size).isGreaterThan(prunedSplits.size)
        assertThat(prunedSplits).isNotEmpty
    }

    @Test
    fun testDailyPartitionSpecHasThreeTransforms() {
        val specs = catalog.getPartitionSpecs(dailyPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(3)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
        assertThat(spec.fields[2].transform).isEqualTo(DucklakePartitionTransform.DAY)
    }

    @Test
    fun testDailyPartitionFileValuesPresent() {
        val values = catalog.getFilePartitionValues(dailyPartitionedTable.tableId, snapshotId)

        assertThat(values).hasSize(4)
        // Each file should have 3 partition values (year + month + day)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(3)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesToSingleDay() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Predicate: event_date = '2023-06-15' (should match only that day's file)
        val june15 = LocalDate.of(2023, 6, 15).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.singleValue(DateType.DATE, june15)
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // 4 files total (Jun 15, Jun 20, Jul 1, Jan 10 2024), only Jun 15 should survive
        assertThat(allSplits).hasSize(4)
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesWithinSameMonth() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Predicate: event_date between Jun 14 and Jun 16 (should match only Jun 15, not Jun 20)
        val june14 = LocalDate.of(2023, 6, 14).toEpochDay()
        val june16 = LocalDate.of(2023, 6, 16).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, june14, true, june16, true)
                        ),
                        false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // Jun 20, Jul 1, and Jan 2024 should all be pruned. Only Jun 15 survives.
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testDailyPrunesByMonthAcrossDays() {
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "event_date" }
            .findFirst().orElseThrow().columnId
        val eventDateColumn = DucklakeColumnHandle(
            eventDateColumnId, "event_date", DateType.DATE, true
        )

        // Predicate: all of June 2023 (should match Jun 15 and Jun 20 but not Jul 1 or Jan 2024)
        val june1 = LocalDate.of(2023, 6, 1).toEpochDay()
        val june30 = LocalDate.of(2023, 6, 30).toEpochDay()

        val tableHandle = DucklakeTableHandle(
            "test_schema", "daily_partitioned_table",
            dailyPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    eventDateColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(DateType.DATE, june1, true, june30, true)
                        ),
                        false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // Jun 15 and Jun 20 survive; Jul 1 and Jan 2024 are pruned
        assertThat(prunedSplits).hasSize(2)
    }

    // --- TIMESTAMP_TZ partition pruning tests ---
    // These verify that temporal partition pruning works with TIMESTAMP WITH TIME ZONE columns,
    // specifically with exclusive Range bounds that Trino does NOT normalize to inclusive
    // (unlike DATE which is discrete and gets normalized).

    @Test
    fun testTimestampTzPartitionSpecHasThreeTransforms() {
        val specs = catalog.getPartitionSpecs(timestampPartitionedTable.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        val spec = specs.first()
        assertThat(spec.fields).hasSize(3)
        assertThat(spec.fields[0].transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(spec.fields[1].transform).isEqualTo(DucklakePartitionTransform.MONTH)
        assertThat(spec.fields[2].transform).isEqualTo(DucklakePartitionTransform.DAY)
    }

    @Test
    fun testTimestampTzFilePartitionValuesPresent() {
        val values = catalog.getFilePartitionValues(timestampPartitionedTable.tableId, snapshotId)

        // 3 files: March 7, March 8, June 15
        assertThat(values).hasSize(3)
        for (fileValues in values.values) {
            assertThat(fileValues).hasSize(3) // year + month + day
        }
    }

    @Test
    fun testTimestampTzApplyFilterClassifiesAsEnforced() {
        val typeConverter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — typical day query with exclusive high
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val constraint = Constraint(
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn as ColumnHandle,
                    Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val result = metadata.applyFilter(SESSION, tableHandle, constraint)

        assertThat(result).isPresent
        val newHandle = result.get().handle as DucklakeTableHandle

        // TIMESTAMP_TZ with temporal transforms should be partially enforced
        assertThat(newHandle.enforcedPredicate.isAll).isFalse()
        assertThat(newHandle.enforcedPredicate.domains.orElseThrow()).containsKey(insertedAtColumn)
        // Partially enforced: engine still needs to verify
        assertThat(newHandle.unenforcedPredicate.isAll).isFalse()
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzExclusiveHighPrunesToSingleDay() {
        // This is the exact real-world pattern: WHERE inserted_at >= TIMESTAMP '...' AND inserted_at < TIMESTAMP '...'
        // The < produces an exclusive high bound. Without the boundary fix, day=8 would NOT be pruned.
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — exclusive high at midnight boundary
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val allSplits = getSplits(
            splitManager, DucklakeTableHandle(
                "test_schema", "timestamp_partitioned_table",
                timestampPartitionedTable.tableId, snapshotId
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // 3 files total (Mar 7, Mar 8, Jun 15). Only Mar 7 should survive.
        assertThat(allSplits).hasSize(3)
        assertThat(prunedSplits).hasSize(1)
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzExclusiveHighNotAtBoundaryKeepsBothDays() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T12:00:00Z) — exclusive high at NOON, not a day boundary
        // Day 8 should NOT be pruned because there's data before noon
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L + 43_200_000L, 0, UTC_KEY
        ) // noon on March 8

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)
                        ), false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // Both Mar 7 and Mar 8 should survive (not Jun 15)
        assertThat(prunedSplits).hasSize(2)
    }

    @Test
    @Throws(Exception::class)
    fun testTimestampTzInclusiveHighAtBoundaryKeepsBothDays() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z] — INCLUSIVE high at midnight
        // Day 8 should be kept because midnight of March 8 is explicitly included
        val splitManager = DucklakeSplitManager(
            catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider()
        )

        val insertedAtColumnId = catalog.getTableColumns(timestampPartitionedTable.tableId, snapshotId).stream()
            .filter { it.columnName == "inserted_at" }
            .findFirst().orElseThrow().columnId
        val insertedAtColumn = DucklakeColumnHandle(
            insertedAtColumnId, "inserted_at", TIMESTAMP_TZ_MICROS, true
        )

        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L, 0, UTC_KEY
        )
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            1_772_841_600_000L + 86_400_000L, 0, UTC_KEY
        )

        val tableHandle = DucklakeTableHandle(
            "test_schema", "timestamp_partitioned_table",
            timestampPartitionedTable.tableId, snapshotId,
            TupleDomain.all(),
            TupleDomain.withColumnDomains(
                ImmutableMap.of(
                    insertedAtColumn, Domain.create(
                        ValueSet.ofRanges(
                            Range.range(TIMESTAMP_TZ_MICROS, low, true, high, true)
                        ), false
                    )
                )
            )
        )

        val prunedSplits = getSplits(splitManager, tableHandle)

        // Both Mar 7 and Mar 8 should survive (inclusive high includes midnight of Mar 8)
        assertThat(prunedSplits).hasSize(2)
    }
}
