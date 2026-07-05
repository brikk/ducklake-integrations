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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.DucklakeTable
import io.airlift.slice.Slice
import io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorAccessControl
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.function.table.AbstractConnectorTableFunction
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.Descriptor
import io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE
import io.trino.spi.function.table.ScalarArgument
import io.trino.spi.function.table.ScalarArgumentSpecification
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.VarcharType.VARCHAR
import java.time.Instant
import java.util.Optional

/**
 * Shared analysis for the DuckLake change-feed functions (`table_insertions` / `table_deletions` /
 * `table_changes`) under `<catalog>.system.*`. Each subclass just names its [ChangeFeedType];
 * this base declares the common argument list, resolves the table + inclusive snapshot window, and
 * returns the [TableFunctionAnalysis] carrying a [ChangeFeedFunctionHandle].
 *
 * Arguments: `SCHEMA_NAME`, `TABLE_NAME`, and the window bounds — each bound may be given as a
 * snapshot id (`START_SNAPSHOT` / `END_SNAPSHOT`, BIGINT) OR a timestamp (`START_TIMESTAMP` /
 * `END_TIMESTAMP`, TIMESTAMP WITH TIME ZONE, resolved to the snapshot active at-or-before it —
 * matching AS OF time-travel). A bound may not be given both ways. `START` is required; `END`
 * defaults to the current snapshot. Bounds are inclusive on both ends (DuckLake spec).
 *
 * The output layout ([ChangeFeedColumns.outputColumns]) prepends `snapshot_id`, `rowid`
 * (+ `change_type` for `table_changes`) to the table's columns read as of the END snapshot.
 */
abstract class AbstractChangeFeedTableFunction(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val feedType: ChangeFeedType,
) : AbstractConnectorTableFunction(
        SYSTEM_SCHEMA,
        feedType.functionName,
        listOf(
                ScalarArgumentSpecification.builder().name(ARG_SCHEMA_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_TABLE_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_START_SNAPSHOT).type(BIGINT).defaultValue(null).build(),
                ScalarArgumentSpecification.builder().name(ARG_END_SNAPSHOT).type(BIGINT).defaultValue(null).build(),
                ScalarArgumentSpecification.builder().name(ARG_START_TIMESTAMP).type(TIMESTAMP_TZ_MILLIS).defaultValue(null).build(),
                ScalarArgumentSpecification.builder().name(ARG_END_TIMESTAMP).type(TIMESTAMP_TZ_MILLIS).defaultValue(null).build()),
        GENERIC_TABLE) {

    override fun analyze(
            session: ConnectorSession,
            transaction: ConnectorTransactionHandle,
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): TableFunctionAnalysis {
        val schemaName: String = stringArgument(arguments, ARG_SCHEMA_NAME)
        val tableName: String = stringArgument(arguments, ARG_TABLE_NAME)

        val startSnapshot: Long = resolveBound(arguments, ARG_START_SNAPSHOT, ARG_START_TIMESTAMP)
                ?: invalidArgument("A start bound is required: pass $ARG_START_SNAPSHOT or $ARG_START_TIMESTAMP")
        val endSnapshot: Long = resolveBound(arguments, ARG_END_SNAPSHOT, ARG_END_TIMESTAMP)
                ?: catalog.currentSnapshotId
        if (startSnapshot > endSnapshot) {
            invalidArgument("Start snapshot $startSnapshot is after end snapshot $endSnapshot")
        }

        val schema: DucklakeSchema = catalog.getSchema(schemaName, endSnapshot)
                ?: invalidArgument("Schema not found at snapshot $endSnapshot: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, endSnapshot)
                ?: invalidArgument("Table not found at snapshot $endSnapshot: $schemaName.$tableName")

        val columns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, endSnapshot)
        accessControl.checkCanSelectFromColumns(
                null,
                SchemaTableName(schemaName, tableName),
                columns.map { it.columnName }.toSet())

        val dataColumns: List<DucklakeColumnHandle> = columns.map { column ->
            DucklakeColumnHandle(
                    column.columnId,
                    column.columnName,
                    typeConverter.toTrinoType(column.columnType),
                    column.nullsAllowed)
        }
        val outputColumns: List<DucklakeColumnHandle> = ChangeFeedColumns.outputColumns(feedType, dataColumns)

        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor(outputColumns.map { Descriptor.Field(it.columnName, Optional.of(it.columnType)) }))
                .handle(ChangeFeedFunctionHandle(
                        feedType,
                        schema.schemaName,
                        table.tableName,
                        table.tableId,
                        startSnapshot,
                        endSnapshot,
                        dataColumns))
                .build()
    }

    /**
     * Resolve one window bound to a snapshot id. Given as a snapshot id ([snapArg]) OR a timestamp
     * ([tsArg], mapped to the snapshot active at-or-before it), not both. Null when neither is set.
     */
    private fun resolveBound(arguments: Map<String, Argument>, snapArg: String, tsArg: String): Long? {
        val snapshotId: Long? = (arguments[snapArg] as ScalarArgument).value as Long?
        val packedTimestamp: Long? = (arguments[tsArg] as ScalarArgument).value as Long?
        if (snapshotId != null && packedTimestamp != null) {
            invalidArgument("Specify $snapArg or $tsArg, not both")
        }
        if (snapshotId != null) {
            catalog.getSnapshot(snapshotId) ?: invalidArgument("Snapshot not found: $snapshotId")
            return snapshotId
        }
        if (packedTimestamp != null) {
            val instant: Instant = Instant.ofEpochMilli(unpackMillisUtc(packedTimestamp))
            val snapshot: DucklakeSnapshot = catalog.getSnapshotAtOrBefore(instant)
                    ?: invalidArgument("No snapshot at or before $instant")
            return snapshot.snapshotId
        }
        return null
    }

    companion object {
        const val SYSTEM_SCHEMA: String = "system"

        const val ARG_SCHEMA_NAME: String = "SCHEMA_NAME"
        const val ARG_TABLE_NAME: String = "TABLE_NAME"
        const val ARG_START_SNAPSHOT: String = "START_SNAPSHOT"
        const val ARG_END_SNAPSHOT: String = "END_SNAPSHOT"
        const val ARG_START_TIMESTAMP: String = "START_TIMESTAMP"
        const val ARG_END_TIMESTAMP: String = "END_TIMESTAMP"

        private fun invalidArgument(message: String): Nothing =
            throw TrinoException(INVALID_FUNCTION_ARGUMENT, message)

        private fun stringArgument(arguments: Map<String, Argument>, name: String): String {
            val raw: Any = (arguments[name] as ScalarArgument).value
                    ?: invalidArgument("$name must not be null")
            val value: String = (raw as Slice).toStringUtf8()
            if (value.isEmpty()) {
                invalidArgument("$name must not be empty")
            }
            return value
        }
    }
}
