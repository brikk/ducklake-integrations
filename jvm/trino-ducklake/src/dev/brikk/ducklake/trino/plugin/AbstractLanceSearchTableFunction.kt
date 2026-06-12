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
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import io.airlift.slice.Slice
import io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorAccessControl
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.function.table.AbstractConnectorTableFunction
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.ArgumentSpecification
import io.trino.spi.function.table.Descriptor
import io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE
import io.trino.spi.function.table.ScalarArgument
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.RealType.REAL
import java.util.Locale
import java.util.Optional

/**
 * Shared analysis machinery for the lance search table functions (`lance_vector_search`,
 * `lance_fts`, `lance_hybrid_search`) under `<catalog>.system.*`. Subclasses declare their
 * argument list, call [resolveSearchTable] for the catalog resolution + v1-scope guards, build
 * their output layout (table columns + score column(s)), and return [analysis].
 *
 * Common scope, enforced here: every data file of the table must be lance-format, no
 * row-level deletes (file-based or inlined), and s3-resident datasets only on the Quack
 * execution engine — its sidecar container carries the `AWS_*` env that lance's object_store
 * needs ([DuckDbS3Config.toObjectStoreEnv], HANDOFF O1); the in-process engine would need the
 * env set on the Trino JVM itself, which the connector can't verify, so it stays rejected.
 * An empty table analyzes fine and yields zero splits.
 */
abstract class AbstractLanceSearchTableFunction(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
        config: DucklakeConfig,
        functionName: String,
        arguments: List<ArgumentSpecification>,
) : AbstractConnectorTableFunction(SYSTEM_SCHEMA, functionName, arguments, GENERIC_TABLE) {
    private val executionEngine: DucklakeExecutionEngine = config.getExecutionEngine()

    /** The catalog-resolved inputs every lance search shares. */
    protected class ResolvedLanceTable(
            val schemaName: String,
            val tableName: String,
            val columnHandles: List<DucklakeColumnHandle>,
            val datasetPaths: List<String>)

    /**
     * Resolve `SCHEMA_NAME`/`TABLE_NAME` from [arguments] to the table's columns (as typed
     * [DucklakeColumnHandle]s, in catalog order) and its lance dataset directories, enforcing
     * the v1 scope and SELECT access on all columns.
     */
    protected fun resolveSearchTable(
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): ResolvedLanceTable {
        val schemaName: String = stringArgument(arguments, ARG_SCHEMA_NAME)
        val tableName: String = stringArgument(arguments, ARG_TABLE_NAME)

        val snapshotId: Long = catalog.currentSnapshotId
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: invalidArgument("Schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: invalidArgument("Table not found: $schemaName.$tableName")

        val columns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
        accessControl.checkCanSelectFromColumns(
                null,
                SchemaTableName(schemaName, tableName),
                columns.map { it.columnName }.toSet())

        return ResolvedLanceTable(
                schemaName,
                tableName,
                columns.map { column ->
                    DucklakeColumnHandle(
                            column.columnId,
                            column.columnName,
                            typeConverter.toTrinoType(column.columnType),
                            column.nullsAllowed)
                },
                resolveLanceDatasetPaths(schema, table, schemaName, tableName, snapshotId))
    }

    /**
     * Look up [columnName] among the table's columns (exact match first, then case-insensitive —
     * the catalog-recorded name is what gets rendered for lance, which matches by name).
     */
    protected fun requireColumn(resolved: ResolvedLanceTable, columnName: String): DucklakeColumnHandle =
        resolved.columnHandles.firstOrNull { it.columnName == columnName }
            ?: resolved.columnHandles.firstOrNull { it.columnName.equals(columnName, ignoreCase = true) }
            ?: invalidArgument("Column '$columnName' not found in ${resolved.schemaName}.${resolved.tableName} "
                    + "(columns: ${resolved.columnHandles.joinToString(", ") { it.columnName }})")

    /** Build the [TableFunctionAnalysis] from the output layout + the function's handle. */
    protected fun analysis(
            outputColumns: List<DucklakeColumnHandle>,
            handle: io.trino.spi.function.table.ConnectorTableFunctionHandle): TableFunctionAnalysis =
        TableFunctionAnalysis.builder()
                .returnedType(Descriptor(outputColumns.map { Descriptor.Field(it.columnName, Optional.of(it.columnType)) }))
                .handle(handle)
                .build()

    private fun resolveLanceDatasetPaths(
            schema: DucklakeSchema,
            table: DucklakeTable,
            schemaName: String,
            tableName: String,
            snapshotId: Long): List<String> {
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(table.tableId, snapshotId)
        if (dataFiles.isEmpty()) {
            return emptyList()
        }
        if (catalog.hasInlinedDeletes(table.tableId, snapshotId)) {
            notSupported("$name does not support tables with row-level deletes yet: $schemaName.$tableName")
        }
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        return dataFiles.map { dataFile ->
            if (dataFile.fileFormat.lowercase(Locale.ROOT) != DucklakeSessionProperties.FORMAT_LANCE) {
                notSupported("$name requires every data file of $schemaName.$tableName to be lance-format; "
                        + "found '${dataFile.fileFormat}' (${dataFile.path})")
            }
            if (dataFile.deleteFilePath != null) {
                notSupported("$name does not support tables with row-level deletes yet: $schemaName.$tableName")
            }
            val resolved: String = pathResolver.resolveFilePath(dataFile.path, dataFile.pathIsRelative, tableDataPath)
            if (isS3Url(resolved) && executionEngine != DucklakeExecutionEngine.QUACK) {
                notSupported("$name over s3-resident lance datasets requires the Quack execution "
                        + "engine (ducklake.execution-engine=quack) with the object_store AWS_* "
                        + "credentials set in the sidecar's environment — the lance extension does "
                        + "not read DuckDB s3 secrets, and the in-process engine cannot set "
                        + "process-global env per query: $resolved")
            }
            resolved
        }
    }

    companion object {
        const val SYSTEM_SCHEMA: String = "system"

        /** Common argument names shared by all lance search functions. */
        const val ARG_SCHEMA_NAME: String = "SCHEMA_NAME"
        const val ARG_TABLE_NAME: String = "TABLE_NAME"
        const val ARG_K: String = "K"
        const val ARG_PREFILTER: String = "PREFILTER"
        const val DEFAULT_K: Long = 10L

        /**
         * Synthetic columnIds for the score columns lance appends after the dataset columns.
         * Real catalog columns have non-negative ids and the queryable virtuals reserve
         * -100..-105 ([VirtualKind]); -1..-3 collide with neither. All are REAL (Arrow float32)
         * and nullable (`_score` is NULL in hybrid output for rows with no text match).
         */
        val DISTANCE_COLUMN: DucklakeColumnHandle = DucklakeColumnHandle(-1L, "_distance", REAL, true)
        val SCORE_COLUMN: DucklakeColumnHandle = DucklakeColumnHandle(-2L, "_score", REAL, true)
        val HYBRID_SCORE_COLUMN: DucklakeColumnHandle = DucklakeColumnHandle(-3L, "_hybrid_score", REAL, true)

        private fun isS3Url(path: String): Boolean =
            path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")

        /** Fail analysis with [INVALID_FUNCTION_ARGUMENT]. */
        @JvmStatic
        protected fun invalidArgument(message: String): Nothing =
            throw TrinoException(INVALID_FUNCTION_ARGUMENT, message)

        /** Fail analysis with [NOT_SUPPORTED]. */
        @JvmStatic
        protected fun notSupported(message: String): Nothing =
            throw TrinoException(NOT_SUPPORTED, message)

        @JvmStatic
        protected fun scalarValue(arguments: Map<String, Argument>, name: String): Any =
            (arguments[name] as ScalarArgument).value
                ?: invalidArgument("$name must not be null")

        @JvmStatic
        protected fun stringArgument(arguments: Map<String, Argument>, name: String): String {
            val value: String = (scalarValue(arguments, name) as Slice).toStringUtf8()
            if (value.isEmpty()) {
                invalidArgument("$name must not be empty")
            }
            return value
        }

        @JvmStatic
        protected fun longArgument(arguments: Map<String, Argument>, name: String): Long =
            scalarValue(arguments, name) as Long

        @JvmStatic
        protected fun booleanArgument(arguments: Map<String, Argument>, name: String): Boolean =
            scalarValue(arguments, name) as Boolean

        /** Extract `K`, validating positivity. (DuckDB-side `k` is best-effort for FTS.) */
        @JvmStatic
        protected fun kArgument(arguments: Map<String, Argument>): Long {
            val k: Long = longArgument(arguments, ARG_K)
            if (k <= 0) {
                invalidArgument("$ARG_K must be positive, got $k")
            }
            return k
        }

        /**
         * The `ARRAY(DOUBLE)` constant arrives as a [Block] of double elements. Reject NULL
         * elements and non-finite values — the vector is rendered as a SQL list literal for
         * DuckDB, where `NaN`/`Infinity` have no portable literal form (and make no sense as
         * an ANN query anyway).
         */
        @JvmStatic
        protected fun queryVectorArgument(arguments: Map<String, Argument>, name: String): List<Double> {
            val block: Block = scalarValue(arguments, name) as Block
            if (block.positionCount == 0) {
                invalidArgument("$name must not be empty")
            }
            val vector: MutableList<Double> = ArrayList(block.positionCount)
            for (i in 0 until block.positionCount) {
                if (block.isNull(i)) {
                    invalidArgument("$name must not contain NULL elements")
                }
                val value: Double = DOUBLE.getDouble(block, i)
                if (!value.isFinite()) {
                    invalidArgument("$name must contain finite values, got $value")
                }
                vector.add(value)
            }
            return vector
        }
    }
}
