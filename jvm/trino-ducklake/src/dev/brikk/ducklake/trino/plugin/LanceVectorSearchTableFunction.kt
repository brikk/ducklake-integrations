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

import com.google.inject.Inject
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
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.VarcharType.VARCHAR
import java.util.Locale
import java.util.Optional

/**
 * `SELECT * FROM TABLE(<catalog>.system.lance_vector_search(...))` — approximate-nearest-neighbor
 * search over a DuckLake table whose data files are lance dataset directories, executed by the
 * DuckDB `lance` extension's `lance_vector_search` table function (Route A, Phase A3).
 *
 * <p>Arguments: `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME` (the embedding column),
 * `QUERY_VEC` (an `ARRAY(DOUBLE)` constant — a bare `ARRAY[0.1, 0.2]` literal coerces),
 * `K` (top-k, default 10), `PREFILTER` (default false; forwarded to lance — only meaningful
 * once predicate pushdown into the function lands, see TODO-lance O2).
 *
 * <p>Returns the table's columns plus a synthetic `_distance` REAL column, ascending. Each lance
 * dataset fragment is searched independently (one split per dataset directory), so a
 * multi-fragment table returns up to `k × fragments` rows — a superset of the global top-k; wrap
 * with `ORDER BY _distance LIMIT k` for exact global semantics.
 *
 * <p>v1 scope: local-path datasets only (s3 is gated on the lance credential channel, HANDOFF O1),
 * no row-level deletes, and every data file of the table must be lance-format.
 */
class LanceVectorSearchTableFunction @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
) : AbstractConnectorTableFunction(
        SCHEMA_NAME,
        FUNCTION_NAME,
        listOf(
                ScalarArgumentSpecification.builder().name(ARG_SCHEMA_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_TABLE_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_COLUMN_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_QUERY_VEC).type(ArrayType(DOUBLE)).build(),
                ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
                ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build()),
        GENERIC_TABLE) {

    override fun analyze(
            session: ConnectorSession,
            transaction: ConnectorTransactionHandle,
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): TableFunctionAnalysis {
        val schemaName: String = stringArgument(arguments, ARG_SCHEMA_NAME)
        val tableName: String = stringArgument(arguments, ARG_TABLE_NAME)
        val columnName: String = stringArgument(arguments, ARG_COLUMN_NAME)
        val queryVector: List<Double> = queryVectorArgument(arguments)
        val k: Long = longArgument(arguments, ARG_K)
        val prefilter: Boolean = booleanArgument(arguments, ARG_PREFILTER)
        if (k <= 0) {
            invalidArgument("$ARG_K must be positive, got $k")
        }

        val snapshotId: Long = catalog.currentSnapshotId
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: invalidArgument("Schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: invalidArgument("Table not found: $schemaName.$tableName")

        val columns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
        val embeddingColumn: DucklakeColumn = columns.firstOrNull { it.columnName == columnName }
            ?: columns.firstOrNull { it.columnName.equals(columnName, ignoreCase = true) }
            ?: invalidArgument("Column '$columnName' not found in $schemaName.$tableName "
                    + "(columns: ${columns.joinToString(", ") { it.columnName }})")
        if (typeConverter.toTrinoType(embeddingColumn.columnType) !is ArrayType) {
            invalidArgument("Column '${embeddingColumn.columnName}' is not an embedding (array) column: "
                    + embeddingColumn.columnType)
        }

        accessControl.checkCanSelectFromColumns(
                null,
                SchemaTableName(schemaName, tableName),
                columns.map { it.columnName }.toSet())

        val datasetPaths: List<String> = resolveLanceDatasetPaths(schema, table, schemaName, tableName, snapshotId)

        val outputColumns: MutableList<DucklakeColumnHandle> = ArrayList(columns.size + 1)
        for (column in columns) {
            outputColumns.add(DucklakeColumnHandle(
                    column.columnId,
                    column.columnName,
                    typeConverter.toTrinoType(column.columnType),
                    column.nullsAllowed))
        }
        outputColumns.add(DucklakeColumnHandle(DISTANCE_COLUMN_ID, DISTANCE_COLUMN_NAME, REAL, false))

        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor(outputColumns.map { Descriptor.Field(it.columnName, Optional.of(it.columnType)) }))
                .handle(LanceVectorSearchFunctionHandle(
                        datasetPaths,
                        embeddingColumn.columnName,
                        queryVector,
                        k,
                        prefilter,
                        outputColumns))
                .build()
    }

    /**
     * Resolve the table's data files to lance dataset directories, enforcing the v1 scope:
     * all-lance, local paths, no row-level deletes. An empty list (empty table) is fine —
     * the split manager then emits zero splits and the function returns no rows.
     */
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
            notSupported("$FUNCTION_NAME does not support tables with row-level deletes yet: $schemaName.$tableName")
        }
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        return dataFiles.map { dataFile ->
            if (dataFile.fileFormat.lowercase(Locale.ROOT) != DucklakeSessionProperties.FORMAT_LANCE) {
                notSupported("$FUNCTION_NAME requires every data file of $schemaName.$tableName to be lance-format; "
                        + "found '${dataFile.fileFormat}' (${dataFile.path})")
            }
            if (dataFile.deleteFilePath != null) {
                notSupported("$FUNCTION_NAME does not support tables with row-level deletes yet: $schemaName.$tableName")
            }
            val resolved: String = pathResolver.resolveFilePath(dataFile.path, dataFile.pathIsRelative, tableDataPath)
            if (isS3Url(resolved)) {
                notSupported("$FUNCTION_NAME supports local-path lance datasets only for now "
                        + "(s3 is pending the lance credential channel): $resolved")
            }
            resolved
        }
    }

    companion object {
        const val SCHEMA_NAME: String = "system"
        const val FUNCTION_NAME: String = "lance_vector_search"

        /** Distance column appended by lance after the dataset columns; REAL (Arrow float32). */
        const val DISTANCE_COLUMN_NAME: String = "_distance"

        /**
         * Synthetic columnId for the `_distance` output handle. Real catalog columns have
         * non-negative ids and the queryable virtuals reserve -100..-105 ([VirtualKind]);
         * -1 collides with neither.
         */
        const val DISTANCE_COLUMN_ID: Long = -1L

        private const val ARG_SCHEMA_NAME: String = "SCHEMA_NAME"
        private const val ARG_TABLE_NAME: String = "TABLE_NAME"
        private const val ARG_COLUMN_NAME: String = "COLUMN_NAME"
        private const val ARG_QUERY_VEC: String = "QUERY_VEC"
        private const val ARG_K: String = "K"
        private const val ARG_PREFILTER: String = "PREFILTER"
        private const val DEFAULT_K: Long = 10L

        private fun isS3Url(path: String): Boolean =
            path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")

        /** Fail analysis with [INVALID_FUNCTION_ARGUMENT]. */
        private fun invalidArgument(message: String): Nothing =
            throw TrinoException(INVALID_FUNCTION_ARGUMENT, message)

        /** Fail analysis with [NOT_SUPPORTED]. */
        private fun notSupported(message: String): Nothing =
            throw TrinoException(NOT_SUPPORTED, message)

        private fun scalarValue(arguments: Map<String, Argument>, name: String): Any =
            (arguments[name] as ScalarArgument).value
                ?: invalidArgument("$name must not be null")

        private fun stringArgument(arguments: Map<String, Argument>, name: String): String {
            val value: String = (scalarValue(arguments, name) as Slice).toStringUtf8()
            if (value.isEmpty()) {
                invalidArgument("$name must not be empty")
            }
            return value
        }

        private fun longArgument(arguments: Map<String, Argument>, name: String): Long =
            scalarValue(arguments, name) as Long

        private fun booleanArgument(arguments: Map<String, Argument>, name: String): Boolean =
            scalarValue(arguments, name) as Boolean

        /**
         * The `ARRAY(DOUBLE)` constant arrives as a [Block] of double elements. Reject NULL
         * elements and non-finite values — the vector is rendered as a SQL list literal for
         * DuckDB, where `NaN`/`Infinity` have no portable literal form (and make no sense as
         * an ANN query anyway).
         */
        private fun queryVectorArgument(arguments: Map<String, Argument>): List<Double> {
            val block: Block = scalarValue(arguments, ARG_QUERY_VEC) as Block
            if (block.positionCount == 0) {
                invalidArgument("$ARG_QUERY_VEC must not be empty")
            }
            val vector: MutableList<Double> = ArrayList(block.positionCount)
            for (i in 0 until block.positionCount) {
                if (block.isNull(i)) {
                    invalidArgument("$ARG_QUERY_VEC must not contain NULL elements")
                }
                val value: Double = DOUBLE.getDouble(block, i)
                if (!value.isFinite()) {
                    invalidArgument("$ARG_QUERY_VEC must contain finite values, got $value")
                }
                vector.add(value)
            }
            return vector
        }
    }
}
