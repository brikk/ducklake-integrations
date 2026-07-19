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
package dev.brikk.duckbridge.trino.plugin

import io.trino.plugin.jdbc.JdbcTransactionManager
import io.trino.spi.connector.ConnectorAccessControl
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.ScalarArgumentSpecification
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * `SELECT * FROM TABLE(<catalog>.system.lance_scan(path => '<dataset dir>'))` — plain scan of a lance
 * dataset directory via DuckDB's `__lance_scan`. Ported from the DuckLake FileScan `__lance_scan`
 * path, now a path-based PTF. Returns the dataset's columns as-is.
 */
class LanceScanTableFunction(
    private val scanExtensions: DuckBridgeScanExtensions,
    transactionManager: JdbcTransactionManager,
) : AbstractDuckBridgeScanFunction(
        transactionManager,
        DuckBridgeScanExtensions.LANCE,
        FUNCTION_NAME,
        listOf(ScalarArgumentSpecification.builder().name(ARG_PATH).type(VARCHAR).build()),
    ) {
    override fun analyze(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        arguments: Map<String, Argument>,
        accessControl: ConnectorAccessControl,
    ): TableFunctionAnalysis {
        scanExtensions.requireEnabled(requiredExtension)
        val path = stringArgument(arguments, ARG_PATH)
        return analyzeScanQuery(session, transaction, LanceSearchSql.lanceScan(path))
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_scan"
    }
}

/**
 * `SELECT * FROM TABLE(<catalog>.system.vortex_scan(path => '<file.vortex>'))` — plain scan of a
 * vortex file via DuckDB's `read_vortex`. Ported from the DuckLake FileScan `read_vortex` path.
 */
class VortexScanTableFunction(
    private val scanExtensions: DuckBridgeScanExtensions,
    transactionManager: JdbcTransactionManager,
) : AbstractDuckBridgeScanFunction(
        transactionManager,
        DuckBridgeScanExtensions.VORTEX,
        FUNCTION_NAME,
        listOf(ScalarArgumentSpecification.builder().name(ARG_PATH).type(VARCHAR).build()),
    ) {
    override fun analyze(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        arguments: Map<String, Argument>,
        accessControl: ConnectorAccessControl,
    ): TableFunctionAnalysis {
        scanExtensions.requireEnabled(requiredExtension)
        val path = stringArgument(arguments, ARG_PATH)
        return analyzeScanQuery(session, transaction, LanceSearchSql.vortexScan(path))
    }

    companion object {
        const val FUNCTION_NAME: String = "vortex_scan"
    }
}
