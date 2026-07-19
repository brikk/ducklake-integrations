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

import com.google.inject.Inject
import com.google.inject.Provider
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction
import io.trino.plugin.jdbc.JdbcTransactionManager
import io.trino.spi.function.table.ConnectorTableFunction

/**
 * Guice providers for the P5 lance/vortex scan + search table functions. Each wraps its PTF in a
 * [ClassLoaderSafeConnectorTableFunction] (the same wrapping base-jdbc's `query` PTF uses) so the
 * plugin classloader is active during analyze. [JdbcTransactionManager] is bound by the base
 * `JdbcModule`; [DuckBridgeScanExtensions] is bound in [DuckBridgeClientModule].
 */
class LanceScanFunctionProvider
    @Inject
    constructor(
        private val scanExtensions: DuckBridgeScanExtensions,
        private val transactionManager: JdbcTransactionManager,
    ) : Provider<ConnectorTableFunction> {
        override fun get(): ConnectorTableFunction =
            ClassLoaderSafeConnectorTableFunction(
                LanceScanTableFunction(scanExtensions, transactionManager),
                javaClass.classLoader,
            )
    }

class VortexScanFunctionProvider
    @Inject
    constructor(
        private val scanExtensions: DuckBridgeScanExtensions,
        private val transactionManager: JdbcTransactionManager,
    ) : Provider<ConnectorTableFunction> {
        override fun get(): ConnectorTableFunction =
            ClassLoaderSafeConnectorTableFunction(
                VortexScanTableFunction(scanExtensions, transactionManager),
                javaClass.classLoader,
            )
    }

class LanceVectorSearchFunctionProvider
    @Inject
    constructor(
        private val scanExtensions: DuckBridgeScanExtensions,
        private val transactionManager: JdbcTransactionManager,
    ) : Provider<ConnectorTableFunction> {
        override fun get(): ConnectorTableFunction =
            ClassLoaderSafeConnectorTableFunction(
                LanceVectorSearchTableFunction(scanExtensions, transactionManager),
                javaClass.classLoader,
            )
    }

class LanceFtsFunctionProvider
    @Inject
    constructor(
        private val scanExtensions: DuckBridgeScanExtensions,
        private val transactionManager: JdbcTransactionManager,
    ) : Provider<ConnectorTableFunction> {
        override fun get(): ConnectorTableFunction =
            ClassLoaderSafeConnectorTableFunction(
                LanceFtsTableFunction(scanExtensions, transactionManager),
                javaClass.classLoader,
            )
    }

class LanceHybridSearchFunctionProvider
    @Inject
    constructor(
        private val scanExtensions: DuckBridgeScanExtensions,
        private val transactionManager: JdbcTransactionManager,
    ) : Provider<ConnectorTableFunction> {
        override fun get(): ConnectorTableFunction =
            ClassLoaderSafeConnectorTableFunction(
                LanceHybridSearchTableFunction(scanExtensions, transactionManager),
                javaClass.classLoader,
            )
    }
