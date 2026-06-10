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
import com.google.inject.Inject
import io.airlift.bootstrap.LifeCycleManager
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorMetadata
import io.trino.spi.connector.ConnectorPageSinkProvider
import io.trino.spi.connector.ConnectorPageSourceProviderFactory
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplitManager
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.function.FunctionProvider
import io.trino.spi.function.table.ConnectorTableFunction
import io.trino.spi.procedure.Procedure
import io.trino.spi.session.PropertyMetadata
import io.trino.spi.transaction.IsolationLevel
import io.trino.spi.transaction.IsolationLevel.SERIALIZABLE
import io.trino.spi.transaction.IsolationLevel.checkConnectorSupports
import java.util.Optional

/**
 * Main connector implementation for Ducklake.
 * Manages lifecycle and provides access to metadata and split management.
 */
class DucklakeConnector @Inject constructor(
    private val lifeCycleManager: LifeCycleManager,
    private val transactionManager: DucklakeTransactionManager,
    private val splitManager: ConnectorSplitManager,
    private val pageSourceProviderFactory: ConnectorPageSourceProviderFactory,
    private val pageSinkProvider: ConnectorPageSinkProvider,
    ducklakeSessionProperties: DucklakeSessionProperties,
    ducklakeTableProperties: DucklakeTableProperties,
    procedures: Set<Procedure>,
    tableFunctions: Set<@JvmSuppressWildcards ConnectorTableFunction>,
    private val functionProvider: DucklakeFunctionProvider) : Connector {
    private val sessionProperties: List<PropertyMetadata<*>> = ImmutableList.copyOf(ducklakeSessionProperties.getSessionProperties())
    private val tableProperties: List<PropertyMetadata<*>> = ImmutableList.copyOf(ducklakeTableProperties.tableProperties)
    private val procedures: Set<Procedure> = procedures.toSet()
    private val tableFunctions: Set<ConnectorTableFunction> = tableFunctions
            .map { ClassLoaderSafeConnectorTableFunction(it, javaClass.classLoader) }
            .toSet()

    override fun getProcedures(): Set<Procedure> {
        return procedures
    }

    override fun getTableFunctions(): Set<ConnectorTableFunction> {
        return tableFunctions
    }

    override fun getFunctionProvider(): Optional<FunctionProvider> {
        return Optional.of(functionProvider)
    }

    override fun getMetadata(session: ConnectorSession, transaction: ConnectorTransactionHandle): ConnectorMetadata {
        val metadata: DucklakeMetadata = transactionManager.getMetadata(transaction)
        return ClassLoaderSafeConnectorMetadata(metadata, javaClass.classLoader)
    }

    override fun getSplitManager(): ConnectorSplitManager {
        return splitManager
    }

    override fun getPageSourceProviderFactory(): ConnectorPageSourceProviderFactory {
        return pageSourceProviderFactory
    }

    override fun getPageSinkProvider(): ConnectorPageSinkProvider {
        return pageSinkProvider
    }

    override fun getSessionProperties(): List<PropertyMetadata<*>> {
        return sessionProperties
    }

    override fun getTableProperties(): List<PropertyMetadata<*>> {
        return tableProperties
    }

    override fun beginTransaction(isolationLevel: IsolationLevel, readOnly: Boolean, autoCommit: Boolean): ConnectorTransactionHandle {
        checkConnectorSupports(SERIALIZABLE, isolationLevel)
        val transaction = DucklakeTransactionHandle()
        transactionManager.begin(transaction)
        return transaction
    }

    override fun commit(transaction: ConnectorTransactionHandle) {
        transactionManager.commit(transaction as DucklakeTransactionHandle)
    }

    override fun rollback(transaction: ConnectorTransactionHandle) {
        transactionManager.rollback(transaction as DucklakeTransactionHandle)
    }

    override fun shutdown() {
        lifeCycleManager.stop()
    }
}
