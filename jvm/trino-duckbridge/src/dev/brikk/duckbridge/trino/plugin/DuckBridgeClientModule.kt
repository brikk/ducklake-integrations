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

import com.google.inject.Binder
import com.google.inject.Provides
import com.google.inject.Scopes
import com.google.inject.Singleton
import com.google.inject.multibindings.Multibinder.newSetBinder
import com.google.inject.multibindings.OptionalBinder.newOptionalBinder
import io.airlift.configuration.AbstractConfigurationAwareModule
import io.airlift.configuration.ConfigBinder.configBinder
import io.opentelemetry.api.OpenTelemetry
import io.trino.plugin.base.expression.ConnectorExpressionRewriter
import io.trino.plugin.jdbc.BaseJdbcConfig
import io.trino.plugin.jdbc.ConnectionFactory
import io.trino.plugin.jdbc.DriverConnectionFactory
import io.trino.plugin.jdbc.ForBaseJdbc
import io.trino.plugin.jdbc.JdbcClient
import io.trino.plugin.jdbc.JdbcStatisticsConfig
import io.trino.plugin.jdbc.JdbcPageSourceProvider
import io.trino.plugin.jdbc.credential.CredentialProvider
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder
import io.trino.plugin.jdbc.expression.ParameterizedExpression
import io.trino.plugin.jdbc.ptf.Query
import io.trino.spi.function.table.ConnectorTableFunction
import com.gizmodata.quack.jdbc.sql.QuackDriver
import org.duckdb.DuckDBDriver
import java.sql.Driver
import java.util.Properties

/**
 * Guice wiring for the DuckBridge connector: binds the [DuckBridgeClient], the transport
 * [DuckBridgeTransport] (derived from the connection-url), a [DriverConnectionFactory] over the
 * transport's JDBC driver ([DuckDBDriver] embedded / [QuackDriver] remote), the config, and the
 * `query` passthrough table function.
 */
class DuckBridgeClientModule : AbstractConfigurationAwareModule() {
    override fun setup(binder: Binder) {
        binder.bind(JdbcClient::class.java)
            .annotatedWith(ForBaseJdbc::class.java)
            .to(DuckBridgeClient::class.java)
            .`in`(Scopes.SINGLETON)
        configBinder(binder).bindConfig(JdbcStatisticsConfig::class.java)
        configBinder(binder).bindConfig(DuckBridgeConfig::class.java)
        binder.bind(DuckBridgeParity::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DuckBridgeScanExtensions::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DuckBridgeExecutorFactory::class.java).`in`(Scopes.SINGLETON)
        // Override base-jdbc's default ConnectorPageSourceProvider with our gating one. It delegates
        // to the default JdbcPageSourceProvider for the JDBC engine (production) and only diverts to
        // the T2 Arrow page source for the DUCKDB_LOCAL engine. JdbcPageSourceProvider's own deps
        // (JdbcClient, executor, retry policy) are bound by the base JdbcModule.
        binder.bind(JdbcPageSourceProvider::class.java).`in`(Scopes.SINGLETON)
        newOptionalBinder(binder, io.trino.spi.connector.ConnectorPageSourceProvider::class.java)
            .setBinding()
            .to(DuckBridgePageSourceProvider::class.java)
            .`in`(Scopes.SINGLETON)
        newSetBinder(binder, io.trino.plugin.base.session.SessionPropertiesProvider::class.java)
            .addBinding()
            .to(DuckBridgeSessionProperties::class.java)
            .`in`(Scopes.SINGLETON)
        val tableFunctions = newSetBinder(binder, ConnectorTableFunction::class.java)
        tableFunctions.addBinding().toProvider(Query::class.java).`in`(Scopes.SINGLETON)
        // lance / vortex scan + search PTFs (P5). They're always registered (so a clear
        // "extension not enabled" error fires at analyze time); the per-connection extension load is
        // config-gated in DuckBridgeClient.getConnection.
        tableFunctions.addBinding().toProvider(LanceScanFunctionProvider::class.java).`in`(Scopes.SINGLETON)
        tableFunctions.addBinding().toProvider(VortexScanFunctionProvider::class.java).`in`(Scopes.SINGLETON)
        tableFunctions.addBinding().toProvider(LanceVectorSearchFunctionProvider::class.java).`in`(Scopes.SINGLETON)
        tableFunctions.addBinding().toProvider(LanceFtsFunctionProvider::class.java).`in`(Scopes.SINGLETON)
        tableFunctions.addBinding().toProvider(LanceHybridSearchFunctionProvider::class.java).`in`(Scopes.SINGLETON)
    }

    /**
     * The parity expression rewriter. base-jdbc's [JdbcConnectorExpressionRewriterBuilder]
     * `addStandardRules` covers variables/constants/comparisons/arithmetic/AND/OR with parameterized
     * SQL; we deliberately do NOT use those and instead supply ONE whole-expression rule
     * ([DuckBridgeParityExpressionRule]) that runs the ported translator, which already covers those
     * shapes AND the `trino_*` parity catalog with identical, verified semantics. Building via the
     * jdbc builder (rather than `new ConnectorExpressionRewriter(...)`) keeps us on the supported seam
     * and the [ParameterizedExpression] result type base-jdbc's `convertPredicate` expects.
     *
     * When parity is disabled, the rewriter is built with no rules, so `convertPredicate` always
     * returns empty and function-shape pushdown is fully off (domain + LIMIT/TopN still push).
     */
    @Provides
    @Singleton
    fun connectorExpressionRewriter(config: DuckBridgeConfig): ConnectorExpressionRewriter<ParameterizedExpression> {
        val builder = JdbcConnectorExpressionRewriterBuilder.newBuilder()
        if (config.isParityEnabled) {
            builder.add(DuckBridgeParityExpressionRule())
        }
        return builder.build()
    }

    /** Transport is fully determined by the connection-url prefix (see [DuckBridgeTransport]). */
    @Provides
    @Singleton
    fun transport(config: BaseJdbcConfig): DuckBridgeTransport =
        DuckBridgeTransport.fromConnectionUrl(config.connectionUrl)

    @Provides
    @Singleton
    @ForBaseJdbc
    @Suppress("LongParameterList")
    fun connectionFactory(
        config: BaseJdbcConfig,
        duckBridgeConfig: DuckBridgeConfig,
        transport: DuckBridgeTransport,
        credentialProvider: CredentialProvider,
        openTelemetry: OpenTelemetry,
        parity: DuckBridgeParity,
        scanExtensions: DuckBridgeScanExtensions,
    ): ConnectionFactory {
        val driver: Driver
        val connectionProperties = Properties()
        when (transport) {
            DuckBridgeTransport.EMBEDDED -> {
                driver = DuckDBDriver()
                if (duckBridgeConfig.isAllowUnsignedExtensions) {
                    // DuckDB JDBC connection property. Required so the in-process DuckDB can LOAD
                    // the locally-built (unsigned) trino_parity.duckdb_extension.
                    connectionProperties.setProperty("allow_unsigned_extensions", "true")
                }
            }
            DuckBridgeTransport.QUACK -> {
                driver = QuackDriver()
                // Host/port come from the URL; credentials + tls come from config, kept out of the
                // copy-pasteable URL. Token resolution order (token → tokenEnv → tokenFile) is the
                // driver's; we only forward whichever the operator set.
                duckBridgeConfig.quackToken?.let { connectionProperties.setProperty("token", it) }
                duckBridgeConfig.quackTokenEnv?.let { connectionProperties.setProperty("tokenEnv", it) }
                duckBridgeConfig.quackTokenFile?.let { connectionProperties.setProperty("tokenFile", it) }
                if (duckBridgeConfig.isQuackTls) {
                    connectionProperties.setProperty("tls", "true")
                }
            }
        }
        val base =
            DriverConnectionFactory.builder(driver, config.connectionUrl, credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build()
        // Decorate so parity + lance/vortex extensions load on EVERY connection — including
        // base-jdbc's metadata probes (getTableHandle/getColumns) that bypass
        // DuckBridgeClient.getConnection. Session-specific SET TimeZone stays in getConnection.
        return DuckBridgeExtensionConnectionFactory(base, parity, scanExtensions)
    }
}
