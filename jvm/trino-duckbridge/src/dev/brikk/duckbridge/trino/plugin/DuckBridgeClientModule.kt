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
import io.trino.plugin.jdbc.credential.CredentialProvider
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder
import io.trino.plugin.jdbc.expression.ParameterizedExpression
import io.trino.plugin.jdbc.ptf.Query
import io.trino.spi.function.table.ConnectorTableFunction
import org.duckdb.DuckDBDriver
import java.util.Properties

/**
 * Guice wiring for the DuckBridge connector: binds the [DuckBridgeClient], the
 * [DriverConnectionFactory] over [DuckDBDriver], the config, and the `query` passthrough
 * table function.
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
        newSetBinder(binder, io.trino.plugin.base.session.SessionPropertiesProvider::class.java)
            .addBinding()
            .to(DuckBridgeSessionProperties::class.java)
            .`in`(Scopes.SINGLETON)
        newSetBinder(binder, ConnectorTableFunction::class.java)
            .addBinding()
            .toProvider(Query::class.java)
            .`in`(Scopes.SINGLETON)
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

    @Provides
    @Singleton
    @ForBaseJdbc
    fun connectionFactory(
        config: BaseJdbcConfig,
        duckBridgeConfig: DuckBridgeConfig,
        credentialProvider: CredentialProvider,
        openTelemetry: OpenTelemetry,
    ): ConnectionFactory {
        val connectionProperties = Properties()
        if (duckBridgeConfig.isAllowUnsignedExtensions) {
            // DuckDB JDBC connection property. Required later so the in-process DuckDB can
            // LOAD the locally-built (unsigned) trino_parity.duckdb_extension.
            connectionProperties.setProperty("allow_unsigned_extensions", "true")
        }
        return DriverConnectionFactory.builder(
            DuckDBDriver(),
            config.connectionUrl,
            credentialProvider,
        )
            .setConnectionProperties(connectionProperties)
            .setOpenTelemetry(openTelemetry)
            .build()
    }
}
