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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.airlift.log.Logger
import io.trino.plugin.base.expression.ConnectorExpressionRewriter
import io.trino.plugin.base.mapping.IdentifierMapping
import io.trino.plugin.jdbc.BaseJdbcClient
import io.trino.plugin.jdbc.BaseJdbcClient.TopNFunction
import io.trino.plugin.jdbc.BaseJdbcConfig
import io.trino.plugin.jdbc.ColumnMapping
import io.trino.plugin.jdbc.ConnectionFactory
import io.trino.plugin.jdbc.JdbcColumnHandle
import io.trino.plugin.jdbc.JdbcOutputTableHandle
import io.trino.plugin.jdbc.JdbcSortItem
import io.trino.plugin.jdbc.JdbcTableHandle
import io.trino.plugin.jdbc.JdbcTypeHandle
import io.trino.plugin.jdbc.LongWriteFunction
import io.trino.plugin.jdbc.QueryBuilder
import io.trino.plugin.jdbc.expression.ParameterizedExpression
import io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction
import io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping
import io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling
import io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR
import io.trino.plugin.jdbc.WriteMapping
import io.trino.plugin.jdbc.logging.RemoteQueryModifier
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ColumnPosition
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.expression.ConnectorExpression
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.CharType
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DecimalType.createDecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Types
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField.EPOCH_DAY
import java.util.Optional
import java.util.function.BiFunction
import java.util.regex.Pattern

/**
 * DuckBridge base-jdbc client for DuckDB, ported from upstream Trino 476's `DuckDbClient` and
 * adapted to the 483 SPI, then extended (P2) with parity-backed expression pushdown, session
 * timezone plumbing, and LIMIT/TopN pushdown.
 *
 * The data plane still flows through the default [io.trino.plugin.jdbc.JdbcRecordSetProvider]
 * wired by [io.trino.plugin.jdbc.JdbcModule]; the type mappings here are structured so a later
 * phase can swap in a custom Arrow-based page source without touching this class's shape.
 *
 * Pushdown seam: [convertPredicate] delegates to [connectorExpressionRewriter], built in
 * [DuckBridgeClientModule] over the ported [DuckBridgeExpressionTranslator]. base-jdbc's
 * `DefaultJdbcMetadata.applyFilter` splits the constraint into conjuncts and calls
 * [convertPredicate] per conjunct, so per-conjunct partial pushdown falls out for free.
 */
class DuckBridgeClient
    @Inject
    constructor(
        config: BaseJdbcConfig,
        connectionFactory: ConnectionFactory,
        queryBuilder: QueryBuilder,
        identifierMapping: IdentifierMapping,
        queryModifier: RemoteQueryModifier,
        private val connectorExpressionRewriter: ConnectorExpressionRewriter<ParameterizedExpression>,
    ) : BaseJdbcClient(
            "\"",
            connectionFactory,
            queryBuilder,
            config.jdbcTypesMappedToVarchar,
            identifierMapping,
            queryModifier,
            false,
        ) {
        override fun getConnection(session: ConnectorSession): Connection {
            // BaseJdbcClient.getConnection calls Connection.setReadOnly, but DuckDB does not
            // support changing read-only status at the connection level, so open directly. The
            // connection factory is decorated (DuckBridgeExtensionConnectionFactory) to load parity
            // + lance/vortex extensions on every connection; here we add the session-specific zone.
            val connection = connectionFactory.openConnection(session)
            try {
                // Set the DuckDB session TimeZone to match Trino's session zone so date/time
                // pushdown over TIMESTAMP WITH TIME ZONE interprets instants identically on both
                // sides. Failure (e.g. a fractional bare offset DuckDB can't parse) is logged once
                // and tolerated — Tier C pushdown degrades to Trino-side eval for this connection.
                applySessionTimeZone(connection, session)
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                // Any failure while preparing the connection must not leak it back to the pool.
                runCatching { connection.close() }
                throw e
            }
            return connection
        }

        private fun applySessionTimeZone(connection: Connection, session: ConnectorSession) {
            val trinoZone = session.timeZoneKey.id
            val duckZone = TrinoTimeZoneNormaliser.normalise(trinoZone) ?: return
            try {
                connection.createStatement().use { stmt ->
                    stmt.execute("SET TimeZone = '" + duckZone.replace("'", "''") + "'")
                }
            } catch (@Suppress("SwallowedException") e: SQLException) {
                if (loggedTimeZoneFailures.add(duckZone)) {
                    log.warn(
                        "duckbridge: could not SET TimeZone = '%s' (from Trino zone '%s'); " +
                            "TIMESTAMP WITH TIME ZONE pushdown will fall back to Trino-side evaluation. Cause: %s",
                        duckZone,
                        trinoZone,
                        e.message,
                    )
                }
            }
        }

        override fun renameSchema(session: ConnectorSession, schemaName: String, newSchemaName: String) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas")
        }

        // DuckDB 1.5.4.0's JDBC metadata reports base tables as "TABLE" (upstream Trino 476 used
        // "BASE TABLE", which this driver version no longer emits — matching on it finds nothing,
        // so base-jdbc's metadata lookups all miss). See DuckDBDatabaseMetaData#getTables.
        override fun getTableTypes(): Optional<List<String>> = Optional.of(ImmutableList.of("TABLE", "VIEW"))

        @Throws(SQLException::class)
        override fun getTables(
            connection: Connection,
            schemaName: Optional<String>,
            tableName: Optional<String>,
        ): java.sql.ResultSet {
            val metadata = connection.metaData
            return metadata.getTables(
                null,
                schemaName.orElse(null),
                escapeObjectNameForMetadataQuery(tableName, metadata.searchStringEscape).orElse(null),
                getTableTypes().map { it.toTypedArray() }.orElse(null),
            )
        }

        override fun renameTable(session: ConnectorSession, handle: JdbcTableHandle, newTableName: SchemaTableName) {
            val remoteTableName = handle.asPlainTable().remoteTableName
            if (remoteTableName.schemaName.orElseThrow() != newTableName.schemaName) {
                throw TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas")
            }
            renameTable(session, null, remoteTableName.schemaName.orElseThrow(), remoteTableName.tableName, newTableName)
        }

        @Throws(SQLException::class)
        override fun renameTable(
            session: ConnectorSession,
            connection: Connection?,
            catalogName: String?,
            remoteSchemaName: String,
            remoteTableName: String,
            newRemoteSchemaName: String,
            newRemoteTableName: String,
        ) {
            execute(
                session,
                connection,
                "ALTER TABLE ${quoted(catalogName, remoteSchemaName, remoteTableName)} " +
                    "RENAME TO ${quoted(catalogName, null, newRemoteTableName)}",
            )
        }

        override fun commitCreateTable(session: ConnectorSession, handle: JdbcOutputTableHandle, pageSinkIds: Set<Long>) {
            if (handle.pageSinkIdColumnName.isPresent) {
                finishInsertTable(session, handle, pageSinkIds)
            } else {
                renameTable(
                    session,
                    null,
                    handle.remoteTableName.schemaName.orElse(null),
                    handle.temporaryTableName.orElseThrow { IllegalStateException("Temporary table name missing") },
                    handle.remoteTableName.schemaTableName,
                )
            }
        }

        override fun addColumn(
            session: ConnectorSession,
            handle: JdbcTableHandle,
            column: ColumnMetadata,
            position: ColumnPosition,
        ) {
            if (!column.isNullable) {
                throw TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns")
            }
            val unsupportedClause: String? =
                when (position) {
                    is ColumnPosition.First -> "FIRST"
                    is ColumnPosition.After -> "AFTER"
                    is ColumnPosition.Last -> null
                }
            if (unsupportedClause != null) {
                throw TrinoException(
                    NOT_SUPPORTED,
                    "This connector does not support adding columns with $unsupportedClause clause",
                )
            }
            super.addColumn(session, handle, column, position)
        }

        // One branch per DuckDB JDBC type; kept as a single dispatch (matches the P1/P2 port shape).
        @Suppress("CyclomaticComplexMethod")
        override fun toColumnMapping(
            session: ConnectorSession,
            connection: Connection,
            typeHandle: JdbcTypeHandle,
        ): Optional<ColumnMapping> {
            val forced = getForcedMappingToVarchar(typeHandle)
            if (forced.isPresent) {
                return forced
            }
            when (typeHandle.jdbcType()) {
                Types.BOOLEAN -> return Optional.of(booleanColumnMapping())
                Types.TINYINT -> return Optional.of(tinyintColumnMapping())
                Types.SMALLINT -> return Optional.of(smallintColumnMapping())
                Types.INTEGER -> return Optional.of(integerColumnMapping())
                Types.BIGINT -> return Optional.of(bigintColumnMapping())
                Types.FLOAT -> return Optional.of(realColumnMapping())
                Types.DOUBLE -> return Optional.of(doubleColumnMapping())
                Types.DECIMAL -> {
                    val decimalTypeName = typeHandle.jdbcTypeName().orElseThrow()
                    // Use type name because DuckDB does not report scale in metadata.
                    val matcher = DECIMAL_PATTERN.matcher(decimalTypeName)
                    require(matcher.matches()) { "Decimal type name does not match pattern: $decimalTypeName" }
                    val precision = matcher.group("precision").toInt()
                    val scale = matcher.group("scale").toInt()
                    return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)))
                }
                Types.VARCHAR ->
                    // CHAR is an alias of VARCHAR in DuckDB https://duckdb.org/docs/sql/data_types/text
                    return Optional.of(varcharColumnMapping(VarcharType.VARCHAR, true))
                Types.DATE ->
                    return Optional.of(
                        ColumnMapping.longMapping(
                            DATE,
                            { resultSet, columnIndex ->
                                DATE_FORMATTER.parse(resultSet.getString(columnIndex)).getLong(EPOCH_DAY)
                            },
                            dateWriteFunction(),
                        ),
                    )
                Types.TIMESTAMP ->
                    // DuckDB TIMESTAMP is microsecond precision.
                    return Optional.of(timestampColumnMapping(TIMESTAMP_MICROS))
                Types.ARRAY -> {
                    // DuckDB list/array (e.g. lance embedding FLOAT[3], VARCHAR[] tags). The element
                    // type is only in the type name; the array mapping parses it. Unsupported element
                    // types fall through to CONVERT_TO_VARCHAR / empty like any other type.
                    val arrayMapping = typeHandle.jdbcTypeName().orElse(null)?.let(DuckBridgeArrayColumnMapping::fromTypeName)
                    if (arrayMapping != null) {
                        return Optional.of(arrayMapping)
                    }
                }
            }

            if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                return mapToUnboundedVarchar(typeHandle)
            }
            return Optional.empty()
        }

        override fun toWriteMapping(session: ConnectorSession, type: Type): WriteMapping =
            simpleWriteMapping(type)
                ?: parametricWriteMapping(type)
                ?: throw TrinoException(NOT_SUPPORTED, "Unsupported column type: ${type.displayName}")

        /** Fixed scalar types whose DuckDB type name carries no precision/scale/length. */
        private fun simpleWriteMapping(type: Type): WriteMapping? =
            when (type) {
                BOOLEAN -> WriteMapping.booleanMapping("boolean", booleanWriteFunction())
                TINYINT -> WriteMapping.longMapping("tinyint", tinyintWriteFunction())
                SMALLINT -> WriteMapping.longMapping("smallint", smallintWriteFunction())
                INTEGER -> WriteMapping.longMapping("integer", integerWriteFunction())
                BIGINT -> WriteMapping.longMapping("bigint", bigintWriteFunction())
                REAL -> WriteMapping.longMapping("float", realWriteFunction())
                DOUBLE -> WriteMapping.doubleMapping("double precision", doubleWriteFunction())
                DATE -> WriteMapping.longMapping("date", dateWriteFunction())
                else -> null
            }

        /** Types whose DuckDB type name is parameterised (decimal/char/varchar/timestamp). */
        private fun parametricWriteMapping(type: Type): WriteMapping? =
            when (type) {
                is DecimalType -> {
                    val dataType = "decimal(${type.precision}, ${type.scale})"
                    if (type.isShort) {
                        WriteMapping.longMapping(dataType, shortDecimalWriteFunction(type))
                    } else {
                        WriteMapping.objectMapping(dataType, longDecimalWriteFunction(type))
                    }
                }
                // CHAR is an alias of VARCHAR in DuckDB https://duckdb.org/docs/sql/data_types/text
                is CharType -> WriteMapping.sliceMapping("varchar", charWriteFunction())
                is VarcharType -> WriteMapping.sliceMapping("varchar", varcharWriteFunction())
                // Short timestamps (precision <= 6) map to DuckDB's microsecond TIMESTAMP.
                is TimestampType -> if (type.isShort) {
                    WriteMapping.longMapping("timestamp", timestampWriteFunction(type))
                } else {
                    null
                }
                else -> null
            }

        // ---- Expression pushdown ------------------------------------------------------------

        /**
         * base-jdbc calls this per top-level conjunct (see `DefaultJdbcMetadata.applyFilter`).
         * Delegates to the parity rewriter. When parity is disabled, function-shape pushdown is off
         * entirely (the rewriter has no rules) — domain and LIMIT/TopN pushdown are unaffected.
         */
        override fun convertPredicate(
            session: ConnectorSession,
            expression: ConnectorExpression,
            assignments: Map<String, io.trino.spi.connector.ColumnHandle>,
        ): Optional<ParameterizedExpression> =
            connectorExpressionRewriter.rewrite(session, expression, assignments)

        // ---- LIMIT / TopN pushdown ----------------------------------------------------------

        override fun supportsLimit(): Boolean = true

        // "Guaranteed" here means "the remote returns at most n rows, so Trino need not re-enforce
        // the limit" — NOT that the row set is deterministic (Trino's LIMIT is inherently
        // non-deterministic). A pushed DuckDB `LIMIT n` always returns at most n rows, so this is
        // safely true (same stance as upstream trino-duckdb 476 and PostgreSqlClient).
        override fun isLimitGuaranteed(session: ConnectorSession): Boolean = true

        override fun limitFunction(): Optional<BiFunction<String, Long, String>> =
            Optional.of(BiFunction { sql, limit -> "$sql LIMIT $limit" })

        override fun supportsTopN(session: ConnectorSession, handle: JdbcTableHandle, sortOrder: List<JdbcSortItem>): Boolean = true

        // ORDER BY ... LIMIT n IS a total order on the sort keys, but ties on the sort keys make the
        // tail non-deterministic, so TopN is not guaranteed and Trino re-applies its TopNOperator.
        override fun isTopNGuaranteed(session: ConnectorSession): Boolean = false

        override fun topNFunction(): Optional<TopNFunction> =
            Optional.of(
                TopNFunction { query, sortItems, limit ->
                    val orderBy =
                        sortItems.joinToString(", ") { item ->
                            val column = quoted(item.column.columnName)
                            // DuckDB and Trino agree on ASC=NULLS LAST / DESC=NULLS FIRST as the default,
                            // but base-jdbc's JdbcSortItem carries the explicit null ordering, so emit it
                            // explicitly to stay correct regardless of engine default drift.
                            val order = if (item.sortOrder.isAscending) "ASC" else "DESC"
                            val nulls = if (item.sortOrder.isNullsFirst) "NULLS FIRST" else "NULLS LAST"
                            "$column $order $nulls"
                        }
                    "$query ORDER BY $orderBy LIMIT $limit"
                },
            )

        private companion object {
            private val log: Logger = Logger.get(DuckBridgeClient::class.java)

            /** Zones we've already warned about failing to SET, so the WARN is one-shot per zone. */
            private val loggedTimeZoneFailures: MutableSet<String> =
                java.util.concurrent.ConcurrentHashMap.newKeySet<String>()

            private val DECIMAL_PATTERN: Pattern = Pattern.compile("DECIMAL\\((?<precision>[0-9]+),(?<scale>[0-9]+)\\)")
            private val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd")

            fun dateWriteFunction(): LongWriteFunction =
                object : LongWriteFunction {
                    override fun getBindExpression(): String = "CAST(? AS DATE)"

                    @Throws(SQLException::class)
                    override fun set(statement: PreparedStatement, index: Int, day: Long) {
                        statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(day)))
                    }
                }
        }
    }
