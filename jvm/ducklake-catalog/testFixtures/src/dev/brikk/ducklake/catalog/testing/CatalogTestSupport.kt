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
package dev.brikk.ducklake.catalog.testing

import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.JDBCUtils
import java.sql.Connection

/**
 * Bootstraps a [DSLContext] configured to match the runtime catalog
 * (see `JdbcDucklakeCatalog`'s `Settings` block) so test queries render
 * the same SQL as production reads — in particular, lowercase unquoted identifiers
 * for the `ducklake_*` tables.
 *
 * The returned [DSLContext] wraps the caller-supplied [Connection]
 * but does **not** close it. Manage the connection lifecycle with
 * try-with-resources at the call site, exactly as you would for raw JDBC.
 */
class CatalogTestSupport private constructor() {
    companion object {
        private val JOOQ_SETTINGS: Settings = Settings()
            .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED)

        /**
         * Wrap an open JDBC connection in a [DSLContext]. Dialect is auto-detected from
         * the connection metadata; for the PostgreSQL test catalog this resolves to
         * [SQLDialect.POSTGRES].
         */
        @JvmStatic
        fun dsl(connection: Connection): DSLContext {
            requireNotNull(connection) { "connection is null" }
            val dialect: SQLDialect = JDBCUtils.dialect(connection)
            return DSL.using(connection, dialect, JOOQ_SETTINGS)
        }
    }
}
