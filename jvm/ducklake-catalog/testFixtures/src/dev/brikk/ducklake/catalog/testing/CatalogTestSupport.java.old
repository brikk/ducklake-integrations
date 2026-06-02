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
package dev.brikk.ducklake.catalog.testing;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;

import java.sql.Connection;

import static java.util.Objects.requireNonNull;

/**
 * Bootstraps a {@link DSLContext} configured to match the runtime catalog
 * (see {@code JdbcDucklakeCatalog}'s {@code Settings} block) so test queries render
 * the same SQL as production reads — in particular, lowercase unquoted identifiers
 * for the {@code ducklake_*} tables.
 *
 * <p>The returned {@link DSLContext} wraps the caller-supplied {@link Connection}
 * but does <strong>not</strong> close it. Manage the connection lifecycle with
 * try-with-resources at the call site, exactly as you would for raw JDBC.
 */
public final class CatalogTestSupport
{
    private static final Settings JOOQ_SETTINGS = new Settings()
            .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED);

    private CatalogTestSupport() {}

    /**
     * Wrap an open JDBC connection in a {@link DSLContext}. Dialect is auto-detected from
     * the connection metadata; for the PostgreSQL test catalog this resolves to
     * {@link SQLDialect#POSTGRES}.
     */
    public static DSLContext dsl(Connection connection)
    {
        requireNonNull(connection, "connection is null");
        SQLDialect dialect = JDBCUtils.dialect(connection);
        return DSL.using(connection, dialect, JOOQ_SETTINGS);
    }
}
