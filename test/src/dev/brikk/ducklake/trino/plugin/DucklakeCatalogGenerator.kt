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

import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import dev.brikk.ducklake.catalog.TestingDucklakeLocalDuckDbCatalogFixture
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Utility to generate a test Ducklake catalog using DuckDB's embedded JDBC driver.
 * Creates PostgreSQL-backed Ducklake catalog metadata and Parquet data files
 * by writing through DuckDB + Ducklake extension.
 */
object DucklakeCatalogGenerator {
    private val TARGET_DIR: Path = Path.of("build", "test-data")
    private val POSTGRESQL_CATALOG_DIR: Path = TARGET_DIR.resolve("test-catalog-postgresql")

    @JvmStatic
    fun getPostgreSqlCatalogDirectory(): Path = POSTGRESQL_CATALOG_DIR

    /**
     * Generate the shared PostgreSQL test catalog with all standard test tables and views.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generatePostgreSqlCatalog(server: TestingDucklakePostgreSqlCatalogServer) {
        generateTestCatalog(
            "postgresql",
            POSTGRESQL_CATALOG_DIR,
            server.getDuckDbAttachUri(),
            listOf("postgres"),
            "catalog database: " + server.getJdbcUrl(),
        )
    }

    /**
     * Generate an isolated PostgreSQL test catalog in a unique database.
     * Each call creates a new database in the shared container, generates all test tables/views,
     * and returns a map with JDBC URL + data directory for the new catalog.
     * Use this for write tests that need their own catalog to avoid cross-test interference.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedPostgreSqlCatalog(
        server: TestingDucklakePostgreSqlCatalogServer,
        testName: String,
    ): IsolatedCatalog {
        val databaseName = "ducklake_" + testName.replace('-', '_')
        server.createDatabase(databaseName)

        val catalogDir = TARGET_DIR.resolve("test-catalog-isolated-$testName")
        generateTestCatalog(
            "postgresql",
            catalogDir,
            server.getDuckDbAttachUri(databaseName),
            listOf("postgres"),
            "isolated catalog for $testName",
        )

        return IsolatedCatalog(
            server.getJdbcUrl(databaseName),
            server.getUser(),
            server.getPassword(),
            catalogDir.resolve("data"),
            server.getDuckDbAttachUri(databaseName),
        )
    }

    @JvmRecord
    data class IsolatedCatalog(
        val jdbcUrl: String,
        val user: String?,
        val password: String?,
        val dataDir: Path,
        val duckDbAttachUri: String,
    )

    /**
     * Generate an isolated local-DuckDB test catalog. Each call creates a fresh
     * `lake.db` file under the fixture's root and bootstraps the full
     * test-data fixture by ATTACHing it as a DuckLake catalog through an
     * in-process DuckDB JDBC connection. The connector then talks to the same
     * `.db` file directly via `jdbc:duckdb:/path/to/lake.db` —
     * no DuckLake-on-X wrapper at runtime, so `JdbcDucklakeCatalog`'s
     * UPDATE/DELETE on `ducklake_*` metadata rows hits regular base tables
     * and is unaffected by the read-only-sibling restriction that DuckLake-on-Quack
     * imposes today.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedLocalDuckDbCatalog(
        fixture: TestingDucklakeLocalDuckDbCatalogFixture,
        testName: String,
    ): IsolatedCatalog {
        val catalogDir = fixture.catalogDirectory(testName)
        val catalogFile = catalogDir.resolve("lake.db")
        val attachUri = "ducklake:" + catalogFile.toAbsolutePath()

        generateTestCatalog(
            "duckdb-local",
            catalogDir,
            attachUri,
            listOf(),
            "isolated local DuckDB catalog for $testName",
        )

        return IsolatedCatalog(
            "jdbc:duckdb:" + catalogFile.toAbsolutePath(),
            /* user */ null,
            /* password */ null,
            catalogDir.resolve("data"),
            attachUri,
        )
    }

    /**
     * Generate an isolated Quack-backed test catalog. Each call picks a unique
     * METADATA_CATALOG name inside the shared container's remote DuckDB, so two
     * isolated catalogs in the same JVM don't see each other's metadata. The
     * returned `jdbcUrl` is the synthetic
     * `jdbc:duckdb:quack://host:port?metadata_catalog=name` URL that the
     * connector's `JdbcDucklakeCatalog` knows how to interpret.
     *
     * Unlike the PG path, this does not pre-create the 17-table test fixture
     * data. Tests that need pre-bootstrapped tables either skip under
     * `DUCKDB_QUACK` or create the rows themselves in setUp(). Most
     * cross-engine tests create their own tables, so this is fine for the
     * smoke set; expanding parity with the PG bootstrap is tracked in
     * `TODO-WRITE-MODE.md`.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedDuckDbQuackCatalog(
        server: TestingDucklakeDuckDbQuackCatalogServer,
        testName: String,
    ): IsolatedCatalog {
        val metadataCatalog = toQuackMetadataCatalogName(testName)
        val catalogDir = TARGET_DIR.resolve("test-catalog-quack-$testName")
        val dataDir = catalogDir.resolve("data")
        if (Files.exists(catalogDir)) {
            deleteDirectory(catalogDir)
        }
        Files.createDirectories(dataDir)

        // Pre-touch the catalog so its metadata schema exists before the connector's
        // JdbcDucklakeCatalog issues its first SELECT. Doing it here keeps the
        // connector-side init script idempotent and front-loads any extension-
        // download cost into test setup (visible) rather than the first query.
        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("INSTALL quack")
                stmt.execute("LOAD quack")
                stmt.execute("INSTALL ducklake")
                stmt.execute("LOAD ducklake")
                stmt.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + escapeSql(server.getToken()) + "')")
                stmt.execute(
                    "ATTACH '" + server.getDucklakeAttachUri() + "' AS lake " +
                        "(DATA_PATH '" + escapeSql(dataDir.toAbsolutePath().toString()) +
                        "', METADATA_CATALOG '" + metadataCatalog + "')",
                )
                // Touch the catalog so DuckLake materialises its metadata schema in the
                // remote DuckDB before any reader hits it. CREATE SCHEMA on the lake
                // is idempotent and produces visible metadata rows we can later assert
                // against from JdbcDucklakeCatalog's jOOQ DSL.
                stmt.execute("USE lake")
                stmt.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
            }
        }

        val syntheticUrl = "jdbc:duckdb:quack://" + server.getHost() + ":" + server.getMappedPort() +
            "?metadata_catalog=" + metadataCatalog
        return IsolatedCatalog(
            syntheticUrl,
            /* user */ null,
            /* password */ server.getToken(),
            dataDir,
            server.getDucklakeAttachUri(),
        )
    }

    private fun toQuackMetadataCatalogName(testName: String): String {
        // METADATA_CATALOG must match [A-Za-z0-9_]+ — QuackBackedDuckDbCatalogUrl
        // validates this on the parsing side. Normalise the test name to fit.
        val normalised = testName.replace(Regex("[^A-Za-z0-9_]"), "_")
        return "qmeta_$normalised"
    }

    @JvmStatic
    @Throws(Exception::class)
    internal fun generateTestCatalog(
        backendName: String,
        catalogDirectory: Path,
        ducklakeAttachUri: String,
        extraExtensions: List<String>,
        catalogDescription: String,
    ) {
        println("==========================================")
        println("Ducklake Test Catalog Generator")
        println("==========================================")
        println("Backend: $backendName")
        println()

        // Create target directory
        Files.createDirectories(TARGET_DIR)

        // Remove old catalog data if exists
        if (Files.exists(catalogDirectory)) {
            println("Removing existing test catalog data...")
            deleteDirectory(catalogDirectory)
        }

        println("Creating test catalog with DuckDB 1.5 + Ducklake extension...")
        println()

        // Create catalog directory and data directory
        Files.createDirectories(catalogDirectory)
        val dataDir = catalogDirectory.resolve("data")
        Files.createDirectories(dataDir)

        // Connect to DuckDB in-memory
        val jdbcUrl = "jdbc:duckdb:"
        DriverManager.getConnection(jdbcUrl).use { conn ->
            conn.createStatement().use { stmt ->
                // Install and load extensions
                println("Installing extensions...")
                stmt.execute("INSTALL ducklake")
                for (extension in extraExtensions) {
                    stmt.execute("INSTALL $extension")
                }
                println("Loading extensions...")
                stmt.execute("LOAD ducklake")
                for (extension in extraExtensions) {
                    stmt.execute("LOAD $extension")
                }

                // Attach Ducklake catalog
                println("Attaching Ducklake catalog...")
                val attachSql = "ATTACH '" + escapeSql(ducklakeAttachUri) + "' AS ducklake_db (DATA_PATH '" +
                    escapeSql(dataDir.toAbsolutePath().toString()) + "')"
                stmt.execute(attachSql)

                // Create test schema in Ducklake
                println("Creating test schema...")
                stmt.execute("CREATE SCHEMA ducklake_db.test_schema")

                // Table 1: Simple primitives only
                println("Creating simple_table (primitives only)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.simple_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE
                    )
                    """.trimIndent(),
                )

                println("Inserting data into simple_table...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.simple_table VALUES
                        (1, 'Product A', 19.99, true, '2024-01-15'),
                        (2, 'Product B', 29.99, true, '2024-02-20'),
                        (3, 'Product C', 39.99, false, '2024-03-10'),
                        (4, 'Product D', 49.99, true, '2024-01-05'),
                        (5, 'Product E', 59.99, false, '2024-02-28')
                    """.trimIndent(),
                )

                // Table 2: Primitives + array(varchar)
                println("Creating array_table (primitives + array)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.array_table (
                        id INTEGER,
                        product_name VARCHAR,
                        tags VARCHAR[],
                        quantity INTEGER
                    )
                    """.trimIndent(),
                )

                println("Inserting data into array_table...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.array_table VALUES
                        (1, 'Widget', ['electronics', 'gadgets', 'sale'], 100),
                        (2, 'Gizmo', ['tools', 'hardware'], 50),
                        (3, 'Doohickey', ['accessories', 'premium', 'new'], 25),
                        (4, 'Thingamajig', ['clearance'], 200),
                        (5, 'Whatchamacallit', ['featured', 'bestseller', 'trending'], 75)
                    """.trimIndent(),
                )

                // Table 3: Partitioned by region (identity transform)
                println("Creating partitioned_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.partitioned_table (
                        id INTEGER,
                        name VARCHAR,
                        region VARCHAR,
                        amount DOUBLE
                    )
                    """.trimIndent(),
                )

                println("Setting partition by region...")
                stmt.execute("ALTER TABLE ducklake_db.test_schema.partitioned_table SET PARTITIONED BY (region)")

                println("Inserting partitioned data...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (1, 'Alice', 'US', 100.0),
                        (2, 'Bob', 'US', 200.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (3, 'Charlie', 'EU', 150.0),
                        (4, 'Diana', 'EU', 250.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (5, 'Emi', 'APAC', 300.0)
                    """.trimIndent(),
                )

                // Table 4: Partitioned by temporal transforms (year, month, day)
                println("Creating temporal_partitioned_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.temporal_partitioned_table (
                        id INTEGER,
                        event_name VARCHAR,
                        event_date DATE,
                        amount DOUBLE
                    )
                    """.trimIndent(),
                )

                println("Setting temporal partition by year(event_date), month(event_date)...")
                stmt.execute(
                    "ALTER TABLE ducklake_db.test_schema.temporal_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date))",
                )

                println("Inserting temporally partitioned data...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (1, 'Jan Event', '2023-01-15', 100.0),
                        (2, 'Jan Meeting', '2023-01-20', 150.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (3, 'Jun Event', '2023-06-10', 200.0),
                        (4, 'Jun Meeting', '2023-06-25', 250.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.temporal_partitioned_table VALUES
                        (5, 'Next Year', '2024-03-05', 300.0),
                        (6, 'Next Year Too', '2024-03-20', 350.0)
                    """.trimIndent(),
                )

                // Table 5: Partitioned down to day level
                println("Creating daily_partitioned_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.daily_partitioned_table (
                        id INTEGER,
                        event_name VARCHAR,
                        event_date DATE,
                        amount DOUBLE
                    )
                    """.trimIndent(),
                )

                println("Setting partition by year/month/day...")
                stmt.execute(
                    "ALTER TABLE ducklake_db.test_schema.daily_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date), day(event_date))",
                )

                println("Inserting daily partitioned data...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (1, 'Morning standup', '2023-06-15', 10.0),
                        (2, 'Afternoon review', '2023-06-15', 20.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (3, 'Sprint planning', '2023-06-20', 30.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (4, 'July kickoff', '2023-07-01', 40.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.daily_partitioned_table VALUES
                        (5, 'New year event', '2024-01-10', 50.0)
                    """.trimIndent(),
                )

                // Table 18: Partitioned by day on a TIMESTAMPTZ column
                println("Creating timestamp_partitioned_table (TIMESTAMPTZ partitioned by day)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.timestamp_partitioned_table (
                        id INTEGER,
                        event_name VARCHAR,
                        inserted_at TIMESTAMPTZ,
                        amount DOUBLE
                    )
                    """.trimIndent(),
                )

                println("Setting partition by year/month/day on inserted_at...")
                stmt.execute(
                    "ALTER TABLE ducklake_db.test_schema.timestamp_partitioned_table SET PARTITIONED BY (year(inserted_at), month(inserted_at), day(inserted_at))",
                )

                println("Inserting timestamp partitioned data...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                        (1, 'Morning event', '2026-03-07 08:00:00+00', 100.0),
                        (2, 'Afternoon event', '2026-03-07 14:30:00+00', 150.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                        (3, 'Next day event', '2026-03-08 09:00:00+00', 200.0),
                        (4, 'Next day late', '2026-03-08 22:00:00+00', 250.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.timestamp_partitioned_table VALUES
                        (5, 'Summer event', '2026-06-15 10:00:00+00', 300.0)
                    """.trimIndent(),
                )

                // Table 6: Nested types (struct, map, nested arrays)
                println("Creating nested_table (struct, map, nested arrays)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.nested_table (
                        id INTEGER,
                        metadata STRUCT(key VARCHAR, value VARCHAR),
                        tags MAP(VARCHAR, INTEGER),
                        nested_list INTEGER[][],
                        complex_struct STRUCT(name VARCHAR, scores INTEGER[], attrs MAP(VARCHAR, VARCHAR))
                    )
                    """.trimIndent(),
                )

                println("Inserting data into nested_table...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.nested_table VALUES
                        (1,
                         {'key': 'color', 'value': 'red'},
                         MAP {'priority': 1, 'severity': 3},
                         [[1, 2], [3, 4]],
                         {'name': 'Alice', 'scores': [90, 85, 92], 'attrs': MAP {'dept': 'eng', 'level': 'senior'}}
                        ),
                        (2,
                         {'key': 'size', 'value': 'large'},
                         MAP {'priority': 2, 'severity': 1},
                         [[10, 20, 30], [40]],
                         {'name': 'Bob', 'scores': [75, 88], 'attrs': MAP {'dept': 'sales', 'level': 'junior'}}
                        ),
                        (3,
                         {'key': 'shape', 'value': 'round'},
                         MAP {'priority': 3},
                         [[100]],
                         {'name': 'Carol', 'scores': [95, 97, 99, 100], 'attrs': MAP {'dept': 'eng', 'level': 'lead'}}
                        )
                    """.trimIndent(),
                )

                // Table 7: Wide types table (covers many primitive types)
                println("Creating wide_types_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.wide_types_table (
                        col_tinyint TINYINT,
                        col_smallint SMALLINT,
                        col_integer INTEGER,
                        col_bigint BIGINT,
                        col_float FLOAT,
                        col_double DOUBLE,
                        col_decimal DECIMAL(10, 2),
                        col_boolean BOOLEAN,
                        col_varchar VARCHAR,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        col_blob BLOB
                    )
                    """.trimIndent(),
                )

                println("Inserting data into wide_types_table...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.wide_types_table VALUES
                        (1, 100, 10000, 1000000000, 1.5, 2.5, 123.45, true, 'hello', '2024-01-01', '2024-01-01 12:00:00', '\x48454C4C4F'),
                        (2, 200, 20000, 2000000000, 2.5, 3.5, 678.90, false, 'world', '2024-06-15', '2024-06-15 18:30:00', '\x574F524C44'),
                        (-1, -100, -10000, -1000000000, -1.5, -2.5, -123.45, true, '', '1970-01-01', '1970-01-01 00:00:00', '\x00')
                    """.trimIndent(),
                )

                // Table 8: Nullable table (NULLs in every column type)
                println("Creating nullable_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.nullable_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE,
                        tags VARCHAR[],
                        metadata STRUCT(key VARCHAR, value VARCHAR)
                    )
                    """.trimIndent(),
                )

                println("Inserting data into nullable_table (with NULLs)...")
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.nullable_table VALUES
                        (1, 'Present', 10.0, true, '2024-01-01', ['a', 'b'], {'key': 'k1', 'value': 'v1'}),
                        (2, NULL, NULL, NULL, NULL, NULL, NULL),
                        (NULL, 'NoId', 20.0, false, '2024-06-01', ['c'], {'key': 'k2', 'value': 'v2'}),
                        (4, NULL, NULL, true, NULL, NULL, {'key': 'k3', 'value': NULL})
                    """.trimIndent(),
                )

                // Table 9: Empty table (no rows, tests empty result handling)
                println("Creating empty_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.empty_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """.trimIndent(),
                )

                // Table 10: Schema evolution table
                println("Creating schema_evolution_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.schema_evolution_table (
                        id INTEGER,
                        original_col VARCHAR
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                        (1, 'row1'),
                        (2, 'row2')
                    """.trimIndent(),
                )

                stmt.execute("CHECKPOINT ducklake_db")

                println("Adding column to schema_evolution_table...")
                stmt.execute("ALTER TABLE ducklake_db.test_schema.schema_evolution_table ADD COLUMN added_col INTEGER")

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.schema_evolution_table VALUES
                        (3, 'row3', 300),
                        (4, 'row4', 400)
                    """.trimIndent(),
                )

                // Table 11: Large-ish table for aggregation/stats tests
                println("Creating aggregation_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.aggregation_table (
                        id INTEGER,
                        category VARCHAR,
                        amount DOUBLE,
                        quantity INTEGER
                    )
                    """.trimIndent(),
                )

                val insertSql = StringBuilder(
                    "INSERT INTO ducklake_db.test_schema.aggregation_table VALUES ",
                )
                val categories = arrayOf("A", "B", "C")
                for (i in 1..30) {
                    if (i > 1) {
                        insertSql.append(", ")
                    }
                    val category = categories[(i - 1) % 3]
                    val amount = 10.0 * i + (i % 7)
                    val quantity = i * 5
                    insertSql.append(String.format("(%d, '%s', %.1f, %d)", i, category, amount, quantity))
                }
                stmt.execute(insertSql.toString())

                // Table 12: Table with deleted rows
                println("Creating deleted_rows_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.deleted_rows_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.deleted_rows_table VALUES
                        (1, 'keep', 10.0),
                        (2, 'delete', 20.0),
                        (3, 'keep', 30.0),
                        (4, 'delete', 40.0),
                        (5, 'keep', 50.0),
                        (6, 'delete', 60.0)
                    """.trimIndent(),
                )

                stmt.execute("CALL ducklake_flush_inlined_data('ducklake_db')")
                println("Deleting rows from deleted_rows_table...")
                stmt.execute("DELETE FROM ducklake_db.test_schema.deleted_rows_table WHERE name = 'delete'")

                // Table 13: Simple struct with full-NULL rows and variable-length arrays with null elements
                println("Creating complex_nulls_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.complex_nulls_table (
                        id INTEGER,
                        pair STRUCT(a INTEGER, b INTEGER),
                        items INTEGER[]
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.complex_nulls_table VALUES
                        (1, {'a': 10, 'b': 20}, [1, 2, 3]),
                        (2, NULL, [NULL]),
                        (3, {'a': NULL, 'b': 40}, NULL),
                        (4, NULL, [4, 5]),
                        (5, {'a': 50, 'b': 60}, [])
                    """.trimIndent(),
                )

                // Table 14: Multi-file table (separate INSERTs produce separate Parquet files)
                println("Creating multi_file_table...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.multi_file_table (
                        id INTEGER,
                        value VARCHAR
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    "CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => 'multi_file_table')",
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (1, 'file1_row1'),
                        (2, 'file1_row2')
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (3, 'file2_row1'),
                        (NULL, NULL)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.multi_file_table VALUES
                        (5, 'file3_row1')
                    """.trimIndent(),
                )

                println("Forcing checkpoint to write Parquet files...")
                stmt.execute("CHECKPOINT ducklake_db")

                println("Detaching Ducklake catalog...")
                stmt.execute("DETACH ducklake_db")

                // Re-attach to create inlined tables AFTER checkpoint/detach.
                println("Re-attaching catalog for inlined table creation...")
                stmt.execute(attachSql)

                // Table 15: Inlined data table
                println("Creating inlined_table (data stays inlined in metadata catalog)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.inlined_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.inlined_table VALUES
                        (1, 'alpha', 10.5),
                        (2, 'beta', 20.0),
                        (3, 'gamma', 30.75)
                    """.trimIndent(),
                )

                // Table 16: Inlined data table with NULLs
                println("Creating inlined_nullable_table (inlined data with NULLs)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.inlined_nullable_table (
                        id INTEGER,
                        name VARCHAR,
                        value DOUBLE
                    )
                    """.trimIndent(),
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.inlined_nullable_table VALUES
                        (1, 'present', 10.0),
                        (NULL, NULL, NULL),
                        (3, NULL, 30.0)
                    """.trimIndent(),
                )

                // Table 17: Mixed inlined + Parquet table
                println("Creating mixed_inline_table (inlined + Parquet in same snapshot)...")
                stmt.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.mixed_inline_table (
                        id INTEGER,
                        source VARCHAR,
                        value DOUBLE
                    )
                    """.trimIndent(),
                )
                stmt.execute(
                    "CALL ducklake_db.set_option('data_inlining_row_limit', 3, schema => 'test_schema', table_name => 'mixed_inline_table')",
                )

                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.mixed_inline_table VALUES
                        (1, 'inlined', 11.0),
                        (2, 'inlined', 22.0)
                    """.trimIndent(),
                )
                stmt.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.mixed_inline_table VALUES
                        (100, 'parquet', 100.0),
                        (101, 'parquet', 101.0),
                        (102, 'parquet', 102.0),
                        (103, 'parquet', 103.0),
                        (104, 'parquet', 104.0)
                    """.trimIndent(),
                )

                // ==================== Views ====================

                println("Creating simple_view (compatible DuckDB view)...")
                stmt.execute(
                    """
                    CREATE VIEW ducklake_db.test_schema.simple_view AS
                        SELECT id, name, price FROM ducklake_db.test_schema.simple_table WHERE active = true
                    """.trimIndent(),
                )

                println("Creating aliased_view (column aliases)...")
                stmt.execute(
                    """
                    CREATE VIEW ducklake_db.test_schema.aliased_view (product_id, product_name, product_price) AS
                        SELECT id, name, price FROM ducklake_db.test_schema.simple_table
                    """.trimIndent(),
                )

                println("Creating duckdb_specific_view (DuckDB-only syntax)...")
                stmt.execute(
                    """
                    CREATE VIEW ducklake_db.test_schema.duckdb_specific_view AS
                        SELECT id, product_name, list_sort(tags) as sorted_tags FROM ducklake_db.test_schema.array_table
                    """.trimIndent(),
                )

                // Detach WITHOUT checkpoint — inlined data stays in metadata catalog
                println("Detaching catalog (no checkpoint — inlined data preserved)...")
                stmt.execute("DETACH ducklake_db")

                println()
                println("Test catalog created successfully!")
                println()
                println("Catalog location: " + catalogDirectory.toAbsolutePath())
                println(catalogDescription)
                println("Data directory: " + dataDir.toAbsolutePath())
            }
        }
    }

    @Throws(Exception::class)
    private fun deleteDirectory(directory: Path) {
        if (Files.exists(directory)) {
            Files.walk(directory)
                .sorted { a, b -> -a.compareTo(b) }
                .forEach { path ->
                    try {
                        Files.delete(path)
                    } catch (e: Exception) {
                        throw RuntimeException("Failed to delete: $path", e)
                    }
                }
        }
    }

    private fun escapeSql(value: String): String = value.replace("'", "''")
}
