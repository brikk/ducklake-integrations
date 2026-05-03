import dev.brikk.jooq.ducklake.DucklakePostgresService
import org.jooq.codegen.gradle.CodegenPluginExtension
import org.jooq.codegen.gradle.CodegenTask
import org.jooq.meta.jaxb.GeneratedAnnotationType
import org.jooq.meta.jaxb.VisibilityModifier

plugins {
    id("buildlogic.kotlin.library")
    alias(libs.plugins.jooq)
    `java-test-fixtures`
}

version = "0.0.1"

// Match the project's flat layout (see buildlogic.kotlin.common). Default would
// look for src/testFixtures/{java,kotlin}; we keep things at testFixtures/src/.
sourceSets {
    named("testFixtures") {
        java.setSrcDirs(listOf("testFixtures/src"))
        resources.setSrcDirs(listOf("testFixtures/resources"))
    }
}

dependencies {
    implementation(enforcedPlatform(libs.kotlin.bom))
    implementation(libs.jackson.databind)
    implementation(libs.java.uuid.generator)
    implementation(libs.hikari)
    implementation(libs.bundles.jooq)

    jooqCodegen(libs.postgres.jdbc)
    jooqCodegen(project(":jooq-custom-naming"))

    // Testcontainers types appear on the public API of the test-fixture classes
    // (e.g. TestingDucklakePostgreSqlCatalogServer wraps PostgreSQLContainer), so
    // they're exposed via testFixturesApi rather than testFixturesImplementation —
    // consumers shouldn't need to redeclare them.
    testFixturesApi(libs.testcontainers.core)
    testFixturesApi(libs.testcontainers.postgresql)

    testImplementation(libs.duckdb.jdbc)
    testRuntimeOnly(libs.postgres.jdbc)
}

// Stand up a Postgres container and bootstrap the DuckLake metadata schema in it. jOOQ
// codegen reads the schema from this container. The service is shared across the build and
// auto-closed at build end.
val ducklakeCodegenPostgres =
    gradle.sharedServices.registerIfAbsent(
        "ducklakeCodegenPostgres",
        DucklakePostgresService::class,
    ) {
        parameters.image.set("postgres:18")
        parameters.databaseName.set("ducklake")
        parameters.dataPath.set(
            layout.buildDirectory
                .dir("jooq-ducklake-data")
                .get()
                .asFile.absolutePath,
        )
    }

jooq {
    configuration {
        generator {
            target {
                directory = projectDir.absoluteFile.resolve("generated").toString()
                isClean = true
            }

            database {
                isUnsignedTypes = false
                isForceIntegerTypesOnZeroScaleDecimals = true
                isIntegerDisplayWidths = false

                // Pin every *_uuid column to java.util.UUID. Postgres maps its native
                // `uuid` column to UUID by default, so this is a no-op on the current
                // backend — but it keeps the binding stable if DuckLake ever widens
                // the column to text/varchar on a non-Postgres catalog.
                forcedTypes {
                    forcedType {
                        name = "UUID"
                        includeExpression = ".*\\.(?:schema|table|view)_uuid"
                        includeTypes = ".*"
                    }
                }
            }

            generate {
                isGlobalCatalogReferences = false
                isGlobalSchemaReferences = true
                isGlobalTableReferences = true
                isGlobalSequenceReferences = false
                isGlobalDomainReferences = false
                isGlobalUDTReferences = false
                isGlobalRoutineReferences = false
                isGlobalQueueReferences = false
                isGlobalLinkReferences = false

                isDeprecated = false
                isRecords = true
                isFluentSetters = true
                isJavaTimeTypes = true
                isPojos = false
                isDaos = false
                isInterfaces = false

                isGeneratedAnnotation = false
                generatedAnnotationType = GeneratedAnnotationType.DETECT_FROM_JDK
                isGeneratedAnnotationDate = false

                isValidationAnnotations = false

                isNonnullAnnotation = true
                nonnullAnnotationType = "org.jetbrains.annotations.NotNull"
                isNullableAnnotation = true
                nullableAnnotationType = "org.jetbrains.annotations.Nullable"

                visibilityModifier = VisibilityModifier.PUBLIC
            }

            strategy {
                name = "dev.brikk.jooq.codegen.JooqCustomNaming"
            }
        }
    }

    executions {
        create("ducklake") {
            configuration {
                // JDBC coordinates are injected at task-execution time from the shared
                // Postgres build service (see the CodegenTask doFirst below). The values
                // here are placeholders so the jOOQ XML schema stays valid at configure
                // time.
                jdbc {
                    driver = "org.postgresql.Driver"
                    url = "jdbc:postgresql://placeholder/ducklake"
                    user = "placeholder"
                    password = "placeholder"
                }
                generator {
                    database {
                        name = "org.jooq.meta.postgres.PostgresDatabase"
                        inputSchema = "public"
                        includes = "ducklake_.*"
                    }
                    target {
                        packageName = "dev.brikk.ducklake.catalog.schema"
                    }
                }
            }
        }
    }
}

tasks.withType<CodegenTask>().configureEach {
    usesService(ducklakeCodegenPostgres)
    // Always re-run: container JDBC port changes each build, and Postgres content is
    // ephemeral. Caching based on the placeholder inputs would be meaningless.
    outputs.upToDateWhen { false }
    doFirst {
        val service = ducklakeCodegenPostgres.get()
        val jooqExt = project.extensions.getByType(CodegenPluginExtension::class.java)
        val namedConfig = jooqExt.executions.getByName("ducklake")
        namedConfig.configuration.jdbc.apply {
            url = service.jdbcUrl
            user = service.username
            password = service.password
        }
    }
}
