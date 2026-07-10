import dev.brikk.jooq.ducklake.DucklakePostgresService
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jooq.codegen.gradle.CodegenPluginExtension
import org.jooq.codegen.gradle.CodegenTask
import org.jooq.meta.jaxb.GeneratedAnnotationType
import org.jooq.meta.jaxb.VisibilityModifier
import org.gradle.api.artifacts.ResolvedDependency
import java.util.zip.ZipFile

plugins {
    id("buildlogic.kotlin.library")
    alias(libs.plugins.jooq)
    `java-test-fixtures`
    alias(libs.plugins.detekt)
}

version = "0.0.1"

// Idiomatic-Kotlin quality gate (detekt 2.0; jvmTarget read from Kotlin compilerOptions).
// Custom src/test layout; generated jOOQ sources are Java so detekt ignores them.
detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    baseline = file("detekt-baseline.xml")
    source.setFrom("src", "test", "testFixtures")
}

// JDK 17 ABI so this library is consumable by the Doris fe-connector plugin,
// whose FE is pinned to JDK 17. Toolchain remains 25 (set in buildlogic.kotlin.common);
// only the produced bytecode level is downgraded.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

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
    // TestingDucklakeMySqlCatalogServer wraps MySQLContainer on its public API.
    testFixturesApi(libs.testcontainers.mysql)
    // jOOQ types (DSLContext, Condition, Field) appear on the public API of CatalogQueries
    // and CatalogPredicates, so consumers (e.g. trino-ducklake test classpath) get them
    // transitively. Main src declares jOOQ as `implementation`, which doesn't expose it
    // to test-fixture consumers, so it has to be declared here too.
    testFixturesApi(libs.bundles.jooq)

    testImplementation(libs.duckdb.jdbc)
    testRuntimeOnly(libs.postgres.jdbc)
    // MySQL Connector/J for the MySQL-backend smoke test. Test-only: the main artifact
    // never names a driver — HikariCP resolves it from the JDBC URL at runtime.
    testRuntimeOnly(libs.mysql.jdbc)

    // Golden wire-format test (TestJacksonWireFormat) exercises the SAME airlift
    // JsonCodec that the production Trino fragment paths use, so the catalog's own
    // wire types are locked against the real serializer rather than a bare
    // ObjectMapper. The Trino BOM (test scope only) supplies the io.airlift:json
    // version; the catalog main classpath stays Jackson-only.
    testImplementation(platform(libs.trino.bom))
    testImplementation("io.airlift:json")
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



tasks.register("printCompileDependencyTree") {
    doLast {
        fun walk(dep: ResolvedDependency, indent: String = "") {
            dep.moduleArtifacts.forEach { artifact ->
                println("$indent${dep.moduleGroup}:${dep.moduleName}:${dep.moduleVersion}")
                println("$indent  -> ${artifact.file.absolutePath}")
            }
            dep.children
                .sortedBy { "${it.moduleGroup}:${it.moduleName}:${it.moduleVersion}" }
                .forEach { walk(it, "$indent  ") }
        }
        configurations
            .getByName("compileClasspath")
            .resolvedConfiguration
            .firstLevelModuleDependencies
            .sortedBy { "${it.moduleGroup}:${it.moduleName}:${it.moduleVersion}" }
            .forEach { walk(it) }
    }
}

// ABI floor guard for the Doris FE deploy target.
//
// This library ships into the Doris fe-connector plugin, whose FE process is pinned
// to JDK 17. Anything on the RUNTIME classpath that carries bytecode newer than
// Java 17 (class file major version 61) will `UnsupportedClassVersionError` at
// deploy time. `targetCompatibility = VERSION_17` only governs the bytecode WE
// compile from source — it does nothing about a PREBUILT dependency jar that was
// itself compiled for a newer JVM. That drift is exactly what bit us when a jOOQ
// 3.21 bump silently shipped newer bytecode; source-level compatibility passed but
// the deployed FE choked. This task reads the real class file major version of
// every runtime .class (our own output + all transitive jars) and fails the build
// if any exceeds 61.
//
// Multi-release-jar (MRJAR) nuance: modern jars ship META-INF/versions/<N>/Foo.class
// variants (9/17/21/...). A META-INF/versions/21/Foo.class legitimately has major 65
// but is INERT on JDK 17 — the JVM uses the base (root) Foo.class there. So we only
// inspect versioned entries where <N> <= 17, and always inspect the base classes.
// Flagging a META-INF/versions/21/ class would be a false positive. module-info.class
// is skipped (its major version reflects the module descriptor's --release, not code
// that runs pre-module-path here, and it's noise for this check).
tasks.register("checkAbi") {
    group = "verification"
    description = "Fails if any runtime .class has bytecode major > 61 (Java 17) — Doris FE ABI floor."

    // Snapshot the inputs at configuration time so the doLast body stays
    // configuration-cache friendly (no Project access at execution time).
    val runtimeClasspath = configurations.getByName("runtimeClasspath")
    val mainOutput = sourceSets["main"].output
    val abiFloorMajor = 61 // Java 17

    doLast {
        // Read a class file's major version from bytes 6-7 (u2 big-endian),
        // after verifying the 0xCAFEBABE magic. Returns null for non-class / short input.
        fun majorVersion(bytes: ByteArray): Int? {
            if (bytes.size < 8) return null
            val magic =
                ((bytes[0].toInt() and 0xFF) shl 24) or
                    ((bytes[1].toInt() and 0xFF) shl 16) or
                    ((bytes[2].toInt() and 0xFF) shl 8) or
                    (bytes[3].toInt() and 0xFF)
            if (magic != -0x35014542) return null // 0xCAFEBABE as signed Int
            val major = ((bytes[6].toInt() and 0xFF) shl 8) or (bytes[7].toInt() and 0xFF)
            return major
        }

        // For an MRJAR entry name, decide whether it is relevant on JDK 17.
        // Returns false only for META-INF/versions/<N>/... where N > 17 (inert here)
        // or for module-info.class. Base classes and versions/<N<=17> are relevant.
        fun isRelevantEntry(name: String): Boolean {
            if (!name.endsWith(".class")) return false
            if (name == "module-info.class" || name.endsWith("/module-info.class")) return false
            val prefix = "META-INF/versions/"
            if (name.startsWith(prefix)) {
                val rest = name.substring(prefix.length)
                val slash = rest.indexOf('/')
                if (slash <= 0) return false
                val ver = rest.substring(0, slash).toIntOrNull() ?: return false
                return ver <= 17
            }
            return true
        }

        val violations = mutableListOf<String>()
        var scannedClasses = 0
        var scannedArtifacts = 0

        // 1) The module's own compiled main output (a .class we produced must be checked too).
        mainOutput.classesDirs.files
            .filter { it.exists() }
            .forEach { dir ->
                scannedArtifacts++
                dir.walkTopDown()
                    .filter { it.isFile && isRelevantEntry(it.toRelativeString(dir).replace('\\', '/')) }
                    .forEach { classFile ->
                        val header = ByteArray(8)
                        classFile.inputStream().use { it.read(header) }
                        val major = majorVersion(header) ?: return@forEach
                        scannedClasses++
                        if (major > abiFloorMajor) {
                            violations +=
                                ":ducklake-catalog (main output) -> " +
                                    "${classFile.absolutePath} has bytecode major $major (> $abiFloorMajor / Java 17)"
                        }
                    }
            }

        // 2) Every jar on the resolved runtime classpath (these are the jars that ship).
        //    Resolve artifacts so we can name the offending coordinate, not just the file.
        val artifactByFile =
            runtimeClasspath.resolvedConfiguration.resolvedArtifacts
                .associateBy({ it.file }, { "${it.moduleVersion.id}" })

        runtimeClasspath.files
            .filter { it.isFile && it.name.endsWith(".jar") }
            .forEach { jar ->
                scannedArtifacts++
                val coord = artifactByFile[jar] ?: jar.name
                ZipFile(jar).use { zip ->
                    val entries = zip.entries()
                    while (entries.hasMoreElements()) {
                        val entry = entries.nextElement()
                        if (entry.isDirectory) continue
                        if (!isRelevantEntry(entry.name)) continue
                        val header = ByteArray(8)
                        zip.getInputStream(entry).use { it.read(header) }
                        val major = majorVersion(header) ?: continue
                        scannedClasses++
                        if (major > abiFloorMajor) {
                            violations +=
                                "$coord -> ${entry.name} has bytecode major $major (> $abiFloorMajor / Java 17)"
                        }
                    }
                }
            }

        if (violations.isNotEmpty()) {
            val msg =
                buildString {
                    appendLine(
                        "checkAbi: found ${violations.size} class(es) with bytecode newer than " +
                            "Java 17 (major > $abiFloorMajor) on the :ducklake-catalog runtime classpath.",
                    )
                    appendLine("The Doris FE runs JDK 17 and would fail with UnsupportedClassVersionError:")
                    violations.sorted().forEach { appendLine("  $it") }
                }
            throw GradleException(msg.trimEnd())
        }

        println(
            "checkAbi: scanned $scannedClasses classes across $scannedArtifacts artifacts, " +
                "all <= major $abiFloorMajor (Java 17)",
        )
    }
}

// Wire into `check` so CI catches dependency ABI drift automatically. This is a plain
// task dependency (check -> checkAbi); checkAbi only reads resolved config + jars, so
// there's no cycle with compile/test tasks.
tasks.named("check") {
    dependsOn("checkAbi")
}
