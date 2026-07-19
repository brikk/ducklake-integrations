plugins {
    id("buildlogic.kotlin.library")
    id("buildlogic.kotlin.brikk")
    java
    alias(libs.plugins.detekt)
}

group = "dev.brikk.ducklake"
version = "483-1-ALPHA"

// Idiomatic-Kotlin quality gate (detekt 2.0 runs on the JDK 25 daemon; jvmTarget is read
// from the Kotlin compilerOptions). Custom src/test layout, so point detekt at it explicitly.
detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    baseline = file("detekt-baseline.xml")
    source.setFrom("src", "test")
}

// The brikk-sql (dev.brikk.house) TEST-ONLY transpiler resolves from the Central Portal snapshots
// repo, wired centrally by the buildlogic.kotlin.brikk convention plugin (applied above).
repositories {
    // dev.brikk.ducklake catalog + corpus-replay: released versions resolve from mavenCentral;
    // -SNAPSHOTs from the Central Portal snapshots repo (wired in buildlogic.kotlin.brikk).
    mavenCentral()
}

// Use Trino BOM for all managed dependency versions
dependencies {
    // BOM import — controls versions for io.trino, io.airlift, jackson, opentelemetry, etc.
    implementation(platform(libs.trino.bom))
    testImplementation(platform(libs.trino.bom))
    implementation(enforcedPlatform(libs.kotlin.bom)) {
        exclude(group = "org.junit")
        exclude(group = "org.junit.jupiter")
    }

    // Core implementation dependencies (versions from BOM)
    implementation("com.google.guava:guava")
    implementation("com.google.inject:guice") {
        artifact {
            classifier = "classes"
        }
    }
    implementation(libs.ducklake.catalog)
    implementation(libs.hikari)
    implementation("io.airlift:bootstrap")
    implementation("io.airlift:configuration")
    implementation("io.airlift:json")
    implementation("io.airlift:log")
    implementation("io.airlift:units")
    implementation("io.trino:trino-filesystem")
    implementation("io.trino:trino-filesystem-manager")
    implementation("io.trino:trino-hive")
    implementation("io.trino:trino-memory-context")
    implementation("io.trino:trino-parquet")
    implementation("io.trino:trino-plugin-toolkit")
    implementation("jakarta.validation:jakarta.validation-api")
    implementation("joda-time:joda-time")
    implementation("org.apache.parquet:parquet-column")
    implementation("org.apache.parquet:parquet-format-structures") {
        exclude(group = "javax.annotation", module = "javax.annotation-api")
    }
    implementation("org.weakref:jmxutils")
    implementation(libs.roaring.bitmap)

    // SPI dependencies — provided by Trino at runtime (compileOnly = Maven provided)
    compileOnly("com.fasterxml.jackson.core:jackson-annotations")
    compileOnly("io.airlift:slice")
    compileOnly("io.opentelemetry:opentelemetry-api")
    compileOnly("io.opentelemetry:opentelemetry-api-incubator")
    compileOnly("io.opentelemetry:opentelemetry-context")
    compileOnly("io.trino:trino-spi")

    // Runtime-only dependencies
    runtimeOnly("io.airlift:log-manager")
    runtimeOnly("org.postgresql:postgresql")
    // MySQL 8+ catalog backend: the driver on the deployed connector's classpath lets a user
    // point ducklake.catalog.database-url at jdbc:mysql://... HikariCP resolves it from the URL.
    runtimeOnly(libs.mysql.jdbc)

    // Test dependencies
    testImplementation("io.airlift:testing")
    testImplementation("io.trino:trino-hdfs")
    testImplementation("io.trino:trino-main")
    testImplementation("io.trino:trino-testing")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.mysql)

    // External DuckDB JDBC driver — TEST-ONLY: cross-engine tests drive a standalone DuckDB
    // process (jdbc:duckdb:) that writes/reads parquet DuckLake tables through the shared catalog,
    // to prove Trino and DuckDB agree. The production connector no longer embeds DuckDB.
    testImplementation(libs.duckdb.jdbc)

    // brikk-sql: DuckDB→Trino SQL transpiler powering the corpus-replay dialect gate
    // (transpile-first — run what transpiles + is mappable, engine-skip the rest). TEST-ONLY:
    // the production connector never sees raw SQL (Trino parses it). The -jvm artifacts are the
    // JVM target of the Kotlin-Multiplatform library; brikk-sql-verify's TrinoVerifier re-parses
    // transpiled SQL under Trino's real grammar as an extra gate signal.
    testImplementation(libs.brikk.sql)
    testImplementation(libs.brikk.sql.metadata)
    testImplementation(libs.brikk.sql.verify)

    // Shared Testcontainer fixture (TestingDucklakePostgreSqlCatalogServer) lives in
    // ducklake-catalog's java-test-fixtures source set, published as the -test-fixtures
    // variant of dev.brikk.ducklake:ducklake-catalog.
    testImplementation(testFixtures(libs.ducklake.catalog))

    // Upstream DuckLake sqllogictest corpus replay (oracle + driver + engine seam);
    // TrinoReplayEngine implements its ReplayReadEngine, TestTrinoCorpusReplay runs it.
    testImplementation(libs.ducklake.test.corpus.replay)

    // compileOnly deps also needed at test time
    testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
    testImplementation("io.airlift:slice")
    testImplementation("io.trino:trino-spi")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf(
        "--enable-preview",
    ))
}

tasks.test {
    useJUnitPlatform {
        // Optional JUnit tag exclusion, e.g. `-PexcludeTags=quack-container,ci-unstable`.
        // CI uses this to skip heavy/environment-specific integration tests on the PR gate
        // (see .github/workflows/ci.yml) while keeping them runnable locally. Comma-separated.
        (project.findProperty("excludeTags") as String?)
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.takeIf { it.isNotEmpty() }
            ?.let { excludeTags(*it.toTypedArray()) }
    }

    maxHeapSize = "3g"
    minHeapSize = "3g"

    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
    systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
    // Podman Docker API compatibility rejects status=restarting filter in ReportLeakedContainers
    systemProperty("ReportLeakedContainers.disabled", "true")

    // Forward the catalog-backend selector from the Gradle CLI to the test JVM. Gradle
    // doesn't propagate `-D` system properties to forked test workers by default.
    // Without this, `-Dducklake.test.catalog-backend=DUCKDB_QUACK` silently runs PG.
    System.getProperty("ducklake.test.catalog-backend")?.let {
        systemProperty("ducklake.test.catalog-backend", it)
        // Treat the property as a task input so Gradle re-runs the test task when the
        // selector value changes instead of returning a cached UP-TO-DATE result.
        inputs.property("ducklake.test.catalog-backend", it)
    }

    // Upstream DuckLake sqllogictest corpus (TestTrinoCorpusReplay): corpus location
    // in the sibling module's pinned submodule + optional dir selection
    // (-Dducklake.corpus.dirs=all for the full corpus).
    systemProperty(
        "ducklake.corpus.root",
        rootProject.projectDir.resolve("ducklake/test/sql").absolutePath,
    )
    System.getProperty("ducklake.corpus.dirs")?.let {
        systemProperty("ducklake.corpus.dirs", it)
        inputs.property("ducklake.corpus.dirs", it)
    }

    jvmArgs(
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:-OmitStackTraceInFastThrow",
        "-XX:G1HeapRegionSize=32M",
        "-XX:+UnlockDiagnosticVMOptions",
        "--add-modules=jdk.incubator.vector",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        // The standalone DuckDB JDBC driver (cross-engine tests) reaches into private
        // DirectByteBuffer internals for off-heap interop.
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}

val pluginAssemble by tasks.registering(Copy::class) {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("trino-plugin/trino-ducklake-$version"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}

tasks.build {
    dependsOn(pluginAssemble)
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