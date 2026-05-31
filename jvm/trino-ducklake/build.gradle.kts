plugins {
    id("buildlogic.kotlin.library")
    java
}

version = "481-1-ALPHA"

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
    implementation(project(":ducklake-catalog"))
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
    implementation(libs.duckdb.jdbc)
    implementation(libs.roaring.bitmap)
    implementation(platform(libs.arrow.bom))
    implementation(libs.arrow.vector)
    implementation(libs.arrow.memory.core)
    implementation(libs.arrow.data)
    runtimeOnly(libs.arrow.memory.netty)

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

    // Shared Testcontainer fixture (TestingDucklakePostgreSqlCatalogServer) lives in
    // ducklake-catalog's java-test-fixtures source set.
    testImplementation(testFixtures(project(":ducklake-catalog")))

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

// ---- Bundle trino_parity DuckDB extension into the plugin jar ----
//
// The connector requires the trino_parity.duckdb_extension binary to be
// LOAD-able by the in-process DuckDB at attach time. We bundle every
// locally-built platform variant into the plugin jar's classpath resources,
// keyed by platform name. TrinoParityExtensionResolver detects the runtime
// platform and extracts the matching binary; for the Quack engine, the
// testcontainer mounts the linux-arm64 binary so the Linux server can LOAD it.
//
// Source paths (one per platform, each independently optional):
//   ../../duckdb-trino-parity-extension/build/release/extension/...    host platform
//                                                                       (output of `make`)
//   ../../duckdb-trino-parity-extension/build/linux-arm64/release/...  output of `make linux-arm64`
//   ../../duckdb-trino-parity-extension/build/linux-amd64/release/...  output of `make linux-amd64`
//
// Bundled to: META-INF/.../duckdb-extensions/<platform>/trino_parity.duckdb_extension
//
// Missing binaries are non-fatal at build time; the plugin jar just ships
// without that platform's variant and operators on that platform have to wire
// the path manually (or build the extension first).

val parityExtensionRoot: File =
    rootProject.projectDir.resolve("../duckdb-trino-parity-extension")

val hostPlatform: String = run {
    val os = System.getProperty("os.name").lowercase()
    val arch = System.getProperty("os.arch").lowercase()
    val osPart = when {
        os.contains("mac") || os.contains("darwin") -> "darwin"
        os.contains("linux") -> "linux"
        os.contains("windows") -> "windows"
        else -> "unknown"
    }
    val archPart = when (arch) {
        "x86_64", "amd64" -> "amd64"
        "aarch64", "arm64" -> "arm64"
        else -> "unknown"
    }
    "$osPart-$archPart"
}

// (platform-name, source path) — the host build lands in build/release;
// cross-built variants land in build/<platform>/release/.
val parityExtensionSources: List<Pair<String, File>> = listOf(
    hostPlatform to parityExtensionRoot.resolve("build/release/extension/trino_parity/trino_parity.duckdb_extension"),
    "linux-arm64" to parityExtensionRoot.resolve("build/linux-arm64/release/extension/trino_parity/trino_parity.duckdb_extension"),
    "linux-amd64" to parityExtensionRoot.resolve("build/linux-amd64/release/extension/trino_parity/trino_parity.duckdb_extension"),
).distinctBy { it.first }  // host might equal linux-arm64 on Linux CI; dedupe

val bundleParityExtension by tasks.registering {
    description = "Copy every available trino_parity.duckdb_extension into the plugin's classpath resources."
    group = "build"
    val outputs = parityExtensionSources.map { (platform, _) ->
        layout.buildDirectory.file(
            "generated-resources/parity-extension/dev/brikk/ducklake/trino/plugin/duckdb-extensions/$platform/trino_parity.duckdb_extension"
        )
    }
    inputs.files(parityExtensionSources.map { it.second }).withPropertyName("sources").optional()
    this.outputs.files(outputs).withPropertyName("bundled")
    doLast {
        var bundled = 0
        for ((platform, source) in parityExtensionSources) {
            if (!source.isFile) {
                logger.lifecycle("trino_parity: $platform binary missing at ${source.relativeToOrSelf(rootProject.projectDir)} — skipping (build it with `(cd duckdb-trino-parity-extension && make ${if (platform == hostPlatform) "" else platform})`).")
                continue
            }
            val target = layout.buildDirectory.file(
                "generated-resources/parity-extension/dev/brikk/ducklake/trino/plugin/duckdb-extensions/$platform/trino_parity.duckdb_extension"
            ).get().asFile
            target.parentFile.mkdirs()
            source.copyTo(target, overwrite = true)
            bundled++
            logger.info("trino_parity: bundled $platform from ${source.relativeToOrSelf(rootProject.projectDir)}")
        }
        if (bundled == 0) {
            logger.lifecycle("trino_parity: NO platform binaries bundled — plugin jar will require ducklake.duckdb.parity-extension-path to be set at deploy time.")
        }
    }
}

sourceSets.main {
    resources.srcDir(layout.buildDirectory.dir("generated-resources/parity-extension"))
}

tasks.named("processResources") {
    dependsOn(bundleParityExtension)
}

tasks.test {
    useJUnitPlatform()

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

    // Forward the trino_parity extension path. When set, DucklakeQueryRunner threads
    // it into the catalog properties as `ducklake.duckdb.parity-extension-path`, and
    // the in-process executor LOADs the binary instead of replaying the in-tree SQL
    // aliases. See https://github.com/brikk/duckdb-trino-parity-extension.
    System.getProperty("ducklake.test.parityExtensionPath")?.let {
        systemProperty("ducklake.test.parityExtensionPath", it)
        inputs.property("ducklake.test.parityExtensionPath", it)
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
        // Arrow's MemoryUtil reaches into private DirectByteBuffer constructor for
        // off-heap interop with DuckDB's Arrow C-data export.
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