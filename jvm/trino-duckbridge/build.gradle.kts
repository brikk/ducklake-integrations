plugins {
    id("buildlogic.kotlin.library")
    id("buildlogic.kotlin.brikk")
    java
    alias(libs.plugins.detekt)
}

version = "483-1-ALPHA"

// Idiomatic-Kotlin quality gate (detekt 2.0 runs on the JDK 25 daemon; jvmTarget is read
// from the Kotlin compilerOptions). Custom src/test layout, so point detekt at it explicitly.
detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    baseline = file("detekt-baseline.xml")
    source.setFrom("src", "test")
}

repositories {
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
    implementation("io.airlift:bootstrap")
    implementation("io.airlift:configuration")
    implementation("io.airlift:json")
    implementation("io.airlift:log")
    implementation("io.airlift:units")
    implementation("io.trino:trino-base-jdbc")
    implementation("io.trino:trino-plugin-toolkit")
    implementation("jakarta.validation:jakarta.validation-api")
    implementation(libs.duckdb.jdbc)

    // SPI dependencies — provided by Trino at runtime (compileOnly = Maven provided)
    compileOnly("com.fasterxml.jackson.core:jackson-annotations")
    compileOnly("io.airlift:slice")
    compileOnly("io.opentelemetry:opentelemetry-api")
    compileOnly("io.opentelemetry:opentelemetry-context")
    compileOnly("io.trino:trino-spi")

    // Runtime-only dependencies
    runtimeOnly("io.airlift:log-manager")

    // Test dependencies
    testImplementation("io.airlift:testing")
    testImplementation("io.trino:trino-main")
    testImplementation("io.trino:trino-testing")
    testImplementation("io.trino:trino-tpch")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // compileOnly deps also needed at test time
    testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
    testImplementation("io.airlift:slice")
    testImplementation("io.trino:trino-spi")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

// ---- Bundle trino_parity DuckDB extension into the plugin jar ----
//
// The connector requires the trino_parity.duckdb_extension binary to be
// LOAD-able by the in-process DuckDB (later phases). We bundle every
// locally-built platform variant into the plugin jar's classpath resources,
// keyed by platform name. A runtime resolver (later phase) detects the runtime
// platform and extracts the matching binary.
//
// Source paths (one per platform, each independently optional):
//   ../../duckdb-trino-parity-extension/build/release/extension/...    host platform
//                                                                       (output of `make`)
//   ../../duckdb-trino-parity-extension/build/linux-arm64/release/...  output of `make linux-arm64`
//   ../../duckdb-trino-parity-extension/build/linux-amd64/release/...  output of `make linux-amd64`
//
// Bundled to: parity-extension/dev/brikk/duckbridge/trino/plugin/duckdb-extensions/<platform>/trino_parity.duckdb_extension
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

val parityExtensionResourcePrefix =
    "generated-resources/parity-extension/dev/brikk/duckbridge/trino/plugin/duckdb-extensions"

val bundleParityExtension by tasks.registering {
    description = "Copy every available trino_parity.duckdb_extension into the plugin's classpath resources."
    group = "build"
    val outputs = parityExtensionSources.map { (platform, _) ->
        layout.buildDirectory.file("$parityExtensionResourcePrefix/$platform/trino_parity.duckdb_extension")
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
                "$parityExtensionResourcePrefix/$platform/trino_parity.duckdb_extension"
            ).get().asFile
            target.parentFile.mkdirs()
            source.copyTo(target, overwrite = true)
            bundled++
            logger.info("trino_parity: bundled $platform from ${source.relativeToOrSelf(rootProject.projectDir)}")
        }
        if (bundled == 0) {
            logger.lifecycle("trino_parity: NO platform binaries bundled — plugin jar will require the parity-extension path to be set at deploy time.")
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
    useJUnitPlatform {
        (project.findProperty("excludeTags") as String?)
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.takeIf { it.isNotEmpty() }
            ?.let { excludeTags(*it.toTypedArray()) }
    }

    maxHeapSize = "3g"
    minHeapSize = "3g"

    jvmArgs(
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:-OmitStackTraceInFastThrow",
        // Trino's BlockEncodingSimdSupport (via trino-main) needs the incubator vector module.
        "--add-modules=jdk.incubator.vector",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        // Trino's server bootstrap reaches into private java.base internals.
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}

val pluginAssemble by tasks.registering(Copy::class) {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("trino-plugin/trino-duckbridge-$version"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}

tasks.build {
    dependsOn(pluginAssemble)
}
