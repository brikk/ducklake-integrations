import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("buildlogic.kotlin.library")
}

version = "0.0.1"

// Doris FE pins to JDK 17, so emit JDK 17 bytecode/ABI — but compile with the project-wide
// JDK 25 toolchain (from buildlogic.kotlin.common). Now that this module is Kotlin, the Kotlin
// compiler emits 17 too (jvmTarget below), so there is NO per-module JDK-17 toolchain anymore —
// the whole tree builds on one toolchain (25). Same approach as :ducklake-catalog.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

// mavenLocal is required ONLY for this module: the Doris fe-connector-api / spi
// artifacts come from a custom branch (PR #62767) installed into ~/.m2. We
// scope mavenLocal to org.apache.doris coordinates via exclusiveContent so it
// can't shadow other dependencies for this module, and so it stays off the
// resolution path for every other module in the build.
repositories {
    exclusiveContent {
        forRepository { mavenLocal() }
        filter { includeGroup("org.apache.doris") }
    }
    mavenCentral()
}

val dorisVersion = "1.2-SNAPSHOT"

dependencies {
    // FE supplies these via the parent classloader at runtime — compile-only.
    compileOnly("org.apache.doris:fe-connector-api:$dorisVersion")
    compileOnly("org.apache.doris:fe-connector-spi:$dorisVersion")
    // fe-thrift is also FE-supplied; we touch it from populateRangeParams to
    // build TIcebergFileDesc per sanity-check §2.1 Option A. Stays compileOnly
    // so the plugin jar does NOT ship a second copy of the thrift classes.
    compileOnly("org.apache.doris:fe-thrift:$dorisVersion")

    implementation(project(":ducklake-catalog"))

    // Bundled in the plugin zip; FE classloader has no Postgres driver of its own.
    runtimeOnly(libs.postgres.jdbc)

    // SPI types are compileOnly above; tests instantiate the plugin so they need them too.
    // (junit/assertj/kotlin-test come from buildlogic.kotlin.common.)
    testImplementation("org.apache.doris:fe-connector-api:$dorisVersion")
    testImplementation("org.apache.doris:fe-connector-spi:$dorisVersion")
    // Parity test serializes TFileRangeDesc with TSerializer, so thrift classes
    // must be on the test runtime classpath.
    testImplementation("org.apache.doris:fe-thrift:$dorisVersion")

    // Shared Testcontainer fixture lives in the catalog's java-test-fixtures source set.
    testImplementation(testFixtures(project(":ducklake-catalog")))
    testImplementation(libs.postgres.jdbc)
    // Used by tests to bootstrap the DuckLake metadata schema in Postgres via DuckDB's
    // postgres + ducklake extensions — same pattern as :ducklake-catalog and :trino-ducklake.
    testImplementation(libs.duckdb.jdbc)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

// Plugin zip mirroring fe-connector-iceberg/src/main/assembly/plugin-zip.xml.
// Produces a flat lib/ layout containing this jar + every runtime dep that
// the FE parent classloader doesn't already supply. The excludes track the
// iceberg reference (api/spi/extension-spi/filesystem-api/logging) plus
// fe-thrift, which is provided by fe-core at runtime.
val pluginZip by tasks.registering(Zip::class) {
    dependsOn(tasks.jar)
    archiveBaseName.set("doris-ducklake")
    archiveClassifier.set("plugin")

    from(tasks.jar) { into("lib") }
    from(configurations.runtimeClasspath) {
        into("lib")
        exclude("fe-connector-api-*.jar")
        exclude("fe-connector-spi-*.jar")
        exclude("fe-extension-spi-*.jar")
        exclude("fe-filesystem-api-*.jar")
        exclude("fe-thrift-*.jar")
        exclude("log4j-api-*.jar")
        exclude("log4j-core-*.jar")
        exclude("log4j-slf4j2-impl-*.jar")
        exclude("log4j-1.2-api-*.jar")
        exclude("slf4j-api-*.jar")
    }
}

tasks.assemble {
    dependsOn(pluginZip)
}
