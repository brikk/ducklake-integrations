import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("buildlogic.kotlin.library")
    alias(libs.plugins.detekt)
}

version = "0.0.1"

// Emit JVM 17 bytecode (built on the tree-wide JDK 25 toolchain). Nothing here
// needs a Java 18+ API — only duckdb-jdbc + kotlin stdlib — and targeting 17
// lets the JVM-17 consumers (the doris-ducklake plugin's tests, which must
// match the JDK-17 Doris FE runtime) depend on this module without a
// target-JVM-version conflict. Trino-ducklake (JVM 25) consumes 17 bytecode
// fine. This keeps doris-ducklake a single consistent 17 toolchain and avoids
// the JDK-25 parquet-format shaded-thrift ABI break in its tests.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    source.setFrom("src", "test")
}

// Replays the upstream DuckLake sqllogictest corpus (git submodule at ./ducklake,
// pinned to the release branch matching our DuckDB version) through an embedded
// DuckDB oracle, and optionally mirrors lake reads through a ReplayReadEngine
// (Trino / Doris adapters live in those modules, not here — this module stays
// engine-agnostic).
dependencies {
    implementation(enforcedPlatform(libs.kotlin.bom))
    implementation(libs.duckdb.jdbc)
}

tasks.withType<Test> {
    // Identity-control runs INSTALL/LOAD ducklake (network on first run, cached
    // in ~/.duckdb afterwards) and executes several thousand corpus records.
    maxHeapSize = "2g"
    // Corpus location for tests; overridable for CI layouts.
    systemProperty("ducklake.corpus.root", layout.projectDirectory.dir("ducklake/test/sql").asFile.absolutePath)
    // Directory selection: default starter set; "-Dducklake.corpus.dirs=all" for the full corpus.
    System.getProperty("ducklake.corpus.dirs")?.let { systemProperty("ducklake.corpus.dirs", it) }
}
