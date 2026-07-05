plugins {
    id("buildlogic.kotlin.library")
}

version = "0.0.1"

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
