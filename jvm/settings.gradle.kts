pluginManagement {
    includeBuild("build-logic")
}

plugins {
    // Resolves and downloads JDK toolchains on demand (jvmToolchain(25), doris JDK 17)
    // from the Foojay Disco API, so builds don't depend on a matching local JDK install.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "ducklake-jvm"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

// The Trino DuckLake connector. The shared catalog + corpus-replay now come in as published
// dev.brikk.ducklake artifacts (from the ducklake-catalog repo), not as included subprojects.
// The Doris connector moved to its own repo (github.com/brikk/doris-ducklake).
include(":trino-ducklake")
