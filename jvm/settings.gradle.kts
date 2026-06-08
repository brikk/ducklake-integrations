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

include(":ducklake-catalog")
include(":trino-ducklake")
// :doris-ducklake is temporarily disabled: it depends on Apache Doris
// fe-connector-api/spi/thrift 1.2-SNAPSHOT artifacts (PR #62767) that must be
// installed into ~/.m2, and we're paused on this connector until the Doris SPI
// is more widely available. Re-add the include below to bring it back.
// include(":doris-ducklake")
include(":jooq-custom-naming")
