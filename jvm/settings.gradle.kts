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
include(":doris-ducklake")
include(":jooq-custom-naming")
