pluginManagement {
    includeBuild("build-logic")
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
