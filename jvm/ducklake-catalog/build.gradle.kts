plugins {
    id("buildlogic.kotlin.library")
}

version = "0.0.1"

dependencies {
    implementation(enforcedPlatform(libs.kotlin.bom))
    implementation(libs.jackson.databind)
    implementation(libs.hikari)

    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.postgresql)
}
