plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

kotlin {
    jvmToolchain(25)
}

dependencies {
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
    implementation(libs.plugins.kotlin.jvm.toDep())
    implementation(libs.plugins.terpal.sql.toDep())

    implementation(libs.testcontainers.postgresql)
    implementation(libs.duckdb.jdbc)
    implementation(libs.postgres.jdbc)
}

fun Provider<PluginDependency>.toDep() = map {
    "${it.pluginId}:${it.pluginId}.gradle.plugin:${it.version}"
}
