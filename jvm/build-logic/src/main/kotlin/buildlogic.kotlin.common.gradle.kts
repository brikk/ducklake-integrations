import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(25)
}

private val Project.libs: LibrariesForLibs
    get() = extensions.getByType()

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)

    testImplementation(libs.assertj.core)

    testImplementation(libs.kotlin.test.junit) {
        exclude(group = "org.junit")
        exclude(group = "org.junit.jupiter")
        exclude(group = "org.junit.platform")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

sourceSets {
    main {
        kotlin.srcDirs("src")
        kotlin.srcDirs("generated")
        java.srcDirs("src")
        java.srcDirs("generated")
        resources.srcDirs("resources")
    }
    test {
        kotlin.srcDirs("test/src")
        java.srcDirs("test/src")
        resources.srcDirs("test/resources")
    }
}
