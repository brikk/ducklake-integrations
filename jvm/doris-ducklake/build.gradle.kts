import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("buildlogic.kotlin.library")
    alias(libs.plugins.detekt)
}

version = "0.0.1"

// Idiomatic-Kotlin quality gate (detekt 2.0; jvmTarget read from Kotlin compilerOptions), same as
// :ducklake-catalog / :trino-ducklake. Custom src + test/src layout; Doris SPI is compileOnly.
detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    baseline = file("detekt-baseline.xml")
    source.setFrom("src", "test/src")
}

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

// TESTS compile at the toolchain's 25 (they already RUN on 25): the corpus
// replay module (:ducklake-corpus-replay) is a JVM-25 library, and only test
// code touches it. Main stays strictly 17 — that's the FE ABI. The classpath
// attributes must agree or Gradle refuses to resolve the corpus project.
tasks.named<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>("compileTestKotlin") {
    compilerOptions.jvmTarget.set(JvmTarget.JVM_25)
}
tasks.named<JavaCompile>("compileTestJava") {
    sourceCompatibility = JavaVersion.VERSION_25.toString()
    targetCompatibility = JavaVersion.VERSION_25.toString()
}
configurations.testCompileClasspath {
    attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 25)
}
configurations.testRuntimeClasspath {
    attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 25)
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
    // Iceberg schema serialization for the write-sink schema_json (SchemaParser,
    // mirroring native IcebergTableSink). FE-supplied at runtime via the iceberg
    // connector libs (like fe-thrift) — compileOnly, version-matched to FE (1.10.1).
    compileOnly("org.apache.iceberg:iceberg-api:1.10.1")
    compileOnly("org.apache.iceberg:iceberg-core:1.10.1")
    // Inlined-data reads (DuckLakeInlinedParquetWriter): the FE synthesizes a
    // temp Parquet from catalog-inlined rows so the BE can scan them. Written
    // with parquet-avro (field_id-carrying), which is FE-supplied at runtime
    // (output/fe/lib) — versions matched to the FE. compileOnly so the plugin
    // jar ships no second copy of parquet/avro/hadoop.
    compileOnly("org.apache.parquet:parquet-avro:1.17.0")
    compileOnly("org.apache.parquet:parquet-hadoop:1.17.0")
    compileOnly("org.apache.parquet:parquet-column:1.17.0")
    compileOnly("org.apache.avro:avro:1.12.1")
    compileOnly("org.apache.hadoop:hadoop-common:3.4.2")

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
    // Write-sink schema tests round-trip schema_json through iceberg's own
    // SchemaParser (the independent oracle), so iceberg is needed at test runtime.
    testImplementation("org.apache.iceberg:iceberg-api:1.10.1")
    testImplementation("org.apache.iceberg:iceberg-core:1.10.1")
    // Inlined-data writer tests read the written Parquet back to assert
    // field_ids/types/values, so parquet-avro + hadoop are on the test runtime.
    testImplementation("org.apache.parquet:parquet-avro:1.17.0")
    testImplementation("org.apache.parquet:parquet-hadoop:1.17.0")
    testImplementation("org.apache.parquet:parquet-column:1.17.0")
    testImplementation("org.apache.avro:avro:1.12.1")
    testImplementation("org.apache.hadoop:hadoop-common:3.4.2")

    // Shared Testcontainer fixture lives in the catalog's java-test-fixtures source set.
    testImplementation(testFixtures(project(":ducklake-catalog")))
    testImplementation(libs.postgres.jdbc)
    // Used by tests to bootstrap the DuckLake metadata schema in Postgres via DuckDB's
    // postgres + ducklake extensions — same pattern as :ducklake-catalog and :trino-ducklake.
    testImplementation(libs.duckdb.jdbc)

    // Corpus replay adapter (DorisReplayEngine): the runner seam + the mysql-protocol
    // driver the adapter uses to reach the live compose FE. Test-only; the corpus
    // test itself runs via the dedicated :doris-ducklake:corpusReplayTest task.
    testImplementation(project(":ducklake-corpus-replay"))
    testRuntimeOnly("com.mysql:mysql-connector-j:9.5.0")
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

// ---- Corpus replay (dev-docs/DESIGN-corpus-replay-adapter.md) ----
// The corpus mirror needs a LIVE compose cluster (`compose/smoke.sh --up-only`)
// and writes oracle data files under a host dir bind-mounted into the BE at
// the SAME absolute path (compose mounts ${DORIS_CORPUS_DIR:-/tmp/ducklake-corpus}).
// java.io.tmpdir is pinned there so DuckDbOracle's per-file temp dirs land
// inside the mount — that's how a host-side oracle and a containerized BE read
// the same lake. Kept OUT of the normal `test` task (no cluster in CI).
val corpusTmpDir = providers.gradleProperty("dorisCorpusDir").orElse("/tmp/ducklake-corpus")

tasks.test {
    exclude("**/DorisCorpusReplayTest*")
}

val corpusReplayTest by tasks.registering(Test::class) {
    description = "Mirrors upstream DuckLake corpus reads through a live compose Doris FE+BE."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    include("**/DorisCorpusReplayTest*")
    maxHeapSize = "2g"
    systemProperty("java.io.tmpdir", corpusTmpDir.get())
    systemProperty(
        "ducklake.corpus.root",
        rootProject.projectDir.resolve("ducklake-corpus-replay/ducklake/test/sql").absolutePath,
    )
    // Directory selection mirrors the runner module: starter dirs by default,
    // "-Dducklake.corpus.dirs=all" for the full corpus.
    System.getProperty("ducklake.corpus.dirs")?.let { systemProperty("ducklake.corpus.dirs", it) }
    outputs.upToDateWhen { false } // always re-run against the live cluster
}
