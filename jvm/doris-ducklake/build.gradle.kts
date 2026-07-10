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

// Main AND tests emit JVM 17 (built on the tree-wide JDK 25 toolchain) — 17 is
// the Doris FE ABI, and running tests at 17 matches how the plugin runs inside
// the FE. The corpus replay module (:ducklake-corpus-replay) also targets 17,
// so the test classpath resolves it with no target-JVM conflict.
//
// Test-classpath ordering: the ~/.m2 fe-thrift (from `mvn install -P flatten`)
// is a FAT jar that also bundles an OLD shaded `org.apache.parquet.format.*`
// (a PageHeader without `setData_page_header`), which shadows the real
// parquet-format-structures-1.17.0 and breaks the inlined-data writer test with
// a NoSuchMethodError. The FE's own fe-thrift (output/fe/lib) is slim (no
// parquet), so this is a fat-m2-jar test artifact, not a runtime problem. The
// test tasks below reorder parquet-format-structures ahead of fe-thrift so the
// real classes win — see prependParquetFormat.

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

    // S3-compatible object-store client for FE-side warehouse blob ops (cleanup_old_files:
    // physically delete files that expire_snapshots scheduled). The connector talks to blob
    // storage itself using the vended s3.* credentials — the sanctioned path, since the FE has no
    // usable Doris FileSystem handle and must NOT assume local-drive access across nodes (the
    // warehouse is S3/MinIO). Bundled child-first in the plugin zip; MinIO's client also drives
    // AWS S3 / OSS / COS / OBS. See dev-docs research on the connector storage abstraction.
    implementation(libs.minio.client)

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
    // parquet-hadoop's ExampleParquetWriter/GroupWriteSupport reference
    // hadoop-mapreduce input/output format classes; FE-supplied at runtime.
    testImplementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.2")

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
        // Host thrift wins. We rely on the FE's fe-thrift (excluded above → parent/host-provided)
        // for the generated TIceberg* types; its runtime uses the host's org.apache.thrift classes.
        // If a future transitive dep pulled libthrift into our runtime it would bundle here and load
        // child-first, giving our code a different org.apache.thrift.TBase than the host's fe-thrift
        // classes use → LinkageError across the SPI boundary. libthrift isn't on our classpath today,
        // so this is defensive (matches fe-connector-iceberg / fe-connector-hive plugin-zip.xml).
        exclude("libthrift-*.jar")
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

// Reorders the real parquet-format-structures ahead of the fat m2 fe-thrift on
// a test classpath, so its (correct) org.apache.parquet.format.* classes win
// over fe-thrift's stale bundled copy. First-on-classpath wins for duplicate
// classes. See the header note above.
fun prependParquetFormat(base: FileCollection): FileCollection {
    val format = base.filter { it.name.startsWith("parquet-format-structures") }
    return format + base
}

tasks.test {
    exclude("**/DorisCorpusReplayTest*")
    classpath = prependParquetFormat(classpath)
}

val corpusReplayTest by tasks.registering(Test::class) {
    description = "Mirrors upstream DuckLake corpus reads through a live compose Doris FE+BE."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = prependParquetFormat(sourceSets.test.get().runtimeClasspath)
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
