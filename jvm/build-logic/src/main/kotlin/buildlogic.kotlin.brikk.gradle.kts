plugins {
    id("buildlogic.kotlin.common")
}

// Shared access to the dev.brikk.house artifacts (e.g. the TEST-ONLY brikk-sql SQL transpiler used
// by the corpus-replay dialect gate). Centralised here so every brikk-consuming module —
// :trino-ducklake, :doris-ducklake, and any future one — shares ONE repository definition instead of
// copy-pasting the block.
//
// Source: the Maven Central Portal SNAPSHOTS repository. This is ANONYMOUS (no token/credentials) —
// unlike GitHub Packages' Maven registry, which 401s even for public packages. Central proper only
// hosts releases, so 0.1.0-SNAPSHOT lives in the separate snapshots repo below; snapshotsOnly() keeps
// this repo off the resolution path for any release version. exclusiveContent pins it to the
// dev.brikk.house group ONLY, so it can never shadow another dependency — everything else resolves
// from mavenCentral. brikk-sql must never enter a production artifact — declare it testImplementation
// only.
//
// The dev.brikk.house *dependencies* stay in each module's build.gradle.kts (they differ per module);
// only the repository wiring lives here.
repositories {
    exclusiveContent {
        forRepository {
            maven {
                name = "centralPortalSnapshots"
                url = uri("https://central.sonatype.com/repository/maven-snapshots/")
                mavenContent { snapshotsOnly() }
            }
        }
        filter { includeGroup("dev.brikk.house") }
    }
}
