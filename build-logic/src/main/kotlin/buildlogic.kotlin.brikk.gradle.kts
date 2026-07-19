plugins {
    id("buildlogic.kotlin.common")
}

// Shared access to the brikk snapshot artifacts:
//   - dev.brikk.house   — the TEST-ONLY brikk-sql SQL transpiler (corpus-replay dialect gate)
//   - dev.brikk.ducklake — the shared ducklake-catalog + ducklake-test-corpus-replay
//     (github.com/brikk/ducklake-catalog), published as 0.0.1-SNAPSHOT from its main.
// Centralised here so every brikk-consuming module shares ONE repository definition.
//
// RELEASES (e.g. 0.1.0) resolve anonymously from Maven Central (mavenCentral(), declared per-module),
// so no repository is needed here for them. SNAPSHOTS (e.g. 0.1.0-SNAPSHOT) are NOT on Central proper;
// they live in the anonymous Central Portal snapshots repo wired below. We keep it configured so
// flipping a version in libs.versions.toml to a -SNAPSHOT "just works" — no other change needed.
// The repo is scoped both ways so it stays inert for the release path: snapshotsOnly() (never
// consulted for a release version) and includeGroup(...) (never consulted for any other dependency).
// Anonymous — unlike GitHub Packages' Maven registry, which 401s even for public packages.
// brikk-sql must never enter a production artifact — declare it testImplementation only.
//
// The brikk *dependencies* stay in each module's build.gradle.kts (they differ per module);
// only the snapshot-repo wiring lives here.
repositories {
    maven {
        name = "centralPortalSnapshots"
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
        content {
            includeGroup("dev.brikk.house")
            includeGroup("dev.brikk.ducklake")
        }
    }
}
