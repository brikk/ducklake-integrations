plugins {
    id("buildlogic.kotlin.common")
}

// Shared access to the dev.brikk.house artifacts (e.g. the TEST-ONLY brikk-sql SQL transpiler used
// by the corpus-replay dialect gate). Centralised here so every brikk-consuming module —
// :trino-ducklake, :doris-ducklake, and any future one — shares ONE repository definition.
//
// RELEASES (e.g. 0.1.0) resolve anonymously from Maven Central (mavenCentral(), declared per-module),
// so no repository is needed here for them. SNAPSHOTS (e.g. 0.1.0-SNAPSHOT) are NOT on Central proper;
// they live in the anonymous Central Portal snapshots repo wired below. We keep it configured so
// flipping the `brikk-sql` version in libs.versions.toml back to a -SNAPSHOT "just works" — no other
// change needed. The repo is scoped both ways so it stays inert for the release path: snapshotsOnly()
// (never consulted for a release version) and includeGroup("dev.brikk.house") (never consulted for any
// other dependency). Anonymous — unlike GitHub Packages' Maven registry, which 401s even for public
// packages. brikk-sql must never enter a production artifact — declare it testImplementation only.
//
// The dev.brikk.house *dependencies* stay in each module's build.gradle.kts (they differ per module);
// only the snapshot-repo wiring lives here.
repositories {
    maven {
        name = "centralPortalSnapshots"
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
        content { includeGroup("dev.brikk.house") }
    }
}
