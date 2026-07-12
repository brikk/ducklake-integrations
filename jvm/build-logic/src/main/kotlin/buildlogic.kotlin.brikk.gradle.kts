plugins {
    id("buildlogic.kotlin.common")
}

// Shared access to brikk's public GitHub Packages Maven registry (repo: brikk/public-maven),
// home of the dev.brikk.house artifacts (e.g. the TEST-ONLY brikk-sql SQL transpiler used by the
// corpus-replay dialect gate). Centralised here so every brikk-consuming module — :trino-ducklake,
// :doris-ducklake, and any future one — shares ONE repository + credentials definition instead of
// copy-pasting the block.
//
// NOTE: GitHub Packages' Maven registry requires a token EVEN FOR PUBLIC packages (anonymous reads
// return 401 — a GitHub limitation, not a brikk setting). The package itself is public, so any
// GitHub token with `read:packages` works. Credentials are read-only and never committed: they come
// from ~/.gradle/gradle.properties (brikk.gpr.user / brikk.gpr.key) or the CI env (GITHUB_ACTOR /
// GITHUB_TOKEN). exclusiveContent pins this authed repo to the dev.brikk.house group ONLY, so it can
// never shadow — nor gate the auth of — any other dependency, which all still resolve unauthed from
// mavenCentral. brikk-sql must never enter a production artifact — declare it testImplementation only.
//
// The dev.brikk.house *dependencies* stay in each module's build.gradle.kts (they differ per module);
// only the repository + credentials wiring lives here.
repositories {
    exclusiveContent {
        forRepository {
            maven {
                name = "brikkPublicMaven"
                url = uri("https://maven.pkg.github.com/brikk/public-maven")
                credentials {
                    username = providers.gradleProperty("brikk.gpr.user").orNull ?: System.getenv("GITHUB_ACTOR")
                    password = providers.gradleProperty("brikk.gpr.key").orNull ?: System.getenv("GITHUB_TOKEN")
                }
            }
        }
        filter { includeGroup("dev.brikk.house") }
    }
}
