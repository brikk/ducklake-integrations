plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.plugin.serialization) apply false
    alias(libs.plugins.terpal.sql) apply false
}

group = "dev.brikk.ducklake"
version = "0.0.1"
