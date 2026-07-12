package dev.brikk.ducklake.doris.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [HivePartitionPath] — the hive `key=value/` path
 * parser + URL-decoder used to constant-fill body-absent partition columns of
 * hive-layout `add_files` files. Mirrors the trino side's parsing.
 */
internal class HivePartitionPathTest {

    @Test
    fun parsesKeyValueSegmentsAndStripsBasename() {
        val values = HivePartitionPath.parse(
            "s3://bucket/data/part_key=1/part_key2=10/file=abc.parquet",
        )
        // The basename (which contains a stray `=`) is stripped, not split on.
        assertThat(values).containsExactly(
            org.assertj.core.api.Assertions.entry("part_key", "1"),
            org.assertj.core.api.Assertions.entry("part_key2", "10"),
        )
    }

    @Test
    fun keysAreLowercasedForCaseInsensitiveMatch() {
        val values = HivePartitionPath.parse("/data/Region=us/f.parquet")
        assertThat(values).containsKey("region")
        assertThat(values.getValue("region")).isEqualTo("us")
    }

    @Test
    fun ignoresNonPartitionSegmentsAndEmptyValues() {
        val values = HivePartitionPath.parse("/warehouse/tbl/not_a_partition/k=/=v/k2=v2/f.parquet")
        // `k=` (empty value) and `=v` (empty key) are not partition segments.
        assertThat(values).containsExactly(
            org.assertj.core.api.Assertions.entry("k2", "v2"),
        )
    }

    @Test
    fun handlesBackslashPathSeparators() {
        val values = HivePartitionPath.parse("C:\\data\\y=2026\\m=1\\f.parquet")
        assertThat(values).containsExactly(
            org.assertj.core.api.Assertions.entry("y", "2026"),
            org.assertj.core.api.Assertions.entry("m", "1"),
        )
    }

    @Test
    fun urlDecodesPercentEscapesAsUtf8() {
        assertThat(HivePartitionPath.urlDecode("hello%20world")).isEqualTo("hello world")
        assertThat(HivePartitionPath.urlDecode("with%3Dequals")).isEqualTo("with=equals")
        // A literal '+' is left as-is (hive does not use it for space).
        assertThat(HivePartitionPath.urlDecode("a+b")).isEqualTo("a+b")
        // No escapes → returned verbatim (fast path).
        assertThat(HivePartitionPath.urlDecode("plain")).isEqualTo("plain")
        // Malformed escape passes through verbatim.
        assertThat(HivePartitionPath.urlDecode("bad%zz")).isEqualTo("bad%zz")
    }
}
