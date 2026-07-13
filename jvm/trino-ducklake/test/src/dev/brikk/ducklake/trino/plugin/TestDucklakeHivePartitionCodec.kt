/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins [DucklakeHivePartitionCodec] to DuckDB's `StringUtil::URLEncode(encode_slash=true)` /
 * `URLDecode(plus_to_space=false)` — the escaping DuckLake's `HivePartitioning::Escape`/`Unescape`
 * use for `BuildHivePartitionPath`. If these expectations drift, Trino-written partition paths stop
 * round-tripping through our own read side AND stop matching DuckDB cross-engine.
 */
internal class TestDucklakeHivePartitionCodec {

    @Test
    fun unreservedCharsPassThrough() {
        // A-Z a-z 0-9 _ - ~ . are the only bytes DuckDB leaves verbatim.
        val unreserved = "AZaz09_-~."
        assertThat(DucklakeHivePartitionCodec.encode(unreserved)).isEqualTo(unreserved)
    }

    @Test
    fun encodesSlashPercentSpaceAndEquals() {
        // encode_slash=true -> '/' is escaped; uppercase hex.
        assertThat(DucklakeHivePartitionCodec.encode("a/b")).isEqualTo("a%2Fb")
        assertThat(DucklakeHivePartitionCodec.encode("100%")).isEqualTo("100%25")
        assertThat(DucklakeHivePartitionCodec.encode("a b")).isEqualTo("a%20b")
        assertThat(DucklakeHivePartitionCodec.encode("k=v")).isEqualTo("k%3Dv")
        assertThat(DucklakeHivePartitionCodec.encode("+")).isEqualTo("%2B")
    }

    @Test
    fun encodesNonAsciiPerUtf8Byte() {
        // 'é' = U+00E9 -> UTF-8 C3 A9; '€' = U+20AC -> E2 82 AC.
        assertThat(DucklakeHivePartitionCodec.encode("é")).isEqualTo("%C3%A9")
        assertThat(DucklakeHivePartitionCodec.encode("€")).isEqualTo("%E2%82%AC")
    }

    @Test
    fun decodeIsInverseOfEncode() {
        val samples = listOf(
                "plain",
                "a/b/c",
                "50% off",
                "co=lon",
                "spaces here",
                "unicode é € 日本語",
                "mix/of %stuff=and+things",
                "",
                DucklakeHivePartitionCodec.HIVE_DEFAULT_PARTITION)
        for (s in samples) {
            assertThat(DucklakeHivePartitionCodec.decode(DucklakeHivePartitionCodec.encode(s)))
                    .describedAs("round-trip of %s", s)
                    .isEqualTo(s)
        }
    }

    @Test
    fun decodeLeavesLiteralPlusAndMalformedEscapesAlone() {
        // hive does NOT map '+' to space (unlike form-urlencoding).
        assertThat(DucklakeHivePartitionCodec.decode("a+b")).isEqualTo("a+b")
        // A short/garbage escape passes through verbatim rather than throwing.
        assertThat(DucklakeHivePartitionCodec.decode("%2")).isEqualTo("%2")
        assertThat(DucklakeHivePartitionCodec.decode("%zz")).isEqualTo("%zz")
        // Lowercase hex still decodes (DuckDB accepts either case on decode).
        assertThat(DucklakeHivePartitionCodec.decode("a%2fb")).isEqualTo("a/b")
    }

    @Test
    fun sentinelEncodesToItself() {
        // All chars in the NULL sentinel are unreserved, so a genuine value equal to it is
        // byte-identical to the NULL marker on disk (a hive-format ambiguity we match DuckDB on).
        assertThat(DucklakeHivePartitionCodec.encode(DucklakeHivePartitionCodec.HIVE_DEFAULT_PARTITION))
                .isEqualTo(DucklakeHivePartitionCodec.HIVE_DEFAULT_PARTITION)
    }
}
