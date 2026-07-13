package dev.brikk.ducklake.trino.plugin

import java.io.ByteArrayOutputStream

/**
 * Hive/DuckDB-compatible percent-encoding for `key=value/` partition path segments.
 *
 * This is the single source of truth for BOTH directions so the connector's write path
 * (`DucklakePageSink.buildRelativePath`) and read path
 * (`DucklakeSplitManager.parseHivePartitionColumnValues`) can never drift apart, and so a
 * Trino-written partitioned table round-trips through Trino AND matches what DuckDB/DuckLake
 * would have written.
 *
 * The rules mirror DuckDB's `StringUtil::URLEncode(encode_slash = true)` /
 * `URLDecode(plus_to_space = false)`, which DuckLake's `HivePartitioning::Escape` /
 * `Unescape` use to build/parse `BuildHivePartitionPath` segments (see
 * `vendor/ducklake/src/storage/ducklake_partition_data.cpp` and
 * `vendor/duckdb/src/common/string_util.cpp`):
 *
 *  * Unreserved bytes pass through verbatim: `A-Z a-z 0-9 _ - ~ .`
 *  * Every other byte — including `/` (encode_slash) and any non-ASCII UTF-8 byte — becomes
 *    `%XX` with UPPERCASE hex, encoded per-byte over the UTF-8 encoding of the value.
 *  * Decoding reverses `%XX` (case-insensitive hex) back to raw bytes; a literal `+` is left
 *    as-is (hive does NOT use `+` for space); a malformed/short `%` escape passes through
 *    verbatim.
 *
 * Note one inherent hive-format ambiguity we deliberately keep (to match DuckDB): a genuine
 * value whose text equals [HIVE_DEFAULT_PARTITION] encodes to itself (all unreserved chars)
 * and is therefore indistinguishable from a NULL partition on disk — it reads back as NULL.
 */
internal object DucklakeHivePartitionCodec {

    /** DuckDB's sentinel directory value for a NULL hive partition (`col=__HIVE_DEFAULT_PARTITION__/`). */
    const val HIVE_DEFAULT_PARTITION: String = "__HIVE_DEFAULT_PARTITION__"

    private val HEX = "0123456789ABCDEF".toCharArray()

    /** Percent-encode a single path segment (a partition key or value) per the rule above. */
    fun encode(segment: String): String {
        val bytes = segment.toByteArray(Charsets.UTF_8)
        val sb = StringBuilder(bytes.size)
        for (raw in bytes) {
            val b = raw.toInt() and 0xFF
            if (b in 'A'.code..'Z'.code ||
                    b in 'a'.code..'z'.code ||
                    b in '0'.code..'9'.code ||
                    b == '_'.code || b == '-'.code || b == '~'.code || b == '.'.code) {
                sb.append(b.toChar())
            }
            else {
                sb.append('%').append(HEX[b ushr 4]).append(HEX[b and 0x0F])
            }
        }
        return sb.toString()
    }

    /**
     * Reverse [encode]: `%XX` byte escapes are decoded as UTF-8 (spaces are written `%20`,
     * `=` as `%3D`, `/` as `%2F`, etc.). Unlike `java.net.URLDecoder`, a literal `+` is left
     * as-is. Malformed/short escapes pass through verbatim.
     */
    fun decode(value: String): String {
        if (value.indexOf('%') < 0) {
            return value
        }
        val bytes = ByteArrayOutputStream(value.length)
        var i = 0
        while (i < value.length) {
            val c: Char = value[i]
            if (c == '%' && i + 2 < value.length) {
                val hi: Int = Character.digit(value[i + 1], 16)
                val lo: Int = Character.digit(value[i + 2], 16)
                if (hi >= 0 && lo >= 0) {
                    bytes.write((hi shl 4) + lo)
                    i += 3
                    continue
                }
            }
            // Literal character: emit its UTF-8 bytes verbatim (leaves `+` untouched).
            bytes.write(c.toString().toByteArray(Charsets.UTF_8))
            i++
        }
        return String(bytes.toByteArray(), Charsets.UTF_8)
    }
}
