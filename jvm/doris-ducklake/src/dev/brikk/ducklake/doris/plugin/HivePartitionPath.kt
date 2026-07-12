package dev.brikk.ducklake.doris.plugin

import java.io.ByteArrayOutputStream
import java.util.Locale

/**
 * Parses hive-style `key=value/` partition segments out of a data-file path,
 * for the `add_files(..., hive_partitioning => true)` layout where partition
 * columns live in the PATH and are physically absent from the parquet body
 * (unlike native DuckLake writes, which carry them in the body).
 *
 * Mirrors the trino side's `DucklakeSplitManager.parseHivePartitionsFromPath` /
 * `hiveUrlDecode` / `__HIVE_DEFAULT_PARTITION__` handling byte-for-byte so both
 * engines resolve the same value from the same path.
 */
internal object HivePartitionPath {

    /** DuckDB/hive sentinel for a NULL partition value. */
    const val HIVE_DEFAULT_PARTITION: String = "__HIVE_DEFAULT_PARTITION__"

    /**
     * `key`(lowercased) `-> raw value` for every `key=value` directory segment
     * of [path]. The basename is stripped first so a stray `=` in the filename
     * is never split on. Values are returned raw (URL-decode with [urlDecode]).
     */
    fun parse(path: String): Map<String, String> {
        val normalized = path.replace('\\', '/')
        val lastSlash = normalized.lastIndexOf('/')
        val dirs = if (lastSlash < 0) "" else normalized.substring(0, lastSlash)
        val out = LinkedHashMap<String, String>()
        for (segment in dirs.split('/')) {
            val eq = segment.indexOf('=')
            if (eq > 0 && eq < segment.length - 1) {
                out[segment.substring(0, eq).lowercase(Locale.ROOT)] = segment.substring(eq + 1)
            }
        }
        return out
    }

    /**
     * Hive/DuckDB URL-decode: `%XX` byte escapes decode as UTF-8 (space is
     * `%20`, `=` is `%3D`, etc.). A literal `+` is left as-is (hive does not use
     * `+` for space, unlike `java.net.URLDecoder`); malformed escapes pass
     * through verbatim.
     */
    fun urlDecode(value: String): String {
        if (value.indexOf('%') < 0) {
            return value
        }
        val bytes = ByteArrayOutputStream(value.length)
        var i = 0
        while (i < value.length) {
            val c = value[i]
            if (c == '%' && i + 2 < value.length) {
                val hi = Character.digit(value[i + 1], 16)
                val lo = Character.digit(value[i + 2], 16)
                if (hi >= 0 && lo >= 0) {
                    bytes.write((hi shl 4) + lo)
                    i += 3
                    continue
                }
            }
            bytes.write(c.toString().toByteArray(Charsets.UTF_8))
            i++
        }
        return String(bytes.toByteArray(), Charsets.UTF_8)
    }
}
