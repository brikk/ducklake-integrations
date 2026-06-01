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
package dev.brikk.ducklake.catalog

import java.util.Locale
import java.util.OptionalInt
import java.util.regex.Pattern

enum class DucklakePartitionTransform {
    IDENTITY,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    BUCKET,
    ;

    fun isIdentity(): Boolean = this == IDENTITY

    fun isTemporal(): Boolean = this == YEAR || this == MONTH || this == DAY || this == HOUR

    fun isBucket(): Boolean = this == BUCKET

    /**
     * Produce the catalog string form for `ducklake_partition_column.transform`.
     * `identity`/`year`/`month`/`day`/`hour` are lowercase enum names; `BUCKET`
     * is `bucket(N)` with `N` the field's arity.
     */
    fun toCatalogString(arity: OptionalInt): String {
        if (this == BUCKET) {
            require(!arity.isEmpty) { "BUCKET transform requires an arity" }
            return "bucket(" + arity.asInt + ")"
        }
        return name.lowercase(Locale.ENGLISH)
    }

    @JvmRecord
    data class ParsedTransform(val transform: DucklakePartitionTransform, val arity: OptionalInt)

    companion object {
        private val BUCKET_PATTERN: Pattern = Pattern.compile("bucket\\((\\d+)\\)", Pattern.CASE_INSENSITIVE)

        @JvmStatic
        fun fromString(value: String): DucklakePartitionTransform {
            return DucklakePartitionTransform.valueOf(value.uppercase(Locale.ENGLISH))
        }

        /**
         * Parse the catalog-stored transform string from `ducklake_partition_column.transform`.
         * Returns the transform kind plus, for `bucket(N)`, the arity `N`.
         * Simple kinds (`identity`, `year`, `month`, `day`, `hour`) return an empty arity.
         */
        @JvmStatic
        fun parseCatalogTransform(value: String): ParsedTransform {
            val matcher = BUCKET_PATTERN.matcher(value.trim())
            if (matcher.matches()) {
                val arity = matcher.group(1).toInt()
                return ParsedTransform(BUCKET, OptionalInt.of(arity))
            }
            return ParsedTransform(fromString(value), OptionalInt.empty())
        }
    }
}
