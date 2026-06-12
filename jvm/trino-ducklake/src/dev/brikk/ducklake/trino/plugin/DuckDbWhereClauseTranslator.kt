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

import io.airlift.slice.Slice
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.ArrayList
import java.util.Locale
import java.util.Optional

/**
 * Best-effort translation of a Trino [TupleDomain] into a DuckDB SQL `WHERE`
 * clause for predicate pushdown into the `.db` reader. Anything we don't know how
 * to translate safely is simply omitted — Trino's own filter operator runs on top, so
 * partial pushdown is always correct (just less selective).
 *
 * Phase 1 type coverage matches the reader/writer scalar set: BOOLEAN, integer
 * family, REAL, DOUBLE, DECIMAL, DATE, short-precision TIMESTAMP, VARCHAR. Other types
 * (TIMESTAMPTZ, VARBINARY, UUID, nested) are intentionally skipped here even when
 * supported on the read path; they fall back to Trino-side filtering.
 */
internal object DuckDbWhereClauseTranslator {
    private val TIMESTAMP_MICROS: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT)

    /**
     * Translate a tuple domain into a DuckDB SQL boolean expression. Returns
     * `null` when the domain is "all" (no predicate to push) or when
     * nothing is translatable.
     */
    fun toWhereClause(predicate: TupleDomain<DucklakeColumnHandle>): String? {
        if (predicate.isAll) {
            return null
        }
        if (predicate.isNone) {
            return "FALSE"
        }
        val domainsOpt: Optional<Map<DucklakeColumnHandle, Domain>> = predicate.getDomains()
        if (domainsOpt.isEmpty) {
            return null
        }
        val conjuncts = domainsOpt.get().entries.mapNotNull { translateDomain(it.key, it.value) }
        if (conjuncts.isEmpty()) {
            return null
        }
        return conjuncts.joinToString(" AND ")
    }

    private fun translateDomain(column: DucklakeColumnHandle, domain: Domain): String? {
        val name = quoteIdentifier(column.columnName)
        val type: Type = column.columnType

        if (domain.isAll) {
            return null
        }
        if (domain.isNone) {
            return "FALSE"
        }

        val values: ValueSet = domain.values
        val nullAllowed = domain.isNullAllowed

        if (values.isNone) {
            // Only NULL or nothing.
            return if (nullAllowed) "$name IS NULL" else "FALSE"
        }
        if (values.isAll) {
            // Any non-null value is fine; honor null disposition.
            return if (nullAllowed) "TRUE" else "$name IS NOT NULL"
        }

        val valuesTerm = translateValues(name, type, values) ?: return null
        if (nullAllowed) {
            return "($valuesTerm OR $name IS NULL)"
        }
        return valuesTerm
    }

    private fun translateValues(name: String, type: Type, values: ValueSet): String? {
        if (values.isDiscreteSet) {
            val discrete: List<Any> = values.discreteSet
            val literals: MutableList<String> = ArrayList(discrete.size)
            for (v in discrete) {
                val lit = formatLiteral(type, v) ?: return null
                literals.add(lit)
            }
            if (literals.size == 1) {
                return name + " = " + literals.first()
            }
            return name + " IN (" + literals.joinToString(", ") + ")"
        }

        // Range-based value sets — sortedRangeSet exposes ordered ranges.
        try {
            val ranges: List<Range> = values.ranges.orderedRanges
            val terms: MutableList<String> = ArrayList(ranges.size)
            for (r in ranges) {
                val rangeTerm = formatRange(name, type, r) ?: return null
                terms.add(rangeTerm)
            }
            if (terms.isEmpty()) {
                return null
            }
            if (terms.size == 1) {
                return terms.first()
            }
            return "(" + terms.joinToString(" OR ") + ")"
        }
        catch (e: UnsupportedOperationException) {
            return null
        }
    }

    private fun formatRange(name: String, type: Type, r: Range): String? {
        if (r.isSingleValue) {
            val lit = formatLiteral(type, r.getSingleValue()) ?: return null
            return "$name = $lit"
        }

        val sb = StringBuilder()
        if (!r.isLowUnbounded) {
            val lit = formatLiteral(type, r.lowBoundedValue) ?: return null
            sb.append(name).append(if (r.isLowInclusive) " >= " else " > ").append(lit)
        }
        if (!r.isHighUnbounded) {
            if (sb.isNotEmpty()) {
                sb.append(" AND ")
            }
            val lit = formatLiteral(type, r.highBoundedValue) ?: return null
            sb.append(name).append(if (r.isHighInclusive) " <= " else " < ").append(lit)
        }
        if (sb.isEmpty()) {
            return null
        }
        return "($sb)"
    }

    private fun formatLiteral(type: Type, value: Any?): String? {
        if (value == null) {
            return null
        }
        when (type) {
            BOOLEAN -> return if ((value as Boolean)) "TRUE" else "FALSE"
            TINYINT, SMALLINT, INTEGER, BIGINT -> {
                // Trino stores all integer-family values as long.
                return (value as Long).toString()
            }
            REAL -> {
                // Trino REAL is encoded as the float bits in a long.
                val f = java.lang.Float.intBitsToFloat((value as Long).toInt())
                if (java.lang.Float.isFinite(f)) {
                    return f.toString()
                }
                return null
            }
            DOUBLE -> {
                val d = value as Double
                if (java.lang.Double.isFinite(d)) {
                    return d.toString()
                }
                return null
            }
            is DecimalType -> {
                val decimalType: DecimalType = type
                val bd: BigDecimal
                if (decimalType.isShort) {
                    bd = BigDecimal.valueOf(value as Long, decimalType.scale)
                }
                else {
                    val i128 = value as Int128
                    bd = BigDecimal(BigInteger(i128.toBigEndianBytes()), decimalType.scale)
                }
                return bd.toPlainString()
            }
            DATE -> {
                val days = value as Long
                val date = LocalDate.ofEpochDay(days)
                // DuckDB's DATE literal parser rejects the signed/extended forms LocalDate emits for
                // years <1 (BC, '-') or >9999 ('+'); leave such values unpushed so Trino evaluates them.
                if (date.year !in 1..9999) {
                    return null
                }
                return "DATE '$date'"
            }
            is TimestampType -> {
                if (!type.isShort) {
                    // Long (picosecond-precision) timestamps are never pushed.
                    return null
                }
                val micros = value as Long
                val epochSeconds = floorDiv(micros, 1_000_000L)
                val nanoOfSecond = (floorMod(micros, 1_000_000L) * 1_000L).toInt()
                val ldt = LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneOffset.UTC)
                // TIMESTAMP_MICROS uses the year-of-era pattern `yyyy`, which silently renders a BC
                // year as a positive year (wrong instant, no era marker) and emits a leading '+' past
                // 9999. Leave out-of-range temporal literals unpushed rather than mis-compare.
                if (ldt.year !in 1..9999) {
                    return null
                }
                return "TIMESTAMP '" + TIMESTAMP_MICROS.format(ldt) + "'"
            }
            is VarcharType -> {
                val slice = value as Slice
                return "'" + slice.toStringUtf8().replace("'", "''") + "'"
            }
        }
        return null
    }

    private fun quoteIdentifier(name: String): String =
        '"' + name.replace("\"", "\"\"") + '"'

    fun formatLiteralOrThrow(type: Type, value: Any?): String =
        formatLiteral(type, value)
                ?: throw IllegalArgumentException("Cannot format $type value as DuckDB literal")
}
