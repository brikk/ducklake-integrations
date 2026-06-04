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
public object DuckDbWhereClauseTranslator {
    private val TIMESTAMP_MICROS: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT)

    /**
     * Translate a tuple domain into a DuckDB SQL boolean expression. Returns
     * [Optional.empty] when the domain is "all" (no predicate to push) or when
     * nothing is translatable.
     */
    public fun toWhereClause(predicate: TupleDomain<DucklakeColumnHandle>): Optional<String> {
        if (predicate.isAll()) {
            return Optional.empty()
        }
        if (predicate.isNone()) {
            return Optional.of("FALSE")
        }
        val domainsOpt: Optional<Map<DucklakeColumnHandle, Domain>> = predicate.getDomains()
        if (domainsOpt.isEmpty) {
            return Optional.empty()
        }
        val conjuncts = domainsOpt.get().entries.mapNotNull { translateDomain(it.key, it.value).orElse(null) }
        if (conjuncts.isEmpty()) {
            return Optional.empty()
        }
        return Optional.of(conjuncts.joinToString(" AND "))
    }

    private fun translateDomain(column: DucklakeColumnHandle, domain: Domain): Optional<String> {
        val name = quoteIdentifier(column.columnName)
        val type: Type = column.columnType

        if (domain.isAll()) {
            return Optional.empty()
        }
        if (domain.isNone()) {
            return Optional.of("FALSE")
        }

        val values: ValueSet = domain.getValues()
        val nullAllowed = domain.isNullAllowed()

        if (values.isNone()) {
            // Only NULL or nothing.
            return Optional.of(if (nullAllowed) name + " IS NULL" else "FALSE")
        }
        if (values.isAll()) {
            // Any non-null value is fine; honor null disposition.
            return Optional.of(if (nullAllowed) "TRUE" else name + " IS NOT NULL")
        }

        val valuesTerm = translateValues(name, type, values)
        if (valuesTerm.isEmpty) {
            return Optional.empty()
        }
        if (nullAllowed) {
            return Optional.of("(" + valuesTerm.get() + " OR " + name + " IS NULL)")
        }
        return valuesTerm
    }

    private fun translateValues(name: String, type: Type, values: ValueSet): Optional<String> {
        if (values.isDiscreteSet()) {
            val discrete: List<Any> = values.getDiscreteSet()
            val literals: MutableList<String> = ArrayList(discrete.size)
            for (v in discrete) {
                val lit = formatLiteral(type, v)
                if (lit.isEmpty) {
                    return Optional.empty()
                }
                literals.add(lit.get())
            }
            if (literals.size == 1) {
                return Optional.of(name + " = " + literals.first())
            }
            return Optional.of(name + " IN (" + literals.joinToString(", ") + ")")
        }

        // Range-based value sets — sortedRangeSet exposes ordered ranges.
        try {
            val ranges: List<Range> = values.getRanges().getOrderedRanges()
            val terms: MutableList<String> = ArrayList(ranges.size)
            for (r in ranges) {
                val rangeTerm = formatRange(name, type, r)
                if (rangeTerm.isEmpty) {
                    return Optional.empty()
                }
                terms.add(rangeTerm.get())
            }
            if (terms.isEmpty()) {
                return Optional.empty()
            }
            if (terms.size == 1) {
                return Optional.of(terms.first())
            }
            return Optional.of("(" + terms.joinToString(" OR ") + ")")
        }
        catch (e: UnsupportedOperationException) {
            return Optional.empty()
        }
    }

    private fun formatRange(name: String, type: Type, r: Range): Optional<String> {
        if (r.isSingleValue()) {
            val lit = formatLiteral(type, r.getSingleValue())
            return lit.map { l -> name + " = " + l }
        }

        val sb = StringBuilder()
        if (!r.isLowUnbounded()) {
            val lit = formatLiteral(type, r.getLowBoundedValue())
            if (lit.isEmpty) {
                return Optional.empty()
            }
            sb.append(name).append(if (r.isLowInclusive()) " >= " else " > ").append(lit.get())
        }
        if (!r.isHighUnbounded()) {
            if (sb.length > 0) {
                sb.append(" AND ")
            }
            val lit = formatLiteral(type, r.getHighBoundedValue())
            if (lit.isEmpty) {
                return Optional.empty()
            }
            sb.append(name).append(if (r.isHighInclusive()) " <= " else " < ").append(lit.get())
        }
        if (sb.length == 0) {
            return Optional.empty()
        }
        return Optional.of("(" + sb + ")")
    }

    private fun formatLiteral(type: Type, value: Any?): Optional<String> {
        if (value == null) {
            return Optional.empty()
        }
        if (type.equals(BOOLEAN)) {
            return Optional.of(if ((value as Boolean)) "TRUE" else "FALSE")
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            // Trino stores all integer-family values as long.
            return Optional.of((value as Long).toString())
        }
        if (type.equals(REAL)) {
            // Trino REAL is encoded as the float bits in a long.
            val f = java.lang.Float.intBitsToFloat((value as Long).toInt())
            if (java.lang.Float.isFinite(f)) {
                return Optional.of(java.lang.Float.toString(f))
            }
            return Optional.empty()
        }
        if (type.equals(DOUBLE)) {
            val d = value as Double
            if (java.lang.Double.isFinite(d)) {
                return Optional.of(java.lang.Double.toString(d))
            }
            return Optional.empty()
        }
        if (type is DecimalType) {
            val decimalType: DecimalType = type
            val bd: BigDecimal
            if (decimalType.isShort()) {
                bd = BigDecimal.valueOf(value as Long, decimalType.getScale())
            }
            else {
                val i128 = value as Int128
                bd = BigDecimal(BigInteger(i128.toBigEndianBytes()), decimalType.getScale())
            }
            return Optional.of(bd.toPlainString())
        }
        if (type.equals(DATE)) {
            val days = value as Long
            val date = LocalDate.ofEpochDay(days)
            // DuckDB's DATE literal parser rejects the signed/extended forms LocalDate emits for
            // years <1 (BC, '-') or >9999 ('+'); leave such values unpushed so Trino evaluates them.
            if (date.year < 1 || date.year > 9999) {
                return Optional.empty()
            }
            return Optional.of("DATE '" + date + "'")
        }
        if (type is TimestampType && type.isShort()) {
            val micros = value as Long
            val epochSeconds = floorDiv(micros, 1_000_000L)
            val nanoOfSecond = (floorMod(micros, 1_000_000L) * 1_000L).toInt()
            val ldt = LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneOffset.UTC)
            // TIMESTAMP_MICROS uses the year-of-era pattern `yyyy`, which silently renders a BC
            // year as a positive year (wrong instant, no era marker) and emits a leading '+' past
            // 9999. Leave out-of-range temporal literals unpushed rather than mis-compare.
            if (ldt.year < 1 || ldt.year > 9999) {
                return Optional.empty()
            }
            return Optional.of("TIMESTAMP '" + TIMESTAMP_MICROS.format(ldt) + "'")
        }
        if (type is VarcharType) {
            val slice = value as Slice
            return Optional.of("'" + slice.toStringUtf8().replace("'", "''") + "'")
        }
        return Optional.empty()
    }

    private fun quoteIdentifier(name: String): String {
        return '"' + name.replace("\"", "\"\"") + '"'
    }

    public fun formatLiteralOrThrow(type: Type, value: Any?): String {
        return formatLiteral(type, value)
                .orElseThrow { IllegalArgumentException("Cannot format $type value as DuckDB literal") }
    }
}
