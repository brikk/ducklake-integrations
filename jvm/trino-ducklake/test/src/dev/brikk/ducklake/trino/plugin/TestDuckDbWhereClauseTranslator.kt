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

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import io.airlift.slice.Slices
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.SortedRangeSet
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Unit tests for [DuckDbWhereClauseTranslator]. Pure-logic checks that the
 * translator emits valid DuckDB SQL for the predicate shapes it claims to support
 * and silently drops anything else (so partial pushdown stays correct).
 */
class TestDuckDbWhereClauseTranslator {
    private fun col(name: String, type: Type): DucklakeColumnHandle {
        return DucklakeColumnHandle(1L, name, type, true)
    }

    private fun tupleDomain(column: DucklakeColumnHandle, domain: Domain): TupleDomain<DucklakeColumnHandle> {
        return TupleDomain.withColumnDomains(ImmutableMap.of(column, domain))
    }

    @Test
    fun allDomainProducesNoWhere() {
        val where = DuckDbWhereClauseTranslator.toWhereClause(TupleDomain.all<DucklakeColumnHandle>())
        assertThat(where).isEmpty()
    }

    @Test
    fun noneDomainProducesFalse() {
        val where = DuckDbWhereClauseTranslator.toWhereClause(TupleDomain.none<DucklakeColumnHandle>())
        assertThat(where).contains("FALSE")
    }

    @Test
    fun integerEquality() {
        val id = col("id", INTEGER)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(id, Domain.singleValue(INTEGER, 42L)))
        assertThat(where).contains("\"id\" = 42")
    }

    @Test
    fun bigintInList() {
        val id = col("id", BIGINT)
        val domain = Domain.multipleValues(BIGINT, ImmutableList.of(10L, 20L, 30L))
        val where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, domain))
        assertThat(where).contains("\"id\" IN (10, 20, 30)")
    }

    @Test
    fun integerHalfOpenRange() {
        // 10 <= id < 100
        val id = col("id", INTEGER)
        val range = Range.range(INTEGER, 10L, true, 100L, false)
        val domain = Domain.create(ValueSet.ofRanges(range), false)
        val where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, domain))
        assertThat(where).isPresent()
        assertThat(where.get())
            .contains("\"id\" >= 10")
            .contains("\"id\" < 100")
            .contains(" AND ")
    }

    @Test
    fun varcharEqualityEscapesQuotes() {
        val name = col("name", VARCHAR)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(name, Domain.singleValue(VARCHAR, Slices.utf8Slice("o'malley"))))
        assertThat(where).contains("\"name\" = 'o''malley'")
    }

    @Test
    fun dateEquality() {
        val d = col("d", DATE)
        // 2026-05-04 epoch days
        val epochDay = LocalDate.of(2026, 5, 4).toEpochDay()
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(d, Domain.singleValue(DATE, epochDay)))
        assertThat(where).contains("\"d\" = DATE '2026-05-04'")
    }

    @Test
    fun timestampMicrosEquality() {
        val ts = col("ts", TIMESTAMP_MICROS)
        // 2026-05-04 12:34:56.123456 UTC -> micros
        val micros = LocalDateTime.of(2026, 5, 4, 12, 34, 56, 123_456_000)
            .toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 123_456L
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(ts, Domain.singleValue(TIMESTAMP_MICROS, micros)))
        assertThat(where).contains("\"ts\" = TIMESTAMP '2026-05-04 12:34:56.123456'")
    }

    @Test
    fun shortDecimalEquality() {
        val type = DecimalType.createDecimalType(10, 2)
        val amount = col("amount", type)
        // 123.45 unscaled = 12345
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(amount, Domain.singleValue(type, 12345L)))
        assertThat(where).contains("\"amount\" = 123.45")
    }

    @Test
    fun longDecimalEquality() {
        val type = DecimalType.createDecimalType(38, 4)
        val amount = col("amount", type)
        // 9876543210 unscaled with scale 4 → 987654.3210
        val unscaled = Int128.valueOf(BigInteger.valueOf(9_876_543_210L))
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(amount, Domain.singleValue(type, unscaled)))
        assertThat(where).contains("\"amount\" = 987654.3210")
    }

    @Test
    fun onlyNull() {
        val id = col("id", INTEGER)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(id, Domain.onlyNull(INTEGER)))
        assertThat(where).contains("\"id\" IS NULL")
    }

    @Test
    fun notNull() {
        val id = col("id", INTEGER)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(id, Domain.notNull(INTEGER)))
        assertThat(where).contains("\"id\" IS NOT NULL")
    }

    @Test
    fun valueOrNull() {
        val id = col("id", INTEGER)
        val valueOrNull = Domain.create(ValueSet.of(INTEGER, 7L), true)
        val where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, valueOrNull))
        assertThat(where).isPresent()
        assertThat(where.get())
            .contains("\"id\" = 7")
            .contains(" OR ")
            .contains("\"id\" IS NULL")
    }

    @Test
    fun multipleColumnsConjoined() {
        val id = col("id", INTEGER)
        val name = col("name", VARCHAR)
        val td = TupleDomain.withColumnDomains(ImmutableMap.of(
            id, Domain.singleValue(INTEGER, 1L),
            name, Domain.singleValue(VARCHAR, Slices.utf8Slice("alice"))))
        val where = DuckDbWhereClauseTranslator.toWhereClause(td)
        assertThat(where).isPresent()
        // Order of conjuncts is not guaranteed (HashMap iteration); check both fragments present.
        assertThat(where.get())
            .contains("\"id\" = 1")
            .contains("\"name\" = 'alice'")
            .contains(" AND ")
    }

    @Test
    fun booleanEquality() {
        val flag = col("flag", BOOLEAN)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(flag, Domain.singleValue(BOOLEAN, true)))
        assertThat(where).contains("\"flag\" = TRUE")
    }

    @Test
    fun columnNameWithDoubleQuotesEscaped() {
        val weird = col("a\"b", INTEGER)
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(weird, Domain.singleValue(INTEGER, 1L)))
        assertThat(where).contains("\"a\"\"b\" = 1")
    }

    @Test
    fun unionOfRangesProducesOrredTerms() {
        val id = col("id", INTEGER)
        val ranges = ValueSet.ofRanges(
            Range.lessThan(INTEGER, 10L),
            Range.greaterThan(INTEGER, 100L)) as SortedRangeSet
        val where = DuckDbWhereClauseTranslator.toWhereClause(
            tupleDomain(id, Domain.create(ranges, false)))
        assertThat(where).isPresent()
        assertThat(where.get())
            .contains("\"id\" < 10")
            .contains("\"id\" > 100")
            .contains(" OR ")
    }
}
