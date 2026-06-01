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
package dev.brikk.ducklake.trino.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DuckDbWhereClauseTranslator}. Pure-logic checks that the
 * translator emits valid DuckDB SQL for the predicate shapes it claims to support
 * and silently drops anything else (so partial pushdown stays correct).
 */
public class TestDuckDbWhereClauseTranslator
{
    private static DucklakeColumnHandle col(String name, Type type)
    {
        return new DucklakeColumnHandle(1L, name, type, true);
    }

    private static TupleDomain<DucklakeColumnHandle> tupleDomain(DucklakeColumnHandle column, Domain domain)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(column, domain));
    }

    @Test
    void allDomainProducesNoWhere()
    {
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(TupleDomain.all());
        assertThat(where).isEmpty();
    }

    @Test
    void noneDomainProducesFalse()
    {
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(TupleDomain.none());
        assertThat(where).contains("FALSE");
    }

    @Test
    void integerEquality()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(id, Domain.singleValue(INTEGER, 42L)));
        assertThat(where).contains("\"id\" = 42");
    }

    @Test
    void bigintInList()
    {
        DucklakeColumnHandle id = col("id", BIGINT);
        Domain domain = Domain.multipleValues(BIGINT, ImmutableList.of(10L, 20L, 30L));
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, domain));
        assertThat(where).contains("\"id\" IN (10, 20, 30)");
    }

    @Test
    void integerHalfOpenRange()
    {
        // 10 <= id < 100
        DucklakeColumnHandle id = col("id", INTEGER);
        Range range = Range.range(INTEGER, 10L, true, 100L, false);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, domain));
        assertThat(where).isPresent();
        assertThat(where.get())
                .contains("\"id\" >= 10")
                .contains("\"id\" < 100")
                .contains(" AND ");
    }

    @Test
    void varcharEqualityEscapesQuotes()
    {
        DucklakeColumnHandle name = col("name", VARCHAR);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(name, Domain.singleValue(VARCHAR, Slices.utf8Slice("o'malley"))));
        assertThat(where).contains("\"name\" = 'o''malley'");
    }

    @Test
    void dateEquality()
    {
        DucklakeColumnHandle d = col("d", DATE);
        // 2026-05-04 epoch days
        long epochDay = java.time.LocalDate.of(2026, 5, 4).toEpochDay();
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(d, Domain.singleValue(DATE, epochDay)));
        assertThat(where).contains("\"d\" = DATE '2026-05-04'");
    }

    @Test
    void timestampMicrosEquality()
    {
        DucklakeColumnHandle ts = col("ts", TIMESTAMP_MICROS);
        // 2026-05-04 12:34:56.123456 UTC -> micros
        long micros = java.time.LocalDateTime.of(2026, 5, 4, 12, 34, 56, 123_456_000)
                .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L + 123_456L;
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(ts, Domain.singleValue(TIMESTAMP_MICROS, micros)));
        assertThat(where).contains("\"ts\" = TIMESTAMP '2026-05-04 12:34:56.123456'");
    }

    @Test
    void shortDecimalEquality()
    {
        DecimalType type = DecimalType.createDecimalType(10, 2);
        DucklakeColumnHandle amount = col("amount", type);
        // 123.45 unscaled = 12345
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(amount, Domain.singleValue(type, 12345L)));
        assertThat(where).contains("\"amount\" = 123.45");
    }

    @Test
    void longDecimalEquality()
    {
        DecimalType type = DecimalType.createDecimalType(38, 4);
        DucklakeColumnHandle amount = col("amount", type);
        // 9876543210 unscaled with scale 4 → 987654.3210
        Int128 unscaled = Int128.valueOf(java.math.BigInteger.valueOf(9_876_543_210L));
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(amount, Domain.singleValue(type, unscaled)));
        assertThat(where).contains("\"amount\" = 987654.3210");
    }

    @Test
    void onlyNull()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(id, Domain.onlyNull(INTEGER)));
        assertThat(where).contains("\"id\" IS NULL");
    }

    @Test
    void notNull()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(id, Domain.notNull(INTEGER)));
        assertThat(where).contains("\"id\" IS NOT NULL");
    }

    @Test
    void valueOrNull()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        Domain valueOrNull = Domain.create(ValueSet.of(INTEGER, 7L), true);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(tupleDomain(id, valueOrNull));
        assertThat(where).isPresent();
        assertThat(where.get())
                .contains("\"id\" = 7")
                .contains(" OR ")
                .contains("\"id\" IS NULL");
    }

    @Test
    void multipleColumnsConjoined()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        DucklakeColumnHandle name = col("name", VARCHAR);
        TupleDomain<DucklakeColumnHandle> td = TupleDomain.withColumnDomains(ImmutableMap.of(
                id, Domain.singleValue(INTEGER, 1L),
                name, Domain.singleValue(VARCHAR, Slices.utf8Slice("alice"))));
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(td);
        assertThat(where).isPresent();
        // Order of conjuncts is not guaranteed (HashMap iteration); check both fragments present.
        assertThat(where.get())
                .contains("\"id\" = 1")
                .contains("\"name\" = 'alice'")
                .contains(" AND ");
    }

    @Test
    void booleanEquality()
    {
        DucklakeColumnHandle flag = col("flag", BOOLEAN);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(flag, Domain.singleValue(BOOLEAN, true)));
        assertThat(where).contains("\"flag\" = TRUE");
    }

    @Test
    void columnNameWithDoubleQuotesEscaped()
    {
        DucklakeColumnHandle weird = col("a\"b", INTEGER);
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(weird, Domain.singleValue(INTEGER, 1L)));
        assertThat(where).contains("\"a\"\"b\" = 1");
    }

    @Test
    void unionOfRangesProducesOrredTerms()
    {
        DucklakeColumnHandle id = col("id", INTEGER);
        SortedRangeSet ranges = (SortedRangeSet) ValueSet.ofRanges(
                Range.lessThan(INTEGER, 10L),
                Range.greaterThan(INTEGER, 100L));
        Optional<String> where = DuckDbWhereClauseTranslator.toWhereClause(
                tupleDomain(id, Domain.create(ranges, false)));
        assertThat(where).isPresent();
        assertThat(where.get())
                .contains("\"id\" < 10")
                .contains("\"id\" > 100")
                .contains(" OR ");
    }
}
