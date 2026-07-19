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

import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.PartitionFieldSpec
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Parsing of {@code partitioned_by} transform expressions. Pins two regex behaviors that
 * diverged from {@code BUCKET_PATTERN}:
 *   - TRANSFORM_PATTERN is case-insensitive, so {@code YEAR(d)} is a temporal transform,
 *     not an identity partition on a column literally named "YEAR(d)".
 *   - the column group rejects parens/commas, so {@code year(foo(bar))} / {@code day(a,b)}
 *     are not swallowed into a transform on a malformed column name.
 */
class TestDucklakePartitionTransformParsing {

    private fun parse(vararg entries: String): List<PartitionFieldSpec> =
        DucklakeTableProperties.getPartitionFields(
            mapOf(DucklakeTableProperties.PARTITIONED_BY_PROPERTY to listOf(*entries)),
        )

    @Test
    fun uppercaseTemporalTransformIsRecognized() {
        val field = parse("YEAR(event_date)").single()
        assertThat(field.transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(field.columnName).isEqualTo("event_date")
    }

    @Test
    fun mixedCaseTemporalTransformIsRecognized() {
        assertThat(parse("Day(d)").single().transform).isEqualTo(DucklakePartitionTransform.DAY)
        assertThat(parse("Month(m)").single().transform).isEqualTo(DucklakePartitionTransform.MONTH)
    }

    @Test
    fun lowercaseTemporalTransformStillWorks() {
        val field = parse("year(event_date)").single()
        assertThat(field.transform).isEqualTo(DucklakePartitionTransform.YEAR)
        assertThat(field.columnName).isEqualTo("event_date")
    }

    @Test
    fun nestedParensAreNotSwallowedIntoTransformColumn() {
        // Must NOT parse as a YEAR transform on column "foo(bar)".
        val field = parse("year(foo(bar))").single()
        assertThat(field.transform).isEqualTo(DucklakePartitionTransform.IDENTITY)
        assertThat(field.columnName).isEqualTo("year(foo(bar))")
    }

    @Test
    fun commasAreNotSwallowedIntoTransformColumn() {
        val field = parse("day(a,b)").single()
        assertThat(field.transform).isEqualTo(DucklakePartitionTransform.IDENTITY)
        assertThat(field.columnName).isEqualTo("day(a,b)")
    }
}
