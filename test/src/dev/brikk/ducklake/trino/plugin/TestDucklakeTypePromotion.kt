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

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Widening-only promotion rules for `ALTER … SET DATA TYPE`. Widening (and no-op) is allowed;
 * narrowing and incompatible changes are rejected so files written under the old physical type
 * still read losslessly under the new one.
 */
class TestDucklakeTypePromotion {

    @Test
    fun sameTypeIsAllowed() {
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, INTEGER)).isTrue()
    }

    @Test
    fun integerWideningChainAllowed() {
        assertThat(DucklakeTypePromotion.isWidening(TINYINT, SMALLINT)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(SMALLINT, INTEGER)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, BIGINT)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(TINYINT, BIGINT)).isTrue()
    }

    @Test
    fun integerNarrowingRejected() {
        assertThat(DucklakeTypePromotion.isWidening(BIGINT, INTEGER)).isFalse()
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, SMALLINT)).isFalse()
        assertThat(DucklakeTypePromotion.isWidening(SMALLINT, TINYINT)).isFalse()
    }

    @Test
    fun integerToFloatingAllowed() {
        assertThat(DucklakeTypePromotion.isWidening(TINYINT, REAL)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, DOUBLE)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(BIGINT, DOUBLE)).isTrue()
    }

    @Test
    fun realToDoubleAllowedButNotReverse() {
        assertThat(DucklakeTypePromotion.isWidening(REAL, DOUBLE)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(DOUBLE, REAL)).isFalse()
    }

    @Test
    fun timestampToTimestampTzAllowedButNotReverse() {
        val ts = TimestampType.createTimestampType(6)
        val tstz = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6)
        assertThat(DucklakeTypePromotion.isWidening(ts, tstz)).isTrue()
        assertThat(DucklakeTypePromotion.isWidening(tstz, ts)).isFalse()
    }

    @Test
    fun incompatibleChangesRejected() {
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, VARCHAR)).isFalse()
        assertThat(DucklakeTypePromotion.isWidening(INTEGER, BOOLEAN)).isFalse()
        assertThat(DucklakeTypePromotion.isWidening(DOUBLE, INTEGER)).isFalse()
    }
}
