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
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type

/**
 * Widening-only type-promotion rules for `ALTER TABLE … ALTER COLUMN … SET DATA TYPE`, expressed in
 * Trino types. DuckLake permits only *widening* promotions so that data files written under the old
 * physical type still read correctly (by a lossless-or-widening cast) under the new type; a
 * narrowing change would corrupt existing rows. We mirror that contract on the Trino write side and
 * reject anything else at DDL time (a `NOT_SUPPORTED` in [DucklakeMetadata.setColumnType]).
 *
 * The rules are the Trino projection of DuckLake's promotion set (see
 * `test/sql/alter/promote_type_all.test`): the signed-integer widening chain, integer → floating,
 * `REAL → DOUBLE`, and `TIMESTAMP → TIMESTAMP WITH TIME ZONE`. Unsigned/HUGEINT rungs of DuckDB's
 * chain have no distinct Trino type (they surface as the next-larger signed / decimal), so they are
 * not separately enumerated. When in doubt we are conservative — declining a promotion is safe
 * (the user can rewrite the data); allowing a narrowing is not.
 */
object DucklakeTypePromotion {

    /** True when a column of [source] type may be promoted in place to [target] (widening or no-op). */
    fun isWidening(source: Type, target: Type): Boolean {
        if (source == target) {
            return true
        }
        val sourceRank: Int? = integerRank(source)
        val targetRank: Int? = integerRank(target)
        return when {
            // Integer widening: TINYINT → SMALLINT → INTEGER → BIGINT.
            sourceRank != null && targetRank != null -> targetRank >= sourceRank
            // Integer → floating point (TINYINT → FLOAT → DOUBLE and wider integers → DOUBLE).
            sourceRank != null && (target == REAL || target == DOUBLE) -> true
            // Floating widening.
            source == REAL && target == DOUBLE -> true
            // TIMESTAMP → TIMESTAMP WITH TIME ZONE.
            source is TimestampType && target is TimestampWithTimeZoneType -> true
            else -> false
        }
    }

    private fun integerRank(type: Type): Int? =
        when (type) {
            TINYINT -> 1
            SMALLINT -> 2
            INTEGER -> 3
            BIGINT -> 4
            else -> null
        }
}
