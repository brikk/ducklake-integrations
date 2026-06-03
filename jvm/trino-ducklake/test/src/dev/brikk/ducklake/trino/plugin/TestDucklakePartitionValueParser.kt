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

import io.trino.spi.type.BooleanType.BOOLEAN
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Identity partition-value parsing. Boolean parsing must accept the "1"/"0" encoding an
 * externally-written (C++ DuckLake) catalog may store — {@code Boolean.parseBoolean} maps
 * "1" to false, which would silently prune the wrong files and project the wrong constant.
 * Anything unrecognized must throw so the callers' catch(RuntimeException) fall back safely.
 */
class TestDucklakePartitionValueParser {

    @Test
    fun booleanAcceptsOneAndZeroEncoding() {
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "1")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "0")).isEqualTo(false)
    }

    @Test
    fun booleanAcceptsTrueFalseCaseInsensitive() {
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "true")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "TRUE")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "False")).isEqualTo(false)
    }

    @Test
    fun booleanRejectsUnrecognizedEncoding() {
        assertThatThrownBy { DucklakePartitionValueParser.parseIdentity(BOOLEAN, "yes") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
