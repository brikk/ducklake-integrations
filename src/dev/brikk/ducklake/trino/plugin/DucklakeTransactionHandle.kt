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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.trino.spi.connector.ConnectorTransactionHandle

import java.util.UUID

/**
 * Transaction handle for Ducklake connector.
 * Each transaction maps to a potential new snapshot in the Ducklake catalog.
 */
open class DucklakeTransactionHandle @JsonCreator constructor(@param:JsonProperty("uuid") private val uuid: UUID)
        : ConnectorTransactionHandle
{
    constructor() : this(UUID.randomUUID())

    @JsonProperty
    fun getUuid(): UUID = uuid

    override fun equals(o: Any?): Boolean
    {
        if (this === o) {
            return true
        }
        if (o == null || javaClass != o.javaClass) {
            return false
        }
        val that = o as DucklakeTransactionHandle
        return uuid == that.uuid
    }

    override fun hashCode(): Int = uuid.hashCode()

    override fun toString(): String = uuid.toString()
}
