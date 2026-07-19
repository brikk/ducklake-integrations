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
import io.trino.spi.connector.SortOrder

/**
 * A resolved write-side sort key: the zero-based page CHANNEL index (its position in
 * [DucklakeWritableTableHandle.columns], which is exactly the block layout the page sink
 * receives) plus the [SortOrder] to apply.
 *
 * The channel — rather than the column name — is stored because that is precisely what
 * [io.trino.spi.PageSorter.sort] consumes, and it stays valid even when the sink appends a
 * trailing synthetic channel (the row-lineage rowid), since sort channels only ever index
 * the leading catalog columns. Resolution (which keys are honored, in what order) is done
 * once in [DucklakeMetadata] via [DucklakeSortPropertyMapper.resolveHonoredPrefix] so the
 * write side sorts by the SAME prefix the read side advertises to the planner.
 */
@JvmRecord
data class DucklakeSortColumn @JsonCreator constructor(
        @get:JvmName("channel")
        @param:JsonProperty("channel") val channel: Int,
        @get:JvmName("sortOrder")
        @param:JsonProperty("sortOrder") val sortOrder: SortOrder)
