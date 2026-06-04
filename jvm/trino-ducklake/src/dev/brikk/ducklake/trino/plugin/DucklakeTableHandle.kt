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
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.predicate.TupleDomain

/**
 * Handle for a Ducklake table, including snapshot context and pushed-down predicates.
 *
 * <p>{@code pushedExpressions} carries function-shape predicates that
 * {@link DuckDbExpressionTranslator} successfully translated into DuckDB SQL
 * fragments (e.g. {@code trino_lower("name") = 'apple'}). They are AND-ed into
 * the WHERE clause sent to DuckDB on the {@code .db} read path. For mixed-format
 * tables, the same conjuncts are also returned in {@code remainingExpression}
 * so Trino re-applies them above the scan — double evaluation on {@code .db}
 * splits is cheap because the result set is already reduced.
 */
@JvmRecord
data class DucklakeTableHandle @JsonCreator constructor(
        @get:JvmName("schemaName")
        @param:JsonProperty("schemaName") val schemaName: String,
        @get:JvmName("tableName")
        @param:JsonProperty("tableName") val tableName: String,
        @get:JvmName("tableId")
        @param:JsonProperty("tableId") val tableId: Long,
        @get:JvmName("snapshotId")
        @param:JsonProperty("snapshotId") val snapshotId: Long,
        @get:JvmName("unenforcedPredicate")
        @param:JsonProperty("unenforcedPredicate") val unenforcedPredicate: TupleDomain<DucklakeColumnHandle>,
        @get:JvmName("enforcedPredicate")
        @param:JsonProperty("enforcedPredicate") val enforcedPredicate: TupleDomain<DucklakeColumnHandle>,
        @get:JvmName("pushedExpressions")
        @param:JsonProperty("pushedExpressions") val pushedExpressions: List<String>)
        : ConnectorTableHandle
{
    constructor(
            schemaName: String,
            tableName: String,
            tableId: Long,
            snapshotId: Long,
            unenforcedPredicate: TupleDomain<DucklakeColumnHandle>,
            enforcedPredicate: TupleDomain<DucklakeColumnHandle>)
            : this(schemaName, tableName, tableId, snapshotId, unenforcedPredicate, enforcedPredicate, emptyList())

    constructor(schemaName: String, tableName: String, tableId: Long, snapshotId: Long)
            : this(schemaName, tableName, tableId, snapshotId, TupleDomain.all(), TupleDomain.all(), emptyList())

    fun getSchemaTableName(): SchemaTableName {
        return SchemaTableName(schemaName, tableName)
    }

    override fun toString(): String {
        return "$schemaName.$tableName@$snapshotId"
    }
}
