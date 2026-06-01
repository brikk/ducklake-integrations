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
import java.util.Objects

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
class DucklakeTableHandle
        : ConnectorTableHandle {
    private val schemaName: String
    private val tableName: String
    private val tableId: Long
    private val snapshotId: Long
    private val unenforcedPredicate: TupleDomain<DucklakeColumnHandle>
    private val enforcedPredicate: TupleDomain<DucklakeColumnHandle>
    private val pushedExpressions: List<String>

    @JsonCreator
    constructor(
            @JsonProperty("schemaName") schemaName: String,
            @JsonProperty("tableName") tableName: String,
            @JsonProperty("tableId") tableId: Long,
            @JsonProperty("snapshotId") snapshotId: Long,
            @JsonProperty("unenforcedPredicate") unenforcedPredicate: TupleDomain<DucklakeColumnHandle>,
            @JsonProperty("enforcedPredicate") enforcedPredicate: TupleDomain<DucklakeColumnHandle>,
            @JsonProperty("pushedExpressions") pushedExpressions: List<String>?) {
        this.schemaName = Objects.requireNonNull(schemaName, "schemaName is null")
        this.tableName = Objects.requireNonNull(tableName, "tableName is null")
        this.tableId = tableId
        this.snapshotId = snapshotId
        this.unenforcedPredicate = Objects.requireNonNull(unenforcedPredicate, "unenforcedPredicate is null")
        this.enforcedPredicate = Objects.requireNonNull(enforcedPredicate, "enforcedPredicate is null")
        this.pushedExpressions = if (pushedExpressions == null) java.util.List.of() else java.util.List.copyOf(pushedExpressions)
    }

    constructor(
            schemaName: String,
            tableName: String,
            tableId: Long,
            snapshotId: Long,
            unenforcedPredicate: TupleDomain<DucklakeColumnHandle>,
            enforcedPredicate: TupleDomain<DucklakeColumnHandle>)
            : this(schemaName, tableName, tableId, snapshotId, unenforcedPredicate, enforcedPredicate, java.util.List.of())

    constructor(schemaName: String, tableName: String, tableId: Long, snapshotId: Long)
            : this(schemaName, tableName, tableId, snapshotId, TupleDomain.all(), TupleDomain.all(), java.util.List.of())

    @JsonProperty("schemaName")
    fun schemaName(): String = schemaName

    @JsonProperty("tableName")
    fun tableName(): String = tableName

    @JsonProperty("tableId")
    fun tableId(): Long = tableId

    @JsonProperty("snapshotId")
    fun snapshotId(): Long = snapshotId

    @JsonProperty("unenforcedPredicate")
    fun unenforcedPredicate(): TupleDomain<DucklakeColumnHandle> = unenforcedPredicate

    @JsonProperty("enforcedPredicate")
    fun enforcedPredicate(): TupleDomain<DucklakeColumnHandle> = enforcedPredicate

    @JsonProperty("pushedExpressions")
    fun pushedExpressions(): List<String> = pushedExpressions

    fun getSchemaTableName(): SchemaTableName {
        return SchemaTableName(schemaName, tableName)
    }

    override fun toString(): String {
        return schemaName + "." + tableName + "@" + snapshotId
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DucklakeTableHandle) return false
        return schemaName == other.schemaName
                && tableName == other.tableName
                && tableId == other.tableId
                && snapshotId == other.snapshotId
                && unenforcedPredicate == other.unenforcedPredicate
                && enforcedPredicate == other.enforcedPredicate
                && pushedExpressions == other.pushedExpressions
    }

    override fun hashCode(): Int {
        return Objects.hash(
                schemaName,
                tableName,
                tableId,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                pushedExpressions)
    }
}
