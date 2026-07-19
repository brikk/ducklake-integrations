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

import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog

/**
 * `TABLE(<catalog>.system.table_insertions(schema, table, ...))` — rows inserted in the snapshot
 * window, prefixed with `snapshot_id` + `rowid`. See [AbstractChangeFeedTableFunction].
 */
class TableInsertionsTableFunction @Inject constructor(
        catalog: DucklakeCatalog,
        typeConverter: DucklakeTypeConverter,
) : AbstractChangeFeedTableFunction(catalog, typeConverter, ChangeFeedType.INSERTIONS)

/**
 * `TABLE(<catalog>.system.table_deletions(schema, table, ...))` — rows deleted in the snapshot
 * window, prefixed with `snapshot_id` + `rowid`. See [AbstractChangeFeedTableFunction].
 */
class TableDeletionsTableFunction @Inject constructor(
        catalog: DucklakeCatalog,
        typeConverter: DucklakeTypeConverter,
) : AbstractChangeFeedTableFunction(catalog, typeConverter, ChangeFeedType.DELETIONS)

/**
 * `TABLE(<catalog>.system.table_changes(schema, table, ...))` — inserts and deletes in the
 * snapshot window, prefixed with `snapshot_id` + `rowid` + `change_type` (`insert`, `delete`,
 * `update_preimage`, `update_postimage`). Lineage-preserving UPDATEs (DuckDB / compaction, which
 * embed the preserved rowid) pair into `update_preimage`/`update_postimage`; this connector's own
 * UPDATE/MERGE writes emit no lineage column, so a Trino-written UPDATE surfaces as `delete` +
 * `insert`. See [AbstractChangeFeedTableFunction] and [ChangeFeedUnit].
 */
class TableChangesTableFunction @Inject constructor(
        catalog: DucklakeCatalog,
        typeConverter: DucklakeTypeConverter,
) : AbstractChangeFeedTableFunction(catalog, typeConverter, ChangeFeedType.CHANGES)
