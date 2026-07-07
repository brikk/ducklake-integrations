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
package dev.brikk.ducklake.corpus

/**
 * UPSTREAM's own per-file skips for running the corpus against a PostgreSQL
 * metadata catalog, transcribed from `ducklake/test/configs/postgres.json`
 * (the config their PG CI uses). Any harness that rewrites the oracle's
 * ATTACHes onto PostgreSQL (the engine-mirror backend axis) inherits these —
 * notably the "Postgres Locking" entry, which otherwise HANGS a replay
 * (multi-connection transaction tests deadlock on PG row locks; upstream calls
 * it "not a bug, rather PG behavior").
 *
 * Re-transcribe when the corpus submodule pin is bumped.
 */
object PostgresAxisSkips {

    val SKIPS: Map<String, String> = buildMap {
        val duckdbCatalogOnly = "upstream postgres.json: tests only for DuckDB as a catalog"
        put("general/missing_parquet.test", duckdbCatalogOnly)
        put("general/paths.test", duckdbCatalogOnly)
        put("general/default_path.test", duckdbCatalogOnly)
        put("autoloading/autoload_data_path.test", duckdbCatalogOnly)
        put("general/metadata_parameters.test", duckdbCatalogOnly)
        put("issues/corrupted_catalog_fault_isolation.test", duckdbCatalogOnly)
        put("metadata/ducklake_settings.test", duckdbCatalogOnly)
        put("remove_orphans/metadata_in_data_path.test", duckdbCatalogOnly)

        val connectionString = "upstream postgres.json: modifies connection string"
        put("catalog/quoted_identifiers.test", connectionString)
        put("stats/count_star_optimization_file_operations.test", connectionString)

        val migration = "upstream postgres.json: migration (duckdb-local catalog files)"
        put("migration/v01_partitioned.test", migration)
        put("migration/v01.test", migration)

        put("metadata/ducklake_settings_sqlite.test",
                "upstream postgres.json: test only for SQLite as catalog")
        put("data_inlining/long_column_name_inlining.test",
                "upstream postgres.json: PG 63-byte identifier limit disables inlining for long column names")
        put("transaction/transaction_conflict_inlining.test",
                "upstream postgres.json: Postgres locking (multi-connection txn test deadlocks on PG; " +
                        "'not a bug, rather PG behavior') — HANGS a replay without this skip")
        put("deletion_inlining/test_deletion_inlining_concurrent.test",
                "upstream postgres.json: current transaction is aborted (PG behavior)")
    }
}
