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
package dev.brikk.ducklake.catalog

/**
 * Engine-agnostic configuration for the Ducklake catalog.
 * Contains only the properties needed by the catalog layer itself (JDBC connection, data path).
 * Engine-specific settings (snapshot defaults, temporal partition encoding, session properties)
 * belong in the engine's own config class.
 */
class DucklakeCatalogConfig {
    var catalogDatabaseUrl: String? = null
    var catalogDatabaseUser: String? = null
    var catalogDatabasePassword: String? = null
    var dataPath: String? = null
    var maxCatalogConnections: Int = 10
}
