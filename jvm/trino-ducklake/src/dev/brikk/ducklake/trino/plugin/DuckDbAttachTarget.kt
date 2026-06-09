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

import java.nio.file.Path
import java.util.Optional

/**
 * What a [DuckDbFilePageSource] reads inside its per-split DuckDB instance.
 *
 * [LocalPath] is the materialize-then-attach path: cache has already pulled
 * the `.db` file to a local filesystem path. [HttpfsS3] is the streaming
 * path: the page source loads DuckDB's httpfs extension, creates an S3 secret, and
 * ATTACHes the `s3://...` URL directly — no full download.
 *
 * [FileScan] is NOT an ATTACH at all: it reads a single non-`.db` file via a DuckDB
 * extension's table function (e.g. `read_vortex('path')`, `lance_scan('path')`). The
 * executor `INSTALL`/`LOAD`s [extension] and uses `FROM <scanFunction>('<path>')` as the
 * query source instead of attaching a database. Used for the `vortex` / `lance` formats.
 */
sealed interface DuckDbAttachTarget {
    @JvmRecord
    data class LocalPath(@get:JvmName("path") val path: Path) : DuckDbAttachTarget

    @JvmRecord
    data class HttpfsS3(
            @get:JvmName("s3Url") val s3Url: String,
            @get:JvmName("s3Config") val s3Config: DuckDbS3Config) : DuckDbAttachTarget

    /**
     * @param path local filesystem path or `s3://...` URL of the file to scan.
     * @param scanFunction the DuckDB table function, e.g. `read_vortex` / `lance_scan`.
     * @param extension the DuckDB extension to INSTALL/LOAD, e.g. `vortex` / `lance`.
     * @param s3Config present iff [path] is an `s3://` URL — then httpfs + a secret are set up.
     */
    @JvmRecord
    data class FileScan(
            @get:JvmName("path") val path: String,
            @get:JvmName("scanFunction") val scanFunction: String,
            @get:JvmName("extension") val extension: String,
            @get:JvmName("s3Config") val s3Config: Optional<DuckDbS3Config>) : DuckDbAttachTarget
}
