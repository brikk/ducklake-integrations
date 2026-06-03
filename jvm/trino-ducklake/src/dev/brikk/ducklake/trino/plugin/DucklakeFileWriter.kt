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

import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.trino.spi.Page

import java.io.IOException

/**
 * One open data-file writer for the Ducklake page sink. Each implementation owns its
 * underlying file-format machinery (ParquetWriter / DuckDB ATTACH+Appender / future
 * formats) and is responsible for producing a self-describing
 * {@link DucklakeWriteFragment} on close.
 */
interface DucklakeFileWriter
{
    /**
     * Append a Trino page to the underlying file.
     */
    @Throws(IOException::class)
    fun write(page: Page)

    /**
     * Approximate bytes either flushed to the destination or buffered in memory.
     * Used by the sink to decide when to rotate the writer (target file size).
     */
    fun getApproximateWrittenBytes(): Long

    /**
     * Bytes currently retained in memory by this writer (buffered row groups, column
     * writers, etc.). Surfaced to Trino via {@link ConnectorPageSink#getMemoryUsage}
     * so the engine can account for sink-side heap pressure. Defaults to 0 for
     * writers that flush per-page.
     */
    fun getRetainedBytes(): Long = 0L

    /**
     * Close the writer, finalize the data file at its destination, and return the
     * fragment record describing what was written. After this returns the writer
     * must not be used again.
     */
    @Throws(IOException::class)
    fun finishAndBuildFragment(): DucklakeWriteFragment

    /**
     * Best-effort cleanup when the surrounding sink is aborting (failed query,
     * cancellation). Implementations should release native handles, delete partial
     * remote files they created, etc. Must not throw.
     */
    fun abort()
}
