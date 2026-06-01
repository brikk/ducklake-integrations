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

/**
 * Carries an add_files validation failure (schema mismatch, missing/extra
 * column, type-incompatibility, hive partition value cast failure). Wrapped
 * to a [io.trino.spi.TrinoException] at the procedure boundary; kept as
 * a plain runtime exception internally so the name-mapper can throw without
 * tying every helper to a Trino error code.
 */
class DucklakeAddFilesException(message: String) : RuntimeException(message)
