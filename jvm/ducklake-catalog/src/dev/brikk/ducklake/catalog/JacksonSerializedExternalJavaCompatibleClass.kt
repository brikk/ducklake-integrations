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
 * Marks a class Jackson-serialized into a wire/persistence contract consumed by ANOTHER system,
 * or a persisted form that outlives a code version. Treat the JSON shape as FROZEN — field names,
 * nullability, and presence semantics are an external API.
 *
 * "JavaCompatible": like the internal marker, assume the reader is a module-less airlift-style
 * `ObjectMapper` (no `jackson-module-kotlin`). Keep the type Java-record-shaped — `@JvmRecord`,
 * no reliance on Kotlin-only deserialization — so it round-trips on the bare
 * `RecordAutoDetectModule` + `ParameterNamesModule`. Nothing carries this yet (reserved); the day
 * a type genuinely crosses a system boundary, mark it here and freeze its shape.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class JacksonSerializedExternalJavaCompatibleClass
