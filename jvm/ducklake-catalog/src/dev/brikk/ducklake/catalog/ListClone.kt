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
 * Defensive copy, spelled with intent. `someList.clone()` reads as "take a private snapshot so a
 * later mutation of the source can't reach into us" — clearer than the bare `.toList()`, which
 * many read as a no-op conversion. Pure delegation to [toList] (an idiomatic rename, not new
 * behavior): the result is a new read-only list that does not reflect later changes to the source.
 */
@Suppress("NOTHING_TO_INLINE") // intentional: inline away the wrapper so `.clone()` is exactly `.toList()`
inline fun <T> List<T>.clone(): List<T> = toList()
