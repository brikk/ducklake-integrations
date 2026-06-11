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
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProvider
import io.trino.spi.function.FunctionProvider
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.function.table.TableFunctionProcessorProvider

/**
 * Dispatches the engine's "who executes this table function?" lookup to the matching
 * processor provider. Today's table functions are the lance searches
 * (`system.lance_vector_search` / `lance_fts` / `lance_hybrid_search`), all executed by
 * [LanceSearchProcessorProvider].
 */
class DucklakeFunctionProvider @Inject constructor(
        private val executorFactory: DucklakeDuckDbExecutorFactory) : FunctionProvider {

    override fun getTableFunctionProcessorProvider(
            functionHandle: ConnectorTableFunctionHandle): TableFunctionProcessorProvider {
        if (functionHandle is LanceSearchHandle) {
            return ClassLoaderSafeTableFunctionProcessorProvider(
                    LanceSearchProcessorProvider(executorFactory),
                    javaClass.classLoader)
        }
        throw IllegalArgumentException("Unknown table function handle: ${functionHandle.javaClass.name}")
    }
}
