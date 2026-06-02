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
package dev.brikk.ducklake.catalog;

/**
 * Engine-agnostic configuration for the Ducklake catalog.
 * Contains only the properties needed by the catalog layer itself (JDBC connection, data path).
 * Engine-specific settings (snapshot defaults, temporal partition encoding, session properties)
 * belong in the engine's own config class.
 */
public class DucklakeCatalogConfig
{
    private String catalogDatabaseUrl;
    private String catalogDatabaseUser;
    private String catalogDatabasePassword;
    private String dataPath;
    private int maxCatalogConnections = 10;

    public String getCatalogDatabaseUrl()
    {
        return catalogDatabaseUrl;
    }

    public DucklakeCatalogConfig setCatalogDatabaseUrl(String catalogDatabaseUrl)
    {
        this.catalogDatabaseUrl = catalogDatabaseUrl;
        return this;
    }

    public String getCatalogDatabaseUser()
    {
        return catalogDatabaseUser;
    }

    public DucklakeCatalogConfig setCatalogDatabaseUser(String catalogDatabaseUser)
    {
        this.catalogDatabaseUser = catalogDatabaseUser;
        return this;
    }

    public String getCatalogDatabasePassword()
    {
        return catalogDatabasePassword;
    }

    public DucklakeCatalogConfig setCatalogDatabasePassword(String catalogDatabasePassword)
    {
        this.catalogDatabasePassword = catalogDatabasePassword;
        return this;
    }

    public String getDataPath()
    {
        return dataPath;
    }

    public DucklakeCatalogConfig setDataPath(String dataPath)
    {
        this.dataPath = dataPath;
        return this;
    }

    public int getMaxCatalogConnections()
    {
        return maxCatalogConnections;
    }

    public DucklakeCatalogConfig setMaxCatalogConnections(int maxCatalogConnections)
    {
        this.maxCatalogConnections = maxCatalogConnections;
        return this;
    }
}
