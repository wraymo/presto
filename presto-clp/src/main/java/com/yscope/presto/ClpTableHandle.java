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
package com.yscope.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

public class ClpTableHandle
        implements ConnectorTableHandle
{
    private final String tableName;
    private final Optional<String> query;

    @JsonCreator
    public ClpTableHandle(@JsonProperty("tableName") String tableName,
                          @JsonProperty("query") Optional<String> query)
    {
        this.tableName = tableName;
        this.query = query;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClpTableHandle other = (ClpTableHandle) obj;
        return this.tableName.equals(other.tableName);
    }

    @Override
    public String toString()
    {
        return tableName;
    }
}
