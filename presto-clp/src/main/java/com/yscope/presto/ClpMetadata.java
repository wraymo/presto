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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClpMetadata
        implements ConnectorMetadata
{
    private final ClpClient clpClient;

    @Inject
    public ClpMetadata(ClpClient clpClient)
    {
        this.clpClient = clpClient;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("default");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String tableName : clpClient.listTables()) {
            builder.add(new SchemaTableName("default", tableName));
        }
        return builder.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        if (!clpClient.listTables().contains(tableName.getTableName())) {
            return null;
        }

        return new ClpTableHandle(tableName.getTableName());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableMetadata(new SchemaTableName("default", "example"),
                ImmutableList.of(new ColumnMetadata("column1", VarcharType.VARCHAR),
                        new ColumnMetadata("column2", BigintType.BIGINT)));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix)
    {
        return ImmutableMap.of(new SchemaTableName("default", "example"),
                ImmutableList.of(new ColumnMetadata("column1", VarcharType.VARCHAR),
                        new ColumnMetadata("column2", BigintType.BIGINT)));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle;
        clpClient.listColumns(clpTableHandle.getTableName());

        for (ClpColumnHandle columnHandle : clpClient.listColumns(clpTableHandle.getTableName())) {
            System.out.println(columnHandle.getColumnName());
        }

        return ImmutableMap.of("column1",
                new ClpColumnHandle("column1", VarcharType.VARCHAR, true),
                "column2",
                new ClpColumnHandle("column2", BigintType.BIGINT, false));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        ClpColumnHandle clpColumnHandle = (ClpColumnHandle) columnHandle;
        return clpColumnHandle.getColumnMetadata();
    }
}
