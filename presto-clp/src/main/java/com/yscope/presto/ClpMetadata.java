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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ClpMetadata
        implements ConnectorMetadata
{
    private final ClpClient clpClient;
    private static final String DEFAULT_SCHEMA_NAME = "default";

    @Inject
    public ClpMetadata(ClpClient clpClient)
    {
        this.clpClient = clpClient;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(DEFAULT_SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schemaNameValue = schemaName.orElse(DEFAULT_SCHEMA_NAME);
        if (!listSchemaNames(session).contains(schemaNameValue)) {
            return ImmutableList.of();
        }

        return clpClient.listTables(schemaNameValue).stream()
                .map(tableName -> new SchemaTableName(schemaNameValue, tableName))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        if (!listSchemaNames(session).contains(schemaName)) {
            return null;
        }

        if (!clpClient.listTables(schemaName).contains(tableName.getTableName())) {
            return null;
        }

        return new ClpTableHandle(tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ClpTableHandle tableHandle = (ClpTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ClpTableLayoutHandle(tableHandle, Optional.empty()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) table;
        SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
        List<ColumnMetadata> columns = clpClient.listColumns(schemaTableName).stream()
                .map(ClpColumnHandle::getColumnMetadata)
                .collect(ImmutableList.toImmutableList());

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        String schemaName = prefix.getSchemaName();
        if (schemaName != null && !schemaName.equals(DEFAULT_SCHEMA_NAME)) {
            return ImmutableMap.of();
        }

        List<SchemaTableName> schemaTableNames;
        if (prefix.getTableName() == null) {
            schemaTableNames = listTables(session, Optional.of(prefix.getSchemaName()));
        }
        else {
            schemaTableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        return schemaTableNames.stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(),
                        tableName -> getTableMetadata(session, getTableHandle(session, tableName)).getColumns()
                ));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle;
        return clpClient.listColumns(clpTableHandle.getSchemaTableName()).stream()
                .collect(ImmutableMap.toImmutableMap(
                        ClpColumnHandle::getColumnName,
                        column -> column));
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
