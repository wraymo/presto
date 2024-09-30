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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;

public class ClpRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(ClpRecordCursor.class);
    private final BufferedReader reader;
    private final Process process;
    private final boolean isPolymorphicTypeEnabled;
    private final List<ClpColumnHandle> columnHandles;
    private final List<JsonNode> fields;

    public ClpRecordCursor(ProcessBuilder processBuilder, boolean isPolymorphicTypeEnabled, List<ClpColumnHandle> columnHandles)
    {
        try {
            this.process = processBuilder.start();
        }
        catch (IOException e) {
            log.error(e, "Failed to search records");
            throw new RuntimeException(e);
        }
        this.reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        this.isPolymorphicTypeEnabled = isPolymorphicTypeEnabled;
        this.columnHandles = columnHandles;
        this.fields = new ArrayList<>(columnHandles.size());
        for (int i = 0; i < columnHandles.size(); i++) {
            fields.add(null);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            String line = reader.readLine();
            if (line == null) {
                return false;
            }
            fields.replaceAll(ignored -> null);
            JsonNode node = new ObjectMapper().readTree(line);
            parseLine(node, "");
        }
        catch (Exception e) {
            return false;
        }

        return true;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return fields.get(field).asBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return fields.get(field).asLong();
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return fields.get(field).asDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        JsonNode node = fields.get(field);
        return Slices.utf8Slice(node.asText());
    }

    @Override
    public Object getObject(int field)
    {
        JsonNode node = fields.get(field);
        if (node.isArray()) {
            BlockBuilder builder = VARCHAR.createBlockBuilder(null, node.size());
            Iterator<JsonNode> elements = node.elements();
            while (elements.hasNext()) {
                VARCHAR.writeString(builder, elements.next().asText());
            }
            return builder.build();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return fields.get(field) == null || fields.get(field).isNull();
    }

    @Override
    public void close()
    {
        try {
            reader.close();
        }
        catch (IOException e) {
            log.warn(e, "Failed to close reader");
        }
        process.destroy();
    }

    private void parseLine(JsonNode node, String prefix)
    {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode value = field.getValue();
                parseLine(value, prefix.isEmpty() ? key : prefix + "." + key);
            }
        }
        else {
            int index = getFieldIndex(prefix, node);
            if (index == -1) {
                return;
            }
            fields.set(index, node);
        }
    }

    private String jsonNodeToTypeString(JsonNode node)
    {
        if (node.isIntegralNumber()) {
            return BIGINT.getDisplayName();
        }
        if (node.isFloatingPointNumber()) {
            return DOUBLE.getDisplayName();
        }
        if (node.isBoolean()) {
            return BOOLEAN.getDisplayName();
        }
        if (node.isTextual() || node.isArray() || node.isNull()) {
            return VARCHAR.getDisplayName();
        }
        return "unknown";
    }

    private int getFieldIndex(String fieldName, JsonNode node)
    {
        for (int i = 0; i < columnHandles.size(); i++) {
            if (columnHandles.get(i).getColumnName().equals(fieldName)) {
                return i;
            }

            if (isPolymorphicTypeEnabled && (fieldName + "_" + jsonNodeToTypeString(node)).equals(columnHandles.get(i)
                    .getColumnName())) {
                return i;
            }
        }
        return -1;
    }
}
