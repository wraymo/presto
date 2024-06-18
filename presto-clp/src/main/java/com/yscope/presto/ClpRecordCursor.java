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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.BufferedReader;
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
    private long readLineTime;
    private long parseLineTime;
    private long parseFieldTime;
    private long getFieldTime;
    private final BufferedReader reader;
    private final boolean isPolymorphicTypeEnabled;
    private final List<ClpColumnHandle> columnHandles;
    private final List<JsonNode> fields;

    public ClpRecordCursor(BufferedReader reader, boolean isPolymorphicTypeEnabled, List<ClpColumnHandle> columnHandles)
    {
        this.reader = reader;
        this.isPolymorphicTypeEnabled = isPolymorphicTypeEnabled;
        this.columnHandles = columnHandles;
        this.fields = new ArrayList<>(columnHandles.size());
        for (int i = 0; i < columnHandles.size(); i++) {
            fields.add(null);
        }
        readLineTime = 0;
        parseLineTime = 0;
        parseFieldTime = 0;
        getFieldTime = 0;
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
            long startTs = System.currentTimeMillis();
            String line = reader.readLine();
            long endTs = System.currentTimeMillis();
            readLineTime += endTs - startTs;
            if (line == null) {
                return false;
            }
            fields.replaceAll(ignored -> null);
            startTs = System.currentTimeMillis();
            JsonNode node = new ObjectMapper().readTree(line);
            endTs = System.currentTimeMillis();
            parseLineTime += endTs - startTs;
            startTs = System.currentTimeMillis();
            parseLine(node, "");
            endTs = System.currentTimeMillis();
            parseFieldTime += endTs - startTs;
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
        long startTs = System.currentTimeMillis();
        checkFieldType(field, BOOLEAN);
        boolean result = fields.get(field).asBoolean();
        long endTs = System.currentTimeMillis();
        getFieldTime += endTs - startTs;
        return result;
    }

    @Override
    public long getLong(int field)
    {
        long startTs = System.currentTimeMillis();
        checkFieldType(field, BIGINT);
        long result = fields.get(field).asLong();
        long endTs = System.currentTimeMillis();
        getFieldTime += endTs - startTs;
        return result;
    }

    @Override
    public double getDouble(int field)
    {
        long startTs = System.currentTimeMillis();
        checkFieldType(field, DOUBLE);
        double result = fields.get(field).asDouble();
        long endTs = System.currentTimeMillis();
        getFieldTime += endTs - startTs;
        return result;
    }

    @Override
    public Slice getSlice(int field)
    {
        long startTs = System.currentTimeMillis();
        checkFieldType(field, createUnboundedVarcharType());
        JsonNode node = fields.get(field);
        Slice result;
        if (node.isArray()) {
            result = Slices.utf8Slice(node.toString());
        }
        else {
            result = Slices.utf8Slice(node.asText());
        }
        long endTs = System.currentTimeMillis();
        getFieldTime += endTs - startTs;
        return result;
    }

    @Override
    public Object getObject(int field)
    {
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
        log.info("Read line time: %d ms", readLineTime);
        log.info("Parse line time: %d ms", parseLineTime);
        log.info("Parse field time: %d ms", parseFieldTime);
        log.info("Get field time: %d ms", getFieldTime);
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
