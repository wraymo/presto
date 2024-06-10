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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ClpRecordSet
        implements RecordSet
{
    private final BufferedReader reader;
    private final List<ClpColumnHandle> columnHandles;
    private final boolean isPolymorphicTypeEnabled;

    public ClpRecordSet(BufferedReader reader, boolean isPolymorphicTypeEnabled, List<ClpColumnHandle> columnHandles)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.isPolymorphicTypeEnabled = isPolymorphicTypeEnabled;
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnHandles.stream().map(ClpColumnHandle::getColumnType).collect(ImmutableList.toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        return new ClpRecordCursor(reader, isPolymorphicTypeEnabled, columnHandles);
    }
}
