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

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMetadata
{
    private ClpMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        ClpConfig config = new ClpConfig().setClpArchiveDir("src/test/resources/clp_archive")
                .setPolymorphicTypeEnabled(true)
                .setClpExecutablePath("/usr/local/bin/clp-s");
        metadata = new ClpMetadata(new ClpClient(config));
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("default"));
    }

    @Test
    public void testListTables()
    {
        HashSet<SchemaTableName> tables = new HashSet<>();
        tables.add(new SchemaTableName("default", "test_1_table"));
        tables.add(new SchemaTableName("default", "test_2_table"));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), tables);
    }

    @Test
    public void testGetTable1Metadata()
    {
        ClpTableHandle clpTableHandle =
                (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_1_table"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
        HashSet<ColumnMetadata> columnMetadata = new HashSet<>();
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_integer")
                .setType(IntegerType.INTEGER)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_double")
                .setType(DoubleType.DOUBLE)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.d")
                .setType(BooleanType.BOOLEAN)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.e")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder().setName("c").setType(DoubleType.DOUBLE).setNullable(true).build());
        assertEquals(columnMetadata, new HashSet<>(tableMetadata.getColumns()));
    }

    @Test
    public void testGetTable2Metadata()
    {
        ClpTableHandle clpTableHandle =
                (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_2_table"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
        HashSet<ColumnMetadata> columnMetadata = new HashSet<>();
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_double")
                .setType(DoubleType.DOUBLE)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_integer")
                .setType(IntegerType.INTEGER)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c")
                .setType(BooleanType.BOOLEAN)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b.c")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.d")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.e")
                .setType(BooleanType.BOOLEAN)
                .setNullable(true)
                .build());
        assertEquals(columnMetadata, new HashSet<>(tableMetadata.getColumns()));
    }
}
