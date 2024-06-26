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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpRecordCursor
{
    private ClpClient clpClient;

    @BeforeMethod
    public void setUp()
    {
        ClpConfig config = new ClpConfig().setClpArchiveDir("src/test/resources/clp_archive")
                .setPolymorphicTypeEnabled(true)
                .setClpExecutablePath("/usr/local/bin/clp-s");
        clpClient = new ClpClient(config);
        clpClient.start();
    }

    public void assertNull(ClpRecordCursor cursor, List<Integer> indices)
    {
        for (int index : indices) {
            assertTrue(cursor.isNull(index));
        }
    }

    @Test
    public void testTable1RecordCursor()
    {
        ClpRecordSetProvider recordSetProvider = new ClpRecordSetProvider(clpClient);
        ClpRecordSet recordSet = (ClpRecordSet) recordSetProvider.getRecordSet(
                ClpTransactionHandle.INSTANCE,
                SESSION,
                new ClpSplit("default", "test_1_table", Optional.empty()),
                new ArrayList<>(clpClient.listColumns("test_1_table")));
        assertNotNull(recordSet, "recordSet is null");
        ClpRecordCursor cursor = (ClpRecordCursor) recordSet.cursor();
        assertNotNull(cursor, "cursor is null");
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 1);
        assertEquals(cursor.getDouble(2), 2.0);
        assertTrue(cursor.getBoolean(5));
        assertEquals(cursor.getSlice(6).toStringUtf8(), "Hello world");
        assertNull(cursor, List.of(1, 3, 4));
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 2);
        assertEquals(cursor.getDouble(2), 3.0);
        assertFalse(cursor.getBoolean(5));
        assertEquals(cursor.getSlice(6).toStringUtf8(), "Goodbye world");
        assertNull(cursor, List.of(1, 3, 4));
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(1).toStringUtf8(), "foo");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "bar");
        assertEquals(cursor.getDouble(4), 2.0);
        assertNull(cursor, List.of(0, 2, 5, 6));
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(1).toStringUtf8(), "baz");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "qux");
        assertNull(cursor, List.of(0, 2, 4, 5, 6));
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testTable2RecordCursor()
    {
        ClpRecordSetProvider recordSetProvider = new ClpRecordSetProvider(clpClient);
        ClpRecordSet recordSet = (ClpRecordSet) recordSetProvider.getRecordSet(
                ClpTransactionHandle.INSTANCE,
                SESSION,
                new ClpSplit("default", "test_2_table", Optional.empty()),
                new ArrayList<>(clpClient.listColumns("test_2_table")));
        assertNotNull(recordSet, "recordSet is null");
        ClpRecordCursor cursor = (ClpRecordCursor) recordSet.cursor();
        assertNotNull(cursor, "cursor is null");
        for (int i = 0; i <= 12; i++) {
            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(3), i);
        }
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testPredicate()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        CallExpression callExpression =
                new CallExpression(EQUAL.name(),
                        functionAndTypeManager.resolveOperator(EQUAL, fromTypes(
                                BigintType.BIGINT, BigintType.BIGINT)),
                        BOOLEAN,
                        ImmutableList.of(new VariableReferenceExpression(Optional.empty(),
                                        "a_bigint",
                                        BigintType.BIGINT),
                                constant(1L, BigintType.BIGINT)));

        ClpRecordSetProvider recordSetProvider = new ClpRecordSetProvider(clpClient);
        ClpRecordSet recordSet = (ClpRecordSet) recordSetProvider.getRecordSet(
                ClpTransactionHandle.INSTANCE,
                SESSION,
                new ClpSplit("default", "test_1_table", Optional.of(callExpression)),
                new ArrayList<>(clpClient.listColumns("test_1_table")));
        assertNotNull(recordSet, "recordSet is null");
        ClpRecordCursor cursor = (ClpRecordCursor) recordSet.cursor();
        assertNotNull(cursor, "cursor is null");
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 1);
        assertEquals(cursor.getDouble(2), 2.0);
        assertTrue(cursor.getBoolean(5));
        assertEquals(cursor.getSlice(6).toStringUtf8(), "Hello world");
        assertNull(cursor, List.of(1, 3, 4));
        assertFalse(cursor.advanceNextPosition());
    }

    @AfterMethod
    public void tearDown()
    {
        clpClient.close();
    }
}
