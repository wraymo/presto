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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
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

    @Test
    public void testRecordCursor()
    {
        ClpRecordSetProvider recordSetProvider = new ClpRecordSetProvider(clpClient);
        ClpRecordSet recordSet = (ClpRecordSet) recordSetProvider.getRecordSet(
                ClpTransactionHandle.INSTANCE,
                SESSION,
                new ClpSplit("default", "test_1_table"),
                new ArrayList<>(clpClient.listColumns("test_1_table")));
        assertNotNull(recordSet, "recordSet is null");
        ClpRecordCursor cursor = (ClpRecordCursor) recordSet.cursor();
        assertNotNull(cursor, "cursor is null");
        cursor.advanceNextPosition();
        assertEquals(cursor.getLong(0), 1);
        assertEquals(cursor.getDouble(2), 2.0);
        assertTrue(cursor.getBoolean(5));
        assertEquals(cursor.getSlice(6).toStringUtf8(), "Hello world");
        cursor.advanceNextPosition();
    }

    @AfterMethod
    public void tearDown()
    {
        clpClient.close();
    }
}
