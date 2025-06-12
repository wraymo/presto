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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.spi.SchemaTableName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpSplit
{
    private ClpSplitProvider clpSplitProvider;
    private ClpMetadataDbSetUp clpMetadataDbSetUp;
    private Map<String, List<String>> tableSplits;

    private final String databaseName = "split_testdb";

    @BeforeMethod
    public void setUp()
    {
        clpMetadataDbSetUp = new ClpMetadataDbSetUp();
        tableSplits = new HashMap<>();

        int numKeys = 3;
        int numValuesPerKey = 10;

        for (int i = 0; i < numKeys; i++) {
            String key = "test_" + i;
            List<String> values = new ArrayList<>();

            for (int j = 0; j < numValuesPerKey; j++) {
                values.add("id_" + j);
            }

            tableSplits.put(key, values);
        }
        clpSplitProvider = clpMetadataDbSetUp.setupSplit(databaseName, tableSplits);
    }

    @AfterMethod
    public void tearDown()
    {
        File dbFile = new File("/tmp/split_testdb.mv.db");
        File lockFile = new File("/tmp/split_testdb.trace.db"); // Optional, H2 sometimes creates this
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Deleted database file: " + dbFile.getAbsolutePath());
        }
        if (lockFile.exists()) {
            lockFile.delete();
        }
    }

    @Test
    public void testListSplits()
    {
        for (Map.Entry<String, List<String>> entry : tableSplits.entrySet()) {
            String tableName = entry.getKey();
            List<String> expectedSplits = entry.getValue();
            ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(
                    new ClpTableHandle(new SchemaTableName(DEFAULT_SCHEMA_NAME, tableName), ClpTableHandle.StorageType.FS),
                    Optional.empty());
            List<ClpSplit> splits = clpSplitProvider.listSplits(layoutHandle);
            assertEquals(splits.size(), expectedSplits.size());
            for (int i = 0; i < splits.size(); i++) {
                assertEquals(splits.get(i).getPath(), "/tmp/archives/" + tableName + "/" + expectedSplits.get(i));
            }
        }
    }
}
