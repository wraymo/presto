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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

public class ClpSplitManager
        implements ConnectorSplitManager
{
    private final ClpClient clpClient;

    @Inject
    public ClpSplitManager(ClpClient clpClient)
    {
        this.clpClient = clpClient;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext)
    {
        ClpTableLayoutHandle layoutHandle = (ClpTableLayoutHandle) layout;
        ClpTableHandle tableHandle = layoutHandle.getTable();
        if (!clpClient.listTables().contains(tableHandle.getTableName())) {
            throw new RuntimeException("Table no longer exists: " + tableHandle.getTableName());
        }
        List<ConnectorSplit> splits = Collections.singletonList(new ClpSplit("default",
                tableHandle.getTableName(),
                tableHandle.getPredicate()));

        return new FixedSplitSource(splits);
    }
}
