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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.File;
import java.util.Set;

public class ClpClient
{
    private final ClpConfig config;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = config;
    }

    public Set<String> listTables()
    {
        File archiveDir = new File(config.getClpArchiveDir());
        if (!archiveDir.exists() || !archiveDir.isDirectory()) {
            return ImmutableSet.of();
        }

        File[] files = archiveDir.listFiles();
        if (files == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (File file : files) {
            if (file.isDirectory()) {
                tableNames.add(file.getName());
            }
        }

        return tableNames.build();
    }

    public Set<String> listColumns(String tableName)
    {
        File tableDir = new File(config.getClpArchiveDir(), tableName);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
            return ImmutableSet.of();
        }

        File[] files = tableDir.listFiles();
        if (files == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<String> columnNames = ImmutableSet.builder();
        for (File file : files) {
            if (file.isFile()) {
                columnNames.add(file.getName());
            }
        }

        return columnNames.build();
    }
}
