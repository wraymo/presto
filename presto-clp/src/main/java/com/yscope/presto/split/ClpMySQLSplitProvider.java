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
package com.yscope.presto.split;

import com.facebook.airlift.log.Logger;
import com.yscope.presto.ClpConfig;
import com.yscope.presto.ClpSplit;
import com.yscope.presto.ClpTableLayoutHandle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class ClpMySQLSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpMySQLSplitProvider.class);

    private static final String ARCHIVE_TABLE_SUFFIX = "archives";
    private static final String TABLE_METADATA_TABLE_SUFFIX = "tables";
    private static final String QUERY_SELECT_ARCHIVE_IDS = "SELECT id FROM %s" + ARCHIVE_TABLE_SUFFIX;
    private static final String QUERY_SELECT_TABLE_METADATA = "SELECT * FROM %s" + TABLE_METADATA_TABLE_SUFFIX + " WHERE AND table_name = ?";

    private final ClpConfig config;

    public ClpMySQLSplitProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
        }
        this.config = config;
    }

    private Connection getConnection() throws SQLException
    {
        return DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
    }

    @Override
    // TODO(Rui): This method is not complete yet
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
//        List<ClpSplit> splits = new ArrayList<>();
//        String tableName = clpTableLayoutHandle.getTable().getSchemaTableName().getTableName();
//        String query = String.format(QUERY_SELECT_TABLE_METADATA, config.getMetadataTablePrefix());
//        try (Connection connection = getConnection();
//            PreparedStatement statement = connection.prepareStatement(query)) {
//            statement.setString(1, schemaTableName.getTableName());
//            ResultSet resultSet = statement.executeQuery();
//            while (resultSet.next()) {
//                String archiveId = resultSet.getString("archive_id");
//            }
//        }
//        catch (SQLException e) {
//            log.error("Failed to retrieve table metadata", e);
//        }
//
//        List<String> archiveIds = new ArrayList<>();
//        String query = String.format(QUERY_SELECT_ARCHIVE_IDS, config.getMetadataTablePrefix());
//
//        try (Connection connection = getConnection();
//             PreparedStatement statement = connection.prepareStatement(query);
//             ResultSet resultSet = statement.executeQuery()) {
//
//            while (resultSet.next()) {
//                archiveIds.add(resultSet.getString("id"));
//            }
//        } catch (SQLException e) {
//            log.error("Failed to retrieve archive IDs", e);
//        }

        return null;
    }
}
