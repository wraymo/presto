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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.plugin.clp.metadata.ClpNodeType;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestClpQueries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestClpQueries.class);
    private final String metadataDbUrl = "jdbc:h2:file:/tmp/metadata_query_testdb;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
    private final String metadataDbTablePrefix = "clp_";
    private static final String TABLE_NAME = "test";
    private final Session defaultSession = testSessionBuilder()
            .setCatalog("clp")
            .setSchema(ClpMetadata.DEFAULT_SCHEMA_NAME)
            .build();

    @BeforeMethod
    public void setUp()
    {
        final String metadataDbUser = "sa";
        final String metadataDbPassword = "";
        final String columnMetadataTableSuffix = "_column_metadata";
        final String datasetsTableSuffix = "datasets";
        final String datasetsTableName = metadataDbTablePrefix + datasetsTableSuffix;
        final String columnMetadataTableName = metadataDbTablePrefix + TABLE_NAME + columnMetadataTableSuffix;

        final String createTableMetadataSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " name VARCHAR(255) PRIMARY KEY," +
                        " archive_storage_type VARCHAR(4096) NOT NULL," +
                        " archive_storage_directory VARCHAR(4096) NOT NULL)", datasetsTableName);

        final String createColumnMetadataSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " name VARCHAR(512) NOT NULL," +
                        " type TINYINT NOT NULL," +
                        " PRIMARY KEY (name, type))", columnMetadataTableName);

        final String insertTableMetadataSQL = String.format(
                "INSERT INTO %s (name, archive_storage_type, archive_storage_directory) VALUES (?, ?, ?)", datasetsTableName);

        final String insertColumnMetadataSQL = String.format(
                "INSERT INTO %s (name, type) VALUES (?, ?)", columnMetadataTableName);

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, metadataDbUser, metadataDbPassword);
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTableMetadataSQL);
            stmt.execute(createColumnMetadataSQL);

            // Insert table metadata
            try (PreparedStatement pstmt = conn.prepareStatement(insertTableMetadataSQL)) {
                pstmt.setString(1, TABLE_NAME);
                pstmt.setString(2, "fs");
                pstmt.setString(3, "/tmp/archives/" + TABLE_NAME);
                pstmt.executeUpdate();
            }

            // Insert column metadata in batch
            List<Pair<String, ClpNodeType>> records = Arrays.asList(
                    new Pair<>("a", ClpNodeType.Integer),
                    new Pair<>("a", ClpNodeType.VarString),
                    new Pair<>("b", ClpNodeType.Float),
                    new Pair<>("b", ClpNodeType.ClpString),
                    new Pair<>("c.d", ClpNodeType.Boolean),
                    new Pair<>("c.e", ClpNodeType.VarString),
                    new Pair<>("f.g.h", ClpNodeType.UnstructuredArray));

            try (PreparedStatement pstmt = conn.prepareStatement(insertColumnMetadataSQL)) {
                for (Pair<String, ClpNodeType> record : records) {
                    pstmt.setString(1, record.getFirst());
                    pstmt.setByte(2, record.getSecond().getType());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @AfterMethod
    public void tearDown()
    {
        File dbFile = new File("/tmp/metadata_query_testdb.mv.db");
        File lockFile = new File("/tmp/metadata_query_testdb.trace.db"); // Optional, H2 sometimes creates this
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Deleted database file: " + dbFile.getAbsolutePath());
        }
        if (lockFile.exists()) {
            lockFile.delete();
        }
    }

    @Test
    public void testExample()
    {
        TransactionId transactionId = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder()
                .setCatalog("clp")
                .setSchema("default")
                .setTransactionId(transactionId)
                .build();

        Plan plan = getQueryRunner().createPlan(
                session,
                "SELECT CLP_GET_STRING('city.Name') FROM test WHERE CLP_GET_INT('city.Region.Id') = 1",
                WarningCollector.NOOP);
        log.info(plan.toString());
//        PlanAssert.assertPlan(
//                session,
//                getQueryRunner().getMetadata(),
//                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
//                plan,
//                anyTree(project(
//                            ImmutableMap.of(
//                            "city.Name", expression("city.Name")),
//                            filter(
//                "city.Region.Id = 1",
//                                tableScan("test", ImmutableMap.of(
//                                    "city.Name", "city.Name",
//                                    "city.Region.Id", "city.Region.Id"))))));
//        PlanAssert.assertPlan(
//                session,
//                getQueryRunner().getMetadata(),
//                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
//                plan,
//                anyTree(
//                        tableScan("test", ImmutableMap.of(
//                                "city.Name", "city.Name",
//                                "city.Region.Id", "city.Region.Id"))));
//        assertPlan(
//                "SELECT a_bigint, c.e FROM test where c.d = true AND a_varchar = 'cc'",
//                anyTree(project(
//                        ImmutableMap.of("a_bigint", expression("a_bigint"), // output symbols and their expressions
//                                        "c.e", expression("c.e")),
//                        filter(
//                                "(\"c.d\" = true) AND (\"a_varchar\" = CAST('cc' AS VARCHAR))",
//                                tableScan("test", ImmutableMap.of(
//                                        "a_bigint", "a_bigint",
//                                        "c.e", "c.e",
//                                        "c.d", "c.d",
//                                        "a_varchar", "a_varchar"))))));
//        assertPlan(
//                "SELECT a_bigint, c.e FROM test WHERE c.d = true AND a_varchar = 'cc'",
//                anyTree(
//                        scanFilterProject(
//                                tableScan("test", ImmutableMap.of(
//                                        "a_bigint", "a_bigint",
//                                        "a_varchar", "a_varchar",
//                                        "c", "c")),
//                                ImmutableMap.of(
//                                        "expr", expression("dereference(c, 1)"), // this matches the 'expr := DEREFERENCE(c, 1)' from the real plan
//                                        "a_bigint", expression("a_bigint")),
//                                expression("((a_varchar = CAST('cc' AS VARCHAR)) AND (dereference(c, 0) = true))"))));
    }

//    private Plan getQueryPlan(String sql)
//    {
//        return getQueryRunner().createPlan(defaultSession, sql, WarningCollector.NOOP);
//    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(3)
                .build();
        queryRunner.installPlugin(new ClpPlugin());
        queryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                "clp",
                ImmutableMap.of(
                        "clp.metadata-db-url", metadataDbUrl,
                        "clp.metadata-db-user", "sa",
                        "clp.metadata-db-password", "",
                        "clp.metadata-table-prefix", metadataDbTablePrefix));
        return queryRunner;
    }
}
