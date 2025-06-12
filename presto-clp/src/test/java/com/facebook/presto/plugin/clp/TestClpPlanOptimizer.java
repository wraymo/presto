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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.clp.metadata.ClpNodeType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.metadataDbPassword;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.metadataDbTablePrefix;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.metadataDbUrlTemplate;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.metadataDbUser;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestClpPlanOptimizer
    extends TestClpQueryBase
{
    private static final Logger log = Logger.get(TestClpPlanOptimizer.class);
    private final String databaseName = "metadata_query_testdb";
    private final Session defaultSession = testSessionBuilder()
            .setCatalog("clp")
            .setSchema(ClpMetadata.DEFAULT_SCHEMA_NAME)
            .build();

    private ClpMetadataDbSetUp clpMetadataDbSetUp;

    private LocalQueryRunner localQueryRunner;
    private FunctionAndTypeManager functionAndTypeManager;
    private FunctionResolution functionResolution;
    private PlanNodeIdAllocator planNodeIdAllocator;

    @BeforeMethod
    public void setUp()
    {
        clpMetadataDbSetUp = new ClpMetadataDbSetUp();
        clpMetadataDbSetUp.setupMetadata(databaseName,
                ImmutableMap.of(
                        "test",
                        ImmutableList.of(
                                new Pair<>("city.Name", ClpNodeType.ClpString),
                                new Pair<>("city.Region.Id", ClpNodeType.Integer),
                                new Pair<>("city.Region.Name", ClpNodeType.VarString),
                                new Pair<>("fare", ClpNodeType.Float),
                                new Pair<>("isHoliday", ClpNodeType.Boolean))));

        localQueryRunner = new LocalQueryRunner(defaultSession);
        localQueryRunner.createCatalog("clp", new ClpConnectorFactory(), ImmutableMap.of(
                "clp.metadata-db-url", String.format(metadataDbUrlTemplate, databaseName),
                "clp.metadata-db-user", metadataDbUser,
                "clp.metadata-db-password", metadataDbPassword,
                "clp.metadata-table-prefix", metadataDbTablePrefix));
        localQueryRunner.getMetadata().registerBuiltInFunctions(extractFunctions(new ClpPlugin().getFunctions()));
        functionAndTypeManager = localQueryRunner.getMetadata().getFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        planNodeIdAllocator = new PlanNodeIdAllocator();
    }

    @AfterMethod
    public void tearDown()
    {
        clpMetadataDbSetUp.tearDown(databaseName);
    }

    @Test
    public void testScanProjectFilter() {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder()
                .setCatalog("clp")
                .setSchema("default")
                .setTransactionId(transactionId)
                .build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT CLP_GET_STRING('user') from test WHERE CLP_GET_INT('user_id') = 0 AND LOWER(city.Name) = 'BEIJING'",
                WarningCollector.NOOP);
        ClpPlanOptimizer optimizer = new ClpPlanOptimizer(functionAndTypeManager, functionResolution);
        PlanNode optimizedPlan = optimizer.optimize(
                plan.getRoot(),
                session.toConnectorSession(),
                null,
                new PlanNodeIdAllocator());
        log.info(plan.toString());
        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                    project(
                            ImmutableMap.of(
                                    "clp_get_string",
                                    PlanMatchPattern.expression("user")
                            ),
                            filter(
                                    expression("lower(city.Name) = 'BEIJING'"),
                                    ClpTableScanMatcher.clpTableScanPattern(
                                            new ClpTableLayoutHandle(table, Optional.of("(user_id: 0)")),
                                            ImmutableSet.of(
                                                    new ClpColumnHandle("user", VarcharType.VARCHAR, true),
                                                    city))))));
    }

    @Test
    public void testScanProject()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder()
                .setCatalog("clp")
                .setSchema("default")
                .setTransactionId(transactionId)
                .build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT CLP_GET_STRING('user') FROM test",
                WarningCollector.NOOP);
        ClpPlanOptimizer optimizer = new ClpPlanOptimizer(functionAndTypeManager, functionResolution);
        PlanNode optimizedPlan = optimizer.optimize(
                plan.getRoot(),
                session.toConnectorSession(),
                null,
                planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(project(
                        ImmutableMap.of(
                                "clp_get_string",
                                PlanMatchPattern.expression("user")
                        ),
                        ClpTableScanMatcher.clpTableScanPattern(
                                new ClpTableLayoutHandle(
                                        new ClpTableHandle(
                                                new SchemaTableName("default", "test"),
                                                ClpTableHandle.StorageType.FS),
                                        Optional.empty()),
                                ImmutableSet.of(new ClpColumnHandle("user", VarcharType.VARCHAR, true))
                        ))));
    }

    @Test
    public void testScanFilter()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder()
                .setCatalog("clp")
                .setSchema("default")
                .setTransactionId(transactionId)
                .build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT CLP_GET_STRING('user') FROM test",
                WarningCollector.NOOP);
        ClpPlanOptimizer optimizer = new ClpPlanOptimizer(functionAndTypeManager, functionResolution);
        PlanNode optimizedPlan = optimizer.optimize(
                plan.getRoot(),
                session.toConnectorSession(),
                null,
                planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(project(
                        ImmutableMap.of(
                                "clp_get_string",
                                PlanMatchPattern.expression("user")
                        ),
                        ClpTableScanMatcher.clpTableScanPattern(
                                new ClpTableLayoutHandle(
                                        new ClpTableHandle(
                                                new SchemaTableName("default", "test"),
                                                ClpTableHandle.StorageType.FS),
                                        Optional.empty()),
                                ImmutableSet.of(new ClpColumnHandle("user", VarcharType.VARCHAR, true))
                        ))));
    }

    private static final class ClpTableScanMatcher
            implements Matcher
    {
        private final ClpTableLayoutHandle expectedLayoutHandle;
        private final Set<ColumnHandle> expectedColumns;

        static PlanMatchPattern clpTableScanPattern(ClpTableLayoutHandle layoutHandle, Set<ColumnHandle> columns)
        {
            return node(TableScanNode.class).with(new ClpTableScanMatcher(layoutHandle, columns));
        }

        private ClpTableScanMatcher(ClpTableLayoutHandle expectedLayoutHandle, Set<ColumnHandle> expectedColumns)
        {
            this.expectedLayoutHandle = expectedLayoutHandle;
            this.expectedColumns = expectedColumns;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(
                PlanNode node,
                StatsProvider stats,
                Session session,
                Metadata metadata,
                SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false");
            TableScanNode tableScanNode = (TableScanNode) node;
            ClpTableLayoutHandle actualLayoutHandle = (ClpTableLayoutHandle) tableScanNode.getTable().getLayout().get();

            // Check layout handle
            if (!expectedLayoutHandle.equals(actualLayoutHandle)) {
                return MatchResult.NO_MATCH;
            }

            // Check assignments contain expected columns
            Map<VariableReferenceExpression, ColumnHandle> actualAssignments = tableScanNode.getAssignments();
            Set<ColumnHandle> actualColumns = new HashSet<>(actualAssignments.values());

            if (!expectedColumns.equals(actualColumns)) {
                return MatchResult.NO_MATCH;
            }

            SymbolAliases.Builder aliasesBuilder = SymbolAliases.builder();
            for (VariableReferenceExpression variable : tableScanNode.getOutputVariables()) {
                aliasesBuilder.put(variable.getName(), new SymbolReference(variable.getName()));
            }

            return MatchResult.match(aliasesBuilder.build());
        }
    }
}
