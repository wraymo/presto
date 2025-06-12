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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;

import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpPlanOptimizer
        extends TestClpQueryBase
{
    private void testFilter(String sqlExpression, Optional<String> expectedKqlExpression,
                            Optional<String> expectedRemainingExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        HashSet<VariableReferenceExpression> clpUdfVariables = new HashSet<>();
        ClpExpression clpExpression = pushDownExpression.accept(new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        variableToColumnHandleMap),
                clpUdfVariables);
        Optional<String> kqlExpression = clpExpression.getDefinition();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression.isPresent()) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression.get());
        }
        else {
            assertFalse(kqlExpression.isPresent());
        }

        if (expectedRemainingExpression.isPresent()) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression.get(), sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }

    @Test
    public void testStringMatchPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter("city.Name = 'hello world'", Optional.of("city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("'hello world' = city.Name", Optional.of("city.Name: \"hello world\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into substring match
        testFilter("city.Name like 'hello%'", Optional.of("city.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '%hello'", Optional.of("city.Name: \"*hello\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter("city.Name like '%hello%'", Optional.empty(), Optional.of("city.Name like '%hello%'"), sessionHolder);

        // Like predicates that are kept in the original forms
        testFilter("city.Name like 'hello_'", Optional.of("city.Name: \"hello?\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '_hello'", Optional.of("city.Name: \"?hello\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello_w%'", Optional.of("city.Name: \"hello?w*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like '%hello_w'", Optional.of("city.Name: \"*hello?w\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello%world'", Optional.of("city.Name: \"hello*world\""), Optional.empty(), sessionHolder);
        testFilter("city.Name like 'hello%wor%ld'", Optional.of("city.Name: \"hello*wor*ld\""), Optional.empty(), sessionHolder);
    }

    @Test
    public void testSubStringPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("substr(city.Name, 1, 2) = 'he'", Optional.of("city.Name: \"he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, 5, 2) = 'he'", Optional.of("city.Name: \"????he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, 5) = 'he'", Optional.of("city.Name: \"????he\""), Optional.empty(), sessionHolder);
        testFilter("substr(city.Name, -2) = 'he'", Optional.of("city.Name: \"*he\""), Optional.empty(), sessionHolder);

        // Invalid substring index is not pushed down
        testFilter("substr(city.Name, 1, 5) = 'he'", Optional.empty(), Optional.of("substr(city.Name, 1, 5) = 'he'"), sessionHolder);
        testFilter("substr(city.Name, -5) = 'he'", Optional.empty(), Optional.of("substr(city.Name, -5) = 'he'"), sessionHolder);
    }

    @Test
    public void testNumericComparisonPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("fare >= 0", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("fare < 0", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("fare <= 0", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("fare = 0", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 < fare", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("0 <= fare", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("0 > fare", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("0 >= fare", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("0 = fare", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 != fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 <> fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testOrPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 OR city.Name like 'b%'", Optional.of("(fare > 0 OR city.Name: \"b*\")"), Optional.empty(),
                sessionHolder);
        testFilter("lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", Optional.empty(), Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)"),
                sessionHolder);

        // Multiple ORs
        testFilter("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                Optional.empty(),
                Optional.of("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1"),
                sessionHolder);
        testFilter("fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                Optional.of("((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)"),
                Optional.empty(),
                sessionHolder);
    }

    @Test
    public void testAndPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 AND city.Name like 'b%'", Optional.of("(fare > 0 AND city.Name: \"b*\")"), Optional.empty(), sessionHolder);
        testFilter("lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", Optional.of("(NOT city.Region.Id: 1)"), Optional.of("lower(city.Region.Name) = 'hello world'"),
                sessionHolder);

        // Multiple ANDs
        testFilter("fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                Optional.of("(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)"),
                Optional.of("(lower(city.Region.Name) = 'hello world')"),
                sessionHolder);
        testFilter("fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                Optional.of("(((fare > 0)) AND NOT city.Region.Id: 1)"),
                Optional.of("city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'"),
                sessionHolder);
    }

    @Test
    public void testNotPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Region.Name NOT LIKE 'hello%'", Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Region.Name LIKE 'hello%')", Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city.Name != 'hello world'", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("city.Name <> 'hello world'", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Name = 'hello world')", Optional.of("NOT city.Name: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0)", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);

        // Multiple NOTs
        testFilter("NOT (NOT fare = 0)", Optional.of("NOT NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0 AND city.Name = 'hello world')", Optional.of("NOT (fare: 0 AND city.Name: \"hello world\")"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0 OR city.Name = 'hello world')", Optional.of("NOT (fare: 0 OR city.Name: \"hello world\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testInPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IN ('hello world', 'hello world 2')", Optional.of("(city.Name: \"hello world\" OR city.Name: \"hello world 2\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testIsNullPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city.Name IS NULL", Optional.of("NOT city.Name: *"), Optional.empty(), sessionHolder);
        testFilter("city.Name IS NOT NULL", Optional.of("NOT NOT city.Name: *"), Optional.empty(), sessionHolder);
        testFilter("NOT (city.Name IS NULL)", Optional.of("NOT NOT city.Name: *"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testComplexPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                Optional.of("((fare > 0 OR city.Name: \"b*\"))"),
                Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)"),
                sessionHolder);
        testFilter("city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                Optional.of("((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))"),
                Optional.of("lower(city.Region.Name) = 'hello world' OR city.Name IS NULL"),
                sessionHolder);
    }

    @Test
    public void testClpUdfFilter()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testFilter("CLP_GET_STRING('city.Name') = 'Beijing'", Optional.of("city.Name: \"Beijing\""),
                Optional.empty(), sessionHolder);
    }

    @Test
    public void testClpUdfScanProject() {
        SessionHolder sessionHolder = new SessionHolder();
        PlanBuilder planBuilder = new PlanBuilder(sessionHolder.getSession(), idAllocator, metadata);
        ClpTableLayoutHandle tableLayoutHandle = new ClpTableLayoutHandle(table, Optional.empty());
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("clp"),
                table,
                new ConnectorTransactionHandle() {},
                Optional.of(tableLayoutHandle));

        // SELECT CLP_GET_STRING('user') from default.test
        PlanNode originalPlan = planBuilder.project(
                planBuilder.tableScan(
                    tableHandle, ImmutableList.of(), ImmutableMap.of()),
                    Assignments.of(
                            new VariableReferenceExpression(
                                    Optional.empty(),
                                    "clp_get_string",
                                    VarcharType.VARCHAR),
                            new CallExpression(
                                    "clp_get_string",
                                    new BuiltInFunctionHandle(
                                            new Signature(
                                                    new QualifiedObjectName(
                                                            "presto",
                                                            "default",
                                                            "clp_get_string"),
                                                    FunctionKind.SCALAR,
                                                    TypeSignature.parseTypeSignature("varchar"),
                                                    List.of(TypeSignature.parseTypeSignature("varchar")))),
                                    VarcharType.VARCHAR,
                                    List.of(new ConstantExpression(
                                            Slices.utf8Slice("user"),
                                            VarcharType.VARCHAR)))));

        ClpPlanOptimizer optimizer = new ClpPlanOptimizer(functionAndTypeManager, standardFunctionResolution);
        PlanNode optimizedPlan = optimizer.optimize(
                originalPlan,
                sessionHolder.getConnectorSession(),
                null,
                idAllocator);

        PlanAssert.assertPlan(
                sessionHolder.getSession(),
                metadata,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, typeProvider, StatsAndCosts.empty()),
                project(
                        ImmutableMap.of(
                                "clp_get_string",
                                PlanMatchPattern.expression("user")
                        ),
                        ClpTableScanMatcher.clpTableScanPattern(
                                tableLayoutHandle,
                                ImmutableSet.of(new ClpColumnHandle("user", VarcharType.VARCHAR, true))
                        )));
    }

    @Test
    public void testClpUdfScanFilterProject() {
        SessionHolder sessionHolder = new SessionHolder();
        PlanBuilder planBuilder = new PlanBuilder(sessionHolder.getSession(), idAllocator, metadata);
        ClpTableLayoutHandle tableLayoutHandle = new ClpTableLayoutHandle(table, Optional.empty());
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("clp"),
                table,
                new ConnectorTransactionHandle() {},
                Optional.of(tableLayoutHandle));

        RowExpression rowExpression = getRowExpression(
                "CLP_GET_INT('user_id') = 0 AND LOWER(city.Name) = 'BEIJING'",
                sessionHolder);

        VariableReferenceExpression cityVariable = new VariableReferenceExpression(
                Optional.empty(),
                "city",
                RowType.from(ImmutableList.of(
                        RowType.field("Name", VARCHAR),
                        RowType.field("Region", RowType.from(
                                ImmutableList.of(
                                        RowType.field("Id", BIGINT),
                                        RowType.field("Name", VARCHAR)))))));

        // SELECT CLP_GET_STRING('user') from default.test WHERE CLP_GET_INT('user_id') = 0 AND LOWER(city.Name) = 'BEIJING'
        // should be optimized to
        // SELECT user from default.test where LOWER(city.Name) = 'BEIJING' (KQL: user_id: 0)
        PlanNode originalPlan = planBuilder.project(
                planBuilder.filter(
                        rowExpression,
                        planBuilder.tableScan(
                                tableHandle,
                                ImmutableList.of(cityVariable),
                                ImmutableMap.of(cityVariable, city))
                ),
                Assignments.of(
                        new VariableReferenceExpression(
                                Optional.empty(),
                                "clp_get_string",
                                VarcharType.VARCHAR),
                        new CallExpression(
                                "clp_get_string",
                                new BuiltInFunctionHandle(
                                        new Signature(
                                                new QualifiedObjectName(
                                                        "presto",
                                                        "default",
                                                        "clp_get_string"),
                                                FunctionKind.SCALAR,
                                                TypeSignature.parseTypeSignature("varchar"),
                                                List.of(TypeSignature.parseTypeSignature("varchar")))),
                                VarcharType.VARCHAR,
                                List.of(new ConstantExpression(
                                        Slices.utf8Slice("user"),
                                        VarcharType.VARCHAR)))));

        ClpPlanOptimizer optimizer = new ClpPlanOptimizer(functionAndTypeManager, standardFunctionResolution);
        PlanNode optimizedPlan = optimizer.optimize(
                originalPlan,
                sessionHolder.getConnectorSession(),
                null,
                idAllocator);

        PlanAssert.assertPlan(
                sessionHolder.getSession(),
                metadata,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, typeProvider, StatsAndCosts.empty()),
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
                                                city)))));
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
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
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
