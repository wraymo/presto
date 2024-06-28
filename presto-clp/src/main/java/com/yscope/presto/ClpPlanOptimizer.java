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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

public class ClpPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(ClpPlanOptimizer.class);
    private final LogicalRowExpressions logicalRowExpressions;
    private final ExpressionOptimizer expressionOptimizer;

    public ClpPlanOptimizer(FunctionMetadataManager functionManager,
                            StandardFunctionResolution functionResolution,
                            DeterminismEvaluator determinismEvaluator,
                            ExpressionOptimizer expressionOptimizer)
    {
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionManager);
        this.expressionOptimizer = expressionOptimizer;
    }

    private static String getVariableName(String variableName)
    {
        if (variableName.endsWith("_bigint") || variableName.endsWith("_double") ||
                variableName.endsWith("_varchar") || variableName.endsWith("_boolean")) {
            return variableName.substring(0, variableName.lastIndexOf('_'));
        }
        return variableName;
    }

    private static String getLiteralString(ConstantExpression literal)
    {
        if (literal.getValue() instanceof Slice) {
            return ((Slice) literal.getValue()).toStringUtf8();
        }
        return literal.toString();
    }

    private static String handleCardinalitySplit(RowExpression additionalPredicate)
    {
        CallExpression cardinalityExpression = (CallExpression) additionalPredicate;
        if (!(cardinalityExpression.getArguments().size() == 1 && cardinalityExpression.getArguments()
                .get(0)
                .toString()
                .startsWith("SPLIT"))) {
            throw new RuntimeException("Unsupported predicate" + cardinalityExpression);
        }

        CallExpression splitExpression = (CallExpression) cardinalityExpression.getArguments().get(0);
        if (!(splitExpression.getArguments().size() == 3 && splitExpression.getArguments()
                .get(0) instanceof VariableReferenceExpression && splitExpression.getArguments()
                .get(1) instanceof ConstantExpression && splitExpression.getArguments()
                .get(2) instanceof ConstantExpression)) {
            throw new RuntimeException("Unsupported predicate" + splitExpression);
        }

        VariableReferenceExpression variableReferenceExpression =
                (VariableReferenceExpression) splitExpression.getArguments().get(0);
        ConstantExpression startExpression = (ConstantExpression) splitExpression.getArguments().get(1);
        ConstantExpression endExpression = (ConstantExpression) splitExpression.getArguments().get(2);
        if (!(startExpression.getType() == VarcharType.VARCHAR && endExpression.toString().equals("2"))) {
            throw new RuntimeException("Unsupported predicate" + splitExpression);
        }

        String variableName = getVariableName(variableReferenceExpression.toString());
        return variableName + ": \"*" + getLiteralString(startExpression) + "*\"";
    }

    public static String buildKqlQuery(RowExpression additionalPredicate)
    {
        if (additionalPredicate instanceof SpecialFormExpression) {
            SpecialFormExpression specialFormExpression = (SpecialFormExpression) additionalPredicate;
            if (specialFormExpression.getForm() == SpecialFormExpression.Form.AND) {
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("(");
                for (RowExpression argument : specialFormExpression.getArguments()) {
                    queryBuilder.append(buildKqlQuery(argument));
                    queryBuilder.append(" AND ");
                }
                return queryBuilder.substring(0, queryBuilder.length() - 5) + ")";
            }
            else if (specialFormExpression.getForm() == SpecialFormExpression.Form.OR) {
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("(");
                for (RowExpression argument : specialFormExpression.getArguments()) {
                    queryBuilder.append(buildKqlQuery(argument));
                    queryBuilder.append(" OR ");
                }
                return queryBuilder.substring(0, queryBuilder.length() - 4) + ")";
            }
            else if (specialFormExpression.getForm() == SpecialFormExpression.Form.IN) {
                String variableName = getVariableName(specialFormExpression.getArguments().get(0).toString());
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("(");
                for (RowExpression argument : specialFormExpression.getArguments()
                        .subList(1, specialFormExpression.getArguments().size())) {
                    ConstantExpression literal = (ConstantExpression) argument;
                    String literalString = getLiteralString(literal);
                    queryBuilder.append(variableName).append(": ");
                    if (literal.getType().equals(VarcharType.VARCHAR)) {
                        queryBuilder.append("\"");
                        queryBuilder.append(literalString);
                        queryBuilder.append("\"");
                    }
                    else {
                        queryBuilder.append(literalString);
                    }
                    queryBuilder.append(" OR ");
                }
                return queryBuilder.substring(0, queryBuilder.length() - 4) + ")";
            }
        }
        else if (additionalPredicate instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) additionalPredicate;
            // Handle "=(CARDINALITY(SPLIT(field, string, 2)), 2)" case specifically
            // TODO: Handle it more generically
            if (callExpression.getDisplayName().equals("=")) {
                if (!(callExpression.getArguments().size() == 2 && callExpression.getArguments()
                        .get(1).toString().equals("2") && callExpression.getArguments()
                        .get(0)
                        .toString()
                        .startsWith("CARDINALITY"))) {
                    throw new RuntimeException("Unsupported predicate" + callExpression);
                }

                return handleCardinalitySplit(callExpression.getArguments().get(0));
            }

            // Handle "<>(CARDINALITY(field), 2)" case specifically
            if (callExpression.getDisplayName().equals("<>") && callExpression.getArguments().size() == 2 &&
                    callExpression.getArguments().get(1).toString().equals("2") &&
                    callExpression.getArguments().get(0).toString().startsWith("CARDINALITY")) {
                return "NOT " + handleCardinalitySplit(callExpression.getArguments().get(0));
            }

            // Handle "not" case specifically
            if (callExpression.getDisplayName().equals("not")) {
                return "NOT(" + buildKqlQuery(callExpression.getArguments().get(0)) + ")";
            }

            // Handle "LIKE(FIELD, CAST(PATTERN))" case specifically
            if (callExpression.getDisplayName().equals("LIKE")) {
                if (!(callExpression.getArguments().size() == 2 && callExpression.getArguments()
                        .get(1)
                        .toString()
                        .startsWith("CAST"))) {
                    throw new RuntimeException("Unsupported predicate" + additionalPredicate);
                }

                CallExpression castExpression = (CallExpression) callExpression.getArguments().get(1);
                if (!(castExpression.getArguments().size() == 1 && castExpression.getArguments()
                        .get(0) instanceof ConstantExpression)) {
                    throw new RuntimeException("Unsupported predicate" + castExpression);
                }

                String variableName = getVariableName(callExpression.getArguments().get(0).toString());
                ConstantExpression literal = (ConstantExpression) castExpression.getArguments().get(0);
                String literalString = getLiteralString(literal);
                return variableName + ": \"" + literalString.replace("%", "*") + "\"";
            }

            String variableName = getVariableName(callExpression.getArguments().get(0).toString());
            ConstantExpression literal = (ConstantExpression) callExpression.getArguments().get(1);
            String literalString = getLiteralString(literal);
            switch (callExpression.getDisplayName()) {
                case "EQUAL":
                    if (literal.getType().equals(VarcharType.VARCHAR)) {
                        return variableName + ": \"" + literalString + "\"";
                    }
                    else {
                        return variableName + ": " + literalString;
                    }
                case "<>":
                    if (literal.getType().equals(VarcharType.VARCHAR)) {
                        return "NOT " + variableName + ": \"" + literalString + "\"";
                    }
                    else {
                        return "NOT " + variableName + ": " + literalString;
                    }
                case "GREATER_THAN":
                    return variableName + " > " + literalString;
                case "GREATER_THAN_OR_EQUAL":
                    return variableName + " >= " + literalString;
                case "LESS_THAN":
                    return variableName + " < " + literalString;
                case "LESS_THAN_OR_EQUAL":
                    return variableName + " <= " + literalString;
            }
        }
        throw new RuntimeException("Unsupported predicate" + additionalPredicate);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
                             ConnectorSession session,
                             VariableAllocator variableAllocator,
                             PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(session, idAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            TableScanNode tableScanNode = (TableScanNode) node.getSource();
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            // Remove them temporarily as we cannot handle io.airlift.joni.Regex
//            RowExpression predicate = expressionOptimizer.optimize(node.getPredicate(), OPTIMIZED, session);
//            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);
            String query = buildKqlQuery(node.getPredicate());
            log.info("Query: " + query);
            ClpTableLayoutHandle clpTableLayoutHandle = new ClpTableLayoutHandle(clpTableHandle, Optional.of(query));
            return new TableScanNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    new TableHandle(
                            tableHandle.getConnectorId(),
                            clpTableHandle,
                            tableHandle.getTransaction(),
                            Optional.of(clpTableLayoutHandle)),
                    tableScanNode.getOutputVariables(),
                    tableScanNode.getAssignments(),
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint());
        }
    }
}
