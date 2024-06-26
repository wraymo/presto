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
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;

import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

public class ClpPlanOptimizer
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode maxSubplan,
                             ConnectorSession session,
                             VariableAllocator variableAllocator,
                             PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), maxSubplan);
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
        }
        else if (additionalPredicate instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) additionalPredicate;
            String variableName = callExpression.getArguments().get(0).toString();
            if (variableName.endsWith("_bigint") || variableName.endsWith("_double") ||
                    variableName.endsWith("_varchar") || variableName.endsWith("_boolean")) {
                variableName = variableName.substring(0, variableName.lastIndexOf('_'));
            }
            String literal = callExpression.getArguments().get(1).toString();
            switch (callExpression.getDisplayName()) {
                case "EQUAL":
                    if (callExpression.getArguments().get(1).getType().equals(BigintType.BIGINT) ||
                            callExpression.getArguments().get(1).getType().equals(DoubleType.DOUBLE) ||
                            callExpression.getArguments().get(1).getType().equals(BooleanType.BOOLEAN)) {
                        return variableName + ": " + literal;
                    }
                    else {
                        return variableName + ": \"" + literal + "\"";
                    }
                case "<>":
                    if (callExpression.getArguments().get(1).getType().equals(BigintType.BIGINT) ||
                            callExpression.getArguments().get(1).getType().equals(DoubleType.DOUBLE) ||
                            callExpression.getArguments().get(1).getType().equals(BooleanType.BOOLEAN)) {
                        return "NOT " + variableName + ": " + literal;
                    }
                    else {
                        return "NOT " + variableName + ": \"" + literal + "\"";
                    }
                case "GREATER_THAN":
                    return variableName + " > " + literal;
                case "GREATER_THAN_OR_EQUAL":
                    return variableName + " >= " + literal;
                case "LESS_THAN":
                    return variableName + " < " + literal;
                case "LESS_THAN_OR_EQUAL":
                    return variableName + " <= " + literal;
            }
        }
        throw new RuntimeException("Unsupported predicate type");
    }

    private static class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
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

            return new TableScanNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    new TableHandle(
                            tableHandle.getConnectorId(),
                            new ClpTableHandle(clpTableHandle.getTableName(),
                                    Optional.of(buildKqlQuery(node.getPredicate()))),
                            tableHandle.getTransaction(),
                            tableHandle.getLayout()),
                    tableScanNode.getOutputVariables(),
                    tableScanNode.getAssignments(),
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint());
        }
    }
}
