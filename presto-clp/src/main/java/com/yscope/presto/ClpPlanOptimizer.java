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

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;

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

    private class Rewriter
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
                            new ClpTableHandle(clpTableHandle.getTableName(), Optional.ofNullable(node.getPredicate())),
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
