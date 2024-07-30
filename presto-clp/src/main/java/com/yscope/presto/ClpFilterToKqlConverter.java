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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.yscope.presto.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static java.util.Objects.requireNonNull;

public class ClpFilterToKqlConverter
        implements RowExpressionVisitor<ClpExpression, Void>
{
    private static final Set<String> LOGICAL_BINARY_OPS_FILTER = ImmutableSet.of("=", "<", "<=", ">", ">=", "<>");

    private final StandardFunctionResolution standardFunctionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final TypeManager typeManager;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;

    public ClpFilterToKqlConverter(StandardFunctionResolution standardFunctionResolution,
                                   FunctionMetadataManager functionMetadataManager,
                                   TypeManager typeManager,
                                   Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        this.standardFunctionResolution =
                requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    private static String getLiteralString(ConstantExpression literal)
    {
        if (literal.getValue() instanceof Slice) {
            return ((Slice) literal.getValue()).toStringUtf8();
        }
        return literal.toString();
    }

    private String getVariableName(VariableReferenceExpression variable)
    {
        String variableName = ((ClpColumnHandle) assignments.get(variable)).getColumnName();
        if (variableName.endsWith("_bigint") || variableName.endsWith("_double") ||
                variableName.endsWith("_varchar") || variableName.endsWith("_boolean")) {
            return variableName.substring(0, variableName.lastIndexOf('_'));
        }
        return variableName;
    }

    private ClpExpression handleNot(CallExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "NOT operator must have exactly one argument. Received: " + node);
        }

        RowExpression input = node.getArguments().get(0);
        ClpExpression expression = input.accept(this, null);
        if (expression.getRemainingExpression().isPresent() || !expression.getDefinition().isPresent()) {
            return new ClpExpression(node);
        }
        return new ClpExpression("NOT " + expression.getDefinition().get());
    }

    private ClpExpression handleAnd(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        ArrayList<RowExpression> remainingExpressions = new ArrayList<>();
        boolean hasDefinition = false;
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getDefinition().isPresent()) {
                hasDefinition = true;
                queryBuilder.append(expression.getDefinition().get());
                queryBuilder.append(" AND ");
            }
            if (expression.getRemainingExpression().isPresent()) {
                remainingExpressions.add(expression.getRemainingExpression().get());
            }
        }
        if (!hasDefinition) {
            return new ClpExpression(node);
        }
        else if (!remainingExpressions.isEmpty()) {
            if (remainingExpressions.size() == 1) {
                return new ClpExpression(Optional.of(queryBuilder.substring(0, queryBuilder.length() - 5) + ")"),
                        Optional.of(remainingExpressions.get(0)));
            }
            else {
                return new ClpExpression(Optional.of(queryBuilder.substring(0, queryBuilder.length() - 5) + ")"),
                        Optional.of(new SpecialFormExpression(node.getSourceLocation(),
                                AND,
                                BOOLEAN,
                                remainingExpressions)));
            }
        }
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 5) + ")");
    }

    private ClpExpression handleOr(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        ArrayList<RowExpression> remainingExpressions = new ArrayList<>();
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getRemainingExpression().isPresent() || !expression.getDefinition().isPresent()) {
                return new ClpExpression(node);
            }
            queryBuilder.append(expression.getDefinition().get());
            queryBuilder.append(" OR ");
        }
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    private ClpExpression handleIn(SpecialFormExpression node)
    {
        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return new ClpExpression(node);
        }
        String variableName = getVariableName((VariableReferenceExpression) node.getArguments().get(0));
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        for (RowExpression argument : node.getArguments().subList(1, node.getArguments().size())) {
            if (!(argument instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
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
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    private ClpExpression handleLike(CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "LIKE operator must have exactly two arguments. Received: " + node);
        }

        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return new ClpExpression(node);
        }

        String variableName = getVariableName((VariableReferenceExpression) node.getArguments().get(0));
        RowExpression argument = node.getArguments().get(1);
        if (argument instanceof ConstantExpression) {
            ConstantExpression literal = (ConstantExpression) argument;
            String literalString = getLiteralString(literal);
            return new ClpExpression(variableName + ": \"" + literalString.replace("%", "*") + "\"");
        }
        else if (argument instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) argument;
            FunctionHandle functionHandle = callExpression.getFunctionHandle();
            if (!standardFunctionResolution.isCastFunction(functionHandle)) {
                return new ClpExpression(node);
            }
            if (callExpression.getArguments().size() != 1) {
                throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                        "CAST function must have exactly one argument. Received: " + callExpression);
            }
            if (!(callExpression.getArguments().get(0) instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
            ConstantExpression literal = (ConstantExpression) callExpression.getArguments().get(0);
            String literalString = getLiteralString(literal);
            return new ClpExpression(variableName + ": \"" + literalString.replace("%", "*") + "\"");
        }
        return new ClpExpression(node);
    }

    private ClpExpression handleLogicalBinary(String operator, CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "Logical binary operator must have exactly two arguments. Received: " + node);
        }

        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression) ||
                !(node.getArguments().get(1) instanceof ConstantExpression)) {
            return new ClpExpression(node);
        }

        ClpExpression leftExpression = node.getArguments().get(0).accept(this, null);
        ClpExpression rightExpression = node.getArguments().get(1).accept(this, null);
        if (!leftExpression.getDefinition().isPresent() || !rightExpression.getDefinition().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = leftExpression.getDefinition().get();
        String literalString = rightExpression.getDefinition().get();
        Type literalType = node.getArguments().get(1).getType();
        if (operator.equals("=")) {
            if (literalType.equals(VarcharType.VARCHAR)) {
                return new ClpExpression(variableName + ": \"" + literalString + "\"");
            }
            else {
                return new ClpExpression(variableName + ": " + literalString);
            }
        }
        else if (operator.equals("<>")) {
            if (literalType.equals(VarcharType.VARCHAR)) {
                return new ClpExpression("NOT " + variableName + ": \"" + literalString + "\"");
            }
            else {
                return new ClpExpression("NOT " + variableName + ": " + literalString);
            }
        }
        else if (LOGICAL_BINARY_OPS_FILTER.contains(operator) && !literalType.equals(VarcharType.VARCHAR)) {
            return new ClpExpression(variableName + " " + operator + " " + literalString);
        }
        else {
            return new ClpExpression(node);
        }
    }

    @Override
    public ClpExpression visitCall(CallExpression node, Void context)
    {
        FunctionHandle functionHandle = node.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(node);
        }

        if (standardFunctionResolution.isLikeFunction(functionHandle)) {
            return handleLike(node);
        }

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isComparisonOperator()) {
                return handleLogicalBinary(operatorType.getOperator(), node);
            }
        }

        return new ClpExpression(node);
    }

    @Override
    public ClpExpression visitConstant(ConstantExpression node, Void context)
    {
        return new ClpExpression(getLiteralString(node));
    }

    @Override
    public ClpExpression visitVariableReference(VariableReferenceExpression node, Void context)
    {
        return new ClpExpression(getVariableName(node));
    }

    @Override
    public ClpExpression visitSpecialForm(SpecialFormExpression node, Void context)
    {
        switch (node.getForm()) {
            case AND:
                return handleAnd(node);
            case OR:
                return handleOr(node);
            case IN:
                return handleIn(node);
            default:
                return new ClpExpression(node);
        }
    }
}
