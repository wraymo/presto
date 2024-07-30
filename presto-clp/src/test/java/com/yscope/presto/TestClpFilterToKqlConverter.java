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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpFilterToKqlConverter
{
    @Test
    public void testSqlToKqlConverter()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        FunctionResolution functionResolution =
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        FunctionAndTypeResolver functionAndTypeResolver = functionAndTypeManager.getFunctionAndTypeResolver();
        // (a > 0 OR b like 'b%') AND (lower(c.e) = 'hello world' OR c IS NULL)
        SpecialFormExpression firstOrExpression =
                new SpecialFormExpression(SpecialFormExpression.Form.OR,
                        BOOLEAN,
                        new CallExpression(GREATER_THAN.name(),
                                functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(
                                        BigintType.BIGINT, BigintType.BIGINT)),
                                BOOLEAN,
                                ImmutableList.of(new VariableReferenceExpression(Optional.empty(),
                                                "a_bigint",
                                                BigintType.BIGINT),
                                        constant(0L, BigintType.BIGINT))),
                        call("LIKE",
                                functionResolution.likeVarcharFunction(),
                                BOOLEAN,
                                new VariableReferenceExpression(Optional.empty(), "b_varchar",
                                        VARCHAR),
                                call(CAST.name(),
                                        functionAndTypeResolver.lookupCast("CAST", VARCHAR, LIKE_PATTERN),
                                        LIKE_PATTERN,
                                        constant(Slices.utf8Slice("b%"), VARCHAR))));
        SpecialFormExpression secondOrExpression =
                new SpecialFormExpression(SpecialFormExpression.Form.OR,
                        BOOLEAN,
                        call(EQUAL.name(), functionResolution.comparisonFunction(EQUAL, VARCHAR, VARCHAR), BOOLEAN,
                                call("lower",
                                        functionAndTypeResolver.lookupFunction("lower", fromTypes(VARCHAR)),
                                        VARCHAR,
                                        new VariableReferenceExpression(Optional.empty(), "c.e",
                                                VARCHAR)),
                                constant(Slices.utf8Slice("hello world"), VARCHAR)),
                        new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL,
                                BOOLEAN,
                                new VariableReferenceExpression(Optional.empty(), "c", VARCHAR)));
        SpecialFormExpression andExpression = new SpecialFormExpression(SpecialFormExpression.Form.AND,
                BOOLEAN,
                firstOrExpression,
                secondOrExpression);
        Map<VariableReferenceExpression, ColumnHandle> assignments = Map.of(
                new VariableReferenceExpression(Optional.empty(), "a_bigint", BigintType.BIGINT),
                new ClpColumnHandle("a_bigint", BigintType.BIGINT, false),
                new VariableReferenceExpression(Optional.empty(), "b_varchar", VARCHAR),
                new ClpColumnHandle("b_varchar", VARCHAR, false),
                new VariableReferenceExpression(Optional.empty(), "c.e", VARCHAR),
                new ClpColumnHandle("c.e", VARCHAR, false),
                new VariableReferenceExpression(Optional.empty(), "c", VARCHAR),
                new ClpColumnHandle("c", VARCHAR, false));
        ClpExpression clpExpression =
                andExpression.accept(new ClpFilterToKqlConverter(
                                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                                functionAndTypeManager,
                                functionAndTypeManager,
                                assignments),
                        null);
        Optional<String> definition = clpExpression.getDefinition();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        assertTrue(definition.isPresent());
        assertTrue(remainingExpression.isPresent());
        assertEquals(definition.get(), "((a > 0 OR b: \"b*\"))");
        assertEquals(remainingExpression.get(), new SpecialFormExpression(SpecialFormExpression.Form.OR,
                BOOLEAN,
                call(EQUAL.name(), functionResolution.comparisonFunction(EQUAL, VARCHAR, VARCHAR), BOOLEAN,
                        call("lower",

                                functionAndTypeResolver.lookupFunction("lower", fromTypes(VARCHAR)),
                                VARCHAR,
                                new VariableReferenceExpression(Optional.empty(), "c.e",
                                        VARCHAR)),
                        constant(Slices.utf8Slice("hello world"), VARCHAR)),
                new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL,
                        BOOLEAN,
                        new VariableReferenceExpression(Optional.empty(), "c", VARCHAR))));
    }
}
