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

import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;

public class ClpExpression
{
    private final Optional<String> definition;
    private final Optional<RowExpression> remainingExpression;

    public ClpExpression(Optional<String> definition, Optional<RowExpression> remainingExpression)
    {
        this.definition = definition;
        this.remainingExpression = remainingExpression;
    }

    public ClpExpression(String definition)
    {
        this(Optional.of(definition), Optional.empty());
    }

    public ClpExpression(RowExpression remainingExpression)
    {
        this(Optional.empty(), Optional.of(remainingExpression));
    }

    public Optional<String> getDefinition()
    {
        return definition;
    }

    public Optional<RowExpression> getRemainingExpression()
    {
        return remainingExpression;
    }
}
