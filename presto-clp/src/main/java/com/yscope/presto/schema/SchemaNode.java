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
package com.yscope.presto.schema;

import java.util.ArrayList;
import java.util.Objects;

public class SchemaNode
{
    private final int id;
    private final int parentId;
    private final ArrayList<Integer> childrenIds;
    private final String name;
    private final NodeType type;
    public SchemaNode(int id, int parentId, String name, NodeType type)
    {
        this.id = id;
        this.parentId = parentId;
        this.name = name;
        this.type = type;
        this.childrenIds = new ArrayList<>();
    }

    public String getName()
    {
        return name;
    }

    public NodeType getType()
    {
        return type;
    }

    public int getId()
    {
        return id;
    }

    public int getParentId()
    {
        return parentId;
    }

    public void addChild(int id)
    {
        childrenIds.add(id);
    }

    public ArrayList<Integer> getChildrenIds()
    {
        return childrenIds;
    }

    public enum NodeType
    {
        Integer, Float, ClpString, VarString, Boolean, Object, UnstructuredArray, NullValue, DateString, StructuredArray
    }

    public static class NodeTuple
    {
        private final int parentId;
        private final String name;
        private final NodeType type;

        public NodeTuple(int parentId, String name, NodeType type)
        {
            this.parentId = parentId;
            this.name = name;
            this.type = type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeTuple tuple = (NodeTuple) o;
            return Objects.equals(type, tuple.type) && Objects.equals(parentId, tuple.parentId) && Objects.equals(name, tuple.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, parentId, name);
        }
    }
}
