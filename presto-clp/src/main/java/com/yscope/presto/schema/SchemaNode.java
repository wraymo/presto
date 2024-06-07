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
        Integer((byte) 0),
        Float((byte) 1),
        ClpString((byte) 2),
        VarString((byte) 3),
        Boolean((byte) 4),
        Object((byte) 5),
        UnstructuredArray((byte) 6),
        NullValue((byte) 7),
        DateString((byte) 8),
        StructuredArray((byte) 9);

        private final byte type;

        NodeType(byte type)
        {
            this.type = type;
        }

        public static NodeType fromType(byte type)
        {
            for (NodeType status : NodeType.values()) {
                if (status.getType() == type) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid type code: " + type);
        }

        public byte getType()
        {
            return type;
        }
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

        public NodeTuple(String name, NodeType type)
        {
            this(-1, name, type);
        }

        public NodeType getType()
        {
            return type;
        }

        public int getParentId()
        {
            return parentId;
        }

        public String getName()
        {
            return name;
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
            return Objects.equals(type, tuple.type) &&
                    Objects.equals(parentId, tuple.parentId) &&
                    Objects.equals(name, tuple.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, parentId, name);
        }
    }
}
