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
import java.util.HashMap;
import java.util.Map;

public class SchemaTree
{
    private final ArrayList<SchemaNode> schemaNodes;
    private final Map<SchemaNode.NodeTuple, Integer> nodeMap;
    private final ArrayList<SchemaNode.NodeTuple> primitiveTypeFields;

    public SchemaTree()
    {
        schemaNodes = new ArrayList<>();
        primitiveTypeFields = new ArrayList<>();
        nodeMap = new HashMap<>();
    }

    public ArrayList<SchemaNode.NodeTuple> getPrimitiveFields()
    {
        return primitiveTypeFields;
    }

    public int addNode(int parentId, String name, SchemaNode.NodeType type)
    {
        SchemaNode.NodeTuple tuple = new SchemaNode.NodeTuple(parentId, name, type);
        if (nodeMap.containsKey(tuple)) {
            return nodeMap.get(tuple);
        }

        int id = schemaNodes.size();
        schemaNodes.add(new SchemaNode(id, parentId, name, type));
        nodeMap.put(tuple, id);

        if (parentId >= 0) {
            schemaNodes.get(parentId).addChild(id);
        }

        if (type != SchemaNode.NodeType.Object) {
            primitiveTypeFields.add(new SchemaNode.NodeTuple(getKeyName(id, name), type));
        }
        return id;
    }

    private String getKeyName(int id, String key)
    {
        SchemaNode node = schemaNodes.get(id);
        if (node.getParentId() < 0) {
            return key;
        }

        return getKeyName(node.getParentId(), node.getName() + "." + key);
    }
}
