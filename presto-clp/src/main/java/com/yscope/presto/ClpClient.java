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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.yscope.presto.schema.SchemaNode;
import com.yscope.presto.schema.SchemaTree;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public class ClpClient
{
    private static final Logger log = Logger.get(ClpClient.class);
    private final ClpConfig config;
    private final Map<String, Set<ClpColumnHandle>> tableNameToColumnHandles;
    private final Map<String, List<String>> tableNameToArchiveIds;
    private final Path decompressDir;
    private Set<String> tableNames;
    private final ClpConfig.InputSource inputSource;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.tableNameToColumnHandles = new HashMap<>();
        this.tableNameToArchiveIds = new HashMap<>();
        this.decompressDir = Paths.get(System.getProperty("java.io.tmpdir"), "clp_decompress");
        this.inputSource = config.getInputSource();
    }

    public ClpConfig getConfig()
    {
        return config;
    }

    public Set<String> listTables()
    {
        if (tableNames != null) {
            return tableNames;
        }
        if (config.getClpArchiveDir() == null || config.getClpArchiveDir().isEmpty()) {
            tableNames = ImmutableSet.of();
            return tableNames;
        }

        if (inputSource == ClpConfig.InputSource.FILESYSTEM) {
            Path archiveDir = Paths.get(config.getClpArchiveDir());
            if (!Files.exists(archiveDir) || !Files.isDirectory(archiveDir)) {
                tableNames = ImmutableSet.of();
                return tableNames;
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(archiveDir)) {
                ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                for (Path path : stream) {
                    if (Files.isDirectory(path)) {
                        tableNames.add(path.getFileName().toString());
                    }
                }
                this.tableNames = tableNames.build();
            }
            catch (Exception e) {
                log.error(e, "Failed to list tables");
                this.tableNames = ImmutableSet.of();
            }
        }
        else if (inputSource == ClpConfig.InputSource.TERRABLOB) {
            try {
                URL url = new URL(config.getClpArchiveDir());
                String queryUrl = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/?prefix=" +
                        url.getPath();

                HttpURLConnection connection = (HttpURLConnection) new URL(queryUrl).openConnection();
                connection.setRequestMethod("GET");
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(connection.getInputStream());
                doc.getDocumentElement().normalize();
                NodeList contents = doc.getElementsByTagName("Contents");
                this.tableNames = IntStream.range(0, contents.getLength())
                        .mapToObj(i -> (Element) contents.item(i))
                        .map(contentElement -> contentElement.getElementsByTagName("Key").item(0).getTextContent())
                        .filter(fullPath -> fullPath.endsWith("/"))
                        .map(fullPath -> {
                            String sanitizedPath = fullPath.substring(0, fullPath.length() - 1);
                            return sanitizedPath.substring(sanitizedPath.lastIndexOf('/') + 1);
                        })
                        .collect(ImmutableSet.toImmutableSet());
            }
            catch (Exception e) {
                this.tableNames = ImmutableSet.of();
                log.error(e, "Failed to list tables");
            }
        }

        return this.tableNames;
    }

    public List<String> listArchiveIds(String tableName)
    {
        if (tableNameToArchiveIds.containsKey(tableName)) {
            return tableNameToArchiveIds.get(tableName);
        }
        Path tableDir = Paths.get(config.getClpArchiveDir(), tableName);
        if (!Files.exists(tableDir) || !Files.isDirectory(tableDir)) {
            return ImmutableList.of();
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir)) {
            ImmutableList.Builder<String> archiveIds = ImmutableList.builder();
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    archiveIds.add(path.getFileName().toString());
                }
            }
            List<String> archiveIdsList = archiveIds.build();
            tableNameToArchiveIds.put(tableName, archiveIdsList);
            return archiveIdsList;
        }
        catch (Exception e) {
            return ImmutableList.of();
        }
    }

    public Set<ClpColumnHandle> listColumns(String tableName)
    {
        if (tableNameToColumnHandles.containsKey(tableName)) {
            return tableNameToColumnHandles.get(tableName);
        }

        LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
        if (config.getInputSource() == ClpConfig.InputSource.FILESYSTEM) {
            Path tableDir = Paths.get(config.getClpArchiveDir(), tableName);
            if (!Files.exists(tableDir) || !Files.isDirectory(tableDir)) {
                return ImmutableSet.of();
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir)) {
                for (Path path : stream) {
                    if (Files.isRegularFile(path)) {
                        continue;
                    }

                    // For each directory, get schema_maps file under it
                    Path schemaMapsFile = path.resolve("schema_tree");
                    if (!Files.exists(schemaMapsFile) || !Files.isRegularFile(schemaMapsFile)) {
                        continue;
                    }

                    columnHandles.addAll(parseSchemaTreeFile(schemaMapsFile));
                }
            }
            catch (Exception e) {
                tableNameToColumnHandles.put(tableName, ImmutableSet.of());
                return ImmutableSet.of();
            }
        }
        else if (config.getInputSource() == ClpConfig.InputSource.TERRABLOB) {
            String schemaFileUrl = config.getClpArchiveDir() + "/" + tableName + "/flattened_schema";
            columnHandles.addAll(parseFlattenedSchemaFile(schemaFileUrl));
        }
        if (!config.isPolymorphicTypeEnabled()) {
            tableNameToColumnHandles.put(tableName, columnHandles);
            return columnHandles;
        }
        Set<ClpColumnHandle> polymorphicColumnHandles = handlePolymorphicType(columnHandles);
        tableNameToColumnHandles.put(tableName, polymorphicColumnHandles);
        return polymorphicColumnHandles;
    }

    public ProcessBuilder getRecords(String tableName, String archiveId, Optional<String> query, List<String> columns)
    {
        if (!listTables().contains(tableName)) {
            return null;
        }

        if (query.isPresent()) {
            return searchTable(tableName, archiveId, query.get(), columns);
        }
        else {
            return searchTable(tableName, archiveId, "*", columns);
        }
    }

    private ProcessBuilder searchTable(String tableName, String archiveId, String query, List<String> columns)
    {
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);
        List<String> argumentList = new ArrayList<>();
        argumentList.add("s");
        argumentList.add(tableArchiveDir.toString());
        argumentList.add("--archive-id");
        argumentList.add(archiveId);
        argumentList.add(query);
        if (!columns.isEmpty()) {
            argumentList.add("--projection");
            argumentList.addAll(columns);
        }
        log.info("Argument list: %s", argumentList.toString());
        return new ProcessBuilder(argumentList);
    }

    private boolean decompressRecords(String tableName)
    {
        Path tableDecompressDir = decompressDir.resolve(tableName);
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);

        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder("x",
                            tableArchiveDir.toString(),
                            tableDecompressDir.toString());
            Process process = processBuilder.start();
            process.waitFor();
            return process.exitValue() == 0;
        }
        catch (IOException | InterruptedException e) {
            log.error(e, "Failed to decompress records for table %s", tableName);
            return false;
        }
    }

    private Set<ClpColumnHandle> parseFlattenedSchemaFile(String schemaFileUrl)
    {
        try {
            HttpURLConnection fileConnection = (HttpURLConnection) new URL(schemaFileUrl).openConnection();
            fileConnection.setRequestMethod("GET");
            InputStream inputStream = fileConnection.getInputStream();
            ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
            DataInputStream dataInputStream = new DataInputStream(zstdInputStream);
            byte[] longBytes = new byte[8];
            byte[] intBytes = new byte[4];
            dataInputStream.readFully(longBytes);
            long numberOfNodes = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
            LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
            for (int i = 0; i < numberOfNodes; i++) {
                dataInputStream.readFully(longBytes);
                long stringSize = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
                byte[] stringBytes = new byte[(int) stringSize];
                dataInputStream.readFully(stringBytes);
                String name = new String(stringBytes, StandardCharsets.UTF_8);
                SchemaNode.NodeType type = SchemaNode.NodeType.fromType(dataInputStream.readByte());
                Type prestoType = null;
                switch (type) {
                    case Integer:
                        prestoType = BigintType.BIGINT;
                        break;
                    case Float:
                        prestoType = DoubleType.DOUBLE;
                        break;
                    case ClpString:
                    case VarString:
                    case DateString:
                    case NullValue:
                        prestoType = VarcharType.VARCHAR;
                        break;
                    case UnstructuredArray:
                        prestoType = new ArrayType(VarcharType.VARCHAR);
                        break;
                    case Boolean:
                        prestoType = BooleanType.BOOLEAN;
                        break;
                    default:
                        break;
                }
                columnHandles.add(new ClpColumnHandle(name, prestoType, true));
            }
            return columnHandles;
        }
        catch (IOException e) {
            return ImmutableSet.of();
        }
    }

    private Set<ClpColumnHandle> parseSchemaTreeFile(Path schemaMapsFile)
    {
        SchemaTree schemaTree = new SchemaTree();
        try (InputStream fileInputStream = Files.newInputStream(schemaMapsFile);
                ZstdInputStream zstdInputStream = new ZstdInputStream(fileInputStream);
                DataInputStream dataInputStream = new DataInputStream(zstdInputStream)) {
            byte[] longBytes = new byte[8];
            byte[] intBytes = new byte[4];
            dataInputStream.readFully(longBytes);
            long numberOfNodes = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
            for (int i = 0; i < numberOfNodes; i++) {
                dataInputStream.readFully(intBytes);
                int parentId = ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).getInt();
                dataInputStream.readFully(longBytes);
                long stringSize = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
                byte[] stringBytes = new byte[(int) stringSize];
                dataInputStream.readFully(stringBytes);
                String name = new String(stringBytes, StandardCharsets.UTF_8);
                SchemaNode.NodeType type = SchemaNode.NodeType.fromType(dataInputStream.readByte());
                schemaTree.addNode(parentId, name, type);
            }

            ArrayList<SchemaNode.NodeTuple> primitiveTypeFields = schemaTree.getPrimitiveFields();
            LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
            for (SchemaNode.NodeTuple nodeTuple : primitiveTypeFields) {
                SchemaNode.NodeType nodeType = nodeTuple.getType();
                Type prestoType = null;
                switch (nodeType) {
                    case Integer:
                        prestoType = BigintType.BIGINT;
                        break;
                    case Float:
                        prestoType = DoubleType.DOUBLE;
                        break;
                    case ClpString:
                    case VarString:
                    case DateString:
                    case NullValue:
                        prestoType = VarcharType.VARCHAR;
                        break;
                    case UnstructuredArray:
                        prestoType = new ArrayType(VarcharType.VARCHAR);
                        break;
                    case Boolean:
                        prestoType = BooleanType.BOOLEAN;
                        break;
                    default:
                        break;
                }
                columnHandles.add(new ClpColumnHandle(nodeTuple.getName(), prestoType, true));
            }
            return columnHandles;
        }
        catch (IOException e) {
            return ImmutableSet.of();
        }
    }

    private Set<ClpColumnHandle> handlePolymorphicType(Set<ClpColumnHandle> columnHandles)
    {
        Map<String, List<ClpColumnHandle>> columnNameToColumnHandles = new HashMap<>();
        LinkedHashSet<ClpColumnHandle> polymorphicColumnHandles = new LinkedHashSet<>();

        for (ClpColumnHandle columnHandle : columnHandles) {
            columnNameToColumnHandles.computeIfAbsent(columnHandle.getColumnName(), k -> new ArrayList<>())
                    .add(columnHandle);
        }
        for (Map.Entry<String, List<ClpColumnHandle>> entry : columnNameToColumnHandles.entrySet()) {
            List<ClpColumnHandle> columnHandleList = entry.getValue();
            if (columnHandleList.size() == 1) {
                polymorphicColumnHandles.add(columnHandleList.get(0));
            }
            else {
                for (ClpColumnHandle columnHandle : columnHandleList) {
                    polymorphicColumnHandles.add(new ClpColumnHandle(
                            columnHandle.getColumnName() + "_" + columnHandle.getColumnType().getDisplayName(),
                            columnHandle.getColumnName(),
                            columnHandle.getColumnType(),
                            columnHandle.isNullable()));
                }
            }
        }
        return polymorphicColumnHandles;
    }
}
