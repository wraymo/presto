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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.ImmutableSet;
import com.yscope.presto.schema.SchemaNode;
import com.yscope.presto.schema.SchemaTree;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ClpClient
{
    private static final Logger log = Logger.get(ClpClient.class);
    private final ClpConfig config;
    private final Path executablePath;
    private final Map<String, Set<ClpColumnHandle>> tableNameToColumnHandles;
    private final Path decompressDir;
    private Set<String> tableNames;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.tableNameToColumnHandles = new HashMap<>();
        this.executablePath = getExecutablePath();
        this.decompressDir = Paths.get(System.getProperty("java.io.tmpdir"), "clp_decompress");
    }

    @PostConstruct
    public void start()
    {
        try {
            Files.createDirectories(decompressDir);
        }
        catch (IOException e) {
            log.error(e, "Failed to create decompression directory");
        }
    }

    @PreDestroy
    public void close()
    {
        try {
            Files.walkFileTree(decompressDir, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
                {
                    if (exc == null) {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                    else {
                        throw exc; // Directory iteration failed
                    }
                }
            });
        }
        catch (IOException e) {
            log.error(e, "Failed to delete decompression directory");
        }
    }

    public ClpConfig getConfig()
    {
        return config;
    }

    private Path getExecutablePath()
    {
        String executablePathString = config.getClpExecutablePath();
        if (executablePathString == null || executablePathString.isEmpty()) {
            Path executablePath = getExecutablePathFromEnvironment();
            if (executablePath == null) {
                throw new RuntimeException("CLP executable path is not set");
            }
            return executablePath;
        }
        Path executablePath = Paths.get(executablePathString);
        if (!Files.exists(executablePath) || !Files.isRegularFile(executablePath)) {
            executablePath = getExecutablePathFromEnvironment();
            if (executablePath == null) {
                throw new RuntimeException("CLP executable path is not set");
            }
        }
        return executablePath;
    }

    private Path getExecutablePathFromEnvironment()
    {
        String executablePathString = System.getenv("CLP_EXECUTABLE_PATH");
        if (executablePathString == null || executablePathString.isEmpty()) {
            return null;
        }

        Path executablePath = Paths.get(executablePathString);
        if (!Files.exists(executablePath) || !Files.isRegularFile(executablePath)) {
            return null;
        }
        return executablePath;
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
            this.tableNames = ImmutableSet.of();
        }
        return this.tableNames;
    }

    public Set<ClpColumnHandle> listColumns(String tableName)
    {
        if (tableNameToColumnHandles.containsKey(tableName)) {
            return tableNameToColumnHandles.get(tableName);
        }

        Path tableDir = Paths.get(config.getClpArchiveDir(), tableName);
        LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
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

        if (!config.isPolymorphicTypeEnabled()) {
            tableNameToColumnHandles.put(tableName, columnHandles);
            return columnHandles;
        }
        Set<ClpColumnHandle> polymorphicColumnHandles = handlePolymorphicType(columnHandles);
        tableNameToColumnHandles.put(tableName, polymorphicColumnHandles);
        return polymorphicColumnHandles;
    }

    public BufferedReader getRecords(String tableName, Optional<RowExpression> additionalPredicate)
    {
        if (!listTables().contains(tableName)) {
            return null;
        }

        if (additionalPredicate.isPresent()) {
            return searchTable(tableName, additionalPredicate.get());
        }
        else {
            Path decompressFile = decompressDir.resolve(tableName).resolve("original");
            if (!Files.exists(decompressFile) || !Files.isRegularFile(decompressFile)) {
                if (!decompressRecords(tableName)) {
                    return null;
                }
                log.info("Decompress records to %s", decompressFile.toString());
            }

            try {
                return Files.newBufferedReader(decompressFile);
            }
            catch (IOException e) {
                log.error(e, "Failed to get records for table %s", tableName);
                return null;
            }
        }
    }

    private String buildKqlQuery(RowExpression additionalPredicate)
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

    private BufferedReader searchTable(String tableName, RowExpression additionalPredicate)
    {
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);
        String query = buildKqlQuery(additionalPredicate);

        // Spawn search process and read from stdout
        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder(executablePath.toString(),
                            "s",
                            tableArchiveDir.toString(),
                            query);
            Process process = processBuilder.start();
            return new BufferedReader(new InputStreamReader(process.getInputStream()));
        }
        catch (IOException e) {
            log.error(e, "Failed to search records for table %s", tableName);
            return null;
        }
    }

    private boolean decompressRecords(String tableName)
    {
        Path tableDecompressDir = decompressDir.resolve(tableName);
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);

        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder(executablePath.toString(),
                            "x",
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
                    case UnstructuredArray:
                    case NullValue:
                        prestoType = VarcharType.VARCHAR;
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
                            columnHandle.getColumnType(),
                            columnHandle.isNullable()));
                }
            }
        }
        return polymorphicColumnHandles;
    }
}
