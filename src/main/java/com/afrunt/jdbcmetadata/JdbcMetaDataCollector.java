/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.afrunt.jdbcmetadata;


import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * @author Andrii Frunt
 */
public class JdbcMetaDataCollector implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(JdbcMetaDataCollector.class.getName());
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private Map<String, TableMetaData> tablesMetaDataCache = new ConcurrentHashMap<>();
    private BiFunction<String, String, Boolean> skipTables = (s, t) -> false;
    private boolean skipIndexes = false;
    private Map<String, Set<String>> tableNames = new HashMap<>();

    private Set<String> allSchemaNames = new HashSet<>();
    private boolean skipSequences;
    private DatabaseStrategy databaseStrategy;
    private ProgressMonitor progressMonitor;
    private boolean quoteTableNames;

    private DataSource dataSource;
    private int parallelism = 1;

    private ExecutorService internalPool;
    private ExecutorService pool;

    public JdbcDatabaseMetaData collectDatabaseMetaData() {
        return collectDatabaseMetaData(s -> true);
    }

    public JdbcDatabaseMetaData collectDatabaseMetaData(Predicate<String> schemaFilter) {
        StopWatch sw = new StopWatch().start();
        JdbcDatabaseMetaData databaseMetaData = new JdbcDatabaseMetaData();
        List<String> allSchemaNames = findAllSchemaNames();
        List<String> filteredSchemas = allSchemaNames.stream()
                .filter(schemaFilter)
                .collect(Collectors.toList());

        if (progressMonitor != null) {
            progressMonitor.collectionStarted(filteredSchemas);
        }

        List<SchemaMetaData> schemas;

        try {
            schemas = getInternalPool().submit(() -> allSchemaNames.parallelStream()
                    .filter(schemaFilter)
                    .map(this::collectSchemaMetaData)
                    .collect(Collectors.toList())
            ).get();
        } catch (Exception e) {
            throw new JdbcMetaDataException(e);
        }


        JdbcDatabaseMetaData jdbcDatabaseMetaData = populateExtraDatabaseData(databaseMetaData)
                .setSchemas(schemas);

        long totalTimeMillis = sw.stop().getTotalTimeMillis();
        if (progressMonitor != null) {
            progressMonitor.databaseMetadataCollected(databaseMetaData, totalTimeMillis);
        }
        return jdbcDatabaseMetaData;
    }

    public SchemaMetaData collectSchemaMetaData(String schema) {
        info("Collecting metadata for schema: %s", schema);
        StopWatch sw = new StopWatch().start();
        if (schemaExists(schema)) {
            //ResultSet tables = databaseMetaData.getTables(null, schema, "%", new String[]{"TABLE"});
            //JdbcUtil.printResultSet(tables);

            SchemaMetaData schemaMetaData = new SchemaMetaData()
                    .setName(schema)
                    .setSequences(collectSequencesMetaData(schema))
                    .setTables(collectTablesMetaDataForSchema(schema));

            if (progressMonitor != null) {
                progressMonitor.schemaMetaDataCollected(schemaMetaData, sw.stop().getTotalTimeMillis());
            }

            return schemaMetaData;

        } else {
            throw new JdbcMetaDataException("Schema not found" + schema);
        }
    }

    public TableMetaData collectTableMetaData(String tableName) {
        return collectTableMetaData(tableName, null);
    }

    public TableMetaData collectTableMetaData(String tableName, String schema) {
        StopWatch sw = new StopWatch().start();
        String fullTableName = fullTableName(schema, tableName);

        if (tablesMetaDataCache.containsKey(fullTableName)) {
            return tablesMetaDataCache.get(fullTableName);
        }

        debug("Collecting metadata for table: %s", fullTableName);
        String query = "SELECT * FROM " + fullTableName + " WHERE 1<>1";

        try {
            Connection conn = getConnection();
            PreparedStatement stmt;

            TableMetaData tableMetaData = new TableMetaData()
                    .setName(tableName)
                    .setSchemaName(schema);
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsMetaData = rs.getMetaData();
            List<String> primaryKeys = findPrimaryKeys(tableName, schema);
            Map<String, ForeignKeyMetaData> foreignKeys = findForeignKeys(tableName, schema);
            List<IndexMetaData> indexes = new ArrayList<>();
            if (!skipIndexes) {
                indexes = findIndexes(tableName, schema);
                tableMetaData.setIndexes(indexes);
            }

            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                ColumnMetaData columnMetadata = createColumnMetadata(tableName, rsMetaData, i, primaryKeys, indexes);
                tableMetaData.addColumn(columnMetadata);
                ForeignKeyMetaData foreignKeyMetaData = foreignKeys.get(columnMetadata.getName());
                columnMetadata.setForeignKeyMetaData(foreignKeyMetaData);
            }

            if (!primaryKeys.isEmpty()) {
                PrimaryKeyMetaData primaryKey = new PrimaryKeyMetaData(tableMetaData.filterColumns(ColumnMetaData::isPrimaryKey));
                tableMetaData.setPrimaryKey(primaryKey);
            }
            tablesMetaDataCache.put(fullTableName, tableMetaData);

            info("Table %s metadata collected in %dms", tableName, sw.stop().getTotalTimeMillis());
            if (progressMonitor != null) {
                progressMonitor.tableMetadataCollected(tableMetaData, sw.getTotalTimeMillis());
            }
            return tableMetaData;

        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcMetaDataException("Error getting metadata for table " + fullTableName, e);
        }
    }

    public JdbcMetaDataCollector quoteTableNames(boolean value) {
        this.quoteTableNames = value;
        return this;
    }

    private String fullTableName(String schema, String tableName) {
        tableName = quoteTableNames ? '"' + tableName + '"' : tableName;
        if (schema != null && !"".equals(schema.trim())) {
            schema = schema + ".";
        } else {
            schema = "";
        }

        return schema + tableName;
    }

    private ColumnMetaData createColumnMetadata(String tableName, ResultSetMetaData rs, int index, List<String> primaryKeys, List<IndexMetaData> indexes) throws SQLException, ClassNotFoundException {
        StopWatch sw = new StopWatch().start();
        String columnClassName = rs.getColumnClassName(index);
        Class<?> clazz = null;
        try {
            clazz = Class.forName(columnClassName);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error getting class for name " + columnClassName, e);
        }

        String columnName = rs.getColumnName(index);
        List<IndexMetaData> columnIndexes = indexes.stream().filter(i -> i.columnNames().contains(columnName)).collect(Collectors.toList());

        ColumnMetaData columnMetaData = new ColumnMetaData()
                .setName(columnName)
                .setSqlType(rs.getColumnType(index))
                .setReadOnly(rs.isReadOnly(index))
                .setWritable(rs.isWritable(index))
                .setSqlTypeName(rs.getColumnTypeName(index))
                .setPrecision(rs.getPrecision(index))
                .setJavaType(clazz)
                .setScale(rs.getScale(index))
                .setTableName(tableName)
                .setAutoIncrement(rs.isAutoIncrement(index))
                .setNullable(rs.isNullable(index) == ResultSetMetaData.columnNullable)
                .setPrimaryKey(primaryKeys.contains(columnName))
                .setIndexes(columnIndexes);

        debug("Column %s metadata created in %dms", columnName, sw.stop().getTotalTimeMillis());

        return columnMetaData;
    }

    private List<IndexMetaData> findIndexes(String tableName, String schema) {
        try {
            DatabaseMetaData databaseMetaData = getDatabaseMetaData();
            ResultSet rs = databaseMetaData.getIndexInfo(null, schema, tableName, false, false);
            //JdbcUtil.printResultSet(rs);

            Map<String, IndexMetaData> indexNameMap = new HashMap<>();

            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                IndexMetaData indexMetaData = indexNameMap.computeIfAbsent(indexName, s -> new IndexMetaData());
                indexMetaData
                        .setName(indexName)
                        .setType(rs.getInt("INDEX_TYPE"))
                        .setCardinality(rs.getInt("CARDINALITY"))
                        .setPages(rs.getInt("PAGES"))
                        .setUnique("FALSE".equals(rs.getString("NON_UNIQUE")))
                        .addIndexColumn(createIndexColumnMetadata(rs))
                ;
            }

            return new ArrayList<>(indexNameMap.values());
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting indexes for " + tableName, e);
        }
    }

    private IndexColumnMetadata createIndexColumnMetadata(ResultSet rs) {
        try {
            return new IndexColumnMetadata()
                    .setName(rs.getString("COLUMN_NAME"))
                    .setAscending("A".equals(rs.getString("ASC_OR_DESC")))
                    .setOrdinalPosition(rs.getInt("ORDINAL_POSITION"))
                    .setSortType(rs.getInt("SORT_TYPE"))
                    ;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error creating index column metadata", e);
        }
    }

    private List<String> findPrimaryKeys(String tableName, String schema) {
        StopWatch sw = new StopWatch().start();
        try {
            ResultSet rs = getDatabaseMetaData().getPrimaryKeys(null, schema, tableName.toUpperCase());
            List<String> fks = new ArrayList<>();

            while (rs.next()) {
                fks.add(rs.getString(4));
            }

            debug("Primary keys for table %s found. Took %dms", tableName, sw.stop().getTotalTimeMillis());
            return fks;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting primary keys for " + tableName, e);
        }
    }

    private Map<String, ForeignKeyMetaData> findForeignKeys(String tableName, String schema) {
        StopWatch sw = new StopWatch().start();
        try {
            ResultSet rs = databaseMetaData.getImportedKeys(null, schema, tableName.toUpperCase());
            Map<String, ForeignKeyMetaData> map = new HashMap<>();
            //JdbcUtil.printResultSet(rs);
            while (rs.next()) {
                String foreignTableSchema = rs.getString(2);
                String foreignTableName = rs.getString(3);
                String foreignColumnName = rs.getString(4);
                Integer updateRule = rs.getInt(10);
                Integer deleteRule = rs.getInt(11);
                ForeignKeyMetaData fk = new ForeignKeyMetaData()
                        .setName(rs.getString(columnName("FK_NAME")))
                        .setForeignTableName(foreignTableName)
                        .setForeignColumnName(foreignColumnName)
                        .setUpdateRule(updateRule)
                        .setDeleteRule(deleteRule)
                        .setForeignTableSchema(foreignTableSchema);

                map.put(rs.getString("FKCOLUMN_NAME"), fk);
            }

            debug("Foreign keys for table %s found. Took %dms", tableName, sw.stop().getTotalTimeMillis());

            return map;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting foreign keys for " + tableName, e);
        }
    }

    private boolean schemaExists(String schema) {
        if (allSchemaNames != null && allSchemaNames.contains(schema)) {
            return true;
        }
        StopWatch sw = new StopWatch().start();
        try {
            DatabaseMetaData databaseMetaData = getDatabaseMetaData();
            schema = schema.toUpperCase();
            ResultSet rs = databaseMetaData.getSchemas(null, schema);

            boolean exists = false;

            while (rs.next()) {
                exists = true;
            }
            if (exists) {
                allSchemaNames.add(schema);
            }
            debug("Schema existence check took %dms", sw.stop().getTotalTimeMillis());
            return exists;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error checking the existence of schema " + schema, e);
        }
    }

    private List<TableMetaData> collectTablesMetaDataForSchema(String schema) {
        try {
            return getInternalPool().submit(() -> findTableNamesForSchema(schema).parallelStream()
                    .filter(tn -> !skipTables.apply(schema, tn))
                    .map(tn -> collectTableMetaData(tn, schema))
                    .collect(Collectors.toList())).get();
        } catch (Exception e) {
            throw new JdbcMetaDataException(e);
        }
    }

    private List<String> findTableNamesForSchema(String schema) {
        StopWatch sw = new StopWatch().start();
        Set<String> names = tableNames.get(schema);

        if (names != null) {
            debug("All table names for schema %s cached %dms", schema, sw.stop().getTotalTimeMillis());
            return new ArrayList<>(names);
        }

        try {
            DatabaseMetaData databaseMetaData = getDatabaseMetaData();
            ResultSet rs = databaseMetaData.getTables(null, schema, "%", new String[]{"TABLE"});
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
            debug("All table names for schema %s cached %dms", schema, sw.stop().getTotalTimeMillis());
            tableNames.put(schema, new HashSet<>(tables));
            return tables;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting table names for schema " + schema, e);
        }
    }

    private DatabaseMetaData getDatabaseMetaData() {
        try {
            databaseMetaData = ofNullable(databaseMetaData).orElse(getConnection().getMetaData());
            return databaseMetaData;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting DB metadata", e);
        }
    }

    public List<SequenceMetaData> collectSequencesMetaData(String schemaName) {
        if (!skipSequences && databaseStrategy != null) {

            if (dataSource != null) {
                databaseStrategy.setDataSource(dataSource);
            } else {
                databaseStrategy.setConnection(getConnection());
            }

            return databaseStrategy
                    .collectSequencesMetaData(schemaName);
        } else {
            return Collections.emptyList();
        }
    }

    private List<String> findAllSchemaNames() {
        StopWatch sw = new StopWatch().start();

        if (allSchemaNames != null && !allSchemaNames.isEmpty()) {
            debug("All schemas names found in cache %dms", sw.stop().getTotalTimeMillis());
            return new ArrayList<>(allSchemaNames);
        }
        try {
            ResultSet rs = getDatabaseMetaData().getSchemas();
            //JdbcUtil.printResultSet(rs);

            List<String> schemaNames = new ArrayList<>();
            while (rs.next()) {
                schemaNames.add(rs.getString(1));
            }

            allSchemaNames = new HashSet<>(allSchemaNames);
            allSchemaNames.addAll(schemaNames);

            debug("All schemas names found in %dms", sw.stop().getTotalTimeMillis());
            return schemaNames;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting all schema names", e);
        }
    }

    private Connection getConnection() {
        if (dataSource != null) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (connection != null) {
            return connection;
        } else {
            throw new IllegalStateException("SQL data source or connection should be provided");
        }
    }

    private JdbcDatabaseMetaData populateExtraDatabaseData(JdbcDatabaseMetaData jdbcDatabaseMetaData) {
        try {
            DatabaseMetaData md = getDatabaseMetaData();
            return jdbcDatabaseMetaData
                    .setDatabaseProductName(md.getDatabaseProductName())
                    ;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error populating extra database data", e);
        }
    }

    private String columnName(String name) throws SQLException {
        int maxColumnNameLength = getDatabaseMetaData().getMaxColumnNameLength();
        if (maxColumnNameLength < name.length() && maxColumnNameLength > 0) {
            return name.substring(0, maxColumnNameLength);
        } else {
            return name;
        }
    }

    public JdbcMetaDataCollector skipTables(BiFunction<String, String, Boolean> skipTables) {
        if (skipTables != null) {
            this.skipTables = skipTables;
        }
        return this;
    }

    public JdbcMetaDataCollector setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public JdbcMetaDataCollector setSkipIndexes(boolean skipIndexes) {
        this.skipIndexes = skipIndexes;
        return this;
    }

    public JdbcMetaDataCollector setSkipSequences(boolean skipSequences) {
        this.skipSequences = skipSequences;
        return this;
    }

    public DatabaseStrategy getDatabaseStrategy() {
        return databaseStrategy;
    }

    public JdbcMetaDataCollector setDatabaseStrategy(DatabaseStrategy databaseStrategy) {
        this.databaseStrategy = databaseStrategy;
        return this;
    }

    public ProgressMonitor getProgressMonitor() {
        return progressMonitor;
    }

    public JdbcMetaDataCollector setProgressMonitor(ProgressMonitor progressMonitor) {
        this.progressMonitor = progressMonitor;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public JdbcMetaDataCollector setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public int getParallelism() {
        return parallelism;
    }

    public JdbcMetaDataCollector setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public ExecutorService getPool() {
        return pool;
    }

    public JdbcMetaDataCollector setPool(ExecutorService pool) {
        this.pool = pool;
        return this;
    }

    private Object[] array(Object... args) {
        return args;
    }

    private ExecutorService getInternalPool() {
        if (internalPool != null) {
            return internalPool;
        }

        if (dataSource == null) {
            internalPool = Executors.newFixedThreadPool(1);
            return internalPool;
        }

        if (pool != null && dataSource != null) {
            internalPool = pool;
            return internalPool;
        }

        if (parallelism > 1 && dataSource != null) {
            internalPool = new ForkJoinPool(parallelism);
            return internalPool;
        }

        internalPool = new ForkJoinPool(1);
        return internalPool;
    }

    private void debug(String message, Object... params) {
        log(Level.FINE, message, params);
    }

    private void info(String message, Object... params) {
        log(Level.INFO, message, params);
    }

    private void log(Level level, String message, Object... params) {
        LOG.log(level, String.format(message, params));
    }

    @Override
    public void close() {
        if(pool == null) {
            internalPool.shutdownNow();
        }
    }
}
