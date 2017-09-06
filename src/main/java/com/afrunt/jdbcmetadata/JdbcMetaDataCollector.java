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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * @author Andrii Frunt
 */
public class JdbcMetaDataCollector {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetaDataCollector.class);
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private Map<String, TableMetaData> tablesMetaDataCache = new ConcurrentHashMap<>();
    private Map<String, SchemaMetaData> schemaMetaDataCache = new ConcurrentHashMap<>();
    private BiFunction<String, String, Boolean> skipTables = (s, t) -> false;
    private boolean skipIndexes = false;
    private String defaultSchemaName;

    public JdbcDatabaseMetaData collectDatabaseMetaData() {
        return collectDatabaseMetaData(s -> true);
    }

    public JdbcDatabaseMetaData collectDatabaseMetaData(Predicate<String> schemaFilter) {
        JdbcDatabaseMetaData databaseMetaData = new JdbcDatabaseMetaData();
        List<SchemaMetaData> schemas = findAllSchemaNames().stream()
                .filter(schemaFilter)
                //.parallel()
                .map(this::collectSchemaMetaData)
                .collect(Collectors.toList());


        return populateExtraDatabaseData(databaseMetaData)
                .setSchemas(schemas)
                ;

    }

    public SchemaMetaData collectSchemaMetaData(String schema) {
        LOG.debug("Collecting metadata for schema: {}", schema);

        boolean alreadyCached = schemaMetaDataCache.containsKey(schema);
        if (alreadyCached || schemaExists(schema)) {
            //ResultSet tables = databaseMetaData.getTables(null, schema, "%", new String[]{"TABLE"});
            //JdbcUtil.printResultSet(tables);
            if (alreadyCached) {
                return schemaMetaDataCache.get(schema);
            } else {
                SchemaMetaData schemaMetaData = new SchemaMetaData()
                        .setName(schema)
                        .setTables(collectTablesMetaDataForSchema(schema));
                schemaMetaDataCache.put(schema, schemaMetaData);
                return schemaMetaData;
            }
        } else {
            throw new JdbcMetaDataException("Schema not found" + schema);
        }
    }

    public TableMetaData collectTableMetaData(String tableName) {
        return collectTableMetaData(tableName, null);
    }

    public TableMetaData collectTableMetaData(String tableName, String schema) {

        String fullTableName = tableName;
        if (schema != null && !"".equals(schema.trim())) {
            fullTableName = schema + "." + tableName;
        }

        if (tablesMetaDataCache.containsKey(fullTableName)) {
            return tablesMetaDataCache.get(fullTableName);
        }

        LOG.debug("Collecting metadata for table: {}", fullTableName);

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
                ColumnMetaData columnMetadata = createColumnMetadata(rsMetaData, i, primaryKeys, indexes);
                tableMetaData.addColumn(columnMetadata);
                ForeignKeyMetaData foreignKeyMetaData = foreignKeys.get(columnMetadata.getName());
                columnMetadata.setForeignKeyMetaData(foreignKeyMetaData);
            }

            if (!primaryKeys.isEmpty()) {
                PrimaryKeyMetaData primaryKey = new PrimaryKeyMetaData(tableMetaData.filterColumns(ColumnMetaData::isPrimaryKey));
                tableMetaData.setPrimaryKey(primaryKey);
            }
            tablesMetaDataCache.put(fullTableName, tableMetaData);
            return tableMetaData;

        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcMetaDataException("Error getting metadata for table " + fullTableName, e);
        }
    }

    private ColumnMetaData createColumnMetadata(ResultSetMetaData rs, int index, List<String> primaryKeys, List<IndexMetaData> indexes) throws SQLException, ClassNotFoundException {
        String columnClassName = rs.getColumnClassName(index);
        Class<?> clazz = null;
        try {
            clazz = Class.forName(columnClassName);
        } catch (Exception e) {
            LOG.error("Error getting class for name {}", columnClassName);
        }
        
        String columnName = rs.getColumnName(index);
        List<IndexMetaData> columnIndexes = indexes.stream().filter(i -> i.columnNames().contains(columnName)).collect(Collectors.toList());

        return new ColumnMetaData()
                .setName(columnName)
                .setSqlType(rs.getColumnType(index))
                .setSqlTypeName(rs.getColumnTypeName(index))
                .setPrecision(rs.getPrecision(index))
                .setJavaType(clazz)
                .setScale(rs.getScale(index))
                .setTableName(rs.getTableName(index))
                .setAutoIncrement(rs.isAutoIncrement(index))
                .setNullable(rs.isNullable(index) == ResultSetMetaData.columnNullable)
                .setPrimaryKey(primaryKeys.contains(columnName))
                .setIndexes(columnIndexes);
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
        try {
            ResultSet rs = getDatabaseMetaData().getPrimaryKeys(null, schema, tableName.toUpperCase());
            List<String> fks = new ArrayList<>();

            while (rs.next()) {
                fks.add(rs.getString(4));
            }

            return fks;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting primary keys for " + tableName, e);
        }
    }

    private Map<String, ForeignKeyMetaData> findForeignKeys(String tableName, String schema) {
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
            return map;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting foreign keys for " + tableName, e);
        }
    }

    private boolean schemaExists(String schema) {
        try {
            DatabaseMetaData databaseMetaData = getDatabaseMetaData();
            schema = schema.toUpperCase();
            ResultSet rs = databaseMetaData.getSchemas(null, schema);

            boolean exists = false;

            while (rs.next()) {
                exists = true;
            }

            return exists;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error checking the existence of schema " + schema, e);
        }
    }

    private List<TableMetaData> collectTablesMetaDataForSchema(String schema) {
        return findTableNamesForSchema(schema).stream()
                .filter(tn -> !skipTables.apply(schema, tn))
                //.parallel()
                .map(tn -> collectTableMetaData(tn, schema))
                .collect(Collectors.toList());
    }

    private List<String> findTableNamesForSchema(String schema) {
        try {
            DatabaseMetaData databaseMetaData = getDatabaseMetaData();
            ResultSet rs = databaseMetaData.getTables(null, schema, "%", new String[]{"TABLE"});
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
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

    private List<String> findAllSchemaNames() {
        try {
            ResultSet rs = getDatabaseMetaData().getSchemas();
            //JdbcUtil.printResultSet(rs);

            List<String> schemaNames = new ArrayList<>();
            while (rs.next()) {
                schemaNames.add(rs.getString(1));
            }
            return schemaNames;
        } catch (SQLException e) {
            throw new JdbcMetaDataException("Error getting all schema names", e);
        }
    }

    private Connection getConnection() {
        return connection;
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
}
