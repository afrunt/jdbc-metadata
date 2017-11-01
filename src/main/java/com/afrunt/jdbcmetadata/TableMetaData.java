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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Andrii Frunt
 */
public class TableMetaData implements WithName, WithPrimaryKey, WithIndexes {
    private String name;
    private String schemaName;
    private PrimaryKeyMetaData primaryKey;
    private List<ColumnMetaData> columns = new ArrayList<>();
    private List<IndexMetaData> indexes = new ArrayList<>();

    public String fullName() {
        if (getSchemaName() != null) {
            return getSchemaName() + "." + getName();
        } else {
            return getName();
        }
    }

    public String getName() {
        return name;
    }

    public TableMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public PrimaryKeyMetaData getPrimaryKey() {
        return primaryKey;
    }

    public TableMetaData setPrimaryKey(PrimaryKeyMetaData primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public TableMetaData setColumns(List<ColumnMetaData> columns) {
        this.columns = columns;
        return this;
    }

    public TableMetaData addColumn(ColumnMetaData cm) {
        columns = Optional.ofNullable(getColumns()).orElse(new ArrayList<>());
        columns.add(cm);
        return this;
    }


    public String getSchemaName() {
        return schemaName;
    }

    public TableMetaData setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public boolean hasForeignFor(TableMetaData other) {
        return other.getForeignKeys().stream()
                .filter(fk -> fk.getForeignKeyMetaData().getForeignTableName().equals(getName()))
                .count() > 0;
    }

    public List<ColumnMetaData> getForeignKeys() {
        return getColumns().stream()
                .filter(ColumnMetaData::isForeignKey)
                .collect(Collectors.toList());
    }

    public boolean isRelatedTo(TableMetaData other) {
        return other.hasForeignFor(this) || hasForeignFor(other);
    }

    public List<ColumnMetaData> getForeignKeysForTable(String schema, String tableName) {
        return getForeignKeys().stream()
                .filter(c ->
                        tableName.equals(c.getForeignKeyMetaData().getForeignTableName())
                                && schema.equals(c.getForeignKeyMetaData().getForeignTableSchema())
                )
                .collect(Collectors.toList());
    }

    public List<ColumnMetaData> getForeignKeysForTable(TableMetaData table) {
        return getForeignKeysForTable(table.getSchemaName(), table.getName());
    }

    public boolean dependsOn(TableMetaData other) {
        return getForeignKeysForTable(other).size() > 0;
    }


    public boolean sameName(TableMetaData other) {
        return fullName().equals(other.fullName());
    }

    public boolean sameName(String otherName) {
        return getName().equals(otherName);
    }

    public boolean hasAllColumns(Collection<String> columnNames) {
        if (columnNames != null && !columnNames.isEmpty()) {
            for (String columnName : columnNames) {
                if (!hasColumn(columnName)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public List<ColumnMetaData> getBlobColumns() {
        return columns.stream().filter(ColumnMetaData::isBLOB).collect(Collectors.toList());
    }

    public List<String> columnNames() {
        return columns().stream().map(ColumnMetaData::getName).collect(Collectors.toList());
    }

    public boolean hasBlobs() {
        return columns.stream().filter(ColumnMetaData::isBLOB).count() > 0;
    }

    public boolean hasColumn(String columnName) {
        return columns().stream().map(c -> c.nameIs(columnName)).reduce(false, (b1, b2) -> b1 || b2);
    }

    public List<String> getRelatedTables() {
        return getForeignKeys().stream()
                .map(fk -> fk.getForeignKeyMetaData().getForeignTableName())
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableMetaData that = (TableMetaData) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
        if (primaryKey != null ? !primaryKey.equals(that.primaryKey) : that.primaryKey != null) return false;
        return columns != null ? columns.equals(that.columns) : that.columns == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
        result = 31 * result + (primaryKey != null ? primaryKey.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }

    public List<IndexMetaData> getIndexes() {
        return indexes;
    }

    public TableMetaData setIndexes(List<IndexMetaData> indexes) {
        this.indexes = indexes;
        return this;
    }

    public List<IndexMetaData> columnIndexes(String columnName) {
        return indexes().stream()
                .filter(i -> i.isForColumn(columnName))
                .collect(Collectors.toList());
    }

    public boolean partOfCompositeKey(ColumnMetaData cm) {
        return hasCompositePrimaryKey() && getPrimaryKey().getColumns().stream()
                .filter(c -> c.nameIs(cm.getName()) && nameIs(cm.getTableName())).count() > 0;
    }

    @Override
    public String toString() {
        return fullName();
    }
}
