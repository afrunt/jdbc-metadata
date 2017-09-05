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

    public boolean isForeignFor(TableMetaData other) {
        return other.getForeignKeys().stream()
                .filter(fk -> fk.getForeignKeyMetaData().getForeignTableName().equals(getName()))
                .count() > 0;
    }

    public List<ColumnMetaData> getForeignKeys(){
        return getColumns().stream()
                .filter(ColumnMetaData::isForeignKey)
                .collect(Collectors.toList());
    }

    public boolean isRelatedTo(TableMetaData other) {
        return other.isForeignFor(this) || isForeignFor(other);
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
        return hasCompositePrimaryKey() && getPrimaryKey().getColumns().contains(cm);
    }

    @Override
    public String toString() {
        return fullName();
    }
}
