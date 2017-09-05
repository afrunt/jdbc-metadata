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

import java.sql.Types;
import java.util.List;

/**
 * @author Andrii Frunt
 */
public class ColumnMetaData implements WithName, WithType, WithIndexes {
    private String tableName;

    private String name;
    private int sqlType = -777;
    private String sqlTypeName;
    private Class<?> javaType;

    private boolean nullable;
    private boolean autoIncrement;

    private int scale;
    private int precision;

    private boolean primaryKey;
    private ForeignKeyMetaData foreignKeyMetaData;
    private List<IndexMetaData> indexes;

    public boolean sqlTypeNameIs(String typeName) {
        return typeName.equals(getSqlTypeName());
    }

    public boolean sqlTypeIs(int sqlType) {
        return getSqlType() == sqlType;
    }

    public String getName() {
        return name;
    }

    public ColumnMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public int getSqlType() {
        return sqlType;
    }

    public ColumnMetaData setSqlType(int sqlType) {
        this.sqlType = sqlType;
        return this;
    }

    public boolean isNullable() {
        return nullable;
    }

    public ColumnMetaData setNullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public int getScale() {
        return scale;
    }

    public boolean scaleIs(int scale) {
        return getScale() == scale;
    }

    public ColumnMetaData setScale(int scale) {
        this.scale = scale;
        return this;
    }

    public int getPrecision() {
        return precision;
    }

    public boolean precisionIs(int precision) {
        return getPrecision() == precision;
    }

    public ColumnMetaData setPrecision(int precision) {
        this.precision = precision;
        return this;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public ColumnMetaData setJavaType(Class<?> javaType) {
        this.javaType = javaType;
        return this;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public ColumnMetaData setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public boolean isForeignKey() {
        return foreignKeyMetaData != null;
    }


    @Override
    public Class<?> getType() {
        return javaType;
    }

    public boolean isLargeObject() {
        return isBLOB() || isCLOB() || isNCLOB();
    }

    public boolean isBLOB() {
        return sqlTypeIs(Types.BLOB);
    }

    public boolean isCLOB() {
        return sqlTypeIs(Types.CLOB);
    }

    public boolean isNCLOB() {
        return sqlTypeIs(Types.NCLOB);
    }

    public boolean isVARCHAR() {
        return sqlTypeIs(Types.VARCHAR) || sqlTypeIs(Types.NVARCHAR) || sqlTypeIs(Types.LONGVARCHAR) || sqlTypeIs(Types.LONGNVARCHAR);
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public ColumnMetaData setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
        return this;
    }

    public String getSqlTypeName() {
        return sqlTypeName;
    }

    public ColumnMetaData setSqlTypeName(String sqlTypeName) {
        this.sqlTypeName = sqlTypeName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public ColumnMetaData setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ForeignKeyMetaData getForeignKeyMetaData() {
        return foreignKeyMetaData;
    }

    public ColumnMetaData setForeignKeyMetaData(ForeignKeyMetaData foreignKeyMetaData) {
        this.foreignKeyMetaData = foreignKeyMetaData;
        return this;
    }

    public List<IndexMetaData> getIndexes() {
        return indexes;
    }

    public ColumnMetaData setIndexes(List<IndexMetaData> indexes) {
        this.indexes = indexes;
        return this;
    }

    @Override
    public String toString() {
        return getTableName() + "->" + getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMetaData that = (ColumnMetaData) o;

        if (sqlType != that.sqlType) return false;
        if (nullable != that.nullable) return false;
        if (autoIncrement != that.autoIncrement) return false;
        if (scale != that.scale) return false;
        if (precision != that.precision) return false;
        if (primaryKey != that.primaryKey) return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (sqlTypeName != null ? !sqlTypeName.equals(that.sqlTypeName) : that.sqlTypeName != null) return false;
        if (javaType != null ? !javaType.equals(that.javaType) : that.javaType != null) return false;
        return foreignKeyMetaData != null ? foreignKeyMetaData.equals(that.foreignKeyMetaData) : that.foreignKeyMetaData == null;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + sqlType;
        result = 31 * result + (sqlTypeName != null ? sqlTypeName.hashCode() : 0);
        result = 31 * result + (javaType != null ? javaType.hashCode() : 0);
        result = 31 * result + (nullable ? 1 : 0);
        result = 31 * result + (autoIncrement ? 1 : 0);
        result = 31 * result + scale;
        result = 31 * result + precision;
        result = 31 * result + (primaryKey ? 1 : 0);
        result = 31 * result + (foreignKeyMetaData != null ? foreignKeyMetaData.hashCode() : 0);
        return result;
    }
}
