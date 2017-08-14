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

import java.sql.DatabaseMetaData;
import java.util.Optional;

/**
 * @author Andrii Frunt
 */
public class ForeignKeyMetaData implements WithName {
    private String name;
    private String foreignTableSchema;
    private String foreignTableName;
    private String foreignColumnName;
    private Integer updateRule;
    private Integer deleteRule;

    @Override
    public String getName() {
        return name;
    }

    public ForeignKeyMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public String getForeignTableName() {
        return foreignTableName;
    }

    public String getForeignColumnName() {
        return foreignColumnName;
    }

    public ForeignKeyMetaData setForeignTableName(String foreignTableName) {
        this.foreignTableName = foreignTableName;
        return this;
    }

    public ForeignKeyMetaData setForeignColumnName(String foreignColumnName) {
        this.foreignColumnName = foreignColumnName;
        return this;
    }

    public Integer getUpdateRule() {
        return updateRule;
    }

    public ForeignKeyMetaData setUpdateRule(Integer updateRule) {
        this.updateRule = updateRule;
        return this;
    }

    public Integer getDeleteRule() {
        return deleteRule;
    }

    public ForeignKeyMetaData setDeleteRule(Integer deleteRule) {
        this.deleteRule = deleteRule;
        return this;
    }

    /**
     * do not allow update of primary key if it has been imported
     *
     * @return
     */
    public boolean onUpdateNoAction() {
        return updateRuleEquals(DatabaseMetaData.importedKeyNoAction);
    }

    /**
     * importedKeyCascade - change imported key to agree with primary key update
     *
     * @return
     */
    public boolean onUpdateCascade() {
        return updateRuleEquals(DatabaseMetaData.importedKeyCascade);
    }

    /**
     * change imported key to <code>NULL</code> if its primary key has been updated
     *
     * @return
     */
    public boolean onUpdateSetNull() {
        return updateRuleEquals(DatabaseMetaData.importedKeySetNull);
    }

    /**
     * change imported key to default values if its primary key has been updated
     *
     * @return
     */
    public boolean onUpdateSetDefault() {
        return updateRuleEquals(DatabaseMetaData.importedKeySetDefault);
    }

    /**
     * Same as No Action
     *
     * @return
     */
    public boolean onUpdateRestrict() {
        return updateRuleEquals(DatabaseMetaData.importedKeyRestrict);
    }

    /**
     * do not allow delete of primary key if it has been imported
     *
     * @return
     */
    public boolean onDeleteNoAction() {
        return deleteRuleEquals(DatabaseMetaData.importedKeyNoAction);
    }

    /**
     * delete rows that import a deleted key
     *
     * @return
     */
    public boolean onDeleteCascade() {
        return deleteRuleEquals(DatabaseMetaData.importedKeyCascade);
    }

    /**
     * change imported key to NULL if its primary key has been deleted
     *
     * @return
     */
    public boolean onDeleteSetNull() {
        return deleteRuleEquals(DatabaseMetaData.importedKeySetNull);
    }

    /**
     * change imported key to default if its primary key has been deleted
     *
     * @return
     */
    public boolean onDeleteSetDefault() {
        return deleteRuleEquals(DatabaseMetaData.importedKeySetDefault);
    }


    /**
     * Same as No Action
     *
     * @return
     */
    public boolean onDeleteRestrict() {
        return deleteRuleEquals(DatabaseMetaData.importedKeyRestrict);
    }

    private boolean ruleEquals(Integer rule, int value) {
        return rule != null && rule.equals(value);
    }

    private boolean updateRuleEquals(int value) {
        return ruleEquals(getUpdateRule(), value);
    }

    private boolean deleteRuleEquals(int value) {
        return ruleEquals(getDeleteRule(), value);
    }

    @Override
    public String toString() {
        return Optional.ofNullable(getForeignTableSchema()).map(s -> s + "." + getForeignTableName()).orElse(getForeignTableName()) + "->" + foreignColumnName;
    }


    public String getForeignTableSchema() {
        return foreignTableSchema;
    }

    public ForeignKeyMetaData setForeignTableSchema(String foreignTableSchema) {
        this.foreignTableSchema = foreignTableSchema;
        return this;
    }

/*    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ForeignKeyMetaData that = (ForeignKeyMetaData) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (foreignTable != null ? !foreignTable.equals(that.foreignTable) : that.foreignTable != null) return false;
        if (foreignTableSchema != null ? !foreignTableSchema.equals(that.foreignTableSchema) : that.foreignTableSchema != null)
            return false;
        if (foreignTableName != null ? !foreignTableName.equals(that.foreignTableName) : that.foreignTableName != null)
            return false;
        if (foreignColumnName != null ? !foreignColumnName.equals(that.foreignColumnName) : that.foreignColumnName != null)
            return false;
        if (updateRule != null ? !updateRule.equals(that.updateRule) : that.updateRule != null) return false;
        return deleteRule != null ? deleteRule.equals(that.deleteRule) : that.deleteRule == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (foreignTable != null ? foreignTable.hashCode() : 0);
        result = 31 * result + (foreignTableSchema != null ? foreignTableSchema.hashCode() : 0);
        result = 31 * result + (foreignTableName != null ? foreignTableName.hashCode() : 0);
        result = 31 * result + (foreignColumnName != null ? foreignColumnName.hashCode() : 0);
        result = 31 * result + (updateRule != null ? updateRule.hashCode() : 0);
        result = 31 * result + (deleteRule != null ? deleteRule.hashCode() : 0);
        return result;
    }*/
}
