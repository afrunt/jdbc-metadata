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
package com.afrunt.jdbcmetadata.test;

import com.afrunt.jdbcmetadata.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Andrii Frunt
 */
public class EmployeeTableMetaDataTest extends BaseTest {
    private static final String EMPLOYEE_TABLE_NAME = "EMPLOYEE";
    private static final String SCHEMA = "TEST";

    @Test
    public void employeeTableTest() {
        TableMetaData table = getMetaDataCollector().collectTableMetaData(EMPLOYEE_TABLE_NAME, SCHEMA);

        assertEquals(SCHEMA, table.getSchemaName());
        assertEquals(EMPLOYEE_TABLE_NAME, table.getName());
        assertEquals(SCHEMA + "." + EMPLOYEE_TABLE_NAME, table.toString());
        assertTrue(table.nameIs(EMPLOYEE_TABLE_NAME));
        assertTrue(table.hasIndexes());

        testPrimaryKeys(table);

        testColumns(table);

        testForeignKeys(table);

        testIndexes(table);
    }

    private void testColumns(TableMetaData table) {
        List<ColumnMetaData> columns = table.columns();
        assertTrue(table.hasColumn("EMPLOYEE_ID"));
        assertEquals(table.column("PHOTO").toString(), "EMPLOYEE->PHOTO");
        assertFalse(table.hasColumn("UNKNOWN_COLUMN"));

        assertTrue(table.column("DATE_OF_BIRTH").isDate());
        assertTrue(table.column("FIRST_NAME").isString());
        assertTrue(table.column("MIDDLE_NAME").isNullable());
        ColumnMetaData lastName = table.column("LAST_NAME");

        assertFalse(lastName.isNullable());
        assertTrue(lastName.hasIndexes());
        assertTrue(lastName.hasIndex("NAME_IDX"));
        assertTrue(table.column("MODIFIED_DATE").isTimestamp());

        assertTrue(table.column("PHOTO").isLargeObject());
        assertTrue(table.column("PHOTO").isBLOB());
    }

    private void testIndexes(TableMetaData table) {
        assertTrue(table.hasIndexes());
        assertTrue(table.hasIndex("NAME_IDX"));

        IndexMetaData nameIndex = table.index("NAME_IDX");

        List<IndexMetaData> indexesForFirstName = table.columnIndexes("FIRST_NAME");

        assertFalse(indexesForFirstName.isEmpty());
        assertTrue(indexesForFirstName.contains(nameIndex));

        assertTrue(nameIndex.isMultiColumn());

        assertTrue(nameIndex.isForColumn("FIRST_NAME"));
        assertTrue(nameIndex.isForColumn("LAST_NAME"));
        assertFalse(nameIndex.isUnique());

        IndexColumnMetadata firstName = nameIndex.indexColumn("FIRST_NAME");
        assertFalse(firstName.isAscending());
        IndexColumnMetadata lastName = nameIndex.indexColumn("LAST_NAME");
        assertTrue(lastName.isAscending());

        assertEquals("NAME_IDX[LAST_NAME,FIRST_NAME]", nameIndex.toString());
    }

    private void testPrimaryKeys(TableMetaData table) {
        assertTrue(table.hasPrimaryKey());
        assertFalse(table.hasCompositePrimaryKey());

        PrimaryKeyMetaData primaryKey = table.getPrimaryKey();
        assertNotNull(primaryKey);

        assertFalse(primaryKey.isComposite());

        List<ColumnMetaData> primaryKeyColumns = primaryKey.columns();

        assertEquals(1, primaryKeyColumns.size());

        ColumnMetaData employeeId = primaryKey.column("EMPLOYEE_ID");
        assertNotNull(employeeId);
        assertTrue(employeeId.isAssignableFrom(Long.class));
        assertTrue(employeeId.isNumber());
        assertTrue(employeeId.isAutoIncrement());
        assertTrue(employeeId.sqlTypeNameIs("BIGINT"));
        assertEquals("EMPLOYEE_ID", employeeId.getName());
        assertEquals("BIGINT", employeeId.getSqlTypeName());
        assertEquals(EMPLOYEE_TABLE_NAME, employeeId.getTableName());
        assertEquals(0, employeeId.getScale());
        assertTrue(employeeId.isPrimaryKey());
        assertFalse(employeeId.isNullable());
        assertFalse(employeeId.isLargeObject());
    }

    private void testForeignKeys(TableMetaData table) {
        List<ColumnMetaData> foreignKeys = table.foreignKeys();

        assertEquals(2, foreignKeys.size());

        List<TableMetaData> foreignTables = table.foreignTables();

        assertEquals(2, foreignTables.size());
        ColumnMetaData departmentId = table.column("DEPARTMENT_ID");
        assertTrue(departmentId.isForeignKey());

        assertEquals(Arrays.asList("DEPARTMENT", "POSITION"), table.foreignTablesNames());

        ForeignKeyMetaData foreignKey = departmentId.getForeignKeyMetaData();

        assertEquals(SCHEMA, foreignKey.getForeignTableSchema());
        assertEquals("DEPARTMENT", foreignKey.getForeignTableName());
        assertEquals("DEPARTMENT_ID", foreignKey.getForeignColumnName());

        TableMetaData departmentTable = getMetaDataCollector().collectTableMetaData("DEPARTMENT", "TEST");

        assertTrue(departmentTable == foreignKey.getForeignTable());

        assertTrue(foreignKey.onDeleteRestrict());
        assertFalse(foreignKey.onDeleteCascade());
        assertFalse(foreignKey.onDeleteNoAction());
        assertFalse(foreignKey.onDeleteSetNull());
        assertFalse(foreignKey.onDeleteSetDefault());

        assertTrue(foreignKey.onUpdateCascade());
        assertFalse(foreignKey.onUpdateNoAction());
        assertFalse(foreignKey.onUpdateRestrict());
        assertFalse(foreignKey.onUpdateSetDefault());
        assertFalse(foreignKey.onUpdateSetNull());

        TableMetaData positionTable = getMetaDataCollector().collectTableMetaData("POSITION");

        assertTrue(positionTable.isForeignFor(table));
        assertTrue(positionTable.isRelatedTo(table));
        assertTrue(table.isRelatedTo(positionTable));

        ColumnMetaData positionId = table.column("POSITION_ID");
        assertEquals("PUBLIC", positionId.getForeignKeyMetaData().getForeignTableSchema());
        assertEquals("POSITION", positionId.getForeignKeyMetaData().getForeignTableName());
    }
}
