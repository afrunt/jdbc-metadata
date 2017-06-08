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

import com.afrunt.jdbcmetadata.ColumnMetaData;
import com.afrunt.jdbcmetadata.PrimaryKeyMetaData;
import com.afrunt.jdbcmetadata.TableMetaData;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrii Frunt
 */
public class EmployeeSkillTest extends BaseTest {
    @Test
    public void test() {
        TableMetaData table = getMetaDataCollector().collectTableMetaData("EMPLOYEE_SKILL");

        assertTrue(table.columnsCountIs(2));

        PrimaryKeyMetaData primaryKey = table.getPrimaryKey();
        assertTrue(primaryKey.isComposite());
        assertTrue(primaryKey.columnsCountIs(2));

        assertTrue(table.columnsExcept(primaryKey.columns()).isEmpty());

        ColumnMetaData employeeId = table.column("EMPLOYEE_ID");
        assertTrue(employeeId.sqlTypeIs(Types.BIGINT));
        assertTrue(employeeId.precisionIs(19));
        assertTrue(employeeId.scaleIs(0));

        String employeeFKString = employeeId.getForeignKeyMetaData().toString();
        assertEquals("TEST.EMPLOYEE->EMPLOYEE_ID", employeeFKString);

        TableMetaData employeeSkillTable = getMetaDataCollector().collectTableMetaData("EMPLOYEE_SKILL");
        assertTrue(table == employeeSkillTable);
        assertTrue(table.equals(employeeSkillTable));

        assertTrue(table.hashCode() == employeeSkillTable.hashCode());
    }
}
