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

import com.afrunt.jdbcmetadata.JdbcMetaDataCollector;
import com.afrunt.jdbcmetadata.JdbcMetaDataException;
import com.afrunt.jdbcmetadata.SchemaMetaData;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrii Frunt
 */
public class SchemaMetaDataTest extends BaseTest {
    @Test
    public void testPublicSchema() {
        JdbcMetaDataCollector metaDataCollector = getMetaDataCollector();
        Assert.assertNotNull(metaDataCollector.collectSchemaMetaData("PUBLIC"));
        Assert.assertNotNull(metaDataCollector.collectSchemaMetaData(""));
        Assert.assertNotNull(metaDataCollector.collectSchemaMetaData("TEST"));

        SchemaMetaData schema = metaDataCollector.collectSchemaMetaData("PUBLIC");

        assertTrue(schema.hasTable("POSITION"));
        assertTrue(schema.hasTable("SKILL"));
        assertTrue(schema.hasTable("EMPLOYEE_SKILL"));
        assertTrue(schema.hasTable("POSITION") == schema.hasTable("POSITION"));
        assertTrue(schema.hasTable("SKILL") == schema.hasTable("SKILL"));
        assertTrue(schema.hasTable("EMPLOYEE_SKILL") == schema.hasTable("EMPLOYEE_SKILL"));

        assertNotNull(schema.table("POSITION"));
        assertNotNull(schema.table("SKILL"));
        assertNotNull(schema.table("EMPLOYEE_SKILL"));
    }

    @Test(expected = JdbcMetaDataException.class)
    public void testUnknownSchema() {
        getMetaDataCollector().collectSchemaMetaData("XXX");
        Assert.fail("Should throw an exception");
    }
}
