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
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Andrii Frunt
 */
public class BaseTest {
    private Connection connection;
    private DataSource dataSource;

    private JdbcMetaDataCollector metaDataCollector;

    @Before
    public void init() throws SQLException {
        dataSource = createDataSource();
        connection = createConnection();
        metaDataCollector = new JdbcMetaDataCollector()
                .setDataSource(dataSource)
                .setParallelism(10)
        ;
    }

    @After
    public void destroy() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("DROP ALL OBJECTS");
    }

    public Connection createConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public DataSource createDataSource() {
        try {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
            dataSource.setUser("sa");
            Connection connection = dataSource.getConnection();
            RunScript.execute(connection, new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("schema.h2.sql")));
            RunScript.execute(connection, new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("constraints.h2.sql")));
            RunScript.execute(connection, new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("data.h2.sql")));
            return dataSource;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public JdbcMetaDataCollector getMetaDataCollector() {
        return metaDataCollector;
    }
}
