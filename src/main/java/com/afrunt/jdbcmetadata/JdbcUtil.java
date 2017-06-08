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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author Andrii Frunt
 */
public class JdbcUtil {
    public static void printResultSet(ResultSet rs) throws SQLException {
        String format = repeat("%20s", rs.getMetaData().getColumnCount());
        List<String> columnNames = getColumnNames(rs);
        System.out.println(String.format(format, columnNames.toArray(new String[columnNames.size()])));
        while (rs.next()) {
            List<String> row = getRow(rs);
            System.out.println(String.format(format, row.toArray(new String[row.size()])));
        }

    }

    private static List<String> getRow(ResultSet rs) throws SQLException {
        List<String> strings = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            strings.add(rs.getString(i));
        }
        return strings;
    }

    public static List<String> getColumnNames(ResultSet rs) throws SQLException {
        List<String> strings = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            strings.add(rs.getMetaData().getColumnName(i));
        }
        return strings;
    }

    private static String repeat(String str, int times) {
        return IntStream.range(1, times).mapToObj(i -> str).reduce(str, (s, s2) -> s + s2);
    }
}
