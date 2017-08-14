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
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Andrii Frunt
 */
public interface WithColumns {
    List<ColumnMetaData> getColumns();

    default List<ColumnMetaData> columns() {
        return Optional.ofNullable(getColumns()).orElse(new ArrayList<>());
    }

    default ColumnMetaData column(String name) {
        return columns().stream()
                .filter(c -> c.nameIs(name))
                .findAny()
                .orElse(null);
    }

    default List<ColumnMetaData> filterColumns(Predicate<ColumnMetaData> fn) {
        return columns().stream().filter(fn).collect(Collectors.toList());
    }

    default List<ColumnMetaData> columnsExcept(List<ColumnMetaData> exceptionList) {
        return filterColumns(c -> !exceptionList.contains(c));
    }

    default int columnsCount() {
        return columns().size();
    }

    default boolean columnsCountIs(int count) {
        return columnsCount() == count;
    }

    default boolean hasColumn(String name) {
        return column(name) != null;
    }

    default List<ColumnMetaData> foreignKeys() {
        return columns().stream()
                .filter(ColumnMetaData::isForeignKey)
                .collect(Collectors.toList());
    }


    default List<String> foreignTablesNames() {
        return foreignKeys().stream()
                .map(c -> c.getForeignKeyMetaData().getForeignTableName())
                .collect(Collectors.toList());
    }


}
