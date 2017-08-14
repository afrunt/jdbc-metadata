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
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Andrii Frunt
 */
public class SchemaMetaData implements WithName {
    private String name;
    private List<TableMetaData> tables = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }

    public List<TableMetaData> tables() {
        if (getTables() == null) {
            setTables(new ArrayList<>());
        }

        return getTables();
    }

    public TableMetaData table(String name) {
        return filterTables(t -> t.nameIs(name)).stream()
                .findAny()
                .orElse(null);
    }

    public boolean hasTable(String name) {
        return !filterTables(t -> t.nameIs(name))
                .isEmpty();
    }

    public List<TableMetaData> filterTables(Predicate<TableMetaData> fn) {
        return tables().stream()
                .filter(fn)
                .collect(Collectors.toList());
    }

    public SchemaMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public List<TableMetaData> getTables() {
        return tables;
    }

    public SchemaMetaData setTables(List<TableMetaData> tables) {
        this.tables = tables;
        return this;
    }

    public int tableCount() {
        return tables().size();
    }

    @Override
    public String toString() {
        return getName() + "[" + tableCount() + "]";
    }
}
