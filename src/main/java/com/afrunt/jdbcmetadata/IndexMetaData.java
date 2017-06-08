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
import java.util.stream.Collectors;

/**
 * @author Andrii Frunt
 */
public class IndexMetaData implements WithName {
    private String name;
    private Boolean unique;
    private Integer type;

    private Integer cardinality;
    private Integer pages;
    private List<IndexColumnMetadata> indexColumns = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }

    public IndexMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isMultiColumn() {
        return indexColumns().size() > 1;
    }

    public Boolean isUnique() {
        return unique;
    }

    public IndexMetaData setUnique(Boolean unique) {
        this.unique = unique;
        return this;
    }

    public Integer getType() {
        return type;
    }

    public IndexMetaData setType(Integer type) {
        this.type = type;
        return this;
    }

    public Integer getCardinality() {
        return cardinality;
    }

    public IndexMetaData setCardinality(Integer cardinality) {
        this.cardinality = cardinality;
        return this;
    }

    public Integer getPages() {
        return pages;
    }

    public IndexMetaData setPages(Integer pages) {
        this.pages = pages;
        return this;
    }

    public List<String> columnNames() {
        return getIndexColumns().stream()
                .map(IndexColumnMetadata::getName)
                .collect(Collectors.toList());
    }


    public List<IndexColumnMetadata> getIndexColumns() {
        return indexColumns;
    }

    public IndexMetaData setIndexColumns(List<IndexColumnMetadata> indexColumns) {
        this.indexColumns = indexColumns;
        return this;
    }

    public IndexMetaData addIndexColumn(IndexColumnMetadata indexColumnMetadata) {
        List<IndexColumnMetadata> columns = new ArrayList<>(indexColumns());
        columns.add(indexColumnMetadata);
        setIndexColumns(columns.stream().sorted().collect(Collectors.toList()));
        return this;
    }

    public List<IndexColumnMetadata> indexColumns() {
        return getIndexColumns() != null ? getIndexColumns() : new ArrayList<>();
    }

    public boolean isForColumn(String columnName) {
        return indexColumns().stream().filter(c -> c.nameIs(columnName)).count() > 0;
    }

    public IndexColumnMetadata indexColumn(String name) {
        return indexColumns().stream().filter(ic -> ic.nameIs(name)).findFirst().orElse(null);
    }

    @Override
    public String toString() {
        return getName() + "[" + String.join(",", columnNames()) + "]";
    }
}
