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

/**
 * @author Andrii Frunt
 */
public class PrimaryKeyMetaData implements WithColumns {
    private List<ColumnMetaData> columns = new ArrayList<>();

    public PrimaryKeyMetaData() {
    }

    public PrimaryKeyMetaData(List<ColumnMetaData> columns) {
        this.columns = columns;
    }

    public boolean isComposite() {
        return columns().size() > 1;
    }

    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public PrimaryKeyMetaData setColumns(List<ColumnMetaData> columns) {
        this.columns = columns;
        return this;
    }

    /* @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrimaryKeyMetaData that = (PrimaryKeyMetaData) o;

        return columns != null ? columns.equals(that.columns) : that.columns == null;
    }

    @Override
    public int hashCode() {
        return columns != null ? columns.hashCode() : 0;
    }*/
}
