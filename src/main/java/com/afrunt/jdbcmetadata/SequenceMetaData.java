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

/**
 * @author Andrii Frunt
 */
public class SequenceMetaData implements WithName {
    private String name;
    private String schema;

    private Integer incrementBy;

    public String getName() {
        return name;
    }

    public SequenceMetaData setName(String name) {
        this.name = name;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public SequenceMetaData setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public Integer getIncrementBy() {
        return incrementBy;
    }

    public SequenceMetaData setIncrementBy(Integer incrementBy) {
        this.incrementBy = incrementBy;
        return this;
    }

    public String getFullName() {
        if (schema != null) {
            return schema + "." + name;
        } else {
            return name;
        }
    }
}
