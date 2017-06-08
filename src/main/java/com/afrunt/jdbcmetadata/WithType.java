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

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author Andrii Frunt
 */
public interface WithType {
    Class<?> getType();

    default boolean typeIs(Class<?> cl) {
        return cl.equals(getType());
    }

    default boolean isAssignableFrom(Class<?> cl) {
        return getType().isAssignableFrom(cl);
    }

    default boolean isAssignableFromThisType(Class<?> cl) {
        return cl.isAssignableFrom(getType());
    }

    default boolean isNumber() {
        return isAssignableFromThisType(Number.class);
    }

    default boolean isDate() {
        return typeIs(Date.class) || typeIs(java.sql.Date.class);
    }

    default boolean isTimestamp() {
        return typeIs(Timestamp.class);
    }

    default boolean isString() {
        return typeIs(String.class);
    }

}
