package com.afrunt.jdbcmetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Andrii Frunt
 */
public class JdbcDatabaseMetaData {
    private String databaseProductName;
    private List<SchemaMetaData> schemas;

    public List<SchemaMetaData> schemas() {
        return getSchemas() != null ? getSchemas() : new ArrayList<>();
    }

    public SchemaMetaData schema(String schema) {
        return schemas().stream()
                .filter(s -> s.nameIs(schema))
                .findFirst()
                .orElse(null);
    }

    public boolean hasSchema(String schema) {
        return schema(schema) != null;
    }

    public List<SchemaMetaData> getSchemas() {
        return schemas;
    }

    public JdbcDatabaseMetaData setSchemas(List<SchemaMetaData> schemas) {
        this.schemas = schemas;
        return this;
    }

    public String getDatabaseProductName() {
        return databaseProductName;
    }

    public JdbcDatabaseMetaData setDatabaseProductName(String databaseProductName) {
        this.databaseProductName = databaseProductName;
        return this;
    }
}
