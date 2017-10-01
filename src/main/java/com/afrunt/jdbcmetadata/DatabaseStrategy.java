package com.afrunt.jdbcmetadata;

import java.sql.Connection;
import java.util.List;

/**
 * @author Andrii Frunt
 */
public abstract class DatabaseStrategy {
    private Connection connection;

    public abstract List<SequenceMetaData> collectSequencesMetaData(String schema);


    public Connection getConnection() {
        return connection;
    }

    public DatabaseStrategy setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }
}
