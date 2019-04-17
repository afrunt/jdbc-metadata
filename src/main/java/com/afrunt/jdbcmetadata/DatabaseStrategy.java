package com.afrunt.jdbcmetadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Andrii Frunt
 */
public abstract class DatabaseStrategy {
    private Connection connection;

    private DataSource dataSource;

    public abstract List<SequenceMetaData> collectSequencesMetaData(String schema);

    public Connection getConnection() {
        if (dataSource != null) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                throw new JdbcMetaDataException(e);
            }
        } else {
            return connection;
        }
    }

    public DatabaseStrategy setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DatabaseStrategy setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }
}
