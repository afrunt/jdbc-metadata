package com.afrunt.jdbcmetadata;

import java.util.Collection;

/**
 * @author Andrii Frunt
 */
public interface ProgressMonitor {
    default void collectionStarted(Collection<String> schemas){
        
    }
    
    default void schemaMetaDataCollected(SchemaMetaData schema, long time) {
        
    }

    default void tableMetadataCollected(TableMetaData table, long time) {
        
    }
    
    default void databaseMetadataCollected(JdbcDatabaseMetaData databaseMetaData, long time) {
        
    }
}
