package com.afrunt.jdbcmetadata.test;

import com.afrunt.jdbcmetadata.JdbcDatabaseMetaData;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Andrii Frunt
 */
public class DatabaseMetaDataTest extends BaseTest {
    @Test
    public void test() {
        JdbcDatabaseMetaData databaseMetaData = getMetaDataCollector().collectDatabaseMetaData(s -> !"INFORMATION_SCHEMA".equals(s));
        assertNotNull(databaseMetaData);
        assertEquals(2, databaseMetaData.getSchemas().size());

        testSchemas(databaseMetaData);
        testExtraData(databaseMetaData);
    }

    private void testSchemas(JdbcDatabaseMetaData md) {
        assertFalse(md.hasSchema("INFORMATION_SCHEMA"));

        md = getMetaDataCollector().collectDatabaseMetaData();

        assertEquals(3, md.getSchemas().size());

        assertTrue(md.hasSchema("TEST"));
        assertTrue(md.hasSchema("PUBLIC"));
        assertTrue(md.hasSchema("INFORMATION_SCHEMA"));

        assertEquals("INFORMATION_SCHEMA[0]", md.schema("INFORMATION_SCHEMA").toString());

    }

    private void testExtraData(JdbcDatabaseMetaData md) {
        assertEquals("H2", md.getDatabaseProductName());
    }
}
