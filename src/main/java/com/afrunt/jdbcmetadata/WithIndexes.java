package com.afrunt.jdbcmetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrii Frunt
 */
public interface WithIndexes {
    List<IndexMetaData> getIndexes();

    default List<IndexMetaData> indexes() {
        return getIndexes() == null ? new ArrayList<>() : getIndexes();
    }

    default boolean hasIndexes() {
        return !indexes().isEmpty();
    }

    default boolean hasIndex(String name) {
        return indexes().stream()
                .filter(i -> i.nameIs(name))
                .count() == 1;
    }

    default IndexMetaData index(String name) {
        return indexes().stream()
                .filter(i -> i.nameIs(name))
                .findFirst()
                .orElse(null);
    }
}
