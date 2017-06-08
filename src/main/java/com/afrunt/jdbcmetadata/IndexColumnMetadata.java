package com.afrunt.jdbcmetadata;

/**
 * @author Andrii Frunt
 */
public class IndexColumnMetadata implements WithName, Comparable<IndexColumnMetadata> {
    public static final int SORT_TYPE_ASC = 0;
    public static final int SORT_TYPE_DESC = 1;
    private String name;
    private Boolean ascending;
    private Integer ordinalPosition;
    private Integer sortType;

    public String getName() {
        return name;
    }

    public IndexColumnMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public Boolean isAscending() {
        return ascending;
    }

    public IndexColumnMetadata setAscending(Boolean ascending) {
        this.ascending = ascending;
        return this;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public IndexColumnMetadata setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
        return this;
    }

    public Integer getSortType() {
        return sortType;
    }

    public IndexColumnMetadata setSortType(Integer sortType) {
        this.sortType = sortType;
        return this;
    }

    @Override
    public int compareTo(IndexColumnMetadata o) {
        return getOrdinalPosition().compareTo(o.getOrdinalPosition());
    }
}
