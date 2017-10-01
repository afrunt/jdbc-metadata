package com.afrunt.jdbcmetadata;

/**
 * @author Andrii Frunt
 */
public class SequenceMetaData implements WithName{
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
