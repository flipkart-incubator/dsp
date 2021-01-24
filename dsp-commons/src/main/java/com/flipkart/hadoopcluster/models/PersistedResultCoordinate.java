package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.ToString;

import java.beans.Transient;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@Getter
@ToString
public abstract class PersistedResultCoordinate extends QueryResult {
    /**
     * Sample of data that is persisted
     */
    private final List<ResultRow> sampleData;
    /**
     * Source to which the data was persisted
     */
    private final String sourceName;

    public PersistedResultCoordinate(final QueryHandle queryHandle, final String sourceName,
                                     final List<ResultRow> sampleData, final QueryResultMeta queryResultMeta) {
        super(queryHandle, queryResultMeta);
        this.sourceName = sourceName;
        this.sampleData = sampleData;
    }

    @Override
    @JsonIgnore
    @Transient
    public QueryResultIterator getIterator() {
        return null;
    }
}
