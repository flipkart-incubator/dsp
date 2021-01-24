package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.ToString;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@Getter
@ToString
public abstract class QueryResult extends QuerySubmitResult {
    /**
     * Metadata of the resultset (contains columns)
     */
    private final QueryResultMeta queryResultMeta;

    public QueryResult(QueryHandle queryHandle, QueryResultMeta queryResultMeta) {
        super(queryHandle);
        this.queryResultMeta = queryResultMeta;
    }

    @JsonIgnore
    public QueryResultIterator getIterator() {
        throw new UnsupportedOperationException("");
    }
}
