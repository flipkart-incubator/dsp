package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Builder
@Getter
@JsonDeserialize(builder = QueryResultMeta.QueryResultMetaBuilder.class)
public class QueryResultMeta implements Serializable {
    /**
     * Ordered collection of columns in the result set.
     */
    private final List<Column> columns;
    /**
     * Fact updated at times when the query executed
     */
    private final Map<String, Long> factUpdatedAtTimes;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class QueryResultMetaBuilder {
    }
}
