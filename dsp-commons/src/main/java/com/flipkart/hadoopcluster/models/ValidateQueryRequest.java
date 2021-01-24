package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidateQueryRequest {

    private final String query;
    private String sourceName;

    @Builder
    @JsonCreator
    public ValidateQueryRequest(@JsonProperty("query") String query,
                                @JsonProperty("sourceName") String sourceName) {
        this.query = query;
        this.sourceName = sourceName;
    }
}
