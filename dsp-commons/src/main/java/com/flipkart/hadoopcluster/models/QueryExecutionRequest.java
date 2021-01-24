package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryExecutionRequest extends QueryBaseRequestModel {

    private long finalBlockingTimeoutSec;
    private String billingOrg;

    @Builder
    @JsonCreator
    public QueryExecutionRequest(@JsonProperty("query") String query,
                                 @JsonProperty("sourceName") String sourceName,
                                 @JsonProperty("finalBlockingTimeoutSec") long finalBlockingTimeoutSec,
                                 @JsonProperty("billingOrg") String billingOrg) {
        super(query, sourceName);
        this.finalBlockingTimeoutSec = finalBlockingTimeoutSec;
        this.billingOrg = billingOrg;
    }

    public QueryExecutionRequest(QueryExecutionRequest queryExecutionRequest) {
        super(queryExecutionRequest.getQuery(), queryExecutionRequest.getSourceName());
        this.finalBlockingTimeoutSec = queryExecutionRequest.finalBlockingTimeoutSec;
        this.billingOrg = queryExecutionRequest.billingOrg;
    }
}
