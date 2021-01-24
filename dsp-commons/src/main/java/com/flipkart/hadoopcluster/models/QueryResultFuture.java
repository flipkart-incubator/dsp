package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = QueryResultFuture.QueryResultFutureBuilder.class)
public class QueryResultFuture extends QuerySubmitResult implements Serializable {
    /**
     * Thread executing the query
     */
    private final String threadId;
    /**
     * Host executing the query
     */
    private final String hostName;
    /**
     * Error message if the query had failed applicable only if the status is true
     */
    private final String errorMessage;
    /**
     * The query that is executed (native translated query)
     */
    private final String query;
    /**
     * Source on which execution is happening
     */
    private final String sourceName;
    /**
     * Current status of the query
     */
    private final QueryStatus status;
    /**
     * The time at which QAAS decided to start a new query submission
     */
    private final long submittedAtTime;
    /**
     * The time at which QAAS submitted the query to the driver.
     */
    private final long executionStartTime;
    /**
     * The time at query is successfully completed.
     */
    private final long executionEndTime;
    /**
     * Fact updated at times when this query was executed
     */
    @ApiModelProperty(dataType = "Map[string,long]")
    private final Map<String, Timestamp> factUpdatedAtTimes;
    /**
     * Set of users who are polling on the same resultset.
     */
    private final Set<String> interestedUsers;

    private final Map<String, String> properties;

    @Builder
//    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    private QueryResultFuture(final QueryHandle queryHandle, final String threadId, final String hostName,
                              final String errorMessage, final String query,
                              final QueryStatus status, final long submittedAtTime,
                              final long executionStartTime, final long executionEndTime,
                              final Map<String, Timestamp> factUpdatedAtTimes, final Set<String> interestedUsers,
                              final String sourceName, final Map<String, String> properties) {
        super(queryHandle);
        this.threadId = threadId;
        this.hostName = hostName;
        this.sourceName = sourceName;
        this.errorMessage = errorMessage;
        this.query = query;
        this.status = status;
        this.submittedAtTime = submittedAtTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.interestedUsers = interestedUsers;
        this.factUpdatedAtTimes = factUpdatedAtTimes;
        this.properties = properties;
    }

//    public static QueryResultFutureBuilder produce(final QueryResultFuture queryResultFuture) {
//        return QueryResultFuture.builder()
//                .threadId(queryResultFuture.threadId)
//                .queryHandle(queryResultFuture.getQueryHandle())
//                .hostName(queryResultFuture.hostName)
//                .errorMessage(queryResultFuture.errorMessage)
//                .query(queryResultFuture.query)
//                .status(queryResultFuture.status)
//                .sourceName(queryResultFuture.sourceName)
//                .submittedAtTime(queryResultFuture.submittedAtTime)
//                .executionStartTime(queryResultFuture.executionStartTime)
//                .executionEndTime(queryResultFuture.executionEndTime)
//                .factUpdatedAtTimes(queryResultFuture.factUpdatedAtTimes)
//                .interestedUsers(queryResultFuture.interestedUsers);
//    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class QueryResultFutureBuilder {
    }
}
