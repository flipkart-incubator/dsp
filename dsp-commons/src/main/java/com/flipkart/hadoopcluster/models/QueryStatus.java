package com.flipkart.hadoopcluster2.models;

public enum QueryStatus {
    /**
     * Initial state when a new query request happens from QAAS.
     * QAAS is now ready to submit the query (lock is acquired)
     */
    PLANNED,
    /**
     * The query is scheduled to the source and is awaiting response.
     */
    SCHEDULED,
    /**
     * The query is submitted to the source and is awaiting response.
     */
    SUBMITTED,
    /**
     * The query is running on the source
     */
    RUNNING,
    /**
     * The query has successfully completed at QAAS
     */
    SUCCESSFUL,
    /**
     * A rerun flow will be started for these queries.
     * Used when the queries are lost during deployments
     */
    RERUN_REQUESTED,
    /**
     * The query is killed by the user
     */
    KILLED,
    /**
     * The query has failed. Reason could be user or server
     */
    FAILED,
    /**
     * Invalid uknown state (corrupted)
     */
    INVALID,
    /**
     * The result for the query is no longer available in the cache store.
     */
    PURGED;

    public boolean isOnGoing() {
        return this == QueryStatus.PLANNED
                || this == QueryStatus.SUBMITTED
                || this == QueryStatus.RUNNING;
    }
}
