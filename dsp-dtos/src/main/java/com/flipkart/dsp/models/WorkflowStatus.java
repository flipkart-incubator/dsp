package com.flipkart.dsp.models;

/**
 */
public enum WorkflowStatus {
        STARTED,
        RUNNING,
        FAILED,//This represent failure while execution
        SUCCESS,
        ABORTED,
        PERMANENT_FAILED //This represent failure when all retries done
}
