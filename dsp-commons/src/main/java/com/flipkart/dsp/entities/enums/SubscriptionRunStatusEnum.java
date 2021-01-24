package com.flipkart.dsp.entities.enums;

/**
 * +
 */
public enum SubscriptionRunStatusEnum {
    FAILED_AT_SOURCE, // Issue with underlying tables or upstream systems
    PROCESSING_FAIL, // Failed to process, even after retries
    SUCCESSFUL, // Successfully processed
    TIMED_OUT, //Exceeded the timeout set by client system for Subscription
}
