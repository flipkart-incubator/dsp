package com.flipkart.dsp.entities.sg.core;

/**
 */
public enum DataFrameAuditStatus {
    ENQUEUED,
    GENERATING_GRANULARITY_AND_FACT_TABLES,
    GENERATING_DATAFRAME,
    GENERATING_PAYLOAD,
    COMPLETED,
    FAILED
}
