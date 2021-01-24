package com.flipkart.dsp.models.sg;

/**
 */
public enum PredicateType {
    NOT_IN,
    IN,
    EQUAL,
    GREATER_THAN,
    LESS_THAN,
    RANGE,
    LOCAL_DATE_TIME_GREATER_THAN,
    TIME_YYYYMMDD_RANGE,
    INCREMENTAL_DATE_RANGE,
    TIME_YYYYWW_RANGE,  // start week and end week relative
    INCREMENTAL_WEEK_RANGE // start week fixed and end week relative
}
