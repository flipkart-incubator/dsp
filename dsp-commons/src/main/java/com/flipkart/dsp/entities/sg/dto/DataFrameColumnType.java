package com.flipkart.dsp.entities.sg.dto;

import com.flipkart.dsp.models.sg.PredicateType;
import lombok.NoArgsConstructor;

/**
 */
@NoArgsConstructor
public enum DataFrameColumnType {
    EQUAL,
    IN,
    GREATER_THAN,
    LESS_THAN,
    RANGE;

    public static DataFrameColumnType from(PredicateType predicateType) throws Exception {
        switch (predicateType) {
            case IN:
                return DataFrameColumnType.IN;
            case EQUAL:
                return DataFrameColumnType.EQUAL;
            case GREATER_THAN:
                return DataFrameColumnType.GREATER_THAN;
            case LESS_THAN:
                return DataFrameColumnType.LESS_THAN;
            case RANGE:
            case INCREMENTAL_DATE_RANGE:
            case TIME_YYYYMMDD_RANGE:
            case INCREMENTAL_WEEK_RANGE:
            case TIME_YYYYWW_RANGE:
                return DataFrameColumnType.RANGE;
            default:
                throw new Exception(String.format("Failed to convert PredicateType :%s to DataFrameColumnType", predicateType));
        }
    }
}
