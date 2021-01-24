package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.models.sg.AggregationType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class SelectColumn {
    private final String name; /* name of column */
    private final Boolean isGranularity;/* is granularity */
    private final AggregationType hiveAggregationType;/* Aggregation type of the column */
    private final String alias; /*column alias*/
}
