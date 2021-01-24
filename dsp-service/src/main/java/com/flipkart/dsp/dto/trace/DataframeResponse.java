package com.flipkart.dsp.dto.trace;

import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@JsonSnakeCase
@Data
public class DataframeResponse {
    private long workflowId;
    private String workflowName;
    private List<DataFramePojo> dataFrames;
}
