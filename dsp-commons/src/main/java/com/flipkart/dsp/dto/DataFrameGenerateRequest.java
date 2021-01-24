package com.flipkart.dsp.dto;

import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameScope;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
@Builder
public class DataFrameGenerateRequest {

    private String jobId;
    private Long requestId;
    private Map<String, Long> tables;
    private Set<DataFrame> dataFrames;
    private Set<DataFrameScope> scopes;
    private Map<String, DataType> inputDataFrameType;
    private Map<String, DataframeOverride> dataFrameOverrideMap;
}
