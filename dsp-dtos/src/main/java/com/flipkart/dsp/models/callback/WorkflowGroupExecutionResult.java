package com.flipkart.dsp.models.callback;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.RequestStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
public class WorkflowGroupExecutionResult {
    @JsonProperty
    private Long requestId;

    @JsonProperty
    private RequestStatus requestStatus;

    @JsonProperty
    private String message;

    @JsonProperty
    private Map<String, WorkflowExecutionResult> workflowExecutionResultMap;

}
