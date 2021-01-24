package com.flipkart.dsp.entities.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.entities.enums.DSPWorkflowExecutionStatus;
import com.flipkart.dsp.models.Label;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * +
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DSPWorkflowExecutionResult implements Serializable {
    @JsonProperty("request_id")
    Long requestId;

    @JsonProperty("status")
    DSPWorkflowExecutionStatus workflowExecutionStatus;

    @JsonProperty("message")
    String message;

    @JsonProperty("partition_overrides")
    Map<@Label("entityName") String, @Label("partitionId") Long> partitionOverrides;
}
