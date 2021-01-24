package com.flipkart.dsp.dto;

import com.flipkart.dsp.entities.run.config.PipelineStepStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 */

@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
public class UpdateEntityDTO {
    @JsonProperty
    long refreshId;

    @JsonProperty
    String workflowExecutionId;

    @JsonProperty
    String status;

    @JsonProperty
    long workflowId;

    @JsonProperty
    String pipelineExecutionId;
}
