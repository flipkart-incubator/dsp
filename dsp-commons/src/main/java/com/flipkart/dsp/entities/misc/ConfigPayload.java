package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Builder
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigPayload implements Serializable {
    @JsonProperty
    Long workflowId;
    @JsonProperty
    String scope;
    @JsonProperty
    Map<String, String> csvLocation;
    @JsonProperty
    Map<String, String> futureCSVLocation;
    @JsonProperty
    long pipelineStepId;
    @JsonProperty
    String timestamp;
    @JsonProperty
    String workflowExecutionId;
    @JsonProperty
    String parentWorkflowExecutionId;
    @JsonProperty
    String pipelineExecutionId;
    @JsonProperty
    long refreshId;
    @JsonProperty
    private Map<String, String> partitionValues;

}
