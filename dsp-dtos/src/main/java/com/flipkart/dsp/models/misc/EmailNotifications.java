package com.flipkart.dsp.models.misc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * +
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmailNotifications {
    @JsonProperty("recipients")
    private Map<String, String> recipients;
    @JsonProperty("workflow_states")
    private String workflowStates;
    @JsonProperty("partition_details")
    private Map<String /*pipelineStepName*/, List<Map<String /*partitionKey*/, Object /*partitionValue*/>>> partitionDetails;
}

