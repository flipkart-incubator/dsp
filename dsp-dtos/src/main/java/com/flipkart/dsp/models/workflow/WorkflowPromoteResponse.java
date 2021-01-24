package com.flipkart.dsp.models.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * +
 */
@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowPromoteResponse {
    private String version;
    private String description;

    @JsonProperty("workflow_name")
    private String workflowName;

    @JsonProperty("workflow_group_name")
    private String workflowGroupName;
}
