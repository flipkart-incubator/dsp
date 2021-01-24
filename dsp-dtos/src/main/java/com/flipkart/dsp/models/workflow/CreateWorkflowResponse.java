package com.flipkart.dsp.models.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * +
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateWorkflowResponse implements Serializable {
    private String name;
    private String version;

    @JsonProperty("is_prod")
    private Boolean isProd;

    @JsonProperty("workflow_group_name")
    private String workflowGroupName;
}
