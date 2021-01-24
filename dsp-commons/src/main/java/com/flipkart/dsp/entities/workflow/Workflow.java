package com.flipkart.dsp.entities.workflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.sg.DataFrame;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 */
@Data
@Builder
@JsonSnakeCase
@JsonAutoDetect
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class Workflow implements Serializable {
    private Long id;
    private String name;
    private String version;
    private String createdBy;
    private Integer retries = 1;
    private String description;
    private Long parentWorkflowId;
    private String subscriptionId;
    private Boolean isProd = false;
    private String defaultOverrides;
    private WorkflowMeta workflowMeta;
    private String workflowGroupName;
    private Set<DataFrame> dataFrames;

}
