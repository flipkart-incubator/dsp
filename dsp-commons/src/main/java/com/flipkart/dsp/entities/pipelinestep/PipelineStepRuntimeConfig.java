package com.flipkart.dsp.entities.pipelinestep;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.run.config.RunConfig;
import lombok.*;

import java.sql.Timestamp;

/**
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@ToString
@Builder
@JsonIgnoreProperties(ignoreUnknown=true)
public class PipelineStepRuntimeConfig {
    private Long id;
    private String workflowExecutionId;
    private String pipelineExecutionId;
    private Long pipelineStepId;
    private String scope;
    private RunConfig runConfig;
    private Timestamp ts;
}
