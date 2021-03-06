package com.flipkart.dsp.entities.pipelinestep;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.models.PipelineStepStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 */
@Data
@JsonAutoDetect
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class PipelineStepAudit {
    private Long id;
    private String logs;
    private String scope;
    private Long refreshId;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private Integer attempt;
    private Long workflowId;
    private Long pipelineStepId;
    private Resources resources;
    private PipelineStepStatus pipelineStepStatus;
    private String workflowExecutionId;
    private String pipelineExecutionId;
}
