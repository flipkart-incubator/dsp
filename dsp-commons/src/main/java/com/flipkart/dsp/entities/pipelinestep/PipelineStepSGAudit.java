package com.flipkart.dsp.entities.pipelinestep;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.PipelineStepStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.sql.Timestamp;

@Data
@JsonAutoDetect
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class PipelineStepSGAudit {
    private Long id;
    private String logs;
    private Long refreshId;
    private Long pipelineStep;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private PipelineStepStatus status;
    private String pipelineExecutionId;
    private String workflowExecutionId;
}
