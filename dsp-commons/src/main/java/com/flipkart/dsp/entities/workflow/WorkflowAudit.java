package com.flipkart.dsp.entities.workflow;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.WorkflowStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@Builder
@JsonIgnoreProperties(ignoreUnknown=true)
public class WorkflowAudit {
    private Long refreshId;
    private Long workflowId;
    private String workflowExecutionId;
    private WorkflowStatus workflowStatus;
    private Date createdAt;
    private Date updatedAt;
}
