package com.flipkart.dsp.entities.request;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 */
@Data
@Builder
@JsonAutoDetect
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Request implements Serializable {
    private Long id;
    private Long requestId;
    private Long workflowId;
    private String triggeredBy;
    private Long azkabanExecId;
    private String callbackUrl;
    private Boolean isNotified;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private ExecuteWorkflowRequest data;
    private RequestStatus requestStatus;
    private WorkflowDetails workflowDetails;
}
