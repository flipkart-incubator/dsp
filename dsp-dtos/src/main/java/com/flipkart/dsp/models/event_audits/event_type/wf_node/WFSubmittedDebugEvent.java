package com.flipkart.dsp.models.event_audits.event_type.wf_node;

import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class WFSubmittedDebugEvent extends Events implements Serializable {
    private String workflowName;
    private Long pipelineStepAuditId;
    private Timestamp submissionTime;
    private double baseMemory;
    private double baseCPU;

    @Override
    public String prettyFormat() {
        return "Workflow " + workflowName + " is submitted for execution.Execution Details are as follow : \nCPU : " + baseCPU
                + "\nMemory : " + baseMemory + "\nSubmission Time : " + submissionTime + "\n PipelineStepId : " + pipelineStepAuditId;
    }
}
