package com.flipkart.dsp.models.event_audits.event_type.wf_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ContainerDetails {
    private String workflowName;
    private Long pipelineStepId;
    private Timestamp submissionTime;
    private String scope;
    private Map<Long /**pipelineStepAuditId */, Integer /**log attempt*/> logAttemptMap;
    private String failureMessage;
    private Double cpu;
    private Double memory;
    private Integer currentAttempt;
}
