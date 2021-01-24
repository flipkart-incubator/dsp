package com.flipkart.dsp.models.event_audits.event_type.wf_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WFContainerStartedInfoEvent extends Events implements Serializable {

    private String workflowName;
    private Long pipelineStepID;
    private Timestamp submissionTime;
    private String scope;
    private Map<Long /**pipelineStepAuditId*/, Integer /**log attempt*/> logAttemptMap;
    private LinkedHashMap<Integer /** attempt Number **/, String /** memoryUsageDashboard **/> attemptToMemoryDashboardMapping;

    // This will get populated by /v1/event_audit/ Resource
    @Setter
    private LinkedHashMap<Integer /** attempt Number **/, String /** logUrl **/> logAttemptUrlMapping;


    @Override
    public String prettyFormat() {
        StringBuffer str = new StringBuffer();
        str.append("Execution has started for Partition with scope : " +
                scope + " with following details : \n Start Time : "
                + submissionTime + "\n");
        str.append("Log Details :\n");
        logAttemptUrlMapping.forEach((attemptNo, url) -> {
            str.append("Attempt : " + attemptNo + "\tUrl : " + url + "\n");
        });
        str.append("Mesos Memory Usage Details: \n");
        attemptToMemoryDashboardMapping.forEach((attempt,  dashboardUrl) ->
                str.append("Attempt " + attempt + ": " + dashboardUrl + "\n") );
        return str.toString();
    }
}
