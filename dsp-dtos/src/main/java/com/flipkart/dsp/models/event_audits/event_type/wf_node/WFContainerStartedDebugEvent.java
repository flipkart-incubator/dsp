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
public class WFContainerStartedDebugEvent extends Events implements Serializable {
    private String workflowName;
    private Long pipelineStepId;
    private Timestamp submissionTime;
    private String scope;
    private Map<Long /**pipelineStepAuditId */, Integer /**log attempt*/> logAttemptMap;
    private Map<String/** dataframeName */, String/** WebHdfs location */> inputDetails;
    private LinkedHashMap<Integer /** attempt Number **/, String /** memoryUsageDashboard **/> attemptToMemoryDashboardMapping;

    @Setter
    private LinkedHashMap<Integer, String> logAttemptUrlMapping;
    @Setter
    private Map<String, String> dataFrameWebHDFSLinkMapping;

    @Override
    public String prettyFormat() {
        StringBuffer str = new StringBuffer();
        str.append("Execution has started for Partition with scope : " +
                scope + " with following details : \nStart Time : "
                + submissionTime + "\n");
        str.append("Log Details :\n");
        logAttemptUrlMapping.forEach((attemptNo, Url) -> str.append("Attempt : " + attemptNo + "\tUrl : " + Url + "\n"));
        str.append("Dataframe Details :\n");
        dataFrameWebHDFSLinkMapping.forEach((dataFrame, webHDFSUrl) -> str.append("Dataframe Name : " + dataFrame + "\nWebHDFS Link : " + webHDFSUrl + "\n"));
        str.append("Mesos Memory Usage Details: \n");
        attemptToMemoryDashboardMapping.forEach((attempt,  dashboardUrl) ->
                str.append("Attempt " + attempt + ": " + dashboardUrl + "\n") );
        return str.toString();
    }
}
