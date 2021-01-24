package com.flipkart.dsp.models.event_audits.event_type.wf_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;

import java.io.Serializable;
import java.util.LinkedHashMap;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WFContainerFailedEvent extends Events implements Serializable {
    private ContainerDetails containerDetails;
    private LinkedHashMap<Integer /** attempt Number **/, String /** memoryUsageDashboard **/> attemptToMemoryDashboardMapping;

    @Setter
    private LinkedHashMap<Integer, String> attemptLogUrlMapping;

    @Override
    public String prettyFormat() {
        StringBuffer str = new StringBuffer();
        str.append("Execution has Failed for Partition with scope : " +
                containerDetails.getScope() + " with following details : \n Completion Time : "
                + containerDetails.getSubmissionTime() + "\n cpu : " + containerDetails.getCpu()
                + "\n memory : " + containerDetails.getMemory()
                + "\n current Attempt : " + containerDetails.getCurrentAttempt() + "\n");
        str.append("Following are the log Details : ");
        str.append("Log Details : \n");
        attemptLogUrlMapping.forEach((attemptNo, LogUrl) -> {
            str.append("Attempt " + attemptNo + " : " + LogUrl + "\n");
        });

        str.append("Mesos Memory Usage Details: \n");
        attemptToMemoryDashboardMapping.forEach((attempt,  dashboardUrl) ->
                str.append("Attempt " + attempt + ": " + dashboardUrl + "\n") );
        return str.toString();
    }
}
