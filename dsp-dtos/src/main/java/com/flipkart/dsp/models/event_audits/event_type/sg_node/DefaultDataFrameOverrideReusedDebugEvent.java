package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DefaultDataFrameOverrideReusedDebugEvent extends Events implements Serializable {
    private String dataFrameName;
    private String workflowName;
    private Long runId;
    private String payload;

    @Override
    public String prettyFormat() {
        return "Force Run is enabled for dataframeName : " + dataFrameName
                + " belonging to workflow :" + workflowName;
    }
}
