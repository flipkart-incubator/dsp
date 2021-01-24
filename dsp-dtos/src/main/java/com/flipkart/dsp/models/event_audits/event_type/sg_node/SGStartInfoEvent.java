package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SGStartInfoEvent extends Events implements Serializable {
    private String workflowName;
    private String stepId;
    private String logUrl;

    @Override
    public String prettyFormat() {
        return "SG is started for workflow " + workflowName + " , Step: "+stepId+" , logs: "+logUrl;
    }
}
