package com.flipkart.dsp.models.event_audits.event_type.wf_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WFEndInfoEvent extends Events implements Serializable {
    private String workflowName;
    private Long refreshId;
    private List<String> outputHiveTables;
    // Service has to populate this

    @Override
    public String prettyFormat() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("Workflow " + workflowName + " has completed successfully.");
        return stringBuffer.toString();
    }
}