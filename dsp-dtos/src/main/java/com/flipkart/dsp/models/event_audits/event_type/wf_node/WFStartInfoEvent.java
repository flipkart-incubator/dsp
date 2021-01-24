package com.flipkart.dsp.models.event_audits.event_type.wf_node;

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
public class WFStartInfoEvent extends Events implements Serializable {
    private String workflowName;

    @Override
    public String prettyFormat() {
        return "Workflow Node has started for Workflow " + workflowName;
    }
}