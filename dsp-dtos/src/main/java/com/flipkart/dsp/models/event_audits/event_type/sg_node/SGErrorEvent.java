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
public class SGErrorEvent extends Events implements Serializable {
    private String workflowName;
    private String errorMessage;

    @Override
    public String prettyFormat() {
        return "SG is failed for workflow " + workflowName + " with following error: " + errorMessage;
    }
}
