package com.flipkart.dsp.models.event_audits.event_type.terminal_node;

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
public class TerminalNodeErrorEvent extends Events implements Serializable {
    private String workflowName;
    private String errorMessage;

    @Override
    public String prettyFormat() {
        return "Varadhi call has failed for workflow " + workflowName + " because of following reason " + errorMessage;
    }
}
