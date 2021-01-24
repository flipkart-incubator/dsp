package com.flipkart.dsp.models.event_audits.event_type.terminal_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TerminalNodeInfoEvent extends Events implements Serializable {
    private String callbackUrl;
    private String varadhiQueue;
    private Timestamp alertTriggerTime;
    private String workflowName;

    @Override
    public String prettyFormat() {
        return "Callback is send to varadhi with following Details : \nworkflow " + workflowName +
                "\nCallback Url : " + callbackUrl + "\nVaradhi Queue : " + varadhiQueue +
                "\nTrigger Time : " + alertTriggerTime;
    }
}
