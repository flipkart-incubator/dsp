package com.flipkart.dsp.models.event_audits.event_type;

import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FlowTerminationSignal extends Events implements Serializable {
    private boolean failed;
    private String message;

    public String prettyFormat() {
        String displayMessage;
        if(failed) {
            displayMessage = "Execution is Failed.Reason for Failure " + message;
        } else {
            displayMessage = "Execution is completed successfully";
        }
        return displayMessage;
    }
}
