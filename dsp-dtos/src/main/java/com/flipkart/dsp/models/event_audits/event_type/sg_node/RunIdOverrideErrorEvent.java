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
public class RunIdOverrideErrorEvent extends Events implements Serializable {

    private String dataFrameName;
    private String workflowName;
    private Long runId;
    private String errorMessage;

    @Override
    public String prettyFormat() {
        return "Error while Executing RunId Override:\n" + "Dataframe : " + dataFrameName +
                "\nworkflow : " + workflowName + "\nrunId : " + runId + "\nError Message : " + errorMessage;
    }
}
