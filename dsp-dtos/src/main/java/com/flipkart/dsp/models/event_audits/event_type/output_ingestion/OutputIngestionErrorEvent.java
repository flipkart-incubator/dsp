package com.flipkart.dsp.models.event_audits.event_type.output_ingestion;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
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
public class OutputIngestionErrorEvent extends Events implements Serializable {
    private String message;
    private String workflowName;
    private String errorMessage;

    @Override
    public String prettyFormat() {
        return "Output Ingestion has failed for workflow " + workflowName + " with error " + errorMessage;
    }
}
