package com.flipkart.dsp.models.event_audits.event_type.sg_node;

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
public class DataFrameQueryGenerationErrorEvent extends Events implements Serializable {
    private String workflowName;
    private String dataFrameName;
    private String errorMessage;
    private Timestamp failureTime;

    @Override
    public String prettyFormat() {
        return "Error occurred while generating signal Generation Query for Dataframe " + dataFrameName + " with following details : \n" +
                "workflow : " + workflowName + "\nerrorMessage : " + errorMessage + "\nfailureTime : " + failureTime;
    }
}
