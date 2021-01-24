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
public class DataFrameGenerationStartInfoEvent extends Events implements Serializable {
    private String workflowName;
    private String dataFrameName;
    private Timestamp generationStartTime;

    @Override
    public String prettyFormat() {
        return "Dataframe Generation is started for : \nworkflow " + workflowName + "\n dataframe " + dataFrameName;
    }
}
