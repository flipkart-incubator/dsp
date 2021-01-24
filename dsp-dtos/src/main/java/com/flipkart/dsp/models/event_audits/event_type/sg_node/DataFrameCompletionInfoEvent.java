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
public class DataFrameCompletionInfoEvent extends Events implements Serializable {
    private String dataFrameName;
    private String workflowName;
    private Timestamp completionTime;
    private Long dataFrameSize;

    @Override
    public String prettyFormat() {
        return "Processing of Dataframe related to workflow " + workflowName + " is completed.Details are as follow : \n"
                + "dataFrameName : " + dataFrameName + "\nCompletionTime : " + completionTime + "\nSize : " + dataFrameSize;
    }
}
