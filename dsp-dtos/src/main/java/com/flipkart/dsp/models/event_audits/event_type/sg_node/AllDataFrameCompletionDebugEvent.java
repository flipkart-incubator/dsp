package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AllDataFrameCompletionDebugEvent extends Events implements Serializable {

    private String workflowName;
    private Timestamp completionTime;
    private List<DataFrameDetails> dataFrameDetails;

    @Override
    public String prettyFormat() {
        StringBuilder str = new StringBuilder();
        str.append("Processing of Dataframe related to workflow " + workflowName + " is completed.Details of Dataframe are as follow\n");
        dataFrameDetails.forEach(dataframeDetail -> {
            str.append("Details for Dataframe : " + dataframeDetail.getDataFrameName() + " are as follow : \n");
            str.append("Dataframe Size : " + dataframeDetail.getDataFrameSize() + "\n");
            str.append("No of Partitions : " + dataframeDetail.getNoOfPartitions() + "\n");
            str.append("Partitions Names : " + dataframeDetail.getPartitionNames() + "\n");
        });
        return str.toString();
    }
}
