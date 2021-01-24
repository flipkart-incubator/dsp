package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
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
public class PartitionOverrideEndDebugEvent  extends Events implements Serializable {
    private String workflowName;
    private String dataFrameName;
    private PartitionDataframeOverride partitionDataframeOverride;

    @Override
    public String prettyFormat() {
        return "Download of PartitionOverride is completed :\n" + "DataFrame : " + dataFrameName +
                "\nworkflow : " + workflowName + "\nPartitionDataframeOverride Details : " + partitionDataframeOverride.toString();
    }
}
