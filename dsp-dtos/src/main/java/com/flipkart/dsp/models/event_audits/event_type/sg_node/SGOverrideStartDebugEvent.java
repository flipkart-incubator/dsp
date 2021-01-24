package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;


@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SGOverrideStartDebugEvent extends Events implements Serializable {
    private String workflowName;
    private Map<String, DataframeOverride> dataFrameOverrideMap;

    @Override
    public String prettyFormat() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("SG Override is Started for " + workflowName + " workflow");
        stringBuffer.append("Following dataframes are considered for override.\n");
        dataFrameOverrideMap.forEach((key, value) -> {
            stringBuffer.append("Dataframe : " + key);
            stringBuffer.append("Details : " + value.toString() + "\n");
        });
        return stringBuffer.toString();
    }
}
