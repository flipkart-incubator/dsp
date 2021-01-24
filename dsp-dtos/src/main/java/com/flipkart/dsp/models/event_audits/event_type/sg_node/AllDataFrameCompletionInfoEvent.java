package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AllDataFrameCompletionInfoEvent extends Events implements Serializable {
    private String workflowName;
    private Timestamp localTime;

    @Override
    public String prettyFormat() {
        StringBuilder str = new StringBuilder();
        str.append("Processing of Dataframe related to workflow " + workflowName + " is completed.");
        return str.toString();
    }
}
