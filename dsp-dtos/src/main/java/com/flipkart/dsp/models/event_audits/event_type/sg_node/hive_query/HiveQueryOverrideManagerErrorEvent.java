package com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query;

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
public class HiveQueryOverrideManagerErrorEvent extends Events implements Serializable {
    private String dataFrameName;
    private String workflowName;
    private String query;
    private String errorMessage;

    @Override
    public String prettyFormat() {
        return "Error while Executing Hive Query:\n" + "Dataframe : " + dataFrameName +
                "\nworkflow : " + workflowName + "\nQuery : " + query + "\nError Message : " + errorMessage;
    }
}
