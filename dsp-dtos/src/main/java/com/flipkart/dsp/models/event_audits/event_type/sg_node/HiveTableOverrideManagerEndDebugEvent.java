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
public class HiveTableOverrideManagerEndDebugEvent extends Events implements Serializable {
    private String dataFrameName;
    private String workflowName;
    private String dbName;
    private String tableName;
    private Long refreshId;

    @Override
    public String prettyFormat() {
        return "HiveTableOverride is complete :\n" + "DataFrame : " + dataFrameName +
                "\nworkflow : " + workflowName + "\nDatabase Name : " + dbName
                + "\nTableName : " + tableName + "\nRefreshId : " + refreshId;
    }
}
