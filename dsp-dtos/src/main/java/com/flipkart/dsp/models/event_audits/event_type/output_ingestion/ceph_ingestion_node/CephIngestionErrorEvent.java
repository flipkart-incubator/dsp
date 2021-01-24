package com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node;

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
public class CephIngestionErrorEvent extends Events implements Serializable {
    private String message;
    private String dataFrameName;
    private CephOutputLocation cephOutputLocation;

    @Override
    public String prettyFormat() {
        return "CephIngestion has failed for dataFrame " + dataFrameName + " with error " + message;
    }
}
