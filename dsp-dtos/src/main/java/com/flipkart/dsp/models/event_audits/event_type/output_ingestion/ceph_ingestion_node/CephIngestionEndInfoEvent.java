package com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CephIngestionEndInfoEvent extends Events implements Serializable {
    private List<URL> urls;
    private String dataFrameName;

    @Override
    public String prettyFormat() {
        return "CephIngestion has completed for dataFrame " + dataFrameName + " with following details: Output Cephurls:\n" + StringUtils.join(urls, "\n");
    }
}
