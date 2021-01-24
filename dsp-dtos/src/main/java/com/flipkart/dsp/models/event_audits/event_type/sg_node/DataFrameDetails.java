package com.flipkart.dsp.models.event_audits.event_type.sg_node;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataFrameDetails {
    private String dataFrameName;
    private Long dataFrameSize;
    private int noOfPartitions;
    private List<String> partitionNames;
}
