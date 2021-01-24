package com.flipkart.dsp.entities.sg.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flipkart.dsp.models.sg.DataFrameConfig;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import lombok.*;


/**
 */

@AllArgsConstructor
@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataFrameAudit {
    private Long runId;
    private Long logAuditId;
    private String partitions;
    private Long dataframeSize;
    private DataFrame dataFrame;
    private Long overrideAuditId;
    private SGUseCasePayload payload;
    private DataFrameConfig dataFrameConfig;
    private DataFrameAuditStatus dataFrameAuditStatus;

}
