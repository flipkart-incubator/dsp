package com.flipkart.dsp.entities.sg.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.time.LocalDateTime;

/**
 */

@AllArgsConstructor
@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataFrameOverrideAudit {
    private Long id;
    private Long requestId;
    private Long purgePolicyId;
    private LocalDateTime expiresAt;
    private Long workflowId;
    private Long dataframeId;
    private Boolean isDeleted;
    private String inputDataId;
    private String inputMetadata;
    private String outputMetadata;
    private DataFrameOverrideState state;
    private DataFrameOverrideType dataFrameOverrideType;
}
