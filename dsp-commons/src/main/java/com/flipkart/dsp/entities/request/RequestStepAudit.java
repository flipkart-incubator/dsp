package com.flipkart.dsp.entities.request;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.enums.RequestStepType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
public class RequestStepAudit {
    private Long id;
    private RequestStepAuditStatus requestStepAuditStatus;
    private long requestStepId;
    private String metaData;
    private Date createdAt;
    private Date updatedAt;
    private RequestStepType requestStepType;
}
