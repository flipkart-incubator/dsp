package com.flipkart.dsp.entities.request;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.entities.enums.RequestStepType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@Builder
@JsonIgnoreProperties(ignoreUnknown=true)
public class RequestStep implements Serializable {
    private long id;
    private long requestId;
    private Date createdAt;
    private Date updatedAt;
    private String jobName;
    private RequestStepType requestStepType;
}
