package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import javax.persistence.Entity;

/**
 */
@Data
@Entity
@Builder
@JsonAutoDetect
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionDetails {
    private long executionTime;
    private String executionTimeStr;
    private String status;
    private Long pipelineStepId;
}
