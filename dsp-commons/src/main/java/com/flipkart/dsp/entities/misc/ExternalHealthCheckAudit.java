package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * +
 */
@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalHealthCheckAudit implements Serializable {
    private String status;
    private String externalClient;
}
