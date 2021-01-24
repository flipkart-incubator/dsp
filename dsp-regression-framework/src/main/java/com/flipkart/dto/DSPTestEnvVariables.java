package com.flipkart.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.*;

import java.io.Serializable;

@Data
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DSPTestEnvVariables extends TestEnvVariables  implements Serializable {
    private String bucketPostfix;
    private String sandboxCliVersion;
    private String executionEnvironment;
    private Long serviceDebianVersion = 0L;
    private Long azkabanDebianVersion = 0L;
    private Long mesosAgentDebianVersion = 0L;
    private String azkabanPackageVersion = "0";
    private String mesosAgentPackageVersion = "0";
}
