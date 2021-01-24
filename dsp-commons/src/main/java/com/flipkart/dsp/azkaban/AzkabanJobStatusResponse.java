package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.entities.enums.AzkabanJobStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 */

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class AzkabanJobStatusResponse {

    @JsonProperty("projectId")
    private int projectId;

    @JsonProperty("project")
    private String project;

    @JsonProperty("status")
    private AzkabanJobStatus status;

    @JsonProperty("flow")
    private String flow;

    @JsonProperty("flowId")
    private String flowId;

    @JsonProperty("execid")
    private String execid;

    @JsonProperty("startTime")
    private Long startTime;

    @JsonProperty("endTime")
    private Long endTime;

    private List<Node> nodes;


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class Node {
        private String id;
    }
}
