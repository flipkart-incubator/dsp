package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 */
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
public class AzkabanWorkflowSubmitResponse {
    @JsonProperty("message")
    String message ;
    @JsonProperty("project")
    String project;
    @JsonProperty("flow")
    String flow;
    @JsonProperty("execid")
    long execid;
    @JsonProperty("error")
    String error;
}
