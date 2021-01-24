package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class AzkabanProjectScheduleRequestResponse {

    @JsonProperty("message")
    private String message;

    @JsonProperty("status")
    private String status;
}
