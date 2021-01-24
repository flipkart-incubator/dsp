package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
public class AzkabanLoginResponse {
    @JsonProperty("session.id")
    String sessionId ;
    @JsonProperty("status")
    String status;
    @JsonProperty("error")
    String error;
}
