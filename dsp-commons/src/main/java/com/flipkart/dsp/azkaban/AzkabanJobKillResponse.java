package com.flipkart.dsp.azkaban;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 */

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class AzkabanJobKillResponse {

    @JsonProperty("error")
    private String error;

}
