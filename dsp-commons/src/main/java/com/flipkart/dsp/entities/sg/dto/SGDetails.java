package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;

/**
 * This class will contain runtime SG details and will evolve in future
 */

@Data
public class SGDetails {

    @JsonProperty("training_dataframe_location")
    private String trainingDatframeLocation;

    @JsonProperty("future_dataframe_location")
    private String futureDataframeLocation;
}
