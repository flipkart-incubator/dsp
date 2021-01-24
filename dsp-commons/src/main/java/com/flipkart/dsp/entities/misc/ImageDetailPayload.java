package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 */

@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ImageDetailPayload implements Serializable {

    @JsonProperty
    ExecutionEnvironmentSummary executionEnvironmentSummary;

    @JsonProperty
    List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshots;

}
