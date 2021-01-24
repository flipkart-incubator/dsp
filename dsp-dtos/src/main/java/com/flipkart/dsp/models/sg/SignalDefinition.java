package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonAutoDetect
public class SignalDefinition implements Serializable {

    @JsonProperty("value_type")
    private SignalValueType signalValueType;

    @JsonProperty("default_value")
    private Object defaultValue;

    @JsonProperty("aggregation_type")
    private AggregationType aggregationType;

    @JsonProperty("group_by")
    private Set<String> groupBy;

    @JsonProperty("scopes")
    private Set<SignalDefinitionScope> signalDefinitionScopeSet;
}
