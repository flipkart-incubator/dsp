package com.flipkart.dsp.models.sg;

import com.fasterxml.jackson.annotation.*;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonSnakeCase
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Signal implements Serializable {
    @JsonProperty
    private Long id;

    @JsonProperty
    private String name;

    @JsonProperty
    private SignalDataType signalDataType;

    @JsonProperty
    private SignalDefinition signalDefinition;

    @JsonProperty
    private String signalBaseEntity;

    public Signal(Long id, String name, SignalDataType signalDataType, SignalDefinition signalDefinition, String signalBaseEntity) {
        this.id = id;
        this.name = name;
        this.signalDataType = signalDataType;
        this.signalDefinition = signalDefinition;
        this.signalBaseEntity = signalBaseEntity;
    }

    private Boolean isPrimary = null;

    private Boolean isVisible = null;

    @JsonProperty
    private String dataTableName;

    @JsonProperty
    private String dataSourceID;

    public Signal(String name) {
        this.name = name;
    }
}
