package com.flipkart.dsp.models.overrides;

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CSVDataframeOverride.class, name = "CSVDataframeOverride"),
        @JsonSubTypes.Type(value = FTPDataframeOverride.class, name = "FTPDataframeOverride"),
        @JsonSubTypes.Type(value = HiveDataframeOverride.class, name = "HiveDataframeOverride"),
        @JsonSubTypes.Type(value = RunIdDataframeOverride.class, name = "RunIdDataframeOverride"),
        @JsonSubTypes.Type(value = DefaultDataframeOverride.class, name = "DefaultDataframeOverride"),
        @JsonSubTypes.Type(value = PartitionDataframeOverride.class, name = "PartitionDataframeOverride"),
        @JsonSubTypes.Type(value = HiveQueryDataframeOverride.class, name = "HiveQueryDataframeOverride"),
})
@JsonIgnoreProperties(ignoreUnknown = true)
public interface DataframeOverride {
}
