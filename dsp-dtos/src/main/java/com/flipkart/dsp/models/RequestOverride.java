package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class RequestOverride {
    private Map<String/*dataframeId*/, DataframeOverride> dataframeOverrideMap;
    private List<ObjectOverride > objectOverrideList;
}
