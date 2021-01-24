package com.flipkart.dsp.models.overrides;

import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.LinkedHashMap;

@Data
@AllArgsConstructor
public class HiveQueryDataframeOverride implements DataframeOverride {
    private String query;
    private LinkedHashMap<String, SignalDataType> columnMapping;
    private LinkedHashMap<String, String> tableMapping;
}
