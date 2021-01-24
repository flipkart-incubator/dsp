package com.flipkart.dsp.models.overrides;

import com.flipkart.dsp.models.CsvFormat;
import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.LinkedHashMap;

@Data
@AllArgsConstructor
public class CSVDataframeOverride implements DataframeOverride {
    private String path;
    private LinkedHashMap<String, SignalDataType> columnMapping;
    private CsvFormat csvFormat;
}
