package com.flipkart.dsp.engine.utils;

import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataTypeResolver {

    private String getRDataTypes(LinkedHashSet<Signal> headers) {
        List<SignalDataType> signalDataTypes = getDataTypes(headers);
        List<String> rDataTypes = signalDataTypes.stream().map(DataTypeMapping.RMapping::get).collect(Collectors.toList());
        String res = rDataTypes.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
        System.out.println(res);
        return res;
    }

    private String getPythonDataTypes(LinkedHashSet<Signal> headers) {
        List<String> signalIds = getSignalIds(headers);
        List<SignalDataType> signalDataTypes = getDataTypes(headers);
        List<String> pythonDataTypes = signalDataTypes.stream().map(DataTypeMapping.PythonMapping::get).collect(Collectors.toList());
        int size = signalDataTypes.size();
        StringBuilder typeBuilder = new StringBuilder();
        for(int i=0; i<size; i++) {
            typeBuilder.append("'").append(signalIds.get(i)).append("':").append(pythonDataTypes.get(i)).append(",");
        }
        return typeBuilder.deleteCharAt(typeBuilder.length()-1).toString();
    }

    private List<String> getSignalIds(LinkedHashSet<Signal> headers) {
        return headers.stream().map(Signal::getName).collect(Collectors.toList());
    }

    private List<SignalDataType> getDataTypes(LinkedHashSet<Signal> headers) {
        return headers.stream().map(Signal::getSignalDataType).collect(Collectors.toList());
    }

    public String getDataTypes(LinkedHashSet<Signal> headers, AbstractDataFrame abstractDataFrame) {
        if (abstractDataFrame instanceof PandasDataFrame) {
            return getPythonDataTypes(headers);
        } else {
            return getRDataTypes(headers);
        }
    }
}
