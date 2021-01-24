package com.flipkart.dsp.engine.utils;

import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DataTypeMapping {
    public static final Map<SignalDataType, String> RMapping;
    public static final Map<SignalDataType, String> PythonMapping;

    static {
        Map<SignalDataType,String> RMap = new HashMap<>();
        RMap.put(SignalDataType.TEXT, "character");
        RMap.put(SignalDataType.BOOLEAN, "logical");
        RMap.put(SignalDataType.INTEGER, "integer");
        RMap.put(SignalDataType.DOUBLE, "double");
        RMap.put(SignalDataType.FLOAT, "numeric");
        RMap.put(SignalDataType.BIG_INTEGER, "numeric");
        RMap.put(SignalDataType.TIME_WEEK, "character");
        RMap.put(SignalDataType.DATETIME, "character");
        RMap.put(SignalDataType.TIME_DAY, "character");
        RMap.put(SignalDataType.TIME_YEAR, "character");
        RMap.put(SignalDataType.TIME_MONTH, "character");
        RMap.put(SignalDataType.DATE, "character");
        RMapping = Collections.unmodifiableMap(RMap);

        Map<SignalDataType,String> PyMap = new HashMap<>();
        PyMap.put(SignalDataType.TEXT, "str");
        PyMap.put(SignalDataType.BOOLEAN, "bool");
        PyMap.put(SignalDataType.INTEGER, "int");
        PyMap.put(SignalDataType.DOUBLE, "float");
        PyMap.put(SignalDataType.FLOAT, "float");
        PyMap.put(SignalDataType.BIG_INTEGER, "int");
        PyMap.put(SignalDataType.TIME_WEEK, "str");
        PyMap.put(SignalDataType.TIME_DAY, "str");
        PyMap.put(SignalDataType.TIME_YEAR, "str");
        PyMap.put(SignalDataType.TIME_MONTH, "str");
        PyMap.put(SignalDataType.DATETIME, "int");
        PyMap.put(SignalDataType.DATE, "str");
        PythonMapping = Collections.unmodifiableMap(PyMap);
    }
}
