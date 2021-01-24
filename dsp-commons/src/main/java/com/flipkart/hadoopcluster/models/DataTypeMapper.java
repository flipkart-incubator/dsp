package com.flipkart.hadoopcluster2.models;

import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class DataTypeMapper {
    public static final Map<SignalDataType, String> HiveMap;

    static {
        Map<SignalDataType,String> hiveMap = new HashMap<>();
        hiveMap.put(SignalDataType.INTEGER, "int");
        hiveMap.put(SignalDataType.FLOAT, "float");
        hiveMap.put(SignalDataType.DOUBLE, "double");
        hiveMap.put(SignalDataType.TEXT, "string");
        hiveMap.put(SignalDataType.TIME_DAY, "string");
        hiveMap.put(SignalDataType.TIME_WEEK, "string");
        hiveMap.put(SignalDataType.TIME_YEAR, "string");
        hiveMap.put(SignalDataType.TIME_MONTH, "string");
        hiveMap.put(SignalDataType.BOOLEAN, "boolean");
        hiveMap.put(SignalDataType.BIG_INTEGER, "bigint");
        hiveMap.put(SignalDataType.DATE, "date");
        hiveMap.put(SignalDataType.DATETIME, "timestamp");
        HiveMap = hiveMap;
    }
}
