package com.flipkart.dsp.utils;

import com.flipkart.dsp.dto.ConfigurableSG.AutomateSGConfigurationDTO;
import com.flipkart.dsp.models.sg.SignalDataType;

public class SignalDataTypeMapper {

    public SignalDataType getSignalDataType(String type) {

        SignalDataType signalDataType = null;


        switch (type) {
            case "INTEGER":
                signalDataType = SignalDataType.INTEGER;
                break;
            case "TINY STRING":
            case "SMALL STRING":
            case "STRING":
                signalDataType = SignalDataType.TEXT;
                break;
            case "DATETIME":
                signalDataType = SignalDataType.TIME_DAY;
                break;
            case "DOUBLE":
                signalDataType = SignalDataType.DOUBLE;
                break;
            case "LONG":
                signalDataType = SignalDataType.FLOAT;
                break;
            case "BOOLEAN":
                signalDataType = SignalDataType.BOOLEAN;
                break;
            default:
                signalDataType = SignalDataType.TEXT;

        }
        return signalDataType;
    }
}
