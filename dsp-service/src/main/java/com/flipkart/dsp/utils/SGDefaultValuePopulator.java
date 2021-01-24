package com.flipkart.dsp.utils;

import com.flipkart.dsp.models.sg.*;

public class SGDefaultValuePopulator {
    public SignalDefinition getSignalDefinition(SignalDefinition signalDefinition, String defaultValue) {
        if (signalDefinition == null) {
            signalDefinition = new SignalDefinition();
            // set default value for newly created signals
            signalDefinition.setDefaultValue(defaultValue);
        }
        SignalValueType signalValueType = signalDefinition.getSignalValueType() == null ? SignalValueType.ONE_TO_ONE : signalDefinition.getSignalValueType();
        AggregationType aggregationType = signalDefinition.getAggregationType() == null ? AggregationType.NA : signalDefinition.getAggregationType();

        signalDefinition.setAggregationType(aggregationType);
        signalDefinition.setSignalValueType(signalValueType);
        return signalDefinition;
    }

}
