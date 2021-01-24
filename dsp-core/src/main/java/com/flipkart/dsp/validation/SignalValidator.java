package com.flipkart.dsp.validation;

import com.flipkart.dsp.exceptions.ConfigurableSGException;
import com.flipkart.dsp.models.sg.*;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SignalValidator {

    public boolean verifyDataTableColumnDetails(DataFrame dataFrame, Map<String, SignalDataType> columnNameDataTypeMap) {
        HashSet<Signal> signalDetails = dataFrame.getSignalGroup().getSignalMetas().stream()
                .map(SignalGroup.SignalMeta::getSignal).collect(Collectors.toCollection(HashSet::new));

        AtomicReference<Boolean> isIdentical = new AtomicReference(true);
        if (signalDetails.size() != columnNameDataTypeMap.size()) {
            return false;
        }
        // check visible sequence
        LinkedHashSet<Signal> visibleSignals = dataFrame.getDataFrameConfig().getVisibleSignals();
        ArrayList<String> columnList = new ArrayList<>(columnNameDataTypeMap.keySet());
        Iterator<Signal> it1 = visibleSignals.iterator();
        Iterator<String> it2 = columnList.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Signal signal = it1.next();
            String colName = it2.next();
            if (signal == null) return false;
            if (signal.getName() == null) return false;
            if (!signal.getName().equals(colName)) return false;
        }

        // check colunm name and datatype
        signalDetails.stream().forEach(e -> {
            if (!(columnNameDataTypeMap.containsKey(e.getSignalBaseEntity()) &&
                    columnNameDataTypeMap.get(e.getSignalBaseEntity()).equals(e.getSignalDataType()))) {
                isIdentical.set(false);
            }
        });
        return isIdentical.get();
    }


    public boolean verifyDataTableColumnDefaults(DataFrame sgDataFrame, Map<SignalDataType, String> datatypeDefaults) {
        HashSet<Signal> signalDetails = sgDataFrame.getSignalGroup().getSignalMetas().stream()
                .map(SignalGroup.SignalMeta::getSignal).collect(Collectors.toCollection(HashSet::new));

        return signalDetails.stream().allMatch(signal -> datatypeDefaults.getOrDefault(signal.getSignalDataType(), "NULL")
                .equals(signal.getSignalDefinition().getDefaultValue()));
    }


    public void checkForMandatoryFields(ConfigurableSGDTO configurableSGDTO) {
        for (DataFrame sgDataFrame : configurableSGDTO.getDataFrameList()) {
            checkForNotNullAndEmpty(sgDataFrame.getName(), "DataFrame Name cannot be empty");
            checkForNotNullAndEmpty(sgDataFrame.getDataFrameConfig(), "DataFrame Config cannot be empty");
            checkForNotNullAndEmpty(sgDataFrame.getDataFrameConfig().getVisibleSignals(), "Visible signal cannot be null");

            for (Signal signal : sgDataFrame.getDataFrameConfig().getVisibleSignals()) {
                checkForNotNullAndEmpty(signal.getDataTableName(), "dataTableName cannot be empty");
                checkForNotNullAndEmpty(signal.getSignalDataType(), "SignalDataType cannot be empty");
                checkForNotNullAndEmpty(signal.getName(), "Signal name cannot be empty");
                checkForNotNullAndEmpty(signal.getSignalDefinition(), "SignalDefinition cannot be empty");
                //making default value optional
                //                checkForNotNullAndEmpty(signal.getSignalDefinition().getDefaultValue(), "Default value cannot be empty");
                checkForNotNullAndEmpty(signal.getSignalBaseEntity(), "SignalBase Entity cannot be empty");
                checkForNotNullAndEmpty(signal.getIsVisible(), "isVisible cannot be empty");
            }
        }
    }

    private void checkForNotNullAndEmpty(Object object, String errorMessgae) {
        if (object == null || StringUtils.isBlank(object.toString())) {
            throw new ConfigurableSGException(String.valueOf(errorMessgae));
        }
    }
}
