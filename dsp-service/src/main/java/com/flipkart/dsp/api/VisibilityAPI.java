package com.flipkart.dsp.api;

import com.flipkart.dsp.dto.VisibilityDTO;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.SignalGroup;
import com.flipkart.dsp.validation.DataFrameValidator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;


@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class VisibilityAPI {
    private final DataFrameValidator dataFrameValidator;

    public Map<String, VisibilityDTO.VisibilityDataFrame> getDataFrameDetails(List<Long> dataFrameIdList) throws ValidationException {
        Map<String, VisibilityDTO.VisibilityDataFrame> visibilityDataFrames = new HashMap<>();
        for (Long dataFrameId : dataFrameIdList) {
            DataFrame sgDataFrame = dataFrameValidator.validateDataFrame(dataFrameId);
            VisibilityDTO.VisibilityDataFrame visibilityDataFrame = convertToVisibilityDataFrame(sgDataFrame);
            visibilityDataFrames.put(sgDataFrame.getName(), visibilityDataFrame);
        }
        return visibilityDataFrames;
    }

    private VisibilityDTO.VisibilityDataFrame convertToVisibilityDataFrame(DataFrame sgDataFrame) {
        return VisibilityDTO.VisibilityDataFrame.builder()
                .signalDetailsMap(getDataFrameSignals(sgDataFrame.getSignalGroup())).build();
    }

    private LinkedHashMap<String, VisibilityDTO.SignalDetails> getDataFrameSignals(SignalGroup signalGroup) {
        LinkedHashMap<String, VisibilityDTO.SignalDetails> signalDetailsMap = new LinkedHashMap<>();
        for (SignalGroup.SignalMeta signalMeta : signalGroup.getSignalMetas()) {
            VisibilityDTO.SignalDetails signalDetails = VisibilityDTO.SignalDetails.builder()
                    .signalDataType(signalMeta.getSignal().getSignalDataType())
                    .signalDefinition(signalMeta.getSignal().getSignalDefinition())
                    .build();
            signalDetailsMap.put(signalMeta.getSignal().getName(), signalDetails);
        }
        return signalDetailsMap;
    }


}
