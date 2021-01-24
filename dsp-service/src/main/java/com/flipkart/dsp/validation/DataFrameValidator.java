package com.flipkart.dsp.validation;

import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.exceptions.ConfigurableSGException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.sg.DataFrame;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;

/**
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameValidator {
    private final DataFrameActor dataFrameActor;

    public List<DataFrame> validateDataFrames(List<Long> dataFrameIds) {
        List<DataFrame> dataFrames = new ArrayList<>();
        List<String> errorList = new ArrayList<>();
        dataFrameIds.forEach(dataFrameId -> {
            try {
                dataFrames.add(validateDataFrame(dataFrameId));
            } catch (ValidationException e) {
                errorList.add("DataFrame with id " + dataFrameId + " does'nt exist");
            }
        });
        if (errorList.size() != 0)
            throw new ConfigurableSGException(errorList.toString());
        return dataFrames;
    }


    public DataFrame validateDataFrame(Long dataFrameId) throws ValidationException {
        DataFrame dataFrame = dataFrameActor.getDataFrameById(dataFrameId);
        if (Objects.isNull(dataFrame))
            throw new ValidationException("No DataFrame found for id " + dataFrameId);
        return dataFrame;
    }

}
