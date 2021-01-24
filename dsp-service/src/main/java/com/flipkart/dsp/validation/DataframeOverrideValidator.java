package com.flipkart.dsp.validation;

import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.SignalGroup;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.utils.SignalDataTypeMapper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;


@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataframeOverrideValidator {
    private final DataFrameActor dataFrameActor;
    private final MetaStoreClient metaStoreClient;
    private final SignalDataTypeMapper signalDataTypeMapper;

    public void verifyOverrides(WorkflowDetails workflowDetails, ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {

        if (executeWorkflowRequest.getRequestOverride() == null || executeWorkflowRequest.getRequestOverride().getDataframeOverrideMap() == null) {
            return;
        }

        RequestOverride requestOverride = executeWorkflowRequest.getRequestOverride();
        Map<String, DataframeOverride> dataframeOverrideMap = requestOverride.getDataframeOverrideMap();
        Map<String, List<SignalGroup.SignalMeta>> dataframeSignals = new HashMap<>();
        List<String> errorMessages = new ArrayList<>();

        workflowDetails.getWorkflow().getDataFrames().forEach(dataframe -> {
            dataframeSignals.put(dataframe.getName(), dataFrameActor.getDataFrameById(dataframe.getId()).getSignalGroup().getSignalMetas());
        });
        if (!errorMessages.isEmpty()) {
            throw new ValidationException(errorMessages.toString());
        }
    }
}
