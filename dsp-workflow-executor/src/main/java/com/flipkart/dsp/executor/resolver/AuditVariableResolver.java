package com.flipkart.dsp.executor.resolver;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.exception.AuditVariableResolutionException;
import com.flipkart.dsp.executor.exception.ModelResolutionException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.Constants;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class AuditVariableResolver {
    private final ModelResolver modelResolver;
    private final LocationManager locationManager;
    private final DSPServiceClient dspServiceClient;
    private final MultiDatastoreClient multiDatastoreClient;

    public Set<ScriptVariable> resolve(String pipelineExecutionId, String scope, final String parentWorkflowExecutionId, PipelineStep pipelineStep, LocalScript script) throws AuditVariableResolutionException {
        RunConfig runConfig = dspServiceClient.getPipelineStepRuntimeConfig(pipelineExecutionId, pipelineStep.getParentPipelineStepId()).getRunConfig();
        //Extracted variables from last pipeline step
        Set<ScriptVariable> extractedVariables = runConfig.getExtractedVariableSet();
        Set<ScriptVariable> inputScriptVariables = script.getInputVariables();
        if (!validateVariables(extractedVariables, inputScriptVariables)) {
            throw new AuditVariableResolutionException("Failed to resolve Script Variables. InputScriptVariables:  "
                    + inputScriptVariables + "extractedVariables: " + extractedVariables + ". Validation failed.");
        }
        try {
            prepareExtractedVariables(extractedVariables);
            extractedVariables.addAll(modelResolver.resolve(inputScriptVariables, scope, parentWorkflowExecutionId));
        } catch (MultiDataStoreClientException | ModelResolutionException e) {
            throw new AuditVariableResolutionException("Failed to resolve Script Variables. InputScriptVariables:  "
                    + inputScriptVariables + "extractedVariables: " + extractedVariables, e);
        }
        return extractedVariables;
    }

    private boolean validateVariables(Set<ScriptVariable> extractedVariableSet, Set<ScriptVariable> inputVariableSet) {
        boolean valid = true;
        Map<String, ScriptVariable> extractedVariableMap = Maps.uniqueIndex(extractedVariableSet, ScriptVariable::getName);
        for (ScriptVariable scriptVariable : inputVariableSet) {
            if (!scriptVariable.getDataType().equals(DataType.MODEL) && !extractedVariableMap.containsKey(scriptVariable.getName())) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    private void prepareExtractedVariables(Set<ScriptVariable> scriptVariableSet) throws MultiDataStoreClientException {
        for (ScriptVariable scriptVariable : scriptVariableSet) {
            if (scriptVariable.getDataType().equals(DataType.BYTEARRAY)) {
                String localFolderPath = locationManager.getLocalFolderPath();
                MultiDataStorePutResponse multiDataStorePutResponse = MultiDataStorePutResponse.valueOf(scriptVariable.getValue());
                MultiDataStoreGetRequest multiDataStoreGetRequest =
                        new MultiDataStoreGetRequest(multiDataStorePutResponse.getPayloadId(), multiDataStorePutResponse.getStorageAdapter()
                                , MultiDataStorePayloadFormat.FILE, localFolderPath);
                MultiDataStoreGetResponse multiDataStoreGetResponse = multiDatastoreClient.get(multiDataStoreGetRequest);
                scriptVariable.setValue(((FilePayload) multiDataStoreGetResponse.getPayload()).getContent());
            }
        }
    }
}
