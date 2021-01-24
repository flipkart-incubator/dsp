package com.flipkart.dsp.executor.resolver;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.exception.ModelResolutionException;
import com.flipkart.dsp.executor.utils.LocationHelper;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.Constants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ModelResolver {

    private final LocationManager locationManager;
    private final DSPServiceClient dspServiceClient;
    private final MultiDatastoreClient multiDatastoreClient;

    public Set<ScriptVariable> resolve(Set<ScriptVariable> inputScriptVariableSet, String scope, final String parentWorkflowExecutionId)
            throws ModelResolutionException {
        Set<ScriptVariable> modelScriptVariableSet = new HashSet<>();
        for (ScriptVariable scriptVariable1 : inputScriptVariableSet) {
            if (scriptVariable1.getDataType().equals(DataType.MODEL)) {
                modelScriptVariableSet.add(resolve(scriptVariable1, scope, parentWorkflowExecutionId));
            }
        }
        return modelScriptVariableSet;
    }


    @Timed
    @Metered
    protected ScriptVariable resolve(ScriptVariable scriptVariable, String scope, final String parentWorkflowExecutionId) throws ModelResolutionException {
        String localFilePath = locationManager.getLocalFolderPath();
        if (parentWorkflowExecutionId == null) {
            log.error("Failed to resolve Model for scriptVariable {}: because no parent workflowExecutionId Found !!", scriptVariable);
            throw new ModelResolutionException("Failed to resolve Model for scriptVariable {}: because no parent workflowExecutionId Found !!");
        }
        List<PipelineStepRuntimeConfig> pipelineStepRuntimeConfigList = dspServiceClient.getPipelineStepRuntimeConfig(parentWorkflowExecutionId, scope);
        log.info("pipelineStepRuntimeConfigList: {}", pipelineStepRuntimeConfigList);

        List<ScriptVariable> scriptVariableList = pipelineStepRuntimeConfigList.stream()
                .flatMap(p -> p.getRunConfig().getPersistedVariableSet().stream().filter(pv -> pv.getDataType().equals(DataType.MODEL)))
                .collect(Collectors.toList());

        if (scriptVariableList.size() != 1) {
            log.error("Failed to resolve Model for scriptVariable {}: because more than one models persisted in parent workflow!!", scriptVariable);
            throw new ModelResolutionException("Failed to resolve Model for scriptVariable {}: because more than one models persisted in parent workflow!!");
        }

        MultiDataStorePutResponse multiDataStorePutResponse = MultiDataStorePutResponse.from((LinkedHashMap<String, String>) scriptVariableList.get(0).getValue());
        String remoteFilePath = multiDataStorePutResponse.getPayloadId();
        try {
            MultiDataStoreGetRequest multiDataStoreGetRequest = new MultiDataStoreGetRequest(remoteFilePath,
                    multiDataStorePutResponse.getStorageAdapter(), MultiDataStorePayloadFormat.FILE, localFilePath);
            MultiDataStoreGetResponse multiDataStoreGetResponse = multiDatastoreClient.get(multiDataStoreGetRequest);

            final String localFileLocation = ((FilePayload) multiDataStoreGetResponse.getPayload()).getContent();
            return new ScriptVariable(scriptVariable.getName(), DataType.MODEL, localFileLocation, null, null, scriptVariable.getRequired());
        } catch (MultiDataStoreClientException e) {
            log.error("Failed to resolve Model for scriptVariable {} ", scriptVariable, e);
            throw new ModelResolutionException(scriptVariable, e);
        }
    }

}
