package com.flipkart.dsp.executor.persist;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.dto.FilePayload;
import com.flipkart.dsp.dto.MultiDataStorePutRequest;
import com.flipkart.dsp.dto.MultiDataStorePutResponse;
import com.flipkart.dsp.dto.MultiDataStoreStorageAdapter;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.exception.ModelPersistenceException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ModelPersister {
    private final LocationManager locationManager;
    private final MultiDatastoreClient multiDatastoreClient;

    @Timed
    @Metered
    public Set<ScriptVariable> persist(Set<ScriptVariable> scriptVariableSet, String workflowName, String pipelineExecutionId) throws ModelPersistenceException {
        Set<ScriptVariable> modelScriptVariableSet = new HashSet<>();
        for (ScriptVariable scriptVariable : scriptVariableSet) {
            if (scriptVariable.getDataType().equals(DataType.MODEL)) {
                modelScriptVariableSet.add(persist(scriptVariable, workflowName, pipelineExecutionId));
            }
        }
        return modelScriptVariableSet;
    }

    private ScriptVariable persist(ScriptVariable scriptVariable, String workflowName, String pipelineExecutionId) throws ModelPersistenceException {
        try {
            String modelRemoteLocation = locationManager.getModelStorageLocation(workflowName, pipelineExecutionId);
            String modelLocalLocation = String.valueOf(scriptVariable.getValue());
            MultiDataStorePutRequest multiDataStorePutRequest = new MultiDataStorePutRequest(new FilePayload(modelLocalLocation), null
                    , modelRemoteLocation, MultiDataStoreStorageAdapter.HDFS);
            MultiDataStorePutResponse multiDataStorePutResponse = multiDatastoreClient.put(multiDataStorePutRequest);
            return new ScriptVariable(scriptVariable.getName(), DataType.MODEL, multiDataStorePutResponse, null, null, scriptVariable.getRequired());
        } catch (MultiDataStoreClientException e) {
            throw new ModelPersistenceException(scriptVariable, e);
        }
    }
}
