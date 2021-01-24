package com.flipkart.dsp.executor.persist;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.dto.FilePayload;
import com.flipkart.dsp.dto.MultiDataStorePutRequest;
import com.flipkart.dsp.dto.MultiDataStorePutResponse;
import com.flipkart.dsp.dto.MultiDataStoreStorageAdapter;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.exception.PersistenceException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class VariablePersister {
    private final HadoopConfig hadoopConfig;
    private final ModelPersister modelPersister;
    private final LocationManager locationManager;
    private final DataFramePersister dataFramePersister;
    private final MultiDatastoreClient multiDatastoreClient;

    public Set<ScriptVariable> persist(String pipelineExecutionId, String workflowName, Set<ScriptVariable> scriptVariableSet,
                                       List<String> partitionSignalMapping, Long refreshId, String scope) throws PersistenceException, TException, TableNotFoundException  {
        Set<ScriptVariable> persistedScriptVariables = dataFramePersister.persist(pipelineExecutionId, scriptVariableSet,
                partitionSignalMapping, refreshId, scope, workflowName);
        persistedScriptVariables.addAll(modelPersister.persist(scriptVariableSet, workflowName, pipelineExecutionId));
        return persistedScriptVariables;
    }

    @Timed
    @Metered
    public Set<ScriptVariable> moveIntermediateVariablesToHDFS(Set<ScriptVariable> outputScriptVariables) throws PersistenceException {
        for (ScriptVariable scriptVariable: outputScriptVariables) {
            if (scriptVariable.getDataType().equals(DataType.BYTEARRAY)) {
                String hdfsIntermediateLocation = locationManager.getHDFSIntermediateFolderPath();
                final String localFileLocation = String.valueOf(scriptVariable.getValue());
                try {
                    MultiDataStorePutRequest multiDataStorePutRequest = new MultiDataStorePutRequest(new FilePayload(localFileLocation),
                            null, hdfsIntermediateLocation, MultiDataStoreStorageAdapter.HDFS);
                    MultiDataStorePutResponse multiDataStorePutResponse = multiDatastoreClient.put(multiDataStorePutRequest);
                    scriptVariable.setValue(multiDataStorePutResponse);
                } catch (MultiDataStoreClientException e) {
                    throw new PersistenceException("Failed to move data from "+ localFileLocation
                            +" to {}" + hdfsIntermediateLocation + " with user" + hadoopConfig.getUser(),e );
                }
            }
        }
        return outputScriptVariables;
    }
}
