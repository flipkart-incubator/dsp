package com.flipkart.dsp.executor.persist;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.dto.FilePayload;
import com.flipkart.dsp.dto.MultiDataStorePutRequest;
import com.flipkart.dsp.dto.MultiDataStoreStorageAdapter;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
import com.flipkart.dsp.executor.exception.DataframePersistenceException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFramePersister {
    private final LocationManager locationManager;
    private final MultiDatastoreClient multiDatastoreClient;

    public Set<ScriptVariable> persist(String pipelineExecutionId, Set<ScriptVariable> scriptVariableSet,
                                       List<String> partitionSignalMapping,
                                       Long refreshId, String scope, String workflowName) throws DataframePersistenceException , TException, TableNotFoundException{
        Set<ScriptVariable> persistedScriptVariable = new HashSet<>();
        Set<ScriptVariable> dataframeScriptVariableSet = filterPersistableVariables(scriptVariableSet);
        for (ScriptVariable scriptVariable : dataframeScriptVariableSet) {
            MesosCosmosTag.dataframeName = scriptVariable.getName();
            persistScriptVariable(pipelineExecutionId, persistedScriptVariable, scriptVariable,
                    partitionSignalMapping, refreshId, scope, workflowName);
        }
        return persistedScriptVariable;
    }

    private Set<ScriptVariable> filterPersistableVariables(Set<ScriptVariable> scriptVariableSet) {
        Set<ScriptVariable> newScriptVariableSet = new HashSet<>();
        scriptVariableSet.forEach(scriptVariable -> {
            if (scriptVariable.getDataType().equals(DataType.DATAFRAME)) {
                newScriptVariableSet.add(scriptVariable);
            }
        });
        return newScriptVariableSet;
    }

    @Timed
    @Metered
    protected void persistScriptVariable(String pipelineExecutionId, Set<ScriptVariable> persistedScriptVariable,
                                         ScriptVariable scriptVariable, List<String> partitionSignalMapping,
                                         Long refreshId, String scope, String workflowName) throws DataframePersistenceException, TException, TableNotFoundException  {
        try {
            List<String> partitionLocations = moveOutputDataFrameToHDFS(scriptVariable, pipelineExecutionId,
                    partitionSignalMapping, refreshId, scope, workflowName);
            persistedScriptVariable.add(new ScriptVariable(scriptVariable.getName(), scriptVariable.getDataType(),
                    partitionLocations, scriptVariable.getAdditionalVariable(), scriptVariable.getOutputLocationDetailsList(), scriptVariable.getRequired()));
        } catch (MultiDataStoreClientException | HiveClientException e) {
            log.error("Failed to move dataframe to hive for scriptVariable: " + scriptVariable, e);
            throw new DataframePersistenceException(scriptVariable, e);
        }
    }

    private List<String> moveOutputDataFrameToHDFS(ScriptVariable scriptVariable, String pipelineExecutionId,
                                                   List<String> partitionSignalMapping,
                                                   Long refreshId, String scope, String workflowName)
            throws HiveClientException, MultiDataStoreClientException , TException, TableNotFoundException{
        final String localPath = String.valueOf(scriptVariable.getValue());
        List<String> remotePaths = getRemotePathsForPartition(scriptVariable, pipelineExecutionId,
                partitionSignalMapping,refreshId, scope, workflowName);
        for (String remotePath : remotePaths) {
            MultiDataStorePutRequest multiDataStorePutRequest = new MultiDataStorePutRequest(new FilePayload(localPath)
                    , null, remotePath, MultiDataStoreStorageAdapter.HDFS);
            multiDatastoreClient.put(multiDataStorePutRequest);
        }
        return remotePaths;
    }

    private LinkedHashMap<String, String> createPartitionGranularityFromWhereClause(List<String> partitionSignalMapping,
                                                                                    Long refreshId, String scope) {
        LinkedHashMap<String, String> partitionGranularity = new LinkedHashMap<>();
        List<WhereClause> whereClause = JsonUtils.DEFAULT.fromJson(scope, new TypeReference<List<WhereClause>>() {});
        Map<String, String> whereClauseMap = buildWhereClauseMap(whereClause);
        partitionGranularity.put(Constants.REFRESH_ID, String.valueOf(refreshId));
        partitionSignalMapping.forEach(k -> partitionGranularity.put(k, whereClauseMap.get(k)));
        return partitionGranularity;
    }

    private Map<String, String> buildWhereClauseMap(List<WhereClause> whereClauseList) {
        Map<String, String> whereClauseMap = new HashMap<>();
        whereClauseList.forEach(whereClause -> {
            whereClauseMap.put(whereClause.getId(), whereClause.getValues().iterator().next());
        });
        return whereClauseMap;
    }

    private List<String> getRemotePathsForPartition(ScriptVariable scriptVariable, String pipelineExecutionId,
                                                    List<String> partitionSignalMapping,
                                                    Long refreshId, String scope, String workflowName) throws HiveClientException, TException, TableNotFoundException {
        List<String> hdfsLocationList = locationManager.getWorkflowOutputLocation(scriptVariable, pipelineExecutionId, workflowName);
        final LinkedHashMap<String, String> partitionGranularityFromWhereClause =
                createPartitionGranularityFromWhereClause(partitionSignalMapping, refreshId, scope);
        return hdfsLocationList.stream().map(hdfsLocation ->
                locationManager.buildHDFSPartitionLocation(hdfsLocation, partitionGranularityFromWhereClause))
                .collect(Collectors.toList());
    }
}
