package com.flipkart.dsp.executor.resolver;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.engine.utils.DataTypeResolver;
import com.flipkart.dsp.engine.utils.HeaderResolver;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
import com.flipkart.dsp.executor.exception.DataframeResolutionException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameConfig;
import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.utils.HdfsUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameResolver {
    private final HdfsUtils hdfsUtils;
    private final LocationManager locationManager;
    private final DSPServiceClient dspServiceClient;
    private final MultiDatastoreClient multiDatastoreClient;
    private final HeaderResolver headerResolver;
    private final DataTypeResolver dataTypeResolver;
    /**
     * The method set all dataframes related variable in given scriptParameters set
     *
     * @return
     * @throws Exception
     */
    public Set<ScriptVariable> resolve(String workflowName, Long parentWorkflowId, Long refreshId,
                                       Map<String, String> inputCsvHdfsLocation, Map<String, DataFrame> dataFrameMap,
                                       Set<ScriptVariable> inputVariables, List<String> partitions) throws DataframeResolutionException {
        Set<ScriptVariable> scriptVariableSet = new HashSet<>();
        for (ScriptVariable scriptVariable : inputVariables)  {
            String dataFrameName = scriptVariable.getName();
            DataFrame dataFrame = dataFrameMap.get(dataFrameName);
            if (dataFrame != null) {
                MesosCosmosTag.dataframeName = dataFrameName;
                extractDataFrameToLocal(partitions, dataFrame, workflowName, refreshId, scriptVariableSet,
                        dataFrameName, inputCsvHdfsLocation.get(dataFrameName), inputVariables, parentWorkflowId);
            }
        }
        return scriptVariableSet;
    }

    @Timed
    @Metered
    protected void extractDataFrameToLocal(List<String> partitions, DataFrame dataFrame, String workflowName, Long refreshId,
                                           Set<ScriptVariable> scriptVariableSet, String dfName, String dfLocation,
                                           Set<ScriptVariable> inputVariables, Long parentWorkflowId) throws DataframeResolutionException {

        List<String> partitionsList = getPartitionsList(dfLocation, dataFrame, partitions);
        String partitionDirectory = locationManager.putPartitionsInPath(partitionsList, refreshId, workflowName, dfName);
        Set<ScriptVariable> parentScriptVariables = getParentScriptVariables(parentWorkflowId);
        Optional<ScriptVariable> scriptVariableOptional = inputVariables.stream().
                filter(v -> v.getName().equalsIgnoreCase(dfName)).findFirst();

        if(!scriptVariableOptional.isPresent() && !CollectionUtils.isEmpty(parentScriptVariables)) {
            scriptVariableOptional = parentScriptVariables.stream().filter(v -> v.getName().equalsIgnoreCase(dfName)).findFirst();
        }
        if (!scriptVariableOptional.isPresent())
            throw new DataframeResolutionException("Can't resolve dataframe type for " + dfName);

        final ScriptVariable scriptVariable = scriptVariableOptional.get();
        AbstractDataFrame abstractDataFrame = (AbstractDataFrame) scriptVariable.getAdditionalVariable();
        resolveHeaders(dataFrame, scriptVariable, abstractDataFrame);
        String value = copyDataToLocal(dfName, dfLocation, partitionDirectory, scriptVariable.getDataType());
        scriptVariableSet.add(new ScriptVariable(dfName, scriptVariable.getDataType(), value,
                    abstractDataFrame, scriptVariable.getOutputLocationDetailsList(), scriptVariable.getRequired()));
    }

    private List<String> getPartitionsList(String dfLocation, DataFrame dataFrame, List<String> partitions) {
        Integer partitionNumbers = partitions.size();
        String[] givenHdfsDirectories = dfLocation.split("/");
        filterVisibleSignalList(dataFrame, partitions);
        return getPartitions(partitionNumbers, givenHdfsDirectories);
    }

    private String copyDataToLocal(String dfName, String dfLocation, String partitionDirectory, DataType dataType) throws DataframeResolutionException {
      try {
          if (dataType.toString().equalsIgnoreCase(DataType.DATAFRAME_PATH.toString())) {
              MultiDataStoreGetResponse response =  copyFileToLocal(new Path(dfLocation), partitionDirectory);
              return ((FilePayload) response.getPayload()).getContent();
          } else {
              for (FileStatus fileStatus : hdfsUtils.getListStatus(dfLocation)) {
                  copyFileToLocal(fileStatus.getPath(), partitionDirectory);
              }
              return partitionDirectory;
          }
      } catch (IOException e) {
          throw new DataframeResolutionException("Unable to copy file to Local");
      }

    }

    private MultiDataStoreGetResponse copyFileToLocal(Path path, String partitionDirectory) throws DataframeResolutionException {
        try {
            MultiDataStoreGetRequest multiDataStoreGetRequest = new MultiDataStoreGetRequest(path.toString(),
                    MultiDataStoreStorageAdapter.HDFS, MultiDataStorePayloadFormat.FILE, partitionDirectory);
            return multiDatastoreClient.get(multiDataStoreGetRequest);
        } catch (MultiDataStoreClientException e) {
            throw new DataframeResolutionException("Failed to copy from HDFS to local:", e);
        }
    }

    private Set<ScriptVariable> getParentScriptVariables(Long parentWorkflowId) {
        Set<ScriptVariable> parentScriptVariables = new HashSet<>();
        if(parentWorkflowId != null) {
            WorkflowDetails parentWorkflowDetails = dspServiceClient.getWorkflowDetails(parentWorkflowId);
            parentScriptVariables = parentWorkflowDetails.getPipelineSteps().stream()
                    .flatMap(ps -> ps.getScript().getInputVariables().stream())
                    .filter(v -> (v.getDataType().equals(DataType.DATAFRAME) || v.getDataType().equals(DataType.DATAFRAME_PATH)))
                    .collect(toSet());
        }
        return parentScriptVariables;
    }

    private void filterVisibleSignalList(DataFrame dataFrame, List<String> partitions) {
        DataFrameConfig dataFrameConfig = dataFrame.getDataFrameConfig();
        if(partitions.size() > 0 && dataFrameConfig!=null) {
            LinkedHashSet<Signal> visibleSignals = dataFrameConfig.getVisibleSignals();
            LinkedHashSet<Signal> visibleSignalWithoutPartitions =
                        visibleSignals.stream()
                        .filter(vSignal -> !partitions.contains(vSignal.getName()))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            dataFrameConfig.setVisibleSignals(visibleSignalWithoutPartitions);
        }
    }

    private void resolveHeaders(DataFrame dataFrame, ScriptVariable scriptVariable,
                                AbstractDataFrame abstractDataFrame) {

        if (scriptVariable.getDataType().equals(DataType.DATAFRAME)) {
            LinkedHashSet<Signal> headers = dataFrame.getDataFrameConfig().getVisibleSignals();
            abstractDataFrame.setHeaders(headerResolver.getHeaders(headers, abstractDataFrame));
            abstractDataFrame.setHeaderDataTypes(dataTypeResolver.getDataTypes(headers, abstractDataFrame));
        }
    }

    /**
     * The method returns all partitions in for the given payload
     * Eg: Input: hdfs://hadoopcluster2/apps/hive/warehouse/dsp.db/dp_asp_usecase__3734/supercategory=ElectronicDevices%23Camera/fulfillment_type=NONFBF/000000_0
     * Output: partitions = [fulfillment_type=NONFBF, supercategory=ElectronicDevices%23Camera]
     *
     * @param partitionNumbers
     * @param givenHdfsDirectories
     * @return
     */
    private static List<String> getPartitions(Integer partitionNumbers, String[] givenHdfsDirectories) {
        List<String> partitions = new ArrayList<>();
        for (int ii = 0; ii <= partitionNumbers - 1; ii++) {
            partitions.add(0, givenHdfsDirectories[givenHdfsDirectories.length - 2 - ii]);
        }
        return partitions;
    }
}
