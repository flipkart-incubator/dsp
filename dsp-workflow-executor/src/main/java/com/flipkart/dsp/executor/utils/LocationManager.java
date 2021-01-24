package com.flipkart.dsp.executor.utils;

import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.*;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.dot;
import static com.flipkart.dsp.utils.Constants.slash;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class LocationManager {
    private final MiscConfig miscConfig;
    private final HadoopConfig hadoopConfig;
    private final LocationHelper locationHelper;
    private final MetaStoreClient metaStoreClient;

    private String FOLDER_SEPARATOR = "/";
    private String DATAFRAME_PATH = "dataframes/";

    public String getLocalFilePath(ScriptVariable scriptVariable) {
        String fileName = scriptVariable.getName() + scriptVariable.getDataType().toString() + UUID.randomUUID().toString();
        return getLocalFolderPath() + FOLDER_SEPARATOR + fileName + ".csv";
    }

    public String getLocalFolderPath() {
        return miscConfig.getDataBasePath();
    }

    public String buildHDFSPartitionLocation(String baseLocation, LinkedHashMap<String, String> partitionGranularity) {
        StringBuilder hdfsPartitionLocationBuilder = new StringBuilder();
        hdfsPartitionLocationBuilder.append(baseLocation);
        partitionGranularity.forEach((k, v) -> hdfsPartitionLocationBuilder.append(FOLDER_SEPARATOR).append(k).append("=").append(v));
        return hdfsPartitionLocationBuilder.toString();
    }

    public String putPartitionsInPath(List<String> partitions, Long requestId, String runtimeWorkflowName, String variableName) {
        String pathWithoutPartitions = uptoWorkflow(requestId, runtimeWorkflowName) + DATAFRAME_PATH;
        StringBuilder pathWithPartitions = new StringBuilder(pathWithoutPartitions);
        for (String partition : partitions) {
            pathWithPartitions.append(partition);
            pathWithPartitions.append(FOLDER_SEPARATOR);
        }
        pathWithPartitions.append(variableName).append(FOLDER_SEPARATOR);
        return pathWithPartitions.toString();
    }


    /**
     * The method will create the base path upto workflow name
     *
     * @param requestId
     * @param runtimeWorkflowName
     * @return
     */
    private String uptoWorkflow(Long requestId, String runtimeWorkflowName) {
        String createdPath = null;
        createdPath = uptoRequestId(requestId);
        createdPath += runtimeWorkflowName + FOLDER_SEPARATOR;
        return createdPath;

    }

    /**
     * The method will create the base path upto reques id
     * Ex: /tmp/dsp-service/sandbox/2017-09-26/3330292/
     * or hdfs://hadoopcluster2/tmp/dsp-service/sandbox/2017-09-26/3330292/
     *
     * @param requestId
     * @return
     */
    private String uptoRequestId(Long requestId) {
        String createdPath;
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        createdPath = locationHelper.getLocalPath() + date + FOLDER_SEPARATOR + requestId + FOLDER_SEPARATOR;
        return createdPath;
    }

    public String getHDFSIntermediateFolderPath() {
        return hadoopConfig.getBasePath() + FOLDER_SEPARATOR + UUID.randomUUID().toString();
    }

    public String getDefaultHDFSOutputLocation(String pipelineExeuctionId) {
        return hadoopConfig.getBasePath() + FOLDER_SEPARATOR + pipelineExeuctionId;
    }

    public String getModelStorageLocation(String workflowName, String pipelineExecutionId) {
        return hadoopConfig.getModelRepoLocation() + FOLDER_SEPARATOR + workflowName + FOLDER_SEPARATOR + pipelineExecutionId;
    }

    public List<String> getWorkflowOutputLocation(ScriptVariable scriptVariable, String pipelineExecutionId, String workflowName) throws HiveClientException, TException, TableNotFoundException {

        AbstractDataFrame outputVariables = ((AbstractDataFrame) scriptVariable.getAdditionalVariable());

        final ArrayList<String> hdfsLocationList = new ArrayList<>();
        if (!Objects.isNull(outputVariables) && !Objects.isNull(outputVariables.getHiveTable())) {
            //for backward compatibility
            hdfsLocationList.add(metaStoreClient.getTableLocation(outputVariables.getHiveTable()));
        } else {
            final List<OutputLocation> outputLocationDetailsList = scriptVariable.getOutputLocationDetailsList();
            for (OutputLocation outputLocationDetails : outputLocationDetailsList) {
                if (outputLocationDetails instanceof HiveOutputLocation) {
                    final String hiveTableName = ((HiveOutputLocation) outputLocationDetails).getDatabase() + dot
                            + ((HiveOutputLocation) outputLocationDetails).getTable();
                    hdfsLocationList.add(metaStoreClient.getTableLocation(hiveTableName));
                } else if (outputLocationDetails instanceof HDFSOutputLocation) {
                    String hdfsLocation = ((HDFSOutputLocation) outputLocationDetails).getLocation();
                    if (hdfsLocation == null || hdfsLocation.isEmpty()) {
                        hdfsLocation = getDefaultHDFSOutputLocation(pipelineExecutionId);
                    }
                    hdfsLocationList.add(hdfsLocation);
                } else if (outputLocationDetails instanceof CephOutputLocation) {
                    String path = ((CephOutputLocation) outputLocationDetails).getPath();
                    String hdfsLocation = miscConfig.getCephBaseHDFSPath() + slash + workflowName + slash
                            + scriptVariable.getName() + (Objects.isNull(path) ? "" : slash + path);
                    hdfsLocationList.add(hdfsLocation);
                }
            }
        }
        return hdfsLocationList;
    }
}
