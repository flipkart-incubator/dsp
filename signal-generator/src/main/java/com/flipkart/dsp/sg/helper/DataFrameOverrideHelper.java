package com.flipkart.dsp.sg.helper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPServiceException;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.sg.core.*;
import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.externalentities.FTPEntity;
import com.flipkart.dsp.models.overrides.FTPDataframeOverride;
import com.flipkart.dsp.models.overrides.HiveDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.utils.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus.COMPLETED;
import static com.flipkart.dsp.utils.Constants.*;


/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameOverrideHelper {
    private final HiveClient hiveClient;
    private final FileSystem fileSystem;
    private final ObjectMapper objectMapper;
    private final DSPServiceClient dspServiceClient;
    private final SignalDataTypeMapper signalDataTypeMapper;
    private final DataframeSizeExtractor dataframeSizeExtractor;

    private static final int WAIT_TIME_PIGGYBACK = 21600; //6 hours
    private static final String DESTINATION_FORMAT = "/%s%s.db/%s/%s=%d/";


    public DataFrameAudit getDataFrameAuditById(Long runId) {
        return dspServiceClient.getDataFrameAudit(runId);
    }

    public Map<String, DataFrame> getDataFrameMap(Workflow workflow) {
        return workflow.getDataFrames().stream().collect(Collectors.toMap(DataFrame::getName, Function.identity()));
    }

    public List<DataFrameKey> getDataFrameKeys(List<String> partitions) {
        List<DataFrameKey> keySet = new ArrayList<>();
        partitions.forEach(partition -> keySet.add(new DataFrameMultiKey(DataFrameColumnType.IN, new HashSet<>(Collections.singletonList(partition)))));
        return keySet;
    }

    public LinkedHashSet<String> getDataframeValues(String hdfsLocation) {
        LinkedHashSet<String> auditValueSet = Sets.newLinkedHashSet();
        auditValueSet.add(hdfsLocation);
        return auditValueSet;
    }

    public LinkedHashMap<String, DataFrameColumnType> getColumnMetadata(List<String> partitions) {
        LinkedHashMap<String, DataFrameColumnType> columnMetaData = Maps.newLinkedHashMap();
        partitions.forEach(partition -> {
            columnMetaData.put(partition, DataFrameColumnType.IN);
        });
        return columnMetaData;
    }

    public long getDataFrameSize(SGUseCasePayload sgUseCasePayload) {
        return dataframeSizeExtractor.getDataframeSize(sgUseCasePayload);
    }

    public DataFrameAudit saveDataFrameAudit(Long dataFrameSize, Long dataFrameOverrideAuditId, List<String> partitions,
                                             DataFrame dataFrame, SGUseCasePayload sgUseCasePayload) throws Exception {
        DataFrameAudit dataFrameAudit = DataFrameAudit.builder().dataFrame(dataFrame).dataframeSize(dataFrameSize)
                .payload(sgUseCasePayload).dataFrameAuditStatus(COMPLETED).overrideAuditId(dataFrameOverrideAuditId)
                .partitions(objectMapper.writeValueAsString(partitions)).build();
        return dspServiceClient.saveDataFrameAudit(dataFrameAudit);
    }

    public Long getDataFrameId(String dataFrameName, Workflow workflow) {
        return getDataFrameByName(dataFrameName, workflow).getId();
    }

    public DataFrame getDataFrameByName(String dataFrameName, Workflow workflow) {
        return getDataFrameMap(workflow).get(dataFrameName);
    }

    public DataFrameAudit getLatestDataFrameAuditByDataFrameId(Long dataFrameId) {
        return dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameId);
    }

    FTPEntity getFTPClientCredentials(FTPDataframeOverride ftpDataframeOverride) throws IOException {
        String clientAlias = ftpDataframeOverride.getClientAlias();
        ExternalCredentials externalCredentials = dspServiceClient.getExternalCredentials(clientAlias);
        return objectMapper.readValue(externalCredentials.getDetails(), FTPEntity.class);
    }

    public DataFrameOverrideAudit saveDataframeOverrideAudit(Long requestId, Long workflowId, Long dataframeId, String inputDataId,
                                                             Object inputMetaData, DataFrameOverrideType dataFrameOverrideType) throws Exception {
        DataFrameOverrideAudit dataFrameOverrideAudit = DataFrameOverrideAudit.builder().requestId(requestId)
                .workflowId(workflowId).dataframeId(dataframeId).inputDataId(inputDataId).isDeleted(false)
                .inputMetadata(objectMapper.writeValueAsString(inputMetaData))
                .state(DataFrameOverrideState.STARTED).dataFrameOverrideType(dataFrameOverrideType).build();
        return dspServiceClient.saveDataFrameOverrideAudit(dataFrameOverrideAudit);
    }

    public void saveDataframeOverrideAudit(Long requestId, Long workflowId, Long dataframeId, String inputDataId,
                                           Object inputMetaData, Object outputMetaData, DataFrameOverrideType dataFrameOverrideType) {
        DataFrameOverrideAudit dataFrameOverrideAudit = DataFrameOverrideAudit.builder().requestId(requestId)
                .workflowId(workflowId).dataframeId(dataframeId).inputDataId(inputDataId).isDeleted(false)
                .inputMetadata(JsonUtils.DEFAULT.toJson(inputMetaData)).outputMetadata(JsonUtils.DEFAULT.toJson(outputMetaData))
                .state(DataFrameOverrideState.SUCCEDED).dataFrameOverrideType(dataFrameOverrideType).build();
        dspServiceClient.saveDataFrameOverrideAudit(dataFrameOverrideAudit);
    }

    public void updateDataframeOverrideAudit(DataFrameOverrideAudit dataframeOverrideAudit, DataFrameOverrideState state) {
        dataframeOverrideAudit.setState(state);
        dspServiceClient.saveDataFrameOverrideAudit(dataframeOverrideAudit);
    }

    public void updateFailedDataframeOverrideAudit(Long requestId) {
        dspServiceClient.updateFailedDataFrameOverrideAudit(requestId);
    }

    public String serializePayloadToString(SGUseCasePayload sgUseCasePayload) throws Exception {
        return objectMapper.writeValueAsString(sgUseCasePayload);
    }

    public DataFrameOverrideAudit getDataFrameOverrideAudit(Long dataFrameId, String inputDataId, DataFrameOverrideType dataFrameOverrideType) {
        return dspServiceClient.getDataFrameOverrideAudit(dataFrameId, inputDataId, dataFrameOverrideType);
    }

    public DataFrameOverrideAudit getDataFrameOverrideAuditByRequestAndType(Long dataFrameId, Long requestId, DataFrameOverrideType dataFrameOverrideType) {
        return dspServiceClient.getDataFrameOverrideAuditByIdRequestAndType(dataFrameId, requestId, dataFrameOverrideType);
    }

    public Object reuseDataframeAudit(String dataframeName, WorkflowDetails workflowDetails,
                                      DataFrameOverrideAudit dataframeOverrideAudit,
                                      PipelineStep pipelineStep) throws IOException, InterruptedException {
        Long dataframeId = getDataFrameId(dataframeName, workflowDetails.getWorkflow());
        List<String> partitions = pipelineStep.getPartitions();

        dataframeOverrideAudit = piggyBackIfNeeded(dataframeOverrideAudit, dataframeName);
        log.info("Fetched a successfully completed override audit: {}", dataframeOverrideAudit.getId());
        DataFrameAudit dataFrameAudit;
        try {
            dataFrameAudit = dspServiceClient.getDataFrameAudit(dataframeId, dataframeOverrideAudit.getId(),
                    objectMapper.writeValueAsString(partitions));
        } catch (DSPServiceException e) {
            // if audit not found, only use override
            if (e.getMessage().equals("Dataframe audit for DataFrame Id: " + dataframeId + " not found!")) {
                log.info("Reusing Only Dataframe Override Audit for dataframe: {}", dataframeName);
                String outputMetadata = dataframeOverrideAudit.getOutputMetadata();
                return objectMapper.readValue(outputMetadata, new TypeReference<Map<String, Long>>() {
                });
            } else throw e;
        }
        log.info("Fetched a successfully completed dataframe audit run Id: {}", dataFrameAudit.getRunId());
        log.info("Reusing Dataframe Audit for dataframe: {}", dataframeName);
        return dataFrameAudit;
    }

    private DataFrameOverrideAudit piggyBackIfNeeded(DataFrameOverrideAudit dataframeOverrideAudit, String dataframeName) throws InterruptedException {
        log.info("Fetching dataframe audit by piggy backing override audits.");
        RequestStatus requestStatus = dspServiceClient.getRequestStatusById(dataframeOverrideAudit.getRequestId());
        if (dataframeOverrideAudit.getState().equals(DataFrameOverrideState.STARTED)
                && !requestStatus.equals(RequestStatus.FAILED)) {
            log.info("Found a running dataframe Override with Same Params for dataframe: {}, Waiting for completion. . .", dataframeName);
            int waitTime = 0;
            do {
                try {
                    waitTime += 10;
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    log.warn("Thread interrupted while waiting");
                    Thread.currentThread().interrupt();
                }
                dataframeOverrideAudit = dspServiceClient.getDataFrameOverrideAuditById(dataframeOverrideAudit.getId());
                requestStatus = dspServiceClient.getRequestStatusById(dataframeOverrideAudit.getRequestId());
            } while (dataframeOverrideAudit.getState().equals(DataFrameOverrideState.STARTED)
                    && requestStatus.equals(RequestStatus.ACTIVE)
                    && waitTime < 21600); //wait for 6 hour
            if (waitTime >= WAIT_TIME_PIGGYBACK) {
                log.error("Dependent job taking too long to finish for dataframe:{} , exiting. . .", dataframeName);
                throw new IllegalStateException("Dependent job taking too long to finish, exiting. . .");
            }
            if (!dataframeOverrideAudit.getState().equals(DataFrameOverrideState.SUCCEDED)) {
                log.error("Dependent job got killed while piggybacking for {}, exiting", dataframeName);
                throw new IllegalStateException("Dependent job got killed while piggybacking");
            }
        }
        return dataframeOverrideAudit;
    }

    public Map<String, Long> getOverrideTableInformationForHive(Long refreshId, String hiveTableName, String hiveQueue) throws HiveClientException {
        Map<String, Long> overrideTableInformation = new HashMap<>();
        if (Objects.isNull(refreshId)) {
            try {
                hiveClient.setQueue(hiveQueue);
                refreshId = hiveClient.getLatestRefreshId(hiveTableName);
                overrideTableInformation.put(hiveTableName, refreshId);
            } catch (HiveClientException e) {
                throw new HiveClientException("Failed to fetch latest refreshId for hive table!", e);
            }
        } else {
            overrideTableInformation.put(hiveTableName, refreshId);
        }
        return overrideTableInformation;
    }

    public String getInputMetaData(Map<String, Long> overrideTableInformation) throws Exception {
        return objectMapper.writeValueAsString(overrideTableInformation);
    }

    public void createHiveTable(String createTableQuery) throws HiveClientException {
        try {
            hiveClient.executeQuery(createTableQuery);
        } catch (HiveClientException e) {
            throw new HiveClientException("One of the Hadoop Dataset override failed!", e);
        }
    }

    public Long getRefreshId(String hiveTableName, HiveDataframeOverride dataframeOverride, Long requestId, String hiveQueue) throws HiveClientException {
        // if intermediate dataframe, return current refresh_Id
        Boolean isIntermediate = dataframeOverride.getIsIntermediate();
        if (Boolean.TRUE.equals(isIntermediate)) return requestId;
        // check refresh is not null and fetch latest if null
        Long refreshId = dataframeOverride.getRefreshId();
        if (refreshId == null) {
            try {
                hiveClient.setQueue(hiveQueue);
                return hiveClient.getLatestRefreshId(hiveTableName);
            } catch (HiveClientException e) {
                throw new HiveClientException("Failed to fetch latest refreshId for hive table!", e);
            }
        }
        return refreshId;
    }


    Set<LinkedHashMap<String, String>> getPartitionMapSet(String basePath) throws HDFSUtilsException {

        Set<LinkedHashMap<String, String>> partitionMap = new HashSet<>();
        try {
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(new Path(basePath), true);
            String basePathWithOutRefreshId = basePath.substring(0, basePath.indexOf(HDFS_CLUSTER_PREFIX + equal));

            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                String partitionAbsolutePath = fileStatus.getPath().toString();
                if (!new Path(partitionAbsolutePath).getName().startsWith(dot)) {
                    int startIndex = partitionAbsolutePath.indexOf(basePathWithOutRefreshId);
                    int endIndex = startIndex + basePathWithOutRefreshId.length();
                    String[] filePath = fileStatus.getPath().toString().substring(endIndex).split(slash);
                    LinkedHashMap<String, String> specificPartitionMap = new LinkedHashMap<>();
                    for (String path : filePath) {
                        String[] keyValue = path.split(equal);
                        if (keyValue.length == 2)
                            specificPartitionMap.put(keyValue[0], keyValue[1]);
                    }
                    partitionMap.add(specificPartitionMap);
                }
            }
        } catch (IOException e) {
            throw new HDFSUtilsException("Error while listing subdirectory for directory " + basePath);
        }
        return partitionMap;
    }

}
