package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.*;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query.HiveQueryOverrideManagerEndDebugEvent;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query.HiveQueryOverrideManagerErrorEvent;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query.HiveQueryOverrideManagerReusedDebugEvent;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query.HiveQueryOverrideManagerStartDebugEvent;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.sql.Timestamp;
import java.util.*;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EventAuditUtil {

    private final DSPServiceClient dspServiceClient;

    public void createDataFrameGenerationStartInfoEvent(Long requestId, String dataFrameName, Timestamp generationStartTime, Workflow workflow) {
        DataFrameGenerationStartInfoEvent dataFrameGenerationStartInfoEvent = DataFrameGenerationStartInfoEvent.builder()
                .workflowName(workflow.getName()).dataFrameName(dataFrameName).generationStartTime(generationStartTime).build();
        saveEventAudit(requestId, workflow.getId(), EventLevel.INFO, dataFrameGenerationStartInfoEvent);
    }

    public void createDataFrameCompletionInfoEvent(Long requestId, String dataFrameName, Timestamp completionTime,
                                                   Long dataFrameSize, Workflow workflow) {
        DataFrameCompletionInfoEvent dataframeCompletionInfoEvent = DataFrameCompletionInfoEvent.builder().workflowName(workflow.getName())
                .dataFrameName(dataFrameName).completionTime(completionTime).dataFrameSize(dataFrameSize).build();
        saveEventAudit(requestId, workflow.getId(), EventLevel.INFO, dataframeCompletionInfoEvent);
    }

    public void createDataframeQueryGenerationErrorEvent(Long requestId, String dataFrameName, String errorMessage,
                                                         Timestamp failureTime, Workflow workflow) {
        DataFrameQueryGenerationErrorEvent dataframeQueryGenerationErrorEvent = DataFrameQueryGenerationErrorEvent.builder()
                .workflowName(workflow.getName()).dataFrameName(dataFrameName).errorMessage(errorMessage).failureTime(failureTime).build();
        saveEventAudit(requestId, workflow.getId(), EventLevel.ERROR, dataframeQueryGenerationErrorEvent);
    }

    public void createDataFrameQueryExecutionErrorEvent(Long requestId, String dataFrameName, String errorMessage,
                                                        Timestamp failureTime, Workflow workflow) {
        DataFrameQueryExecutionErrorEvent dataframeQueryExecutionErrorEvent = DataFrameQueryExecutionErrorEvent.builder()
                .workflowName(workflow.getName()).dataFrameName(dataFrameName).errorMessage(errorMessage)
                .failureTime(failureTime).build();
        saveEventAudit(requestId, workflow.getId(), EventLevel.ERROR, dataframeQueryExecutionErrorEvent);
    }

    public void createDataFrameGenerationErrorEvent(Long requestId, String errorMessage, Workflow workflow) {
        DataFrameGenerationErrorEvent dataframeGenerationErrorEvent = DataFrameGenerationErrorEvent.builder()
                .workflowName(workflow.getName()).errorMessage(errorMessage).build();
        saveEventAudit(requestId, workflow.getId(), EventLevel.ERROR, dataframeGenerationErrorEvent);
    }

    public void createAllDataFrameCompletionInfoEvent(Long requestId, Timestamp completionTime, Workflow workflow) {
        AllDataFrameCompletionInfoEvent dataFrameCompletionInfoEvent = AllDataFrameCompletionInfoEvent
                .builder().workflowName(workflow.getName()).localTime(completionTime).build();
        EventAudit eventAudit = EventAudit.builder().requestId(requestId).workflowId(workflow.getId())
                .eventType(EventType.SGNode).eventLevel(EventLevel.INFO).payload(dataFrameCompletionInfoEvent).build();
        dspServiceClient.saveEventAudit(eventAudit);
    }

    public void createAllDataFrameCompletionDebugEvent(Long requestId, Timestamp completionTime, Workflow workflow,
                                                       Set<DataFrameAudit> dataFrameAuditSet, Map<String, List<String>> dataframePartitions) {
        List<DataFrameDetails> dataFrameDetails = getDataFrameDetails(dataFrameAuditSet, dataframePartitions);
        AllDataFrameCompletionDebugEvent dataFrameCompletionDebugEvent = AllDataFrameCompletionDebugEvent
                .builder().workflowName(workflow.getName()).completionTime(completionTime).dataFrameDetails(dataFrameDetails).build();
        EventAudit eventAudit = EventAudit.builder().requestId(requestId).workflowId(workflow.getId())
                .eventType(EventType.SGNode).eventLevel(EventLevel.DEBUG).payload(dataFrameCompletionDebugEvent).build();
        dspServiceClient.saveEventAudit(eventAudit);
    }

    private List<DataFrameDetails> getDataFrameDetails(Set<DataFrameAudit> dataFrameAuditSet, Map<String, List<String>> dataframePartitions) {
        List<DataFrameDetails> dataFrameDetailsList = new ArrayList<>();
        for (DataFrameAudit dataFrameAudit : dataFrameAuditSet) {
            List<String> partitions = dataframePartitions.get(dataFrameAudit.getDataFrame().getName());
            int noOFPartitions = partitions == null ? 0 : partitions.size();
            DataFrameDetails dataFrameDetails = DataFrameDetails.builder()
                    .dataFrameName(dataFrameAudit.getDataFrame().getName())
                    .dataFrameSize(dataFrameAudit.getDataframeSize())
                    .noOfPartitions(noOFPartitions)
                    .partitionNames(partitions).build();
            dataFrameDetailsList.add(dataFrameDetails);
        }
        return dataFrameDetailsList;
    }

    public void createOverrideStartDebugEvent(Long requestId, long workflowId, String workflowName,
                                              Map<String, DataframeOverride> dataframeOverrideMap) {
        SGOverrideStartDebugEvent sgOverrideStartDebugEvent = SGOverrideStartDebugEvent.builder()
                .workflowName(workflowName).dataFrameOverrideMap(dataframeOverrideMap).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, sgOverrideStartDebugEvent);
    }

    public void createOverrideEndDebugEvent(Long requestId, Long workflowId, String workflowName) {
        SGOverrideEndDebugEvent sgOverrideEndDebugEvent = SGOverrideEndDebugEvent.builder().workflowName(workflowName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, sgOverrideEndDebugEvent);
    }

    public void createRunIdOverrideStartDebugEvent(Long requestId, Long workflowId, String workflowName, String dataFrameName) {
        RunIdOverrideStartDebugEvent runIdOverrideStartDebugEvent = RunIdOverrideStartDebugEvent.builder()
                .workflowName(workflowName).dataFrameName(dataFrameName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, runIdOverrideStartDebugEvent);
    }

    public void createRunIdOverrideReusedDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                    String dataFrameName, Long runId) {
        RunIdOverrideReusedDebugEvent runIdOverrideReusedDebugEvent = RunIdOverrideReusedDebugEvent.builder()
                .dataFrameName(dataFrameName).workflowName(workflowName).runId(runId).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, runIdOverrideReusedDebugEvent);
    }

    public void createRunIdOverrideErrorEvent(Long requestId, Long workflowId, String workflowName,
                                              String dataframeName, Long runId, String errorMessage) {
        RunIdOverrideErrorEvent runIdOverrideErrorEvent = RunIdOverrideErrorEvent.builder().dataFrameName(dataframeName)
                .workflowName(workflowName).runId(runId).errorMessage(errorMessage).build();
        saveEventAudit(requestId, workflowId, EventLevel.ERROR, runIdOverrideErrorEvent);
    }

    public void createPartitionOverrideStartDebugEvent(Long requestId, Long workflowId, String workflowName, String dataFrameName) {
        PartitionOverrideStartDebugEvent event = PartitionOverrideStartDebugEvent.builder().dataFrameName(dataFrameName)
                .workflowName(workflowName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, event);
    }

    public void createPartitionOverrideEndDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                     String dataFrameName, PartitionDataframeOverride partitionDataframeOverride) {
        PartitionOverrideEndDebugEvent event = PartitionOverrideEndDebugEvent.builder().dataFrameName(dataFrameName)
                .workflowName(workflowName).partitionDataframeOverride(partitionDataframeOverride).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, event);
    }

    public void createHiveTableOverrideManagerStartDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                              String dataframeName, String dbName, String tableName) {
        HiveTableOverrideManagerStartDebugEvent hiveTableOverrideManagerStartDebugEvent = HiveTableOverrideManagerStartDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).dbName(dbName).tableName(tableName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, hiveTableOverrideManagerStartDebugEvent);
    }

    public void createHiveTableOverrideManagerEndDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                            String dataframeName, String dbName, String tableName, Long refreshId) {
        HiveTableOverrideManagerEndDebugEvent hiveTableOverrideManagerEndDebugEvent = HiveTableOverrideManagerEndDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).dbName(dbName)
                .tableName(tableName).refreshId(refreshId).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, hiveTableOverrideManagerEndDebugEvent);
    }

    public void createDefaultDataframeOverrideStartDebugEvent(Long requestId, Long workflowId, String workflowName, String dataframeName) {
        DefaultDataFrameOverrideStartDebugEvent defaultDataframeOverrideStartDebugEvent = DefaultDataFrameOverrideStartDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, defaultDataframeOverrideStartDebugEvent);
    }

    public void createDefaultDataframeOverrideReusedDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                               String dataframeName, String payload) {
        DefaultDataFrameOverrideReusedDebugEvent defaultDataframeOverrideReusedDebugEvent = DefaultDataFrameOverrideReusedDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).payload(payload).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, defaultDataframeOverrideReusedDebugEvent);
    }

    public void createDefaultDataframeOverrideForceRunDebugEvent(Long requestId, Long workflowId, String workflowName, String dataframeName) {
        DefaultDataFrameOverrideForceRunDebugEvent defaultDataframeOverrideForceRunDebugEvent = DefaultDataFrameOverrideForceRunDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, defaultDataframeOverrideForceRunDebugEvent);
    }

    public void createHiveQueryOverrideManagerReusedDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                               String dataframeName, String outputDetails) {
        HiveQueryOverrideManagerReusedDebugEvent hiveQueryOverrideManagerReusedDebugEvent = HiveQueryOverrideManagerReusedDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).outputDetails(outputDetails).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, hiveQueryOverrideManagerReusedDebugEvent);
    }

    public void createHiveQueryOverrideManagerEndDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                            String dataframeName, String outputDetails) {
        HiveQueryOverrideManagerEndDebugEvent hiveQueryOverrideManagerEndDebugEvent = HiveQueryOverrideManagerEndDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).outputDetails(outputDetails).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, hiveQueryOverrideManagerEndDebugEvent);
    }

    public void createHiveQueryOverrideManagerErrorEvent(Long requestId, Long workflowId, String workflowName,
                                                         String dataframeName, String query, String errorMessage) {
        HiveQueryOverrideManagerErrorEvent hiveQueryOverrideManagerErrorEvent = HiveQueryOverrideManagerErrorEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).query(query).errorMessage(errorMessage).build();
        saveEventAudit(requestId, workflowId, EventLevel.ERROR, hiveQueryOverrideManagerErrorEvent);
    }

    public void createHiveQueryOverrideManagerStartDebugEvent(Long requestId, Long workflowId, String workflowName,
                                                              String dataframeName, String query) {
        HiveQueryOverrideManagerStartDebugEvent hiveQueryOverrideManagerStartDebugEvent = HiveQueryOverrideManagerStartDebugEvent
                .builder().dataFrameName(dataframeName).workflowName(workflowName).query(query).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, hiveQueryOverrideManagerStartDebugEvent);
    }

    public void createCSVOverrideStartDebugEvent(Long requestId, Long workflowId, String workflowName, String dataFrameName, String csvPath) {
        CSVOverrideStartDebugEvent csvOverrideStartDebugEvent = CSVOverrideStartDebugEvent.builder()
                .dataFrameName(dataFrameName).workflowName(workflowName).csvPath(csvPath).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, csvOverrideStartDebugEvent);
    }

    public void createCSVOverrideEndDebugEvent(Long requestId, Long workflowId, String workflowName, String dataFrameName) {
        CSVOverrideEndDebugEvent csvOverrideEndDebugEvent = CSVOverrideEndDebugEvent
                .builder().dataFrameName(dataFrameName).workflowName(workflowName).build();
        saveEventAudit(requestId, workflowId, EventLevel.DEBUG, csvOverrideEndDebugEvent);
    }

    public void createCSVOverrideErrorEvent(Long requestId, Long workflowId, String workflowName, String dataFrameName,
                                            String errorMessage) {
        CSVOverrideErrorEvent csvOverrideErrorEvent = CSVOverrideErrorEvent.builder().dataFrameName(dataFrameName)
                .workflowName(workflowName).errorMessage(errorMessage).build();
        saveEventAudit(requestId, workflowId, EventLevel.ERROR, csvOverrideErrorEvent);
    }

    public void saveEventAudit(Long requestId, Long workflowId, EventLevel eventLevel, Events event) {
        EventAudit eventAudit = EventAudit.builder().requestId(requestId).workflowId(workflowId)
                .eventType(EventType.SGNode).eventLevel(eventLevel).payload(event).build();
        dspServiceClient.saveEventAudit(eventAudit);
    }

}
