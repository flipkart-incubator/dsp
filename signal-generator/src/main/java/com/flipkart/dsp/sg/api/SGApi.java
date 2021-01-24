package com.flipkart.dsp.sg.api;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.generator.DataFrameFactory;
import com.flipkart.dsp.sg.override.OverrideManager;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.dsp.utils.Constants.dot;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 *
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SGApi {
    private final EventAuditUtil eventAuditUtil;
    private final OverrideManager overrideManager;
    private final DSPServiceClient dspServiceClient;
    private final DataFrameFactory dataFrameFactory;

    public void submitJob(Long requestId, Long workFlowId, Long pipelineStepId, Long executionAuditId) throws Exception {
        WorkflowDetails workflowDetails = dspServiceClient.getWorkflowDetails(workFlowId);

        Set<DataFrameAudit> dataFrameAudits = generateDataFrames(requestId, workflowDetails, pipelineStepId);
        dataFrameAudits.forEach(dataFrameAudit -> dataFrameAudit.setLogAuditId(executionAuditId));
        Map<String, List<String>> dataFramePartitionMapping = dspServiceClient.persistRequestDataframeAudit(requestId, workFlowId, pipelineStepId, dataFrameAudits);
        eventAuditUtil.createAllDataFrameCompletionInfoEvent(requestId, new Timestamp(new Date().getTime()), workflowDetails.getWorkflow());
        eventAuditUtil.createAllDataFrameCompletionDebugEvent(requestId, new Timestamp(new Date().getTime()),
                workflowDetails.getWorkflow(), dataFrameAudits, dataFramePartitionMapping);
    }

    @Timed
    @Metered
    protected Set<DataFrameAudit> generateDataFrames(Long requestId, WorkflowDetails workflowDetails, Long pipelineStepId)
            throws DSPClientException, DataFrameGeneratorException {
        Workflow workflow = workflowDetails.getWorkflow();
        try {
            Request request = dspServiceClient.getRequest(requestId);
            Map<String, DataType> inputDataFrameType = getInputDataFrameType(request.getWorkflowDetails());
            // generate dataframe only for particular pipeline step
            Map<String, DataframeOverride> dataFrameOverrideMap = getDataFrameOverrideMap(request.getData());

            // filter dataframeOverrideMap for the pipeline step inputs
            PipelineStep pipelineStep = getPipelineStepById(workflowDetails, pipelineStepId);
            Set<String> inputVariables = pipelineStep.getScript().getInputVariables().stream()
                    .map(ScriptVariable::getName).collect(Collectors.toSet());
            dataFrameOverrideMap = filterDataFrameOverrideMap(dataFrameOverrideMap, inputVariables);
            log.info("List of Dataframes to be generated: {}", dataFrameOverrideMap.keySet().toString());

            Set<DataFrameAudit> dataFrameAudits = getDataFrameAudits(request, workflowDetails,
                    dataFrameOverrideMap, pipelineStep);
            Map<String, DataFrameAudit> overrideAuditMap = dataFrameAudits.stream()
                    .collect(toMap(dataFrameAudit -> dataFrameAudit.getDataFrame().getName(), Function.identity(), (x, y) -> x));
            Map<String, Long> tableInformation = Objects.isNull(request.getData().getTables()) ? new HashMap<>() : request.getData().getTables();
            Map<String, Long> tableInformationWithDbName = tableInformation.entrySet().stream()
                    .collect(toMap(e -> getFullyQualifiedTableName(e.getKey()), Map.Entry::getValue));
            Set<DataFrame> dataFrames = getDataFrames(workflow, dataFrameOverrideMap, overrideAuditMap, inputVariables, pipelineStep);

            DataFrameGenerateRequest dataframeGenerateRequest = DataFrameGenerateRequest.builder()
                    .requestId(requestId).tables(tableInformationWithDbName).scopes(null).dataFrames(dataFrames)
                    .inputDataFrameType(inputDataFrameType).dataFrameOverrideMap(dataFrameOverrideMap).build();
            Set<DataFrameAudit> generatedDataFrameAudits = dataFrameFactory.invoke(workflow, pipelineStepId, dataframeGenerateRequest);
            return Sets.union(dataFrameAudits, generatedDataFrameAudits);
        } catch (Exception e) {
            log.info(e.getMessage());
            eventAuditUtil.createDataFrameGenerationErrorEvent(requestId, e.getMessage() + " " + e.toString(), workflow);
            throw new DataFrameGeneratorException("Data frame Generation Failed.", e);
        }
    }

    private Map<String, DataframeOverride> getDataFrameOverrideMap(ExecuteWorkflowRequest executeWorkflowRequest) {
        if (Objects.nonNull(executeWorkflowRequest) && Objects.nonNull(executeWorkflowRequest.getRequestOverride())
                && Objects.nonNull(executeWorkflowRequest.getRequestOverride().getDataframeOverrideMap()))
            return executeWorkflowRequest.getRequestOverride().getDataframeOverrideMap();
        return new HashMap<>();
    }

    private PipelineStep getPipelineStepById(WorkflowDetails workflowDetails, Long pipelineStepId) throws DataFrameGeneratorException {
        for (PipelineStep pipelineStep : workflowDetails.getPipelineSteps()) {
            if (pipelineStepId.equals(pipelineStep.getId())) {
                return pipelineStep;
            }
        }
        throw new DataFrameGeneratorException("Pipeline Step details missing for step id: " + pipelineStepId);
    }

    private Map<String, DataframeOverride> filterDataFrameOverrideMap(Map<String, DataframeOverride> dataFrameOverrideMap, Set<String> inputVariables) {
        Map<String, DataframeOverride> filteredOverrideMap = new HashMap<>();
        for (String dataframeName : dataFrameOverrideMap.keySet()) {
            if (inputVariables.contains(dataframeName))
                filteredOverrideMap.put(dataframeName, dataFrameOverrideMap.get(dataframeName));
        }
        return filteredOverrideMap;
    }

    private Set<DataFrameAudit> getDataFrameAudits(Request request, WorkflowDetails workflowDetails,
                                                   Map<String, DataframeOverride> dataframeOverrideMap,
                                                   PipelineStep pipelineStep) throws DataFrameGeneratorException {
        try {
            if (dataframeOverrideMap.size() > 0)
                return overrideManager.override(request, workflowDetails, dataframeOverrideMap, pipelineStep);
            else return new HashSet<>();
        } catch (DataframeOverrideException e) {
            throw new DataFrameGeneratorException("Data frame Generation Failed while applying overrides.", e);
        }
    }

    private String getFullyQualifiedTableName(String tableName) {
        if (tableName.contains(dot))
            return tableName;

        DataTable dataTable = dspServiceClient.getDataTable(tableName);
        return format("%s.%s", dataTable.getDataSource().getId(), tableName);
    }

    private Map<String, DataType> getInputDataFrameType(WorkflowDetails workflowDetails) {
        List<PipelineStep> pipelineStepList = workflowDetails.getPipelineSteps();
        List<ScriptVariable> scriptVariables = pipelineStepList.stream().
                flatMap(s -> s.getScript().getInputVariables().stream()).collect(Collectors.toList());
        Map<String, DataType> dataFrameDatatypeMap = new HashMap<>();
        for (ScriptVariable scriptVariable : scriptVariables) {
            if (!dataFrameDatatypeMap.containsKey(scriptVariable.getName())) {
                dataFrameDatatypeMap.put(scriptVariable.getName(), scriptVariable.getDataType());
            }
        }
        return dataFrameDatatypeMap;
    }


    // Please change this function carefully with proper testing, if required
    private Set<DataFrame> getDataFrames(Workflow workflow, Map<String, DataframeOverride> dataFrameOverrideMap,
                                         Map<String, DataFrameAudit> overrideAuditMap,
                                         Set<String> inputVariables, PipelineStep pipelineStep) {
        Set<DataFrame> dataFrames = workflow.getDataFrames().stream().filter(dataFrame -> {
                    Long dataFrameId = dataFrame.getId();
                    String dataFrameName = dataFrame.getName();
                    // filter out dataframe not in the particular step
                    if (!inputVariables.contains(dataFrameName)) return false;
                    if (!dataFrameOverrideMap.containsKey(dataFrameName)) {
                        return true;
                    } else {
                        DataframeOverride dataframeOverride = dataFrameOverrideMap.get(dataFrameName);
                        if ((dataframeOverride instanceof DefaultDataframeOverride)) {
                            return ((DefaultDataframeOverride) dataframeOverride).getForceRun() || !isDataFrameAuditPresent(dataFrameId);
                        } else if ((dataframeOverride instanceof HiveDataframeOverride)
                                || (dataframeOverride instanceof HiveQueryDataframeOverride)){
                            boolean generateDataFrame = overrideAuditMap.containsKey(dataFrameName);
                            if (!generateDataFrame)
                                log.info("Sending dataFrame: {} to Legacy SG", dataFrameName);
                            else
                                log.info("Skipping SG for dataFrame: {}", dataFrameName);
                            return !generateDataFrame;
                        }
                        return false;
                    }
                }
        ).collect(toSet());
        dataFrames.forEach(e -> e.setPartitions(pipelineStep.getPartitions()));
        return dataFrames;
    }

    private boolean isDataFrameAuditPresent(Long dataFrameId) {
        try {
            dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameId);
            return true;
        } catch (DSPClientException e) {
            return false;
        }
    }
}
