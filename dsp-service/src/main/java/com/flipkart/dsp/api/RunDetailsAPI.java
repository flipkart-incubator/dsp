package com.flipkart.dsp.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.dao.DataFrameAuditDAO;
import com.flipkart.dsp.dao.RequestDataframeAuditDAO;
import com.flipkart.dsp.db.entities.DataFrameAuditEntity;
import com.flipkart.dsp.dto.RunDetailsDTO;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.entities.pipelinestep.*;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.utils.*;
import com.flipkart.dsp.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.dsp.utils.Constants.equal;
import static com.flipkart.dsp.utils.Constants.slash;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RunDetailsAPI {
    private final Validator validator;
    private final DSPClientConfig dspClientConfig;
    private final DataFrameAuditDAO dataFrameAuditDAO;
    private final WorkflowAuditActor workflowAuditActor;
    private final PipelineStepAuditActor pipelineStepAuditActor;
    private final RequestDataframeAuditDAO requestDataframeAuditDAO;
    private final PipelineStepRuntimeConfigActor pipelineStepRuntimeConfigActor;

    public RunStatusDTO getRunStatus(Long requestId) throws ValidationException {
        Request request = validator.verifyRequestId(requestId);
        WorkflowDetails workflowDetails = request.getWorkflowDetails();
        Workflow workflow = workflowDetails.getWorkflow();
        String workflowExecutionId = workflowAuditActor.getLatestWorkflowExecutionId(request.getId(), workflow.getId());

        RunStatusDTO.WorkflowRunStatus workflowRunStatus = new RunStatusDTO.WorkflowRunStatus();
        workflowRunStatus.setWorkflowName(workflow.getName());
        workflowRunStatus.setWorkflowStatus(workflowAuditActor.getWorkflowStatus(workflowExecutionId, request.getRequestStatus()));
        workflowRunStatus.setPipelineStepInfos(workflowDetails.getPipelineSteps().stream().map(ps ->
                populatePipelineStepInfo(request.getId(), ps, workflow.getName(), workflowExecutionId)).collect(Collectors.toList()));
        return new RunStatusDTO(request.getRequestStatus(), workflowRunStatus);
    }

    private RunStatusDTO.PipelineStepInfo populatePipelineStepInfo(Long requestId, PipelineStep pipelineStep, String workflowName, String workflowExecutionId) {
        RunStatusDTO.PipelineStepInfo pipelineStepInfo = new RunStatusDTO.PipelineStepInfo();

        Script script = pipelineStep.getScript();
        pipelineStepInfo.setStepName(pipelineStep.getName().toString());
        pipelineStepInfo.setInputs(populateVariables(requestId, workflowExecutionId, script.getInputVariables()));
        pipelineStepInfo.setOutputs(populateVariables(requestId, workflowExecutionId, script.getOutputVariables()));

        pipelineStepInfo.setLogLocation(populateLogLocation(requestId, workflowName));
        pipelineStepInfo.setPartitionInfos(getPartitionInfoForStep(pipelineStep.getId(), workflowExecutionId));
        return pipelineStepInfo;
    }

    private String populateLogLocation(Long requestId, String workflowName) {
        String path = String.format(Constants.LOG_EXECUTION_PATH, requestId, workflowName);
        return HTTPRequestUtil.buildHTTPURL(dspClientConfig.getHost(), dspClientConfig.getPort(), path);
    }

    private List<Variable> populateVariables(Long requestId, String workflowExecutionId, Set<ScriptVariable> scriptVariables) {
        List<Variable> variables = new ArrayList<>();
        for (ScriptVariable scriptVariable : scriptVariables) {
            if (scriptVariable.getDataType().equals(DataType.DATAFRAME) && scriptVariable.getAdditionalVariable() != null
                    && ((AbstractDataFrame) scriptVariable.getAdditionalVariable()).getHiveTable() != null) {
                variables.add(getDataFrameVariable(requestId, scriptVariable));
            } else if (scriptVariable.getDataType().equals(DataType.MODEL)) {
                variables.add(getModelGroupVariable(workflowExecutionId));
            }
        }
        return variables;
    }

    private Variable getDataFrameVariable(Long requestId, ScriptVariable scriptVariable) {
        RunStatusDTO.DataFrameVariable variable = new RunStatusDTO.DataFrameVariable();
        variable.setRefreshId(requestId);
        variable.setTableName(((AbstractDataFrame) scriptVariable.getAdditionalVariable()).getHiveTable());
        return variable;
    }

    private Variable getModelGroupVariable(String workflowExecutionId) {
        RunStatusDTO.ModelGroupVariable variable = new RunStatusDTO.ModelGroupVariable();
        variable.setModelGroupId(workflowExecutionId);
        return variable;
    }

    private List<RunStatusDTO.PartitionInfo> getPartitionInfoForStep(long pipelineStepId, String workflowExecutionId) {
        List<PipelineStepRuntimeConfig> runtimeConfigs = pipelineStepRuntimeConfigActor.getPipelineStepRuntimeConfigs(workflowExecutionId);
        Map<String/*pipelineExecutionId*/, PipelineStepRuntimeConfig> execIdToRunConfig =
                runtimeConfigs.stream().filter(runtimeConfig -> runtimeConfig.getPipelineStepId() == pipelineStepId)
                        .collect(Collectors.toMap(PipelineStepRuntimeConfig::getPipelineExecutionId, Function.identity()));

        List<PipelineStepAudit> pipelineStepAudits = pipelineStepAuditActor.getPipelineStepAudits(pipelineStepId, workflowExecutionId);
        Map<String, PipelineStepAudit> pipelineExecIdToPipelineStepAudit = new HashMap<>();
        pipelineStepAudits.forEach(psa -> {
            if (pipelineExecIdToPipelineStepAudit.containsKey(psa.getPipelineExecutionId())) {
                if (pipelineExecIdToPipelineStepAudit.get(psa.getPipelineExecutionId()).getAttempt() < psa.getAttempt()) {
                    pipelineExecIdToPipelineStepAudit.put(psa.getPipelineExecutionId(), psa);
                }
            } else {
                pipelineExecIdToPipelineStepAudit.put(psa.getPipelineExecutionId(), psa);
            }
        });
        List<RunStatusDTO.PartitionInfo> partitionInfos = new ArrayList<>();
        for (PipelineStepStatus pipelineStepStatus : PipelineStepStatus.values()) {
            Set<String> partitions = new HashSet<>();
            for (Map.Entry<String, PipelineStepRuntimeConfig> entry : execIdToRunConfig.entrySet()) {
                if (Objects.nonNull(pipelineExecIdToPipelineStepAudit.get(entry.getKey()))
                        && pipelineExecIdToPipelineStepAudit.get(entry.getKey()).getPipelineStepStatus().equals(pipelineStepStatus)) {
                    partitions.add(populateScope(entry.getValue().getScope()));
                }
            }
            if (partitions.size() > 0) {
                RunStatusDTO.PartitionInfo partitionInfo = new RunStatusDTO.PartitionInfo();
                partitionInfo.setStepStatus(pipelineStepStatus);
                partitionInfo.setPartitions(partitions);
                partitionInfo.setCount(partitions.size());
                partitionInfos.add(partitionInfo);
            }
        }
        return partitionInfos;
    }

    private String populateScope(String serializedWhereClauses) {
        List<WhereClause> whereClauses = JsonUtils.DEFAULT.fromJson(serializedWhereClauses, new TypeReference<List<WhereClause>>() {
        });
        StringBuilder formattedScope = new StringBuilder();
        for (WhereClause whereClause : whereClauses) {
            String values = null == whereClause.getValues() ? "null" : whereClause.getValues().toString();
            values = values.replaceAll("%23", "_");
            formattedScope.append("#").append(whereClause.getId()).append(" => ").append(values);
        }
        return formattedScope.toString();
    }

    public RunDetailsDTO getRunIDDetails(String dataFrameName, Integer noOfRuns) {
        Map<Long, RunDetailsDTO.LocationDetails> runDetailsMap = new HashMap<>();
        List<DataFrameAuditEntity> dataFrameAuditEntityList = dataFrameAuditDAO.getLatestSuccessfulDataFrameAudits(dataFrameName, noOfRuns);
        for (DataFrameAuditEntity dataFrameAuditEntity : dataFrameAuditEntityList) {
            Map<List<DataFrameKey>, Set<String>> dataFramePartitionDetailsSet = dataFrameAuditEntity.getPayload().getDataframes();
            if (!MapUtils.isEmpty(dataFramePartitionDetailsSet)) {
                String hashSet = dataFrameAuditEntity.getPayload().getDataframes().entrySet().stream().iterator().next().getValue().toString();
                String[] detailsArray = hashSet.split(slash);
                String db = detailsArray[0].contains(".db") ? detailsArray[0].substring(0, detailsArray[0].indexOf(".db")) : detailsArray[0];
                String refreshId = detailsArray.length == 3 ? detailsArray[2].substring(detailsArray[2].indexOf(equal) + 1) : null;
                RunDetailsDTO.LocationDetails locationDetails = RunDetailsDTO.LocationDetails.builder()
                        .dataBaseName(db).tableName(detailsArray[1]).refreshId(refreshId).build();
                runDetailsMap.put(dataFrameAuditEntity.getRunId(), locationDetails);
            }
        }
        return RunDetailsDTO.builder().runIDMap(runDetailsMap).build();
    }
}
