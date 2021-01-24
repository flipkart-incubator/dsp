package com.flipkart.dsp.validation;

import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.GithubClient;
import com.flipkart.dsp.dao.RequestDAO;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.exceptions.MetaStoreException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.HdfsUtils;
import com.flipkart.dsp.utils.HivePathUtils;
import com.flipkart.dsp.utils.HiveUtils;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class Validator {
    private final WorkFlowActor workFlowActor;
    private final RequestActor requestActor;
    private final HdfsUtils hdfsUtils;
    private final HivePathUtils hivePathUtils;
    private final DataTableActor dataTableActor;
    private final MetaStoreClient metaStoreClient;
    private final HiveClient hiveClient;
    private final RequestDAO requestDAO;
    private final ScriptActor scriptActor;
    private final GithubClient githubClient;
    private final DataFrameAuditActor dataFrameAuditActor;
    private final ExternalCredentialsActor externalCredentialsActor;



    public void validateWorkflowCreateRequest(CreateWorkflowRequest createWorkflowRequest) throws ValidationException {
        validatePrimitive(createWorkflowRequest);
        validateNullInDefaults(createWorkflowRequest);
    }

    private void validatePrimitive(CreateWorkflowRequest createWorkflowRequest) throws ValidationException {
        for (CreateWorkflowRequest.PipelineStep step : createWorkflowRequest.getWorkflow().getPipelineSteps()) {
            Set<ScriptVariable> variables = step.getScript().getInputs();
            for (ScriptVariable variable : variables) {
                if (variable.getDataType().equals(DataType.DATE_TIME)) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    try {
                        simpleDateFormat.parse(variable.getValue().toString());
                    } catch (ParseException e) {
                        throw new ValidationException("DATE_TIME format must be yyyy-MM-ddTHH:mm:ss");
                    }
                } else if (variable.getDataType().equals(DataType.DATE)) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        simpleDateFormat.parse(variable.getValue().toString());
                    } catch (ParseException e) {
                        throw new ValidationException("DATE format must be yyyy-MM-dd");
                    }
                }
            }

        }
    }

    private void validateNullInDefaults(CreateWorkflowRequest createWorkflowRequest){
        for(CreateWorkflowRequest.PipelineStep pipelineStep: createWorkflowRequest.getWorkflow().getPipelineSteps()) {
            CreateWorkflowRequest.Script script = pipelineStep.getScript();
            for(ScriptVariable input : script.getInputs()) {
                if (input.getAdditionalVariable() != null) {
                    ((AbstractDataFrame) input.getAdditionalVariable()).getNaStrings().add("");
                }
            }
        }
    }

    public void validateWorkflowExecuteRequest(WorkflowDetails workflowDetails, ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        validateRunIdDataFrames(executeWorkflowRequest);
        validateRefreshIdWorkflowExecute(executeWorkflowRequest);
        validateScriptVariableOverride(workflowDetails, executeWorkflowRequest);
        EmailNotificationsValidator.validateEmailNotificationDetails(executeWorkflowRequest.getEmailNotifications(), workflowDetails);
    }

    private void validateRunIdDataFrames(ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        RequestOverride requestOverride = executeWorkflowRequest.getRequestOverride();
        if (requestOverride != null) {
            final Map<String, DataframeOverride> dataFrameOverrideMap = requestOverride.getDataframeOverrideMap();
            if (dataFrameOverrideMap != null) {
                for (Map.Entry<String, DataframeOverride> entry : dataFrameOverrideMap.entrySet()) {
                    String dataframeName = entry.getKey();
                    DataframeOverride dataframeOverride = entry.getValue();
                    if (dataframeOverride instanceof RunIdDataframeOverride) {
                        final Long runId = ((RunIdDataframeOverride) dataframeOverride).getRunId();
                        DataFrameAudit dataFrameAudit = dataFrameAuditActor.getDataFrameAuditById(runId);
                        if (Objects.isNull(dataFrameAudit))
                            throw new ValidationException("No DataFrame Audit found for run_id " + runId);
                        if (!dataFrameAudit.getDataFrame().getName().equals(dataframeName)) {
                            throw new ValidationException("DataFrame Run Id " + runId + " is not of type " + dataframeName);
                        }
                    }
                }
            }
        }
    }

    private void validateScriptVariableOverride(WorkflowDetails workflowDetails, ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        if (Objects.nonNull(executeWorkflowRequest.getRequestOverride())) {
            RequestOverride requestOverride = executeWorkflowRequest.getRequestOverride();
            HashMap<String, ScriptVariableOverride> scriptVariableOverrideHashMap = new HashMap<>();
            if(requestOverride.getObjectOverrideList() !=null && !requestOverride.getObjectOverrideList().isEmpty()) {
                scriptVariableOverrideHashMap.putAll(requestOverride.getObjectOverrideList().stream()
                        .filter(objectOverride -> objectOverride instanceof ScriptVariableOverride)
                        .map(objectOverride -> (ScriptVariableOverride) objectOverride)
                        .collect(toMap(ScriptVariableOverride::getName, Function.identity(), (e1, e2) -> e2, HashMap::new)));
            }
            Map<String, DataframeOverride> dataFrameOverrideMap = requestOverride.getDataframeOverrideMap();
            Set<String> variableSet = new HashSet<>();
            for (PipelineStep pipelineStep : workflowDetails.getPipelineSteps()) {
                for (ScriptVariable scriptVariable : pipelineStep.getScript().getInputVariables()) {
                    final String scriptVariableName = scriptVariable.getName();
                    if (workflowDetails.getWorkflow().getIsProd() && scriptVariable.getRequired() != null && scriptVariable.getRequired()
                            && !scriptVariableOverrideHashMap.containsKey(scriptVariableName)
                            && !dataFrameOverrideMap.containsKey(scriptVariableName)) {
                        throw new ValidationException("Required variable missing from request payload! Name: "
                                + scriptVariable.getName() + ", Datatype: " + scriptVariable.getDataType());
                    }
                    if (scriptVariableOverrideHashMap.containsKey(scriptVariableName)) {
                        final ScriptVariableOverride scriptVariableOverride = scriptVariableOverrideHashMap.get(scriptVariableName);
                        variableSet.add(scriptVariable.getName());
                        if (scriptVariableOverride.getDataType() == null) {
                            throw new ValidationException("Invalid datatype mentioned in Script Variable Override " + scriptVariableOverride.getName() + ".");
                        }
                        if (!scriptVariableOverride.getDataType().equals(scriptVariable.getDataType())) {
                            throw new ValidationException("Invalid ScriptVariable override! Name: "
                                    + scriptVariable.getName() + ". Datatype Mismatch, Expected: "
                                    + scriptVariable.getDataType() + " Found: " + scriptVariableOverride.getDataType());
                        }
                    }
                }
            }
            if (!variableSet.containsAll(scriptVariableOverrideHashMap.keySet())) {
                scriptVariableOverrideHashMap.keySet().removeAll(variableSet);
                throw new ValidationException("Invalid Script variable overrides!: " + scriptVariableOverrideHashMap);
            }
        }
    }

    public void validateRefreshIdWorkflowExecute(ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException{
        if (Objects.nonNull(executeWorkflowRequest.getRequestOverride())) {
            Map<String/*dataframeId*/, DataframeOverride> dataframeOverrideMap = executeWorkflowRequest.getRequestOverride().getDataframeOverrideMap();

            for (String dataframe : dataframeOverrideMap.keySet()) {
                DataframeOverride dataframeOverride = dataframeOverrideMap.get(dataframe);
                if (dataframeOverride instanceof HiveDataframeOverride) {
                    String table = ((HiveDataframeOverride) dataframeOverride).getDatabase() + "." +
                            ((HiveDataframeOverride) dataframeOverride).getTableName();
                    Long refreshId = ((HiveDataframeOverride) dataframeOverride).getRefreshId();
                    if (refreshId == null) return;
                    Boolean isIntermediate = ((HiveDataframeOverride) dataframeOverride).getIsIntermediate();
                    if (Boolean.TRUE.equals(isIntermediate)) return;
                    runPartitionExistsValidation(table, refreshId);
                    runPartitionDataExistsValidation(table, refreshId);
                }
            }
        }
    }

    private void runPartitionExistsValidation(String table, Long refreshId) throws ValidationException {
        try {
            if (!metaStoreClient.checkPartitionExists(table, "refresh_id", refreshId.toString())) {
                throw new ValidationException("Validation failed!! Table "+table+" does not have partition "+refreshId+"!");
            }
        } catch (TException e) {
            log.error("Unable to perform validation on "+table+" for correct refresh ID!!");
            throw new ValidationException("Unable to perform hive validation on "+table+" for correct refresh ID!!");
        } catch (TableNotFoundException e){
            log.error("Unable to perform hive validation as {} table does not exists", table);
            throw new ValidationException("Unable to perform hive validation as "+table+" does not exists");
        }
    }

    private void runPartitionDataExistsValidation(String table, Long refreshId) throws ValidationException  {
        try {
            if (!hiveClient.checkPartitionHasData(table, "refresh_id", refreshId.toString())) {
                throw new ValidationException("Validation failed!! Table "+table+" with partition "+
                        refreshId+" does not have any data!");
            }
        } catch (HiveClientException e) {
            log.error("Unable to perform validation on "+table+" for correct refresh ID!!");
            throw new ValidationException("Unable to perform hive validation on "+table+" for correct refresh ID!!");
        }
    }

    public void validateTableInformation(Workflow workflow,  ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        Map<String, Long> tables = executeWorkflowRequest.getTables();
        Set<DataFrame> dataFrames = workflow.getDataFrames();
        Set<String> dataTables = dataFrames.stream().flatMap(d -> d.getSignalGroup().getSignalMetas().stream())
                .map(signalMeta -> signalMeta.getDataTable().getId()).collect(toSet());
        if (MapUtils.isNotEmpty(tables)) {
            Sets.SetView<String> dataTablesWithoutInput = Sets.difference(dataTables, tables.keySet());
            if (!dataTablesWithoutInput.isEmpty()) {
                throw new ValidationException("Workflow Execution for request does not have table information for all the required tables." +
                        "Following are the tables which are missing " + dataTablesWithoutInput);
            }
        }
    }



    public Request verifyRequestId(Long requestId) throws ValidationException {
        Request request = requestActor.getRequest(requestId);
        if (Objects.isNull(request))
            throw new ValidationException("Request for Id: " + requestId + " not found!");
        return request;
    }

    public WorkflowDetails verifyWorkflowId(Long workflowId) throws ValidationException {
        WorkflowDetails workflowDetails = workFlowActor.getWorkflowDetailsById(workflowId);
        if (Objects.isNull(workflowDetails))
            throw new ValidationException("Workflow for Id: " + workflowId + " not found!");
        return workflowDetails;
    }

    public Script verifyScriptId(Long scriptId) {
        Script script = scriptActor.getScriptById(scriptId);
        if (Objects.isNull(script))
            throw new DSPCoreException("Script for Id: " + scriptId + "not found!");
        return script;

    }

    public void verifyAzkabanExecId(Long azkabanExecId, Long requestId) throws ValidationException {
        if (azkabanExecId == 0)
            throw new ValidationException("Could not find azkaban job with id: " + azkabanExecId + " for request Id: " + requestId);
    }

    public void validateParentWorkflowRefreshId(RequestEntity requestEntity, Long parentRefreshId, Long parentWorkflowId) throws ValidationException {
        if (Objects.isNull(requestEntity))
            throw new ValidationException("No Successful Request found for Training workflow refresh_id: " + parentRefreshId);
        if (!requestEntity.getRequestStatus().equals(RequestStatus.COMPLETED))
            throw new ValidationException("Training workflow request with id: " + requestEntity.getId() + " is not in completed state.");
        if (!requestEntity.getWorkflowId().equals(parentWorkflowId))
            throw new ValidationException("Request with id: " + requestEntity.getId() + " does not belong to workflow id: " + parentWorkflowId);
    }

    public RequestEntity validateLatestSuccessRequest(Long parentWorkflowId) throws ValidationException {
        Optional<RequestEntity> request = requestDAO.getLatestSuccessFullRequest(null, parentWorkflowId);
        if (!request.isPresent())
            throw new ValidationException("No Successful request Found for parent WorkflowId: " + parentWorkflowId);
        return request.get();
    }

    public void validateCommitId(String commitId, WorkflowDetails workflowDetails) throws ValidationException {
        String gitRepo = workflowDetails.getPipelineSteps().get(0).getScript().getGitRepo();
        try {
            githubClient.checkValidity(gitRepo, commitId);
        } catch (IOException e) {
            throw new ValidationException("Given commit id: " + commitId + " is not valid." + e.getMessage());
        }
    }

    public void validateFtpCredentials(RequestOverride requestOverride) throws ValidationException {
        if (requestOverride != null && requestOverride.getDataframeOverrideMap() != null) {
            Map<String, DataframeOverride> dataframeOverrideMap = requestOverride.getDataframeOverrideMap();
            Map<String, DataframeOverride> ftpDataframOverirdeMap = dataframeOverrideMap.entrySet()
                    .stream()
                    .filter(map -> map.getValue() instanceof FTPDataframeOverride)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<String> errorMessages = new ArrayList<>();
            for (Map.Entry<String, DataframeOverride> entry : ftpDataframOverirdeMap.entrySet()) {
                FTPDataframeOverride ftpDataframeOverride = (FTPDataframeOverride) entry.getValue();
                try {
                    externalCredentialsActor.getCredentials(ftpDataframeOverride.getClientAlias());
                } catch (DSPCoreException e) {
                    errorMessages.add("Ftp alias: " + ftpDataframeOverride.getClientAlias() + " for " + ftpDataframeOverride.getPath() + " is not registered with DSP, " + e.getMessage());
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new ValidationException(errorMessages.toString());
            }
        }
    }
}
