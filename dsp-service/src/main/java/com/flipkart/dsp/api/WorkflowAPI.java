package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.actors.NotificationPreferencesActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.dto.AzkabanFlow;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.DSPWorkflowExecutionRequest;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.AzkabanProjectCreationException;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exception.CreateWorkflowException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exception.ExecuteWorkflowException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.models.workflow.CreateWorkflowResponse;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.models.workflow.WorkflowPromoteRequest;
import com.flipkart.dsp.service.AzkabanProjectHelper;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.flipkart.dsp.validation.DataframeOverrideValidator;
import com.flipkart.dsp.validation.EmailNotificationsValidator;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowAPI {
    private final Validator validator;
    private final RequestAPI requestAPI;
    private final WorkFlowActor workFlowActor;
    private final EventAuditUtil eventAuditUtil;
    private final PipelineStepAPI pipelineStepAPI;
    private final WorkflowDetailsAPI workflowDetailsAPI;
    private final WorkflowVersionAPI workflowVersionAPI;
    private final AzkabanProjectHelper azkabanProjectHelper;
    private final NotificationPreferencesActor notificationPreferencesActor;
    private final DataframeOverrideValidator dataframeOverrideValidator;

    public CreateWorkflowResponse createWorkflow(String triggeredBy, CreateWorkflowRequest createWorkflowRequest, List<DataFrameEntity> sgDataFrameEntities)
            throws CreateWorkflowException, ValidationException {
        validator.validateWorkflowCreateRequest(createWorkflowRequest);
        validator.validateFtpCredentials(createWorkflowRequest.getRequestOverride());
        WorkflowDetails existingWorkflowDetails = workflowDetailsAPI.getLatestWorkflowDetails(createWorkflowRequest.getWorkflow().getName(),
                createWorkflowRequest.getWorkflowGroupName(), null, createWorkflowRequest.getWorkflow().getIsProd());
        WorkflowDetails createWorkflowDetails = workflowDetailsAPI.convertToWorkflowDetails(triggeredBy, createWorkflowRequest);
        EmailNotificationsValidator.validateEmailNotificationDetails(createWorkflowRequest.getEmailNotifications(), createWorkflowDetails);
        String newVersion = workflowVersionAPI.getNewVersionForWorkflow(existingWorkflowDetails, createWorkflowDetails, sgDataFrameEntities);
        if (Objects.isNull(existingWorkflowDetails) || !existingWorkflowDetails.getWorkflow().getVersion().equalsIgnoreCase(newVersion)) {
            createWorkflowDetails.getWorkflow().setVersion(newVersion);
            printCreateMessage(createWorkflowDetails.getWorkflow(), "started");
            WorkflowEntity persistedWorkflow = workFlowActor.save(createWorkflowDetails.getWorkflow());
            pipelineStepAPI.createPipelineSteps(persistedWorkflow, createWorkflowDetails.getPipelineSteps());
            printCreateMessage(createWorkflowDetails.getWorkflow(), "completed");
        }
        return CreateWorkflowResponse.builder().name(createWorkflowDetails.getWorkflow().getName())
                .isProd(createWorkflowDetails.getWorkflow().getIsProd()).version(newVersion)
                .workflowGroupName(createWorkflowDetails.getWorkflow().getWorkflowGroupName()).build();
    }

    private void printCreateMessage(Workflow workflow, String workflowState) {
        log.info("Creation of {} workflow {} for workflow_name: {} ,version: {}", workflow.getIsProd() ? "Prod" : "Draft",
                workflowState, workflow.getName(), workflow.getVersion());
    }

    public ExecutionOutput executeWorkflow(String triggeredBy, String workflowGroupName,  ExecuteWorkflowRequest executeWorkflowRequest)
            throws ValidationException, ExecuteWorkflowException {
        Long requestId = 0L;
        printExecuteMessage(executeWorkflowRequest, "started");
        WorkflowDetails workflowDetails = getWorkflowDetails(workflowGroupName, executeWorkflowRequest);
        Long workflowId = workflowDetails.getWorkflow().getId();
        validator.validateWorkflowExecuteRequest(workflowDetails, executeWorkflowRequest);
        if (executeWorkflowRequest.getIsProd()){
            dataframeOverrideValidator.verifyOverrides(workflowDetails, executeWorkflowRequest);
        }
        try {
            AzkabanFlow azkabanFlow = azkabanProjectHelper.setupAzkabanJob(workflowDetails);
            RequestEntity requestEntity = requestAPI.createRequest(triggeredBy, workflowDetails, executeWorkflowRequest);
            requestAPI.triggerAzkabanJob(requestEntity, azkabanFlow);
            requestId = requestEntity.getId();
            String azkabanUrl = azkabanProjectHelper.getAzkabanUrl(requestEntity.getAzkabanExecId());
            eventAuditUtil.makeFlowStartInfoEventEntry(executeWorkflowRequest.getWorkflowVersion(), requestEntity.getId(), azkabanUrl, workflowDetails);
            printExecuteMessage(executeWorkflowRequest, "completed");
            return ExecutionOutput.builder().jobId(requestEntity.getId()).azkabanUrl(azkabanUrl).build();
        } catch (AzkabanProjectCreationException | AzkabanException e) {
            eventAuditUtil.makeFlowTerminatingEntry(e.getMessage(), requestId, workflowId, EventType.Service, true);
            throw new ExecuteWorkflowException(e.getMessage());
        }
    }

    private WorkflowDetails getWorkflowDetails(String workflowGroupName, ExecuteWorkflowRequest executeWorkflowRequest) throws ValidationException {
        WorkflowDetails workflowDetails = workflowDetailsAPI.getLatestWorkflowDetails(executeWorkflowRequest.getWorkflowName(), workflowGroupName,
                executeWorkflowRequest.getWorkflowVersion(), executeWorkflowRequest.getIsProd());
        if (Objects.isNull(workflowDetails))
            throw new ValidationException("Workflow Details can't be null for workflow_name: " + executeWorkflowRequest.getWorkflowName());
        return workflowDetails;
    }

    private void printExecuteMessage(ExecuteWorkflowRequest executeWorkflowRequest, String message) {
        log.info("Execution request for {} workflow {} for workflow_name: {}{}", executeWorkflowRequest.getIsProd() ? "Prod" : "Draft",
                message, executeWorkflowRequest.getWorkflowName(),
                message.equalsIgnoreCase("completed") ? ". Azkaban Job Submitted." : "");
    }


    public WorkflowDetails promoteWorkflow(Long requestId, WorkflowPromoteRequest workflowPromoteRequest) throws DSPSvcException, ValidationException {
        WorkflowDetails workflowDetails = prepareWorkflowForPromote(requestId, workflowPromoteRequest);
        WorkflowEntity persistedWorkflow = workFlowActor.save(workflowDetails.getWorkflow());
        pipelineStepAPI.createPipelineSteps(persistedWorkflow, workflowDetails.getPipelineSteps());
        EmailNotificationsValidator.validateEmailNotificationDetails(workflowPromoteRequest.getEmailNotifications(), workflowDetails);
        log.debug("entering validation function");
        validator.validateCommitId(workflowPromoteRequest.getCommitId(), workflowDetails);
        notificationPreferencesActor.createNotificationPreferences(persistedWorkflow, workflowPromoteRequest.getEmailNotifications());
        return workflowDetails;
    }

    private WorkflowDetails prepareWorkflowForPromote(Long requestId, WorkflowPromoteRequest workflowPromoteRequest) throws ValidationException, DSPSvcException {
        Request request = validator.verifyRequestId(requestId);
        WorkflowDetails workflowDetails = validator.verifyWorkflowId(request.getWorkflowId());
        WorkflowDetails currentLatestProdWorkflow = workflowDetailsAPI.getLatestWorkflowDetails(workflowDetails.getWorkflow().getName(), null, null, true);
        updateWorkflowDetails(workflowPromoteRequest, currentLatestProdWorkflow, workflowDetails);
        updateScriptDetails(workflowDetails, currentLatestProdWorkflow, workflowPromoteRequest.getCommitId());
        return workflowDetails;
    }

    private void updateWorkflowDetails(WorkflowPromoteRequest workflowPromoteRequest, WorkflowDetails currentLatestProdWorkflow,
                                       WorkflowDetails workflowDetails) {
        Workflow workflow = workflowDetails.getWorkflow();
        workflow.setIsProd(true);
        if (Objects.nonNull(workflowPromoteRequest.getMesosQueue())) workflow.getWorkflowMeta().setMesosQueue(workflowPromoteRequest.getMesosQueue());
        if (Objects.nonNull(workflowPromoteRequest.getHiveQueue())) workflow.getWorkflowMeta().setHiveQueue(workflowPromoteRequest.getHiveQueue());
        workflow.setVersion(workflowVersionAPI.getNewProdWorkflowVersion(currentLatestProdWorkflow, workflowDetails));
    }

    private void updateScriptDetails(WorkflowDetails workflowDetails, WorkflowDetails currentLatestProdWorkflow, String commitId) {
        for (PipelineStep pipelineStep : workflowDetails.getPipelineSteps()) {
            if (Objects.isNull(currentLatestProdWorkflow))
                pipelineStep.getScript().setVersion(1.0);
            else {
                PipelineStep prodStep = currentLatestProdWorkflow.getPipelineSteps().stream()
                        .filter(pipelineStep1 -> Objects.nonNull(pipelineStep.getName()) && pipelineStep.getName().equals(pipelineStep1.getName())).findFirst()
                        .orElse(null);
                if (Objects.isNull(prodStep))
                    pipelineStep.getScript().setVersion(1.0);
                else
                    pipelineStep.getScript().setVersion(prodStep.getScript().getVersion() + 1.0);
            }
            pipelineStep.getScript().setIsDraft(false);
            pipelineStep.getScript().setGitCommitId(commitId);
        }
    }

}
