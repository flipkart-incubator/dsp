package com.flipkart.dsp.executor.application;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPClientProcessingException;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.Resources;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.exception.PersistenceException;
import com.flipkart.dsp.executor.exception.ResolutionException;
import com.flipkart.dsp.executor.helper.EmailNotificationHelper;
import com.flipkart.dsp.executor.helper.EventAuditHelper;
import com.flipkart.dsp.executor.orchestrator.WorkFlowOrchestrator;
import com.flipkart.dsp.executor.utils.ExecutorLogManager;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkFlowMesosApplication extends AbstractApplication {
    private static final String EXECUTOR_NAME = "WorkFlowMesosApplication";

    private final CosmosReporter cosmosReporter;
    private final DSPServiceClient dspServiceClient;
    private final EventAuditHelper eventAuditHelper;
    private final ExecutorLogManager executorLogManager;
    private final WorkFlowOrchestrator workFlowOrchestrator;
    private final EmailNotificationHelper emailNotificationHelper;

    @Override
    public String getName() {
        return EXECUTOR_NAME;
    }

    @Override
    public void execute(String[] args) throws ApplicationException {
        try {
            processApplication(args);
        } finally {
            cosmosReporter.forceFlush();
        }
    }

    @Timed
    @Metered
    protected void processApplication(String[] args) throws ApplicationException {
        String configPayloadString = args[0];
        String executorId = args[4];
        Double cpus = Double.valueOf(args[5]);
        Double mem = Double.valueOf(args[6]);
        String clusterRole = args[7];
        Integer attempt = Integer.valueOf(args[8]);
        String previousAttemptContainerId = args[9];
        Resources resources = new Resources(cpus, mem);
        ConfigPayload configPayload = deserializeConfigPayload(configPayloadString);


        WorkflowDetails workflowDetails = dspServiceClient.getWorkflowDetails(configPayload.getWorkflowId());
        Workflow workflow = workflowDetails.getWorkflow();
        Long pipelineStepId = configPayload.getPipelineStepId();
        PipelineStep pipelineStep = getPipelineStep(workflowDetails, pipelineStepId);
        populateCosmosTag(configPayload, workflow.getName(), clusterRole, attempt);
        updateLogPathForPreviousAttempt(attempt, workflow.getId(), previousAttemptContainerId, resources, configPayload, pipelineStep);

        String logs = "";
        Map<Long /**pipelineStepAuditId*/, Integer /**log attempt*/> logAttemptMapping = new HashMap<>();
        try {
            logs = executorLogManager.getMesosLogURL(args[3], args[2], args[1], executorId);
            PipelineStepAudit pipelineStepAudit = cretePipelineStepAudit(workflow.getId(), configPayload, PipelineStepStatus.STARTED,
                    pipelineStep, logs, resources, attempt);

            logAttemptMapping = getLogAttemptMapping(attempt, executorId, pipelineStepAudit.getId(), configPayload);
            createContainerStartInfoEventAudits(workflowDetails, configPayload, pipelineStepAudit, logAttemptMapping);
            workFlowOrchestrator.run(workflowDetails, configPayload, pipelineStep);

            PipelineStepAudit successfulPipelineStepAudit = cretePipelineStepAudit(workflow.getId(), configPayload, PipelineStepStatus.SUCCESS,
                    pipelineStep, logs, resources, attempt);
            eventAuditHelper.createWFContainerCompletedInfoEvent(workflowDetails.getWorkflow().getName(), resources,
                    logAttemptMapping, configPayload, successfulPipelineStepAudit);
            emailNotificationHelper.sendNotifications(workflow.getId(), successfulPipelineStepAudit.getId(), configPayload, WorkflowStateNotificationType.SUCCESS);
        } catch (ResolutionException | PersistenceException | ScriptExecutionEngineException | DSPClientProcessingException | UnsupportedEncodingException | TException | TableNotFoundException e) {
            PipelineStepAudit failurePipelineStepAudit = cretePipelineStepAudit(workflow.getId(), configPayload, PipelineStepStatus.FAILED,
                    pipelineStep, logs, resources, attempt);
            String errorMessage = String.format("Failed to run application %s because of following reason: %s", getName(), e);
            eventAuditHelper.createWFContainerFailedEvent(workflow.getName(), resources, logAttemptMapping,
                    configPayload, errorMessage, failurePipelineStepAudit);
            emailNotificationHelper.sendNotifications(workflow.getId(), failurePipelineStepAudit.getId(), configPayload, WorkflowStateNotificationType.FAILURE);
            throw new ApplicationException(getName(), e);
        }
    }

    private void populateCosmosTag(ConfigPayload configPayload, String workflowName, String clusterRole, Integer attempt) {
        MesosCosmosTag.populateValue(workflowName, clusterRole, attempt, configPayload);
    }

    private void updateLogPathForPreviousAttempt(Integer attempt, Long workflowId, String previousAttemptContainerId,
                                                 Resources resources, ConfigPayload configPayload, PipelineStep pipelineStep) {
        // if attempt > 0, update log of attempt-1 with proper log path and persist
        if (attempt > 0) {
            String currentLogs = dspServiceClient.getPipelineStepLogDetails(attempt - 1, configPayload.getPipelineStepId()
                    , configPayload.getPipelineExecutionId(), configPayload.getRefreshId()).get(0).getLogs();
            if (Objects.nonNull(currentLogs))
                cretePipelineStepAudit(workflowId, configPayload, PipelineStepStatus.FAILED, pipelineStep,
                        currentLogs.replace("latest", previousAttemptContainerId), resources, attempt - 1);
        }

    }

    private Map<Long, Integer> getLogAttemptMapping(Integer attempt, String executorId, Long pipelineStepAuditId, ConfigPayload configPayload) {
        Map<Long /**pipelineStepAuditId*/, Integer /**log attempt*/> logAttemptMapping = new HashMap<>();
        if (attempt == 0) {
            logAttemptMapping.put(pipelineStepAuditId, 0);
            return logAttemptMapping;
        }
        int separatorIndex = executorId.lastIndexOf("#");
        separatorIndex = separatorIndex == -1 ? executorId.length() : separatorIndex;
        String pipelineExecutionId = executorId.substring(0, separatorIndex);
        List<PipelineStepAudit> pipelineStepAuditList = dspServiceClient.getPipelineStepLogDetails(null, configPayload.getPipelineStepId(),
                pipelineExecutionId, configPayload.getRefreshId());
        return pipelineStepAuditList.stream().collect(Collectors.toMap(PipelineStepAudit::getId, PipelineStepAudit::getAttempt));
    }

    private void createContainerStartInfoEventAudits(WorkflowDetails workflowDetails, ConfigPayload configPayload,
                                                     PipelineStepAudit pipelineStepAudit, Map<Long, Integer> logAttemptMapping) {
        eventAuditHelper.createWFContainerStartInfoEvent(workflowDetails.getWorkflow().getName(), configPayload,
                pipelineStepAudit, logAttemptMapping);
        eventAuditHelper.createWFContainerStartedDebugEvent(workflowDetails.getWorkflow().getName(), configPayload,
                pipelineStepAudit, logAttemptMapping);
    }

    private static PipelineStep getPipelineStep(WorkflowDetails workflowDetails, Long pipelineStepId) {
        Optional<PipelineStep> pipelineStep = workflowDetails.getPipelineSteps().stream()
                .filter(p -> p.getId().equals(pipelineStepId)).findAny();
        if (pipelineStep.isPresent())
            return pipelineStep.get();
        else {
            log.error("Pipeline step {} not found in workflow {} ", pipelineStepId, workflowDetails.getWorkflow().getId());
            throw new IllegalStateException("Pipeline step " + pipelineStepId + " not found in workflow " + workflowDetails.getWorkflow().getId());
        }
    }

    public PipelineStepAudit cretePipelineStepAudit(Long workflowId, ConfigPayload configPayload, PipelineStepStatus status,
                                       PipelineStep pipelineStep, String logs, Resources resources, Integer attempt) {
        String configPayloadString = JsonUtils.DEFAULT.toJson(configPayload);
        String pipelineStepString = JsonUtils.DEFAULT.toJson(pipelineStep);
        log.info(" saving pipeline audit payload: {}, pipelineStatus: {}, pipelineStep: {}", configPayloadString, status.toString(), pipelineStepString);
        PipelineStepAudit pipelineStepAudit = PipelineStepAudit.builder().refreshId(configPayload.getRefreshId())
                .pipelineStepId(pipelineStep.getId()).pipelineExecutionId(configPayload.getPipelineExecutionId())
                .workflowExecutionId(configPayload.getWorkflowExecutionId()).pipelineStepStatus(status).resources(resources).logs(logs)
                .attempt(attempt).scope(configPayload.getScope()).workflowId(workflowId).build();
        return dspServiceClient.savePipelineStepAuditRequest(pipelineStepAudit);
    }
}
