package com.flipkart.dsp.executor.application;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.utils.ExecutorLogManager;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.SGEndInfoEvent;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.SGErrorEvent;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.SGStartInfoEvent;
import com.flipkart.dsp.sg.api.SGApi;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SignalShuffleApplication extends AbstractApplication {
    private static final String EXECUTOR_NAME = "SignalShuffleApplication";
    private final CosmosReporter cosmosReporter;
    private final SGApi sgApi;
    private final DSPServiceClient dspServiceClient;
    private final ExecutorLogManager executorLogManager;

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
        log.info("started shuffler!");
        String configPayloadString = args[0];
        String frameworkId = args[1];
        String slaveId = args[2];
        String hostIP = args[3];
        String executorId = args[4];

        ConfigPayload configPayload = deserializeConfigPayload(configPayloadString);
        Long requestId = configPayload.getRefreshId();
        Long workFlowId = configPayload.getWorkflowId();
        Long pipelineStepId = configPayload.getPipelineStepId();
        String workflowName = configPayload.getWorkflowId().toString();
        String pipelineExecution = configPayload.getPipelineExecutionId();
        String workflowExecutionId = configPayload.getWorkflowExecutionId();
        String hiveQueue = dspServiceClient.getQueueInfo(workFlowId).getHiveQueue();
        log.info("Setting hive queue to: {}", hiveQueue);

        populateCosmosTag(configPayload, workflowName, null);
        try {
            String logs = executorLogManager.getMesosLogURL(hostIP, slaveId, frameworkId, executorId);
            Long auditId = updateAudit(requestId, pipelineStepId, pipelineExecution, workflowExecutionId, logs, PipelineStepStatus.STARTED);
            log.info("SG Started for workflowName: {} step id: {}", workflowName, pipelineStepId);
            createSGStartInfoEvent(requestId, workFlowId, workflowName, pipelineStepId.toString(), auditId.toString());
            sgApi.submitJob(requestId, workFlowId, pipelineStepId, auditId);
            log.info("SG Job for workflow {}  step id {} has completed", workflowName, pipelineStepId);
            updateAudit(requestId, pipelineStepId, pipelineExecution, workflowExecutionId, logs, PipelineStepStatus.SUCCESS);
            createSGEndInfoEvent(requestId, workFlowId, workflowName);
        } catch (Exception e) {
            updateAudit(requestId, pipelineStepId, pipelineExecution, workflowExecutionId, "", PipelineStepStatus.FAILED);
            createSGErrorEvent(e.getMessage() + " " + e.toString(), requestId, workflowName, workFlowId);
            e.printStackTrace();
            String errorMessage = String.format("Failed to run application %s because of following reason: %s", getName(), e);
            log.error(errorMessage);
            throw new ApplicationException(getName(), e);
        }

        log.info("shuffler completed!");
    }

    private void populateCosmosTag(ConfigPayload configPayload, String workflowName, String clusterRole) {
        MesosCosmosTag.populateValue(workflowName, clusterRole, null, configPayload);
    }

    private void createSGStartInfoEvent(Long requestId, Long workflowId, String workflowName, String stepId, String auditId) {
        SGStartInfoEvent sgStartInfoEvent = SGStartInfoEvent.builder().workflowName(workflowName)
                .stepId(stepId).logUrl(auditId).build();
        createEventAudit(requestId, workflowId, EventLevel.INFO, sgStartInfoEvent);
    }

    private void createSGEndInfoEvent(Long requestId, Long workflowId, String workflowName) {
        SGEndInfoEvent sgEndInfoEvent = SGEndInfoEvent.builder().workflowName(workflowName).build();
        createEventAudit(requestId, workflowId, EventLevel.INFO, sgEndInfoEvent);
    }

    private void createSGErrorEvent(String errorMessage, Long requestId, String workflowName, Long workflowId) {
        SGErrorEvent sgErrorEvent = SGErrorEvent.builder().workflowName(workflowName).errorMessage(errorMessage).build();
        createEventAudit(requestId, workflowId, EventLevel.ERROR, sgErrorEvent);
    }

    private void createEventAudit(Long requestId, Long workflowId, EventLevel eventLevel, Events event) {
        EventAudit eventAudit = EventAudit.builder().requestId(requestId).workflowId(workflowId)
                .eventType(EventType.SGNode).eventLevel(eventLevel).payload(event).build();

        dspServiceClient.saveEventAudit(eventAudit);
    }

    private long updateAudit(Long requestId, Long pipelineStepId, String pipelineExecution,
                             String workflowExecutionId, String logs, PipelineStepStatus status) {
        log.info("saving pipeline sg audit pipelineStatus: {}, pipelineStep: {}", status.toString(), pipelineStepId);
        PipelineStepSGAudit newPipelineStepAudit = PipelineStepSGAudit.builder().refreshId(requestId).pipelineStep(pipelineStepId)
                .status(status).logs(logs).pipelineExecutionId(pipelineExecution).workflowExecutionId(workflowExecutionId).build();
        return dspServiceClient.savePipelineStepSgAuditRequest(newPipelineStepAudit);
    }

}
