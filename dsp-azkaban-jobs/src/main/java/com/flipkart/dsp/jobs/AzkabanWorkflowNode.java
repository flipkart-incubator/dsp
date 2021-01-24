package com.flipkart.dsp.jobs;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.request.RequestStep;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.helper.WorkflowOutputTableHelper;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.models.WorkflowStatus;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.service.WorkflowExecutionService;
import com.flipkart.dsp.utils.*;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.EmailException;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
@Slf4j
public class AzkabanWorkflowNode extends AbstractAzkabanNode {

    private final HiveConfig hiveConfig;
    private final EventAuditUtil eventAuditUtil;
    private final DataSourceActor dataSourceActor;
    private final RequestStepActor requestStepActor;
    private final WorkflowAuditActor workflowAuditActor;
    private final DataFrameAuditActor dataFrameAuditActor;
    private final RequestStepAuditActor requestStepAuditActor;
    private final PipelineStepAuditActor pipelineStepAuditActor;
    private final EmailNotificationHelper emailNotificationHelper;
    private final WorkflowExecutionService workflowExecutionService;
    private final WorkflowOutputTableHelper workflowOutputTableHelper;

    @Override
    protected void setClientQueue() {
    }

    @Inject
    public AzkabanWorkflowNode(HiveConfig hiveConfig,
                               RequestActor requestActor,
                               ObjectMapper objectMapper,
                               WorkFlowActor workFlowActor,
                               CosmosReporter cosmosReporter,
                               EventAuditUtil eventAuditUtil,
                               DataSourceActor dataSourceActor,
                               DSPServiceClient dspServiceClient,
                               RequestStepActor requestStepActor,
                               WorkflowAuditActor workflowAuditActor,
                               DataFrameAuditActor dataFrameAuditActor,
                               RequestStepAuditActor requestStepAuditActor,
                               PipelineStepAuditActor pipelineStepAuditActor,
                               EmailNotificationHelper emailNotificationHelper,
                               WorkflowExecutionService workflowExecutionService,
                               WorkflowOutputTableHelper workflowOutputTableHelper) {
        super(requestActor, objectMapper, workFlowActor, cosmosReporter, eventAuditUtil, dspServiceClient,
                requestStepActor, requestStepAuditActor);
        this.hiveConfig = hiveConfig;
        this.eventAuditUtil = eventAuditUtil;
        this.dataSourceActor = dataSourceActor;
        this.requestStepActor = requestStepActor;
        this.workflowAuditActor = workflowAuditActor;
        this.dataFrameAuditActor = dataFrameAuditActor;
        this.requestStepAuditActor = requestStepAuditActor;
        this.pipelineStepAuditActor = pipelineStepAuditActor;
        this.emailNotificationHelper = emailNotificationHelper;
        this.workflowExecutionService = workflowExecutionService;
        this.workflowOutputTableHelper = workflowOutputTableHelper;
    }

    @Override
    public String getName() {
        return Constants.WORKFLOW_NODE;
    }

    @Override
    @Timed
    @Metered
    protected void performAction(Long requestStepId, NodeMetaData nodeMetaData, WorkflowDetails workflowDetails) throws AzkabanException {
        Long requestId = nodeMetaData.getRequestId();
        Workflow workflow = workflowDetails.getWorkflow();
        Long workflowId = workflow.getId();
        String workflowName = workflow.getName();
        String hiveQueue = workflow.getWorkflowMeta().getHiveQueue();
        String mesosQueue = workflow.getWorkflowMeta().getMesosQueue();
        try {
            Request request = getRequest(requestId);
            Boolean testRun = request.getData().getTestRun();
            eventAuditUtil.createWFStartInfoEvent(requestId, workflowId, workflowName);
            log.info("Workflow node: Workflow id: {}, dynamic Args map: {} ", workflowName, nodeMetaData);
            String workflowExecId = UUID.randomUUID().toString();
            workflowAuditActor.createWorkflowAudit(requestId, workflow.getId(), workflowExecId);
            // run signal-generator and script executor step by step
            for (PipelineStep pipelineStep : workflowDetails.getPipelineSteps()) {
                log.info("Running SG for Step: {}", pipelineStep.getName());
                // executing sg mesos job
                if (!workflowExecutionService.executeSG(requestId, workflowDetails, pipelineStep, workflowExecId,
                        mesosQueue, workflow.getIsProd())) {
                    throw new AzkabanException("There are failure for workflowEntity execution id: " + workflowExecId);
                }
                // executing steps workflow jobs
                SGJobOutputPayload payload = getSGJobOutput(requestId, workflowId, pipelineStep.getId(), testRun);
                log.info("Running Script for Step: {}", pipelineStep.getName());
                if (!workflowExecutionService.executeWorkflow(request, workflowDetails, payload,
                        workflowExecId, mesosQueue, workflow.getIsProd(), pipelineStep)) {
                    break;
                }
                log.info("Request Id: {}, workflowId: {}, workflow Execution id: {}", requestId, workflowId, workflowExecId);

            }

            try {
                markWorkflowCompletion(workflowExecId, requestStepId);
            } catch (AzkabanException e) {
                workflowAuditActor.update(requestId, workflowDetails.getWorkflow().getId(), workflowExecId, WorkflowStatus.FAILED);
                throw new AzkabanException(e.getMessage());
            }
            workflowAuditActor.update(requestId, workflowDetails.getWorkflow().getId(), workflowExecId, WorkflowStatus.SUCCESS);
            log.info("{} Workflow with id: {} has completed. Execution id: {}", workflowName, workflowId, workflowExecId);

            nodeMetaData.setPrevNode(getName());
            emailNotificationHelper.sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.WORKFLOW);
        } catch (EmailException e) {
            e.printStackTrace();
            log.info("Error while Sending Email. ErrorMessage: " + e.getMessage() + " " + e.toString());
        } catch (Exception e) {
            eventAuditUtil.createWFErrorEvent(requestId, workflowId, workflowName, e.getMessage() + " " + e.toString());
            throw e;
        }
    }

    private void createDSEntry(String hiveQueue, List<String> hiveTables) {
        for (String table : hiveTables) {
            String dbName = StringUtils.split(table, ".")[0];
            if (!dataSourceActor.DoesDSExistsInDB(dbName)) {
                dataSourceActor.persistIfNotExist(dbName);
            }
        }
    }

    private SGJobOutputPayload getSGJobOutput(Long requestId, Long workflowId, Long pipelineStepId, Boolean testRun) {
        Set<DataFrameAudit> dataFrameAudits = dataFrameAuditActor.getDataframeAudits(requestId, workflowId, pipelineStepId);
        SGJobOutputPayload jobOutputPayload = new SGJobOutputPayload();
        Set<SGUseCasePayload> sgUseCasePayloads = dataFrameAudits.stream()
                .map(dataFrameAudit -> {
                    SGUseCasePayload payload = dataFrameAudit.getPayload();
                    if (testRun != null && testRun) {
                        Map<List<DataFrameKey>, Set<String>> alternateDataframes = new HashMap<>();
                        Map<List<DataFrameKey>, Set<String>> dataframes = payload.getDataframes();
                        List<DataFrameKey> firstKey = dataframes.keySet().iterator().next();
                        alternateDataframes.put(firstKey, dataframes.get(firstKey));
                        payload.setDataframes(alternateDataframes);
                    }
                    log.info("SG payload: {}", payload.toString());
                    return payload;
                }).collect(Collectors.toSet());
        jobOutputPayload.setSgUseCasePayloadSet(sgUseCasePayloads);
        enrichPayload(jobOutputPayload);
        return jobOutputPayload;
    }

    @Override
    protected RequestStepType getRequestStepType() {
        return RequestStepType.WF;
    }

    private void enrichPayload(SGJobOutputPayload payload) {
        if (payload.getSgUseCasePayloadSet() == null || payload.getSgUseCasePayloadSet().isEmpty()) {
            throw new IllegalStateException("Something is wrong, null or empty SgUseCasePayload Found!!");
        }
        payload.getSgUseCasePayloadSet().forEach(SGUseCasePayload::addColumnNameToDataFrameKeys);
    }

    private void markWorkflowCompletion(String workflowExecId, long requestStepId) throws AzkabanException {
        Map<PipelineStepStatus, Long> statusMap = pipelineStepAuditActor.getPipelineStepAuditStatusMap(workflowExecId);
        log.info("Workflow status map: {}", statusMap);
        if ((statusMap.containsKey(PipelineStepStatus.FAILED) && statusMap.get(PipelineStepStatus.FAILED) > 0) ||
                (statusMap.containsKey(PipelineStepStatus.INITIATED) && statusMap.get(PipelineStepStatus.INITIATED) > 0) ||
                (statusMap.containsKey(PipelineStepStatus.STARTED) && statusMap.get(PipelineStepStatus.STARTED) > 0)
        ) {
            log.error("There were failures for workflow execution id: " + workflowExecId);
            RequestStep requestStep = requestStepActor.getRequestStep(requestStepId);
            requestStepAuditActor.createRequestStepAudit(requestStep, RequestStepAuditStatus.FAILED, null);
            throw new AzkabanException("There were failures for workflow execution id: " + workflowExecId);
        }
        Long totalAudits = statusMap.values().stream().mapToLong(Long::longValue).sum();
        if (statusMap.containsKey(PipelineStepStatus.SUCCESS) && statusMap.get(PipelineStepStatus.SUCCESS).equals(totalAudits)) {
            log.info("All {} workflow audit entries succeeded for execution id: {} ", totalAudits, workflowExecId);
        }
    }
}
