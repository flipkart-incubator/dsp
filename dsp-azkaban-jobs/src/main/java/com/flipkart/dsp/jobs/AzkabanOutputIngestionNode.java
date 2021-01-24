package com.flipkart.dsp.jobs;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.helper.CephIngestionHelper;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.utils.*;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@Slf4j
public class
AzkabanOutputIngestionNode extends AbstractAzkabanNode {
    private final CephIngestionHelper cephIngestionHelper;
    private final RequestStepAuditActor requestStepAuditActor;
    private final EmailNotificationHelper emailNotificationHelper;

    @Inject
    public AzkabanOutputIngestionNode(RequestActor requestActor,
                                      ObjectMapper objectMapper,
                                      WorkFlowActor workFlowActor,
                                      CosmosReporter cosmosReporter,
                                      EventAuditUtil eventAuditUtil,
                                      DSPServiceClient dspServiceClient,
                                      RequestStepActor requestStepActor,
                                      CephIngestionHelper cephIngestionHelper,
                                      RequestStepAuditActor requestStepAuditActor,
                                      EmailNotificationHelper emailNotificationHelper) {
        super(requestActor, objectMapper, workFlowActor, cosmosReporter, eventAuditUtil, dspServiceClient,
                requestStepActor, requestStepAuditActor);
        this.cephIngestionHelper = cephIngestionHelper;
        this.requestStepAuditActor = requestStepAuditActor;
        this.emailNotificationHelper = emailNotificationHelper;
    }

    @Override
    public String getName() {
        return Constants.OUTPUT_INGESTION_NODE;
    }

    @Override
    @Timed
    @Metered
    protected void performAction(Long requestStepId, NodeMetaData nodeMetaData, WorkflowDetails workflowDetails) throws AzkabanException {
        Long requestId = nodeMetaData.getRequestId();
        Request request = getRequest(requestId);
        Workflow workflow = workflowDetails.getWorkflow();
        String hiveQueue = workflow.getWorkflowMeta().getHiveQueue();
        log.info("Output Ingestion node: Workflow id: {}, dynamic Args map: {} ", workflow.getName(), nodeMetaData);

        List<String> errorMessages = new ArrayList<>();

        if (workflowDetails.getCephOutputs().size() != 0)
            errorMessages.addAll(cephIngestionHelper.ingestInCeph(requestId, workflowDetails));

        try {
            emailNotificationHelper.sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);
        } catch (EmailException e) {
            e.printStackTrace();
            String errorMessage = "Error while Sending Email. ErrorMessage:" + e.getMessage() + " " + e.toString();
            log.info(errorMessage);
        }

        if (errorMessages.size() != 0) {
            createRequestStepAudit(requestStepId);
            eventAuditUtil.creatOutputIngestionErrorEvent(requestId, workflow.getId(), workflow.getName(), "Error,Error");
            throw new AzkabanException(String.join("\n", errorMessages));
        }
        nodeMetaData.setPrevNode(getName());
    }

    private void createRequestStepAudit(Long requestStepId) {
        requestStepAuditActor.createRequestStepAudit(RequestStepAudit.builder().requestStepId(requestStepId)
                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
    }

    @Override
    protected RequestStepType getRequestStepType() {
        return RequestStepType.OI;
    }

    @Override
    protected void setClientQueue() {
    }
}
