package com.flipkart.dsp.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.CallbackException;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import javax.inject.Inject;

import static com.flipkart.dsp.models.RequestStatus.COMPLETED;

/**
 *
 */
@Slf4j
public class AzkabanTerminalNode extends AbstractAzkabanNode {
    private final RequestActor requestActor;
    private final EventAuditUtil eventAuditUtil;
    private final NotificationActor notificationActor;

    @Inject
    public AzkabanTerminalNode(RequestActor requestActor,
                               ObjectMapper objectMapper,
                               WorkFlowActor workFlowActor,
                               CosmosReporter cosmosReporter,
                               EventAuditUtil eventAuditUtil,
                               DSPServiceClient dspServiceClient,
                               RequestStepActor requestStepActor,
                               NotificationActor notificationActor,
                               RequestStepAuditActor requestStepAuditActor) {
        super(requestActor, objectMapper, workFlowActor, cosmosReporter, eventAuditUtil, dspServiceClient,
                requestStepActor, requestStepAuditActor);
        this.requestActor = requestActor;
        this.eventAuditUtil = eventAuditUtil;
        this.notificationActor = notificationActor;
    }

    @Override
    public String getName() {
        return Constants.TERMINAL_NODE;
    }

    @Override
    protected void performAction(Long requestStepId, NodeMetaData nodeMetaData, WorkflowDetails workflowDetails) throws AzkabanException {
        log.info("Terminal Node- dynamic args map: {}", nodeMetaData);
        Long requestId = nodeMetaData.getRequestId();
        Workflow workflow = workflowDetails.getWorkflow();
        try {
            Request request = requestActor.getRequest(requestId);
            notificationActor.notifyExecutionStatus(RequestStatus.COMPLETED, request.getAzkabanExecId(),
                    " is successfully completed.", requestId);
            eventAuditUtil.makeFlowTerminatingEntry("Flow is Executed Successfully", requestId, workflow.getId(),
                    EventType.valueOf(getName()), false);
            requestActor.updateRequestStatus(request, COMPLETED);
        } catch (EmailException | CallbackException e) {
            eventAuditUtil.createTerminalNodeErrorEvent(requestId, workflow.getId(), workflow.getName(), e.getMessage() + " " + e.toString());
            throw new AzkabanException("Azkaban notifier node Failed", e);
        }
    }

    @Override
    protected RequestStepType getRequestStepType() {
        return RequestStepType.TERMINAL;
    }

    @Override
    protected void setClientQueue() {
    }
}
