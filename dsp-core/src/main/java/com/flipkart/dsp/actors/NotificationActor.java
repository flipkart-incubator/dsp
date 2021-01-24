package com.flipkart.dsp.actors;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.CallbackException;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

/**
 */
@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class NotificationActor {
    private final RequestActor requestActor;
    private final WorkFlowActor workFlowActor;
    private final EventAuditUtil eventAuditUtil;
    private final EmailNotificationActor emailNotificationActor;
    private final WorkflowExecutionResponseActor workflowExecutionResponseActor;

    /**
     * There are 2 paths for this method to getDataFrameAuditByRunId called:
     * First:  Through terminal node (Happy case)Mark requestEntity status as COMPLETED and send email instantly
     * Second: Through poller, when the poller identifies that the DAG is failed/killed Mark requestEntity status as FAILED and send email instantly
     *
     * @param requestStatus : status of requestEntity(COMPLETED/FAILED)
     * @param azkabanExecId : azkaban exec Id
     * @param message       : message
     * @param requestId     : requestId
     * @throws EmailException, CallbackException
     */
    public void notifyExecutionStatus(RequestStatus requestStatus, Long azkabanExecId, String message, Long requestId) throws EmailException, CallbackException {
        Request request = requestActor.getRequest(requestId);
        WorkflowDetails workflowDetails = workFlowActor.getWorkflowDetailsById(request.getWorkflowId());
        Object workflowExecutionPayload = workflowExecutionResponseActor.getWorkflowExecutionPayload(request, requestStatus, workflowDetails);
        requestActor.updateRequestNotificationStatus(request.getId(), requestStatus, workflowExecutionPayload);
        emailNotificationActor.sendEmail(request, azkabanExecId, message, workflowDetails.getWorkflow(), workflowExecutionPayload, requestStatus);
    }



}
