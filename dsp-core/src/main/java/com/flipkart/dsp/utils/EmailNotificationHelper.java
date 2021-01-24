package com.flipkart.dsp.utils;

import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.actors.WorkflowExecutionResponseActor;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.callback.WorkflowGroupExecutionResult;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.notifier.EmailNotification;
import com.flipkart.dsp.notifier.EmailNotifier;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailNotificationHelper {
    private final RequestActor requestActor;
    private final WorkFlowActor workFlowActor;
    private final EmailNotifier emailNotifier;
    private final EmailBuilderUtility emailBuilderUtility;
    private final EmailRecipientHelper emailRecipientHelper;
    private final WorkflowExecutionResponseActor workflowExecutionResponseActor;

    public void sendWorkflowStateChangeNotification(Request request, WorkflowDetails workflowDetails,
                                                    WorkflowStepStateNotificationType workflowStepStateNotificationType) throws EmailException {
        String emailRecipients = emailRecipientHelper.getEmailRecipientsForStepStateNotifications(workflowDetails.getWorkflow(),
                request.getData().getEmailNotifications(), workflowStepStateNotificationType);
        if (isNotBlank(emailRecipients)) {
            WorkflowGroupExecutionResult workflowGroupExecutionResult = null;
            if (workflowStepStateNotificationType.equals(WorkflowStepStateNotificationType.OUTPUT_INGESTION))
                workflowGroupExecutionResult = workflowExecutionResponseActor.getNewWorkflowExecutionResult(request, workflowDetails, request.getRequestStatus(), true);
            EmailNotification emailNotification = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request,
                    workflowDetails.getWorkflow().getName(), emailRecipients, workflowStepStateNotificationType, workflowGroupExecutionResult);
            emailNotifier.notify(emailNotification);
        }
    }

    public void sendPartitionStateChangeEmail(PartitionDetailsEmailNotificationRequest emailNotificationRequest) throws EmailException {
        RequestStatus requestStatus = emailNotificationRequest.getWorkflowStateNotificationType().equals(WorkflowStateNotificationType.SUCCESS)
                ? RequestStatus.COMPLETED : RequestStatus.FAILED;
        Request request = requestActor.getRequest(emailNotificationRequest.getRequestId());
        Workflow workflow = workFlowActor.getWorkFlowById(emailNotificationRequest.getWorkflowId());
        String emailRecipients = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow, requestStatus, request.getData().getEmailNotifications());
        if (isNotBlank(emailRecipients) && Objects.nonNull(emailNotificationRequest.getPartitionDetails())) {
            EmailNotification emailNotification = emailBuilderUtility.constructPartitionStateChangeEmailNotification(request,
                    workflow.getName(), emailRecipients, emailNotificationRequest);
            emailNotifier.notify(emailNotification);
        }

    }
}
