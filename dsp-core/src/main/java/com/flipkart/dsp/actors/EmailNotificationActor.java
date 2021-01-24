package com.flipkart.dsp.actors;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.notifier.EmailNotification;
import com.flipkart.dsp.notifier.EmailNotifier;
import com.flipkart.dsp.utils.EmailBuilderUtility;
import com.flipkart.dsp.utils.EmailRecipientHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import javax.inject.Inject;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailNotificationActor {
    private final EmailNotifier emailNotifier;
    private final EmailBuilderUtility emailBuilderUtility;
    private final EmailRecipientHelper emailRecipientHelper;

    void sendEmail(Request request, Long azkabanExecId, String message, Workflow workflow,
                   Object workflowExecutionPayload, RequestStatus requestStatus) throws EmailException {
        String emailRecipients = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow, requestStatus,
                request.getData().getEmailNotifications());
        if (isNotBlank(emailRecipients)) {
            EmailNotification emailNotification = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request,
                    workflowExecutionPayload, azkabanExecId, message, workflow.getName(), emailRecipients, requestStatus);
            emailNotifier.notify(emailNotification);
        }
    }
}
