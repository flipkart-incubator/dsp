package com.flipkart.dsp.utils;

import com.flipkart.dsp.actors.NotificationPreferencesActor;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;

import static com.flipkart.dsp.models.enums.WorkflowStateNotificationType.FAILURE;
import static com.flipkart.dsp.models.enums.WorkflowStateNotificationType.SUCCESS;
import static com.flipkart.dsp.utils.Constants.comma;
import static java.util.stream.Collectors.toMap;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailRecipientHelper {
    private final MiscConfig miscConfig;
    private final NotificationPreferencesActor notificationPreferencesActor;

    public String getEmailRecipientsForStepStateNotifications(Workflow workflow, EmailNotifications emailNotifications,
                                                              WorkflowStepStateNotificationType workflowStepStateNotificationType) {
        String emailRecipients = getEmailRecipientsFromRequestByWorkflowStepStates(emailNotifications, workflowStepStateNotificationType, workflow.getIsProd());
        if (emailRecipients.length() == 0) {
            NotificationPreference notificationPreference = notificationPreferencesActor.getNotificationPreference(workflow.getId());
            return getWorkflowEmailRecipientsByWorkflowState(notificationPreference, SUCCESS, workflow.getIsProd());
        }
        return emailRecipients;
    }

    private String getEmailRecipientsFromRequestByWorkflowStepStates(EmailNotifications emailNotifications, WorkflowStepStateNotificationType stateNotificationType, Boolean isProd) {
        if (Objects.nonNull(emailNotifications) && Objects.nonNull(emailNotifications.getWorkflowStates())) {
            List<String> workflowStates = Arrays.asList(emailNotifications.getWorkflowStates().split(comma));
            if (workflowStates.stream().anyMatch(workflowState -> workflowState.trim().equalsIgnoreCase(stateNotificationType.toString())))
                return getEmailRecipientsByWorkflowStateNotificationType(emailNotifications.getRecipients(), SUCCESS, isProd);
        }
        return "";
    }

    public String getEmailRecipientsForWorkflowStateNotifications(Workflow workflow, RequestStatus requestStatus, EmailNotifications emailNotifications) {
        String emailRecipients = getEmailRecipientsFromRequestForWorkflowState(requestStatus, emailNotifications, workflow.getIsProd());
        if (emailRecipients.length() == 0) {
            NotificationPreference notificationPreference = notificationPreferencesActor.getNotificationPreference(workflow.getId());
            if (requestStatus == RequestStatus.COMPLETED) {
                return getWorkflowEmailRecipientsByWorkflowState(notificationPreference, SUCCESS, workflow.getIsProd());
            } else if (requestStatus == RequestStatus.FAILED)
                return getWorkflowEmailRecipientsByWorkflowState(notificationPreference, FAILURE, workflow.getIsProd()); }
        return emailRecipients;
    }

    private String getEmailRecipientsFromRequestForWorkflowState(RequestStatus requestStatus, EmailNotifications emailNotifications, Boolean isProd) {
        if (Objects.nonNull(emailNotifications)) {
            if (emailNotifications.getRecipients().size() != 0) {
                if (requestStatus.equals(RequestStatus.COMPLETED))
                    return getEmailRecipientsByWorkflowStateNotificationType(emailNotifications.getRecipients(), SUCCESS, isProd);
                else if (requestStatus.equals(RequestStatus.FAILED)) {
                    return getEmailRecipientsByWorkflowStateNotificationType(emailNotifications.getRecipients(), FAILURE, isProd);
                }
            }
        }
        return "";
    }

    private String getEmailRecipientsByWorkflowStateNotificationType(Map<String, String> recipients, WorkflowStateNotificationType workflowStateNotificationType, Boolean isProd) {
        List<String> emailRecipients = new ArrayList<>();
        if (Objects.nonNull(recipients))
            emailRecipients = new ArrayList<>(recipients.keySet().stream()
                    .filter(k -> Arrays.stream(recipients.get(k).split(comma))
                            .anyMatch(preference -> preference.trim().equalsIgnoreCase(workflowStateNotificationType.toString())))
                    .collect(toMap(k -> k, v -> v)).keySet());
        if (workflowStateNotificationType.equals(FAILURE) && isProd)
            emailRecipients.add(miscConfig.getDefaultNotificationEmailId());
        return String.join(",", emailRecipients);
    }

    private String getWorkflowEmailRecipientsByWorkflowState(NotificationPreference notificationPreference, WorkflowStateNotificationType workflowStateNotificationType, Boolean isProd) {
        return getRecipientsFromNotification(notificationPreference, workflowStateNotificationType, isProd);
    }

    private String getRecipientsFromNotification(NotificationPreference notificationPreference, WorkflowStateNotificationType workflowStateNotificationType, Boolean isProd) {
        if (Objects.nonNull(notificationPreference) && Objects.nonNull(notificationPreference.getEmailNotificationPreferences())
                && Objects.nonNull(notificationPreference.getEmailNotificationPreferences().getRecipients())) {
            return getEmailRecipientsByWorkflowStateNotificationType(notificationPreference.getEmailNotificationPreferences().getRecipients(), workflowStateNotificationType, isProd);
        } else if (workflowStateNotificationType.equals(FAILURE) && isProd)
            return miscConfig.getDefaultNotificationEmailId();
        return "";
    }
}
