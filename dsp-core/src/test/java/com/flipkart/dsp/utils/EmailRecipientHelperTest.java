package com.flipkart.dsp.utils;

import com.flipkart.dsp.actors.NotificationPreferencesActor;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.models.misc.EmailNotifications;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class EmailRecipientHelperTest {

    @Mock private Workflow workflow;
    @Mock private MiscConfig miscConfig;
    @Mock private NotificationPreference notificationPreference;
    @Mock private NotificationPreferencesActor notificationPreferencesActor;

    private Long workflowId = 1L;
    private Boolean isProd = true;
    private EmailNotifications emailNotifications;
    private String workflowStates = "STARTED, SG";
    private EmailRecipientHelper emailRecipientHelper;
    private String emailRecipient = "gupta.g@flipkart.com";
    private String notificationRecipient = "dsp-notifications@flipkart.com";
    private Map<String, String> recipients = new HashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.emailRecipientHelper = spy(new EmailRecipientHelper(miscConfig, notificationPreferencesActor));
        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getIsProd()).thenReturn(isProd);
    }

    // RequestEntity EmailNotification = null, NotificationPreferencesEntity = null, empty recipients
    @Test
    public void testGetEmailRecipientsForStepStateNotificationsCase1() {
        when(notificationPreferencesActor.getNotificationPreference(workflow.getId())).thenReturn(null);
        String expected = emailRecipientHelper.getEmailRecipientsForStepStateNotifications(workflow,
                null, WorkflowStepStateNotificationType.STARTED);
        assertEquals(expected.length(), 0);
        verify(notificationPreferencesActor).getNotificationPreference(workflow.getId());
    }

    // RequestEntity EmailNotification != null requestStatus = SUCCESS
    @Test
    public void testGetEmailRecipientsForStepStateNotificationsCase2() {
        recipients.put(emailRecipient, "SUCCESS");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();

        String expected = emailRecipientHelper.getEmailRecipientsForStepStateNotifications(workflow,
                emailNotifications, WorkflowStepStateNotificationType.STARTED);
        assertEquals(expected, emailRecipient);
    }

    // RequestEntity EmailNotification = null, NotificationPreferencesEntity != null,
    @Test
    public void testGetEmailRecipientsForStepStateNotificationsCase4() {
        recipients.put(emailRecipient, "SUCCESS");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();
        when(notificationPreferencesActor.getNotificationPreference(workflow.getId())).thenReturn(notificationPreference);
        when(notificationPreference.getEmailNotificationPreferences()).thenReturn(emailNotifications);

        String expected = emailRecipientHelper.getEmailRecipientsForStepStateNotifications(workflow,
                null, WorkflowStepStateNotificationType.STARTED);
        assertEquals(expected, emailRecipient);
        verify(notificationPreferencesActor).getNotificationPreference(workflow.getId());
        verify(notificationPreference, times(3)).getEmailNotificationPreferences();
    }

    // RequestEntity EmailNotification = null, RequestStatus = COMPLETED
    @Test
    public void testGetEmailRecipientsForWorkflowStateNotificationsCase1() {
        recipients.put(emailRecipient, "SUCCESS");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();
        when(notificationPreferencesActor.getNotificationPreference(workflow.getId())).thenReturn(notificationPreference);
        when(notificationPreference.getEmailNotificationPreferences()).thenReturn(emailNotifications);

        String expected = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow,
                RequestStatus.COMPLETED, null);
        assertEquals(expected, emailRecipient);
        verify(notificationPreferencesActor).getNotificationPreference(workflow.getId());
        verify(notificationPreference, times(3)).getEmailNotificationPreferences();
    }

    // RequestEntity EmailNotification = null, RequestStatus = FAILED
    @Test
    public void testGetEmailRecipientsForWorkflowStateNotificationsCase2() {
        recipients.put(emailRecipient, "SUCCESS");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();
        when(notificationPreferencesActor.getNotificationPreference(workflow.getId())).thenReturn(notificationPreference);
        when(notificationPreference.getEmailNotificationPreferences()).thenReturn(emailNotifications);

        String expected = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow,
                RequestStatus.FAILED, null);
        verify(notificationPreferencesActor).getNotificationPreference(workflow.getId());
        verify(notificationPreference, times(3)).getEmailNotificationPreferences();
    }

    // RequestEntity EmailNotification != null, notification_preference = null, RequestStatus = COMPLETED
    @Test
    public void testGetEmailRecipientsForWorkflowStateNotificationsCase3() {
        recipients.put(emailRecipient, "SUCCESS");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();

        String expected = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow,
                RequestStatus.COMPLETED, emailNotifications);
        assertEquals(expected, emailRecipient);
    }

    // RequestEntity EmailNotification = null, notification_preference = null, recipients != null,  RequestStatus = FAILED
    @Test
    public void testGetEmailRecipientsForWorkflowStateNotificationsCase4() {
        recipients.put(emailRecipient, "SUCCESS, FAILURE");
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();
        when(miscConfig.getDefaultNotificationEmailId()).thenReturn(notificationRecipient);

        String expected = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow,
                RequestStatus.FAILED, emailNotifications);
        assertEquals(expected.split(",")[0], emailRecipient);
        assertEquals(expected.split(",")[1], notificationRecipient);
        verify(miscConfig).getDefaultNotificationEmailId();
    }

    // RequestEntity EmailNotification = null, notification_preference = null, recipients = null,  RequestStatus = FAILED
    @Test
    public void testGetEmailRecipientsForWorkflowStateNotificationsCase5() {
        emailNotifications = EmailNotifications.builder().recipients(recipients).workflowStates(workflowStates).build();
        when(miscConfig.getDefaultNotificationEmailId()).thenReturn(notificationRecipient);

        String expected = emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflow,
                RequestStatus.FAILED, emailNotifications);
        assertEquals(expected.split(",")[0], notificationRecipient);
        verify(miscConfig).getDefaultNotificationEmailId();
    }

}
