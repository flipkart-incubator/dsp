//package com.flipkart.dsp.utils;
//
//import com.flipkart.dsp.actors.RequestActor;
//import com.flipkart.dsp.actors.WorkFlowActor;
//import com.flipkart.dsp.actors.WorkflowExecutionResponseActor;
//import com.flipkart.dsp.entities.request.Request;
//import com.flipkart.dsp.entities.workflow.Workflow;
//import com.flipkart.dsp.entities.workflow.WorkflowDetails;
//import com.flipkart.dsp.models.RequestStatus;
//import com.flipkart.dsp.models.callback.WorkflowExecutionResult;
//import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
//import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
//import com.flipkart.dsp.models.misc.EmailNotifications;
//import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
//import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
//import com.flipkart.dsp.notifier.EmailNotification;
//import com.flipkart.dsp.notifier.EmailNotifier;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import java.util.HashMap;
//
//import static org.mockito.Mockito.*;
//
///**
// * +
// */
//public class EmailNotificationHelperTest {
//    @Mock private Request request;
//    @Mock private Workflow workflow;
//    @Mock private RequestActor requestActor;
//    @Mock private WorkFlowActor workFlowActor;
//    @Mock private EmailNotifier emailNotifier;
//    @Mock private WorkflowDetails workflowDetails;
//    @Mock private EmailNotification emailNotification;
//    @Mock private EmailBuilderUtility emailBuilderUtility;
//    @Mock private EmailNotifications emailNotifications;
//    @Mock private EmailRecipientHelper emailRecipientHelper;
//    @Mock private ExecuteWorkflowRequest executeWorkflowRequest;
//    @Mock private WorkflowExecutionResult workflowExecutionResult;
//    @Mock private WorkflowExecutionResponseActor workflowExecutionResponseActor;
//
//    private Long workflowId = 1L;
//    private String workflowName = "workflowName";
//    private String emailRecipients = "gupta.g@flipkart.com";
//    private EmailNotificationHelper emailNotificationHelper;
//
//    @Before
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//        this.emailNotificationHelper = spy(new EmailNotificationHelper(requestActor, workFlowActor , emailNotifier, emailBuilderUtility,
//                emailRecipientHelper, workflowExecutionResponseActor));
//    }
//
//    @Test
//    public void testSendWorkflowStateChangeNotification() throws Exception {
//        when(workflowDetails.getWorkflow()).thenReturn(workflow);
//        when(workflow.getId()).thenReturn(workflowId);
//        when(workflow.getName()).thenReturn(workflowName);
//        when(request.getData()).thenReturn(executeWorkflowRequest);
//        when(executeWorkflowRequest.getEmailNotifications()).thenReturn(emailNotifications);
//        when(emailRecipientHelper.getEmailRecipientsForStepStateNotifications(workflowId, emailNotifications,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION)).thenReturn(emailRecipients);
//        when(workflowExecutionResponseActor.getNewWorkflowGroupExecutionResult(request, workflowDetails, true)).thenReturn(workflowExecutionResult);
//        when(emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, emailRecipients,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION, workflowExecutionResult)).thenReturn(emailNotification);
//        when(emailNotifier.notify(emailNotification)).thenReturn(true);
//
//        emailNotificationHelper.sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);
//        verify(workflowDetails, times(2)).getWorkflow();
//        verify(workflow).getId();
//        verify(request).getData();
//        verify(executeWorkflowRequest).getEmailNotifications();
//        verify(emailRecipientHelper).getEmailRecipientsForStepStateNotifications(workflowId, emailNotifications,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION);
//        verify(workflow).getName();
//        verify(workflowExecutionResponseActor).getNewWorkflowGroupExecutionResult(request, workflowDetails, true);
//        verify(emailBuilderUtility).constructWorkflowStateChangeEmailNotification(request, workflowName, emailRecipients,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION, workflowExecutionResult);
//        verify(emailNotifier).notify(emailNotification);
//    }
//
//    @Test
//    public void testSendPartitionStateChangeEmail() throws Exception {
//        Long requestId = 1L;
//        PartitionDetailsEmailNotificationRequest emailNotificationRequest = PartitionDetailsEmailNotificationRequest.builder()
//                .workflowId(workflowId).workflowStateNotificationType(WorkflowStateNotificationType.SUCCESS)
//                .requestId(requestId).partitionDetails(new HashMap<>()).build();
//
//        when(requestActor.getRequest(requestId)).thenReturn(request);
//        when(workFlowActor.getWorkFlowById(workflowId)).thenReturn(workflow);
//        when(request.getData()).thenReturn(executeWorkflowRequest);
//        when(executeWorkflowRequest.getEmailNotifications()).thenReturn(emailNotifications);
//        when(emailRecipientHelper.getEmailRecipientsForWorkflowStateNotifications(workflowId, RequestStatus.COMPLETED,
//                emailNotifications)).thenReturn(emailRecipients);
//        when(workflow.getName()).thenReturn(workflowName);
//        when(emailBuilderUtility.constructPartitionStateChangeEmailNotification(request, workflowName, emailRecipients,
//                emailNotificationRequest)).thenReturn(emailNotification);
//        when(emailNotifier.notify(emailNotification)).thenReturn(true);
//
//        emailNotificationHelper.sendPartitionStateChangeEmail(emailNotificationRequest);
//        verify(requestActor).getRequest(requestId);
//        verify(workFlowActor).getWorkFlowById(workflowId);
//        verify(request).getData();
//        verify(executeWorkflowRequest).getEmailNotifications();
//        verify(emailRecipientHelper).getEmailRecipientsForWorkflowStateNotifications(workflowId, RequestStatus.COMPLETED,
//                emailNotifications);
//        verify(workflow).getName();
//        verify(emailBuilderUtility).constructPartitionStateChangeEmailNotification(request, workflowName, emailRecipients,
//                emailNotificationRequest);
//        verify(emailNotifier).notify(emailNotification);
//    }
//}
