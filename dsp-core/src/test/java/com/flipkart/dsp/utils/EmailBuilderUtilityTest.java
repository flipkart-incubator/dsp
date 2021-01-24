//package com.flipkart.dsp.utils;
//
//import com.flipkart.dsp.actors.EventAuditActor;
//import com.flipkart.dsp.actors.PipelineStepAuditActor;
//import com.flipkart.dsp.actors.RequestStepAuditActor;
//import com.flipkart.dsp.config.IPPDSPConfiguration;
//import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
//import com.flipkart.dsp.entities.enums.RequestStepType;
//import com.flipkart.dsp.entities.misc.WhereClause;
//import com.flipkart.dsp.entities.pipeline_step.PipelineStepAudit;
//import com.flipkart.dsp.entities.request.Request;
//import com.flipkart.dsp.entities.request.RequestStepAudit;
//import com.flipkart.dsp.entities.workflow.DSPWorkflowExecutionResult;
//import com.flipkart.dsp.models.EventLevel;
//import com.flipkart.dsp.models.EventType;
//import com.flipkart.dsp.models.PipelineStepStatus;
//import com.flipkart.dsp.models.RequestStatus;
//import com.flipkart.dsp.models.callback.*;
//import com.flipkart.dsp.models.enums.WorkflowStateNotificationType;
//import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
//import com.flipkart.dsp.models.event_audits.EventAudit;
//import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.CephIngestionEndInfoEvent;
//import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.CephIngestionErrorEvent;
//import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.fdp_Ingestion_node.FDPIngestionEndInfoEvent;
//import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.fdp_Ingestion_node.FDPIngestionErrorEvent;
//import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
//import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
//import com.flipkart.dsp.models.outputVariable.FDPEntityOutputLocation;
//import com.flipkart.dsp.notifier.EmailNotification;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.io.IOException;
//import java.net.URL;
//import java.util.*;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.mock;
//import static org.powermock.api.mockito.PowerMockito.verifyStatic;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({AzkabanLinkCreator.class})
//public class EmailBuilderUtilityTest {
//    @Mock private Request request;
//    @Mock private EventAuditActor eventAuditActor;
//    @Mock private PipelineStepAudit pipelineStepAudit;
//    @Mock private RequestStepAuditActor requestStepAuditActor;
//    @Mock private PipelineStepAuditActor pipelineStepAuditActor;
//    @Mock private IPPDSPConfiguration.AzkabanConfig azkabanConfig;
//    @Mock private IPPDSPConfiguration.DSPClientConfig dspClientConfig;
//    @Mock private IPPDSPConfiguration.EmailNotificationConfig emailNotificationConfig;
//
//    private URL url;
//    private Long requestId = 1L;
//    private Long requestStep1 = 1L;
//    private Long requestStep2 = 2L;
//    private Long azkabanExecId = 1L;
//    private Long workflowId = 1L;
//    private Object payload = "payload";
//    private String message = " is failed";
//    private String workflowName = "workflowName";
//    private String partitionName = "partitionName";
//    private String partitionValue = "partitionValue";
//    private String recipients = "gupta.g@flipkart.com";
//
//    private List<URL> urls = new ArrayList<>();
//    private EmailBuilderUtility emailBuilderUtility;
//    private List<WhereClause> whereClauses = new ArrayList<>();
//    private List<RequestStepAudit> requestStepAudits = new ArrayList<>();
//    private List<EventAudit> fdpIngestionErrorAudits = new ArrayList<>();
//    private List<EventAudit> cephIngestionErrorAudits = new ArrayList<>();
//    private List<PipelineStepAudit> pipelineStepAudits = new ArrayList<>();
//    private List<EventAudit> fdpIngestionEndInfoAudits = new ArrayList<>();
//    private List<EventAudit> cephIngestionEndInfoAudits = new ArrayList<>();
//
//    @Before
//    public void setUp() {
//        url = mock(URL.class);
//        MockitoAnnotations.initMocks(this);
//        PowerMockito.mockStatic(AzkabanLinkCreator.class);
//        this.emailBuilderUtility = spy(new EmailBuilderUtility(eventAuditActor, requestStepAuditActor, pipelineStepAuditActor,
//                azkabanConfig, dspClientConfig, emailNotificationConfig));
//
//        Set<String> values = new HashSet<>();
//        values.add(partitionValue);
//        urls.add(url);
//
//        StringBuilder azkabanLink = new StringBuilder("azkabanLink");
//        CephOutputLocation cephOutputLocation = CephOutputLocation.builder().bucket("ceph_bucket").path("path").clientAlias("ceph_alias").build();
//        String fdpEntityName = "fdpEntityName";
//        FDPEntityOutputLocation fdpEntityOutputLocation = FDPEntityOutputLocation.builder().fdpEntityName(fdpEntityName).build();
//        WhereClause whereClause = new WhereClause(partitionName, WhereClause.WhereType.IN, values, null, null);
//        FDPIngestionErrorEvent fdpIngestionErrorEvent = FDPIngestionErrorEvent.builder().fdpEntityOutputLocation(fdpEntityOutputLocation).build();
//        CephIngestionErrorEvent cephIngestionErrorEvent = CephIngestionErrorEvent.builder().cephOutputLocation(cephOutputLocation).build();
//        FDPIngestionEndInfoEvent fdpIngestionEndInfoEvent = FDPIngestionEndInfoEvent.builder().fdpEntityOutputLocation(fdpEntityOutputLocation).build();
//        CephIngestionEndInfoEvent cephIngestionEndInfoEvent = CephIngestionEndInfoEvent.builder().urls(urls).build();
//
//        pipelineStepAudits.add(pipelineStepAudit);
//        whereClauses.add(whereClause);
//        fdpIngestionErrorAudits.add(EventAudit.builder().payload(fdpIngestionErrorEvent).build());
//        fdpIngestionErrorAudits.add(EventAudit.builder().payload(fdpIngestionErrorEvent).build());
//        cephIngestionErrorAudits.add(EventAudit.builder().payload(cephIngestionErrorEvent).build());
//        cephIngestionErrorAudits.add(EventAudit.builder().payload(cephIngestionErrorEvent).build());
//
//        fdpIngestionEndInfoAudits.add(EventAudit.builder().payload(fdpIngestionEndInfoEvent).build());
//        fdpIngestionEndInfoAudits.add(EventAudit.builder().payload(fdpIngestionEndInfoEvent).build());
//        cephIngestionEndInfoAudits.add(EventAudit.builder().payload(cephIngestionEndInfoEvent).build());
//        cephIngestionEndInfoAudits.add(EventAudit.builder().payload(cephIngestionEndInfoEvent).build());
//
//        when(request.getId()).thenReturn(requestId);
//        when(pipelineStepAudit.getPipelineStepStatus()).thenReturn(PipelineStepStatus.FAILED);
//        when(requestStepAuditActor.getRequestStepAudits(requestId)).thenReturn(requestStepAudits);
//        when(emailNotificationConfig.getDefaultNotificationEmailId()).thenReturn("dsp-notification@flipkart.com");
//        PowerMockito.when(AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId)).thenReturn(azkabanLink);
//    }
//
//    // OTS/SG/Terminal failure
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationFailureCase1() {
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep1).requestStepType(RequestStepType.OTS)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.FAILED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is failed");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(4)).getId();
//        verify(requestStepAuditActor).getRequestStepAudits(requestId);
//    }
//
//    // WorkflowNode failure
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationFailureCase2() throws Exception {
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep1).requestStepType(RequestStepType.OTS)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep1).requestStepType(RequestStepType.OTS)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep2).requestStepType(RequestStepType.WF)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//
//        when(pipelineStepAuditActor.getPipelineStepAudits(requestId)).thenReturn(pipelineStepAudits);
//        when(pipelineStepAudit.getPipelineStepStatus()).thenReturn(PipelineStepStatus.SUCCESS);
//        when(pipelineStepAudit.getScope()).thenReturn(JsonUtils.DEFAULT.mapper.writeValueAsString(whereClauses));
//        Long pipelineStepAuditId = 1L;
//        when(pipelineStepAudit.getId()).thenReturn(pipelineStepAuditId);
//        when(dspClientConfig.getHost()).thenReturn("dspHost");
//        when(dspClientConfig.getPort()).thenReturn(8080);
//
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.FAILED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is failed");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(4)).getId();
//        verify(requestStepAuditActor).getRequestStepAudits(requestId);
////        verify(pipelineStepAudit).getId();
////        verify(dspClientConfig).getHost();
////        verify(dspClientConfig).getPort();
//    }
//
//    // WorkflowNode failure  IOException(While Generating email body
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationFailureCase3() throws Exception {
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep2).requestStepType(RequestStepType.WF)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//
//        when(pipelineStepAuditActor.getPipelineStepAudits(requestId)).thenReturn(pipelineStepAudits);
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.FAILED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is failed");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(3)).getId();
//        verify(requestStepAuditActor).getRequestStepAudits(requestId);
//        verify(pipelineStepAuditActor).getPipelineStepAudits(requestId);
//    }
//
//    // OutputIngestion failure
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationFailureCase4() {
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep1).requestStepType(RequestStepType.OI)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//        when(eventAuditActor.getEvents(requestId, EventLevel.ERROR, EventType.FDPIngestion)).thenReturn(fdpIngestionErrorAudits);
//        when(eventAuditActor.getEvents(requestId, EventLevel.ERROR, EventType.CephIngestion)).thenReturn(cephIngestionErrorAudits);
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.FAILED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is failed");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(4)).getId();
//        verify(requestStepAuditActor).getRequestStepAudits(requestId);
//    }
//
//    // OutputIngestion failure Partial Failure
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationFailureCase5() {
//        requestStepAudits.add(RequestStepAudit.builder().requestStepId(requestStep1).requestStepType(RequestStepType.OI)
//                .requestStepAuditStatus(RequestStepAuditStatus.FAILED).build());
//        when(eventAuditActor.getEvents(requestId, EventLevel.ERROR, EventType.FDPIngestion)).thenReturn(fdpIngestionErrorAudits);
//        when(eventAuditActor.getEvents(requestId, EventLevel.ERROR, EventType.CephIngestion)).thenReturn(cephIngestionErrorAudits);
//        when(eventAuditActor.getEvents(requestId, EventLevel.INFO, EventType.FDPIngestion)).thenReturn(fdpIngestionEndInfoAudits);
//        when(eventAuditActor.getEvents(requestId, EventLevel.INFO, EventType.CephIngestion)).thenReturn(cephIngestionEndInfoAudits);
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.FAILED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is failed");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(4)).getId();
//        verify(requestStepAuditActor).getRequestStepAudits(requestId);
//    }
//
//
//    // Request Success
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationSuccessCase1() {
//        message = " is successful";
//        Map<String, Long> partitionOverrides = new HashMap<>();
//        partitionOverrides.put("partitionName", 1L);
//        payload = DSPWorkflowExecutionResult.builder().partitionOverrides(partitionOverrides).build();
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.COMPLETED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is successful");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//    }
//
//    // Request Success
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationSuccessCase2() throws Exception {
//        message = " is successful";
//        Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap = getScriptExecutionResultMap();
//        payload = WorkflowExecutionResult.builder().scriptExecutionResultMap(scriptExecutionResultMap).build();
//
//        EmailNotification expected = emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, payload,
//                azkabanExecId, message, workflowName, recipients, RequestStatus.COMPLETED);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Request: 1 for workflow: workflowName is successful");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//    }
//
//    private Map<String, WorkflowExecutionResult> getWorkflowExecutionResultMap() {
//        Map<String, WorkflowExecutionResult> workflowExecutionResultMap = new HashMap<>();
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//        workflowExecutionResultMap.put(workflowName, workflowExecutionResult);
//        return workflowExecutionResultMap;
//    }
//
//    private Map<String, List<ScriptExecutionResult>> getScriptExecutionResultMap() {
//        Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap = new HashMap<>();
//        List<ScriptExecutionResult> scriptExecutionResults = new ArrayList<>();
//        DDPScriptExecutionResult ddpScriptExecutionResult = new DDPScriptExecutionResult("db","table", 1L);
//        HiveScriptExecutionResult hiveScriptExecutionResult = new HiveScriptExecutionResult("db", "table", 1L);
//        FDPScriptExecutionResult fdpScriptExecutionResult = new FDPScriptExecutionResult("fdp_entity", "2.1", 1L);
//        CephScriptExecutionResult cephScriptExecutionResult = new CephScriptExecutionResult("bucket", "path", urls);
//        HDFSScriptExecutionResult hdfsScriptExecutionResult = new HDFSScriptExecutionResult("/hdfs-path");
//        scriptExecutionResults.add(ddpScriptExecutionResult);
//        scriptExecutionResults.add(hiveScriptExecutionResult);
//        scriptExecutionResults.add(cephScriptExecutionResult);
//        scriptExecutionResults.add(fdpScriptExecutionResult);
//        scriptExecutionResults.add(hdfsScriptExecutionResult);
//        String dataFrameName = "dataFrameName";
//        scriptExecutionResultMap.put(dataFrameName, scriptExecutionResults);
//        return scriptExecutionResultMap;
//    }
//
//    // WorkflowEntity state = STARTED
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationStarted() throws Exception {
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.STARTED, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Execution of Request: 1 for workflow: workflowName has started.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//    // WorkflowEntity State = SG
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationSGSuccessful() throws Exception {
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.SG, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "SG of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//    // WorkflowEntity State = WORKFLOW  IOException(While Generating email body
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationWorkflowSuccessfulCase2() throws Exception {
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        when(pipelineStepAuditActor.getPipelineStepAudits(requestId)).thenReturn(pipelineStepAudits);
//
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.WORKFLOW, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Script execution of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(2)).getId();
//        verify(request).getAzkabanExecId();
//        verify(pipelineStepAuditActor).getPipelineStepAudits(requestId);
//    }
//
//    // WorkflowEntity State = WORKFLOW
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationWorkflowSuccessful() throws Exception {
//        Map<String, WorkflowExecutionResult> workflowExecutionResultMap = getWorkflowExecutionResultMap();
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        when(pipelineStepAuditActor.getPipelineStepAudits(requestId)).thenReturn(pipelineStepAudits);
//        when(pipelineStepAudit.getPipelineStepStatus()).thenReturn(PipelineStepStatus.FAILED);
//
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.WORKFLOW, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Script execution of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request, times(2)).getId();
//        verify(request).getAzkabanExecId();
//        verify(pipelineStepAuditActor).getPipelineStepAudits(requestId);
//        verify(pipelineStepAudit).getPipelineStepStatus();
//    }
//
//
//    // WorkflowEntity State = OUTPUT_INGESTION
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationOutputIngestionSuccessfulCase1() throws Exception {
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(getScriptExecutionResultMap()).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Output ingestion of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//    // WorkflowEntity State = OUTPUT_INGESTION, Empty Execution Details
//    @Test
//    public void testConstructWorkflowStateChangeEmailNotificationOutputIngestionSuccessfulCase2() throws Exception {
//        Map<String, List<ScriptExecutionResult>> scriptExecutionResultMap = new HashMap<>();
//        WorkflowExecutionResult workflowExecutionResult = WorkflowExecutionResult.builder()
//                .scriptExecutionResultMap(scriptExecutionResultMap).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//        EmailNotification expected =  emailBuilderUtility.constructWorkflowStateChangeEmailNotification(request, workflowName, recipients,
//                WorkflowStepStateNotificationType.OUTPUT_INGESTION, workflowExecutionResult);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Output ingestion of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//
//    @Test
//    public void testConstructPartitionStateChangeEmailNotificationCase1() {
//        Map<String, Object> partitionDetails = new HashMap<>();
//        partitionDetails.put(partitionName, partitionValue);
//        PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest = PartitionDetailsEmailNotificationRequest.builder()
//                .requestId(requestId).workflowId(workflowId).logs("logs")
//                .workflowStateNotificationType(WorkflowStateNotificationType.SUCCESS)
//                .partitionDetails(partitionDetails).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//
//        EmailNotification expected = emailBuilderUtility.constructPartitionStateChangeEmailNotification(request, workflowName,
//                recipients, partitionDetailsEmailNotificationRequest);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Script execution for partition granularity : [partitionName=partitionValue] of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//    //default_partition
//    @Test
//    public void testConstructPartitionStateChangeEmailNotificationCase2() {
//        Map<String, Object> partitionDetails = new HashMap<>();
//        PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest = PartitionDetailsEmailNotificationRequest.builder()
//                .requestId(requestId).workflowId(workflowId).logs("logs")
//                .workflowStateNotificationType(WorkflowStateNotificationType.SUCCESS)
//                .partitionDetails(partitionDetails).build();
//
//        when(request.getAzkabanExecId()).thenReturn(azkabanExecId);
//
//        EmailNotification expected = emailBuilderUtility.constructPartitionStateChangeEmailNotification(request, workflowName,
//                recipients, partitionDetailsEmailNotificationRequest);
//        assertNotNull(expected);
//        assertEquals(expected.getSubject(), "Script execution for partition granularity : [default_partition] of Request: 1 for workflow: workflowName is successfully completed.");
//        assertEquals(expected.getTo()[0], recipients);
//        verifyStatic(AzkabanLinkCreator.class);
//        AzkabanLinkCreator.createAzkabanLink(azkabanConfig, azkabanExecId);
//        verify(request).getId();
//        verify(request).getAzkabanExecId();
//    }
//
//}
