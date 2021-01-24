//package com.flipkart.dsp.executor.application;
//
//import com.flipkart.dsp.client.DSPServiceClient;
//import com.flipkart.dsp.config.IPPDSPConfiguration;
//import com.flipkart.dsp.cosmos.CosmosReporter;
//import com.flipkart.dsp.dto.QueueInfoDTO;
//import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
//import com.flipkart.dsp.entities.misc.ConfigPayload;
//import com.flipkart.dsp.entities.misc.NotificationPreference;
//import com.flipkart.dsp.entities.misc.WhereClause;
//import com.flipkart.dsp.entities.pipeline_step.PipelineStep;
//import com.flipkart.dsp.entities.pipeline_step.PipelineStepAudit;
//import com.flipkart.dsp.entities.requestEntity.Request;
//import com.flipkart.dsp.entities.run.config.RunConfig;
//import com.flipkart.dsp.entities.scriptEntity.LocalScript;
//import com.flipkart.dsp.entities.scriptEntity.Script;
//import com.flipkart.dsp.entities.workflowEntity.Workflow;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowDetails;
//import com.flipkart.dsp.executor.cosmos.MesosCosmosTag;
//import com.flipkart.dsp.executor.exception.ApplicationException;
//import com.flipkart.dsp.executor.orchestrator.WorkFlowOrchestrator;
//import com.flipkart.dsp.executor.utils.AuditHelper;
//import com.flipkart.dsp.executor.utils.ExecutorLogManager;
//import com.flipkart.dsp.executor.utils.ScriptHelper;
//import com.flipkart.dsp.models.ObjectOverride;
//import com.flipkart.dsp.models.RequestOverride;
//import com.flipkart.dsp.models.WorkflowGroupExecuteRequest;
//import com.flipkart.dsp.models.misc.EmailNotifications;
//import com.flipkart.dsp.qe.clients.HiveClient;
//import com.flipkart.dsp.utils.JsonUtils;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.util.*;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.verifyStatic;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({MesosCosmosTag.class, AuditHelper.class})
//public class WorkflowMesosApplicationTest {
//
//    private @Mock Script scriptEntity;
//    private @Mock Request requestEntity;
//    private @Mock Workflow workflowEntity;
//    private @Mock RunConfig runConfig;
//    private @Mock HiveClient hiveClient;
//    private @Mock AuditHelper auditHelper;
//    private @Mock LocalScript localScript;
//    private @Mock ScriptHelper scriptHelper;
//    private @Mock PipelineStep pipelineStep;
//    private @Mock QueueInfoDTO queueInfoDTO;
//    private @Mock CosmosReporter cosmosReporter;
//    private @Mock RequestOverride requestOverride;
//    private @Mock WorkflowDetails workflowDetails;
//    private @Mock DSPServiceClient dspServiceClient;
//    private @Mock PipelineStepAudit pipelineStepAudit;
//    private @Mock ExecutorLogManager executorLogManager;
//    private @Mock IPPDSPConfiguration ippdspConfiguration;
//    private @Mock WorkFlowOrchestrator workFlowOrchestrator;
//    private @Mock NotificationPreference notificationPreference;
//    private @Mock IPPDSPConfiguration.DSPClientConfig dspClientConfig;
//    private @Mock WorkflowGroupExecuteRequest workflowGroupExecuteRequest;
//
//
//    private String attempt = "1";
//    private Long refreshId = 1L;
//    private Long pipelineId = 1L;
//    private Long pipelineStepId = 1L;
//    private Long workflowId = 1L;
//    private String hiveQueue = "hiveQueue";
//    private String clusterRole = "clusterRole";
//    private String workflowName = "workflowName";
//    private String pipelineExecutionId = "pipelineExecutionId";
//    private List<WhereClause> whereClauses = new ArrayList<>();
//    private Map<String, String> partitionValues = new HashMap<>();
//    private Map<Long, Integer> logAttemptMapping = new HashMap<>();
//    private List<ObjectOverride> objectOverrides = new ArrayList<>();
//    private List<WorkflowDetails> workflowDetailsList = new ArrayList<>();
//    private List<PipelineStepAudit> pipelineStepAudits = new ArrayList<>();
//    private Map<String, RequestOverride> requestOverrideMap = new HashMap<>();
//    private Map<String, List<Map<String, Object>>> partitionDetailsMap = new HashMap<>();
//
//    private ConfigPayload configPayload;
//    private EmailNotifications emailNotifications;
//    private WorkFlowMesosApplication workFlowMesosApplication;
//
//    @Before
//    public void setUp() throws Exception {
//        MockitoAnnotations.initMocks(this);
//        PowerMockito.mockStatic(AuditHelper.class);
//        PowerMockito.mockStatic(MesosCosmosTag.class);
//        this.workFlowMesosApplication = new WorkFlowMesosApplication(hiveClient, auditHelper, scriptHelper, cosmosReporter,
//                dspServiceClient, executorLogManager, ippdspConfiguration, workFlowOrchestrator);
//
//        Set<String> values = new HashSet<>();
//        Map<String, Object> partitionDetails = new HashMap<>();
//        List<Map<String, Object>> partitionDetailsList = new ArrayList<>();
//
//        Long workflowId = 1L;
//        String partitionValue = "Iron";
//        String partitionName = "partitionName";
//        String pipelineStepName = "pipelineStepName";
//
//        WhereClause whereClause = new WhereClause(partitionName, WhereClause.WhereType.IN, values, null , null);
//
//        values.add(partitionValue);
//        whereClauses.add(whereClause);
//        workflowDetailsList.add(workflowDetails);
//        pipelineStepAudits.add(pipelineStepAudit);
//        partitionValues.put(partitionName, partitionValue);
//        requestOverrideMap.put(workflowName, requestOverride);
//
//        partitionDetailsList.add(partitionDetails);
//        partitionDetails.put(partitionName, partitionValue);
//        partitionDetailsMap.put(pipelineStepName, partitionDetailsList);
//
//        configPayload = ConfigPayload.builder().workflowId(workflowId).pipelineId(pipelineId).pipelineStepId(pipelineStepId)
//                .refreshId(refreshId).partitionValues(partitionValues).pipelineExecutionId(pipelineExecutionId)
//                .scope(JsonUtils.DEFAULT.mapper.writeValueAsString(whereClauses)).build();
//        emailNotifications = EmailNotifications.builder().partitionDetails(partitionDetailsMap).build();
//
//        when(dspServiceClient.getRequest(refreshId)).thenReturn(requestEntity);
//        when(dspServiceClient.getQueueInfo(workflowId)).thenReturn(queueInfoDTO);
//        when(dspServiceClient.getWorkflowGroupId(refreshId)).thenReturn(workflowId);
//        when(dspServiceClient.getWorkflowDetails(any())).thenReturn(workflowDetailsList);
//        when(dspServiceClient.getPipelineStepById(pipelineStepId)).thenReturn(pipelineStep);
//        doNothing().when(dspServiceClient).sendEmailNotificationForPartitionStateChangeRequest(any());
//        when(dspServiceClient.getPipelineStepLogDetails(Integer.valueOf(attempt) - 1, pipelineStepId, pipelineExecutionId, refreshId)).thenReturn(pipelineStepAudits);
//
//        doNothing().when(auditHelper).savePipelineStepRuntimeConfig(configPayload, pipelineStep, runConfig);
//        when(auditHelper.savePipelineAuditRequest(any(), any(), any(), any(), any(), any(), any())).thenReturn(1L);
//        doNothing().when(auditHelper).createWFContainerStartInfoEvent(any(), any(), any(), any(), any(), any(), any());
//        when(auditHelper.getLogAttemptMapping(pipelineStepId, pipelineExecutionId, refreshId)).thenReturn(logAttemptMapping);
//        doNothing().when(auditHelper).createWFContainerStartedDebugEvent(any(), any(), any(), any(), any(), any(), any(), any(), any());
//        doNothing().when(auditHelper).createWFContainerCompletedInfoEvent(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
//
//        doNothing().when(hiveClient).setQueue(hiveQueue);
//        when(dspClientConfig.getPort()).thenReturn(8080);
//        when(workflowEntity.getName()).thenReturn(workflowName);
//        when(pipelineStep.getScript()).thenReturn(scriptEntity);
//        when(dspClientConfig.getHost()).thenReturn("host");
//        when(pipelineStepAudit.getLogs()).thenReturn("logs");
//        when(pipelineStep.getId()).thenReturn(pipelineStepId);
//        when(queueInfoDTO.getHiveQueue()).thenReturn(hiveQueue);
//        when(workflowDetails.getWorkflow()).thenReturn(workflowEntity);
//        when(pipelineStep.getName()).thenReturn(pipelineStepName);
//        when(requestEntity.getData()).thenReturn(workflowGroupExecuteRequest);
//        when(scriptHelper.importScript(scriptEntity)).thenReturn(localScript);
//        when(requestOverride.getObjectOverrideList()).thenReturn(objectOverrides);
//        when(ippdspConfiguration.getDspClientConfig()).thenReturn(dspClientConfig);
//        when(workflowGroupExecuteRequest.getOverrides()).thenReturn(requestOverrideMap);
//        when(executorLogManager.getMesosLogURL(any(), any(), any(), any())).thenReturn("logs");
//        when(workflowGroupExecuteRequest.getEmailNotifications()).thenReturn(emailNotifications);
//        when(workFlowOrchestrator.run(workflowEntity, localScript, configPayload, pipelineStep, objectOverrides)).thenReturn(runConfig);
//        PowerMockito.when(AuditHelper.getPipelineStepFrom(workflowDetails, pipelineId, pipelineStepId)).thenReturn(pipelineStep);
//        PowerMockito.doNothing().when(MesosCosmosTag.class, "populateValue", workflowName, pipelineStepId, refreshId, partitionValues.values().toString(), clusterRole);
//    }
//
//    @Test
//    public void testGetName() {
//        assertEquals(workFlowMesosApplication.getName(), "WorkFlowMesosApplication");
//    }
//
//    @Test
//    public void testExecuteSuccessCase1() throws Exception {
//        String[] payload = {JsonUtils.DEFAULT.mapper.writeValueAsString(configPayload), "frameworkId",
//                "slaveId", "hostIP", pipelineExecutionId, "1", "10", clusterRole, attempt, "0"};
//
//        workFlowMesosApplication.execute(payload);
//        verify(dspServiceClient).getWorkflowDetails(any());
//        verify(workflowDetails, times(4)).getWorkflow();
//        verify(workflowEntity, times(5)).getName();
//        verifyStatic(MesosCosmosTag.class);
//        MesosCosmosTag.populateValue(workflowName, pipelineStepId, refreshId, partitionValues.values().toString(), clusterRole);
//        verifyStatic(AuditHelper.class);
//        AuditHelper.getPipelineStepFrom(workflowDetails, pipelineId, pipelineStepId);
//        verify(dspServiceClient).getWorkflowGroupId(refreshId);
//        verify(dspServiceClient).getQueueInfo(workflowId);
//        verify(dspServiceClient, times(2)).getRequest(refreshId);
//        verify(requestEntity, times(4)).getData();
//        verify(workflowGroupExecuteRequest).getOverrides();
//        verify(requestOverride).getObjectOverrideList();
//        verify(queueInfoDTO).getHiveQueue();
//        verify(hiveClient).setQueue(hiveQueue);
//        verify(dspServiceClient).getPipelineStepLogDetails(Integer.valueOf(attempt) - 1, pipelineStepId, pipelineExecutionId,
//                refreshId);
//        verify(pipelineStepAudit).getLogs();
//        verify(auditHelper, times(3)).savePipelineAuditRequest(any(), any(), any(), any(), any(), any(), any());
//        verify(executorLogManager).getMesosLogURL(any(), any(), any(), any());
//        verify(pipelineStep).getScript();
//        verify(scriptHelper).importScript(scriptEntity);
//        verify(pipelineStep).getId();
//        verify(auditHelper).getLogAttemptMapping(pipelineStepId, pipelineExecutionId, refreshId);
//        verify(auditHelper).createWFContainerStartInfoEvent(any(), any(), any(), any(), any(), any(), any());
//        verify(auditHelper).createWFContainerStartedDebugEvent(any(), any(), any(), any(), any(), any(), any(), any(), any());
//        verify(workFlowOrchestrator).run(workflowEntity, localScript, configPayload, pipelineStep, objectOverrides);
//        verify(auditHelper).savePipelineStepRuntimeConfig(configPayload, pipelineStep, runConfig);
//        verify(auditHelper).createWFContainerCompletedInfoEvent(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
//        verify(ippdspConfiguration, times(2)).getDspClientConfig();
//        verify(dspClientConfig).getHost();
//        verify(dspClientConfig).getPort();
//        verify(dspServiceClient).getPipelineStepById(pipelineStepId);
//        verify(workflowGroupExecuteRequest, times(2)).getEmailNotifications();
//        verify(pipelineStep).getName();
//        verify(dspServiceClient).sendEmailNotificationForPartitionStateChangeRequest(any());
//    }
//
//    @Test
//    public void testExecuteSuccessCase2() throws Exception {
//        String[] payload = {JsonUtils.DEFAULT.mapper.writeValueAsString(configPayload), "frameworkId",
//                "slaveId", "hostIP", pipelineExecutionId, "1", "10", clusterRole, attempt, "0"};
//
//        when(workflowGroupExecuteRequest.getEmailNotifications()).thenReturn(null);
//        when(dspServiceClient.getNotificationPreference(workflowId)).thenReturn(notificationPreference);
//        when(notificationPreference.getEmailNotificationPreferences()).thenReturn(emailNotifications);
//        doNothing().when(dspServiceClient).sendEmailNotificationForPartitionStateChangeRequest(any());
//
//
//        workFlowMesosApplication.execute(payload);
//        verify(dspServiceClient).getWorkflowDetails(any());
//        verify(workflowDetails, times(4)).getWorkflow();
//        verify(workflowEntity, times(5)).getName();
//        verifyStatic(MesosCosmosTag.class);
//        MesosCosmosTag.populateValue(workflowName, pipelineStepId, refreshId, partitionValues.values().toString(), clusterRole);
//        verifyStatic(AuditHelper.class);
//        AuditHelper.getPipelineStepFrom(workflowDetails, pipelineId, pipelineStepId);
//        verify(dspServiceClient).getWorkflowGroupId(refreshId);
//        verify(dspServiceClient).getQueueInfo(workflowId);
//        verify(dspServiceClient, times(2)).getRequest(refreshId);
//        verify(requestEntity, times(3)).getData();
//        verify(workflowGroupExecuteRequest).getOverrides();
//        verify(requestOverride).getObjectOverrideList();
//        verify(queueInfoDTO).getHiveQueue();
//        verify(hiveClient).setQueue(hiveQueue);
//        verify(dspServiceClient).getPipelineStepLogDetails(Integer.valueOf(attempt) - 1, pipelineStepId, pipelineExecutionId,
//                refreshId);
//        verify(pipelineStepAudit).getLogs();
//        verify(auditHelper, times(3)).savePipelineAuditRequest(any(), any(), any(), any(), any(), any(), any());
//        verify(executorLogManager).getMesosLogURL(any(), any(), any(), any());
//        verify(pipelineStep).getScript();
//        verify(scriptHelper).importScript(scriptEntity);
//        verify(pipelineStep).getId();
//        verify(auditHelper).getLogAttemptMapping(pipelineStepId, pipelineExecutionId, refreshId);
//        verify(auditHelper).createWFContainerStartInfoEvent(any(), any(), any(), any(), any(), any(), any());
//        verify(auditHelper).createWFContainerStartedDebugEvent(any(), any(), any(), any(), any(), any(), any(), any(), any());
//        verify(workFlowOrchestrator).run(workflowEntity, localScript, configPayload, pipelineStep, objectOverrides);
//        verify(auditHelper).savePipelineStepRuntimeConfig(configPayload, pipelineStep, runConfig);
//        verify(auditHelper).createWFContainerCompletedInfoEvent(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
//        verify(ippdspConfiguration, times(2)).getDspClientConfig();
//        verify(dspClientConfig).getHost();
//        verify(dspClientConfig).getPort();
//        verify(dspServiceClient).getPipelineStepById(pipelineStepId);
//        verify(workflowGroupExecuteRequest).getEmailNotifications();
//        verify(pipelineStep).getName();
//        verify(dspServiceClient).sendEmailNotificationForPartitionStateChangeRequest(any());
//    }
//
//    @Test
//    public void testExecuteFailure() throws Exception {
//        attempt = "0";
//        String[] payload = {JsonUtils.DEFAULT.mapper.writeValueAsString(configPayload), "frameworkId",
//                "slaveId", "hostIP", pipelineExecutionId, "1", "10", clusterRole, attempt, "0"};
//
//        when(workFlowOrchestrator.run(workflowEntity, localScript, configPayload, pipelineStep, objectOverrides)).thenThrow(new ScriptExecutionEngineException("Error"));
//        doNothing().when(auditHelper).createWFContainerFailedEvent(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
//        when(workflowGroupExecuteRequest.getEmailNotifications()).thenReturn(null);
//        when(dspServiceClient.getNotificationPreference(workflowId)).thenReturn(notificationPreference);
//        when(notificationPreference.getEmailNotificationPreferences()).thenReturn(null);
//        doNothing().when(dspServiceClient).sendEmailNotificationForPartitionStateChangeRequest(any());
//
//        boolean isException = false;
//        try {
//            workFlowMesosApplication.execute(payload);
//        } catch (ApplicationException e) {
//            isException = true;
//        }
//
//        assertTrue(isException);
//        verify(dspServiceClient).getWorkflowDetails(any());
//        verify(workflowDetails, times(4)).getWorkflow();
//        verify(workflowEntity, times(5)).getName();
//        verifyStatic(MesosCosmosTag.class);
//        MesosCosmosTag.populateValue(workflowName, pipelineStepId, refreshId, partitionValues.values().toString(), clusterRole);
//        verifyStatic(AuditHelper.class);
//        AuditHelper.getPipelineStepFrom(workflowDetails, pipelineId, pipelineStepId);
//        verify(dspServiceClient).getWorkflowGroupId(refreshId);
//        verify(dspServiceClient).getQueueInfo(workflowId);
//        verify(dspServiceClient, times(2)).getRequest(refreshId);
//        verify(requestEntity, times(3)).getData();
//        verify(workflowGroupExecuteRequest).getOverrides();
//        verify(requestOverride).getObjectOverrideList();
//        verify(queueInfoDTO).getHiveQueue();
//        verify(hiveClient).setQueue(hiveQueue);
//        verify(auditHelper, times(2)).savePipelineAuditRequest(any(), any(), any(), any(), any(), any(), any());
//        verify(executorLogManager).getMesosLogURL(any(), any(), any(), any());
//        verify(pipelineStep).getScript();
//        verify(scriptHelper).importScript(scriptEntity);
//        verify(auditHelper).createWFContainerStartInfoEvent(any(), any(), any(), any(), any(), any(), any());
//        verify(auditHelper).createWFContainerStartedDebugEvent(any(), any(), any(), any(), any(), any(), any(), any(), any());
//        verify(workFlowOrchestrator).run(workflowEntity, localScript, configPayload, pipelineStep, objectOverrides);
//        verify(ippdspConfiguration, times(2)).getDspClientConfig();
//        verify(dspClientConfig).getHost();
//        verify(dspClientConfig).getPort();
//        verify(dspServiceClient).getPipelineStepById(pipelineStepId);
//        verify(workflowGroupExecuteRequest).getEmailNotifications();
//        verify(dspServiceClient).getNotificationPreference(workflowId);
//        verify(notificationPreference).getEmailNotificationPreferences();
//        verify(auditHelper).createWFContainerFailedEvent(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
//    }
//}
