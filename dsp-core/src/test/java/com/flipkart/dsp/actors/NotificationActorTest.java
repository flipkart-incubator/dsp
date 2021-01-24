//package com.flipkart.dsp.callback;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.actors.ExternalCredentialsActor;
//import com.flipkart.dsp.actors.WorkFlowActor;
//import com.flipkart.dsp.clients.VaradhiClient;
//import com.flipkart.dsp.config.IPPDSPConfiguration;
//import com.flipkart.dsp.entities.script.Script;
//import com.flipkart.dsp.entities.workflow.Workflow;
//import com.flipkart.dsp.entities.workflow.WorkflowDetails;
//import com.flipkart.dsp.exceptions.CallbackException;
//import com.flipkart.dsp.exceptions.DSPCoreException;
//import com.flipkart.dsp.models.ExternalCredentials;
//import com.flipkart.dsp.models.RequestStatus;
//import com.flipkart.dsp.models.ScriptVariable;
//import com.flipkart.dsp.models.VaradhiResponseDto;
//import com.flipkart.dsp.models.externalentities.CephEntity;
//import com.flipkart.dsp.models.outputVariable.*;
//import com.flipkart.dsp.utils.AmazonS3Utils;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.net.URL;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import static org.junit.Assert.*;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.verifyStatic;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({CallbackManager.class, AmazonS3Utils.class})
//public class CallbackManagerTest {
//
//    @Mock private Script script;
//    @Mock private Workflow workflow;
//    @Mock private CephEntity cephEntity;
//    @Mock private ObjectMapper objectMapper;
//    @Mock private PipelineStep pipelineStep;
//    @Mock private WorkFlowActor workFlowActor;
//    @Mock private VaradhiClient varadhiClient;
//    @Mock private ScriptVariable scriptVariable;
//    @Mock private PipelineDetails pipelineDetails;
//    @Mock private WorkflowDetails workflowDetails;
//    @Mock private VaradhiResponseDto varadhiResponseDto;
//    @Mock private IPPDSPConfiguration ippdspConfiguration;
//    @Mock private ExternalCredentials externalCredentials;
//    @Mock private IPPDSPConfiguration.HadoopConfig hadoopConfig;
//    @Mock private ExternalCredentialsActor externalCredentialsActor;
//
//    private Long requestId = 1L;
//    private Object payload = "payload";
//    private String callbackUrl = "testUrl";
//    private String dataFrameName = "dataFrameName";
//
//    private CallbackManager callbackManager;
//    private List<URL> urls = new ArrayList<>();
//    private CephOutputLocation cephOutputLocation;
//    private List<Workflow> workflows = new ArrayList<>();
//    private List<String> callBackEntities = new ArrayList<>();
//    private List<PipelineStep> pipelineSteps = new ArrayList<>();
//    private List<String> legacyOutputHiveTables = new ArrayList<>();
//    private Set<ScriptVariable> scriptVariableSet = new HashSet<>();
//    private List<OutputLocation> outputLocationList = new ArrayList<>();
//    private List<PipelineDetails> pipelineDetailsList = new ArrayList<>();
//
//    @Before
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//        PowerMockito.mockStatic(AmazonS3Utils.class);
//        this.callbackManager = spy(new CallbackManager(objectMapper, varadhiClient, workFlowActor, ippdspConfiguration, externalCredentialsActor));
//
//
//        workflows.add(workflow);
//        pipelineSteps.add(pipelineStep);
//        callBackEntities.add("callback");
//        scriptVariableSet.add(scriptVariable);
//        pipelineDetailsList.add(pipelineDetails);
//
//        when(workflow.getId()).thenReturn(1L);
//        when(pipelineStep.getScript()).thenReturn(script);
//        when(hadoopConfig.getBasePath()).thenReturn("/base_path");
//        when(workflowGroup.getWorkflowsList()).thenReturn(workflows);
//        when(scriptVariable.getName()).thenReturn(dataFrameName);
//        when(script.getOutputVariables()).thenReturn(scriptVariableSet);
//        when(pipelineDetails.getPipelineSteps()).thenReturn(pipelineSteps);
//        when(workFlowActor.getWorkflowFromId(1L)).thenReturn(workflowDetails);
//        when(workflowDetails.getPipelines()).thenReturn(pipelineDetailsList);
//        when(ippdspConfiguration.getHadoopConfig()).thenReturn(hadoopConfig);
//        when(workflowGroup.getWorkflowGroupMeta()).thenReturn(workflowGroupMeta);
//        when(scriptVariable.getOutputLocationDetailsList()).thenReturn(outputLocationList);
//        when(workflowDetails.getLegacyOutputHiveTables()).thenReturn(legacyOutputHiveTables);
//    }
//
//    @Test
//    public void testAttemptCallbackToClientSuccess() throws Exception {
//        when(varadhiClient.triggerClientCallbackRequest(requestId, callbackUrl, payload)).thenReturn(varadhiResponseDto);
//        VaradhiResponseDto actual = callbackManager.attemptCallbackToClient(requestId, callbackUrl, payload);
//        assertNotNull(actual);
//        verify(varadhiClient, times(1)).triggerClientCallbackRequest(requestId, callbackUrl, payload);
//    }
//
//    @Test
//    public void testAttemptCallbackToClientFailure() throws Exception {
//        boolean isException = false;
//        when(varadhiClient.triggerClientCallbackRequest(requestId, callbackUrl, payload)).thenThrow(new Exception());
//
//        try {
//            callbackManager.attemptCallbackToClient(requestId, callbackUrl, payload);
//        } catch (CallbackException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Client callBack Failed!");
//        }
//        assertTrue(isException);
//        verify(varadhiClient, times(1)).triggerClientCallbackRequest(requestId, callbackUrl, payload);
//    }
//
//    @Test
//    public void testGetPayloadSuccessCase1() {
//        when(workflowGroupMeta.getCallbackEntities()).thenReturn(callBackEntities);
//        Object payload = callbackManager.getPayload(workflowGroup, requestId, requestId, RequestStatus.COMPLETED);
//        assertNotNull(payload);
//
//        verify(workflowGroup, times(1)).getWorkflowsList();
//        verify(workflow, times(1)).getId();
//        verify(workFlowActor, times(1)).getWorkflowFromId(1L);
//        verify(workflowDetails, times(1)).getLegacyOutputHiveTables();
//        verify(workflowGroup, times(3)).getWorkflowGroupMeta();
//        verify(workflowGroupMeta, times(3)).getCallbackEntities();
//    }
//
//    @Test
//    public void testGetPayloadSuccessCase2() throws Exception {
//        String workflowName = "workflowName";
//        outputLocationList = getOutputLocationList();
//
//        when(workflow.getName()).thenReturn(workflowName);
//        when(workflowGroupMeta.getCallbackEntities()).thenReturn(null);
//        when(externalCredentialsActor.getCredentials("client_alias")).thenReturn(externalCredentials);
//        when(externalCredentials.getDetails()).thenReturn("details");
//        when(objectMapper.readValue("details", CephEntity.class)).thenReturn(cephEntity);
//        when(ippdspConfiguration.getSaltKey()).thenReturn("saltKey");
//        PowerMockito.when(AmazonS3Utils.getCephkey(requestId, "/ceph_path", workflowName, dataFrameName)).thenReturn("ceph_key");
//        PowerMockito.when(AmazonS3Utils.getCephUrls("saltKey",requestId, workflowName, dataFrameName, cephEntity, cephOutputLocation)).thenReturn(urls);
//
//        Object payload = callbackManager.getPayload(workflowGroup, requestId, requestId, RequestStatus.COMPLETED);
//        assertNotNull(payload);
//
//        verify(workflowGroup, times(2)).getWorkflowsList();
//        verify(workflow, times(2)).getId();
//        verify(workFlowActor, times(2)).getWorkflowFromId(1L);
//        verify(workflowDetails, times(1)).getLegacyOutputHiveTables();
//        verify(workflowGroup, times(1)).getWorkflowGroupMeta();
//        verify(workflowGroupMeta, times(1)).getCallbackEntities();
//        verify(workflowDetails, times(1)).getPipelines();
//        verify(pipelineDetails, times(1)).getPipelineSteps();
//        verify(pipelineStep, times(1)).getScript();
//        verify(script, times(1)).getOutputVariables();
//        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
//        verify(workflow, times(6)).getName();
//        verify(ippdspConfiguration, times(1)).getHadoopConfig();
//        verify(hadoopConfig, times(1)).getBasePath();
//        verify(externalCredentialsActor, times(1)).getCredentials("client_alias");
//        verify(externalCredentials, times(1)).getDetails();
//        verify(objectMapper, times(1)).readValue("details", CephEntity.class);
//        verify(ippdspConfiguration, times(1)).getSaltKey();
//        verifyStatic(AmazonS3Utils.class);
//        AmazonS3Utils.getCephkey(requestId, "/ceph_path", workflowName, dataFrameName);
//        verifyStatic(AmazonS3Utils.class);
//        AmazonS3Utils.getCephUrls("saltKey",requestId, workflowName, dataFrameName, cephEntity, cephOutputLocation);
//    }
//
//    @Test
//    public void testGetPayloadFailure() throws Exception {
//        boolean isException = false;
//        outputLocationList = getOutputLocationList();
//        when(externalCredentialsActor.getCredentials("client_alias")).thenThrow(new DSPCoreException("Error"));
//
//        try {
//            callbackManager.getPayload(workflowGroup, requestId, requestId, RequestStatus.COMPLETED);
//        } catch (Exception e) {
//            isException = true;
//            assertTrue(e.getMessage().contains("Error in getting Ceph Entity. Error: "));
//        }
//
//        assertTrue(isException);
//        verify(workflowGroup, times(2)).getWorkflowsList();
//        verify(workflow, times(2)).getId();
//        verify(workFlowActor, times(2)).getWorkflowFromId(1L);
//        verify(workflowDetails, times(1)).getLegacyOutputHiveTables();
//        verify(workflowGroup, times(2)).getWorkflowGroupMeta();
//        verify(workflowGroupMeta, times(2)).getCallbackEntities();
//        verify(workflowDetails, times(1)).getPipelines();
//        verify(pipelineDetails, times(1)).getPipelineSteps();
//        verify(pipelineStep, times(1)).getScript();
//        verify(script, times(1)).getOutputVariables();
//        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
//        verify(ippdspConfiguration, times(1)).getHadoopConfig();
//        verify(hadoopConfig, times(1)).getBasePath();
//        verify(externalCredentialsActor, times(1)).getCredentials("client_alias");
//    }
//
//    private List<OutputLocation> getOutputLocationList() {
//        DDPOutputLocation ddpOutputLocation = new DDPOutputLocation();
//        ddpOutputLocation.setDatabase("test_db");
//        ddpOutputLocation.setTable("test_table");
//
//        HiveOutputLocation hiveOutputLocation = new HiveOutputLocation();
//        hiveOutputLocation.setDatabase("test_db");
//        hiveOutputLocation.setTable("test_table");
//
//        HDFSOutputLocation hdfsOutputLocation = new HDFSOutputLocation();
//        hdfsOutputLocation.setLocation("/hdfs_location");
//
//        FDPEntityOutputLocation fdpEntityOutputLocation = new FDPEntityOutputLocation();
//        fdpEntityOutputLocation.setFdpEntityName("fdp_entity");
//        fdpEntityOutputLocation.setFdpEntitySchemaVersion("2.0");
//
//        cephOutputLocation = new CephOutputLocation();
//        cephOutputLocation.setBucket("ceph_bucket");
//        cephOutputLocation.setPath("/ceph_path");
//        cephOutputLocation.setClientAlias("client_alias");
//
//        outputLocationList.add(ddpOutputLocation);
//        outputLocationList.add(hiveOutputLocation);
//        outputLocationList.add(hdfsOutputLocation);
//        outputLocationList.add(fdpEntityOutputLocation);
//        outputLocationList.add(cephOutputLocation);
//        return outputLocationList;
//    }
//}
