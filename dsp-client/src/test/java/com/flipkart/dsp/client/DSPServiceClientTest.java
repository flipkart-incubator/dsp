package com.flipkart.dsp.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.dataframe.*;
import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.client.exceptions.DSPServiceException;
import com.flipkart.dsp.client.misc.*;
import com.flipkart.dsp.client.notification.callback.UpdateEntityBatchRequest;
import com.flipkart.dsp.client.notification.email.GetNotificationPreferenceRequest;
import com.flipkart.dsp.client.notification.email.SendEmailNotificationForPartitionStateChangeRequest;
import com.flipkart.dsp.client.pipelinestep.*;
import com.flipkart.dsp.client.request.GetRequestByRequestId;
import com.flipkart.dsp.client.request.GetRequestStatusByRequestId;
import com.flipkart.dsp.client.script.GetScriptMetaRequest;
import com.flipkart.dsp.client.workflow.*;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.pipelinestep.*;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.entities.sg.core.*;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.utils.HTTPRequestUtil;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DSPServiceClient.class, HTTPRequestUtil.class, GetWorkflowDetailsRequest.class})
public class DSPServiceClientTest {
    private int port = 8080;
    private int maxRetries = 5;
    private Long requestId = 1L;
    private String host = "host";
    private Long dataFrameId = 1L;
    private String method = "POST";
    private String path = "/urlPath";
    private Long pipelineStepId = 1L;
    private Long dataFrameAuditId = 1L;
    private int retryGapInMillis = 10000;
    private int requestTimeoutMillis = 10000;
    private Long dataFrameOverrideAuditId = 1L;
    private String scriptBaseDir = "scriptBaseDir";

    @Mock
    private Request request;
    @Mock
    private Response response;
    @Mock
    private RunConfig runConfig;
    @Mock
    private DataTable dataTable;
    @Mock
    private EventAudit eventAudit;
    @Mock
    private ScriptMeta scriptMeta;
    @Mock
    private InputStream inputStream;
    @Mock
    private PipelineStep pipelineStep;
    @Mock
    private QueueInfoDTO queueInfoDTO;
    @Mock
    private ConfigPayload configPayload;
    @Mock
    private RequestBuilder requestBuilder;
    @Mock
    private DataFrameAudit dataFrameAudit;
    @Mock
    private AsyncHttpClient asyncHttpClient;
    @Mock
    private WorkflowDetails workflowDetails;
    @Mock
    private UpdateEntityDTO updateEntityDTO;
    @Mock
    private PipelineStepAudit pipelineStepAudit;
    @Mock
    private PipelineStepSGAudit pipelineStepSGAudit;
    @Mock
    private GetQueueInfoRequest getQueueInfoRequest;
    @Mock
    private GetDataTableRequest getDataTableRequest;
    @Mock
    private ExternalCredentials externalCredentials;
    @Mock
    private NotificationPreference notificationPreference;

    @Mock
    private GetScriptMetaRequest getScriptMetaRequest;
    @Mock
    private GetRequestByRequestId getRequestByRequestId;
    @Mock
    private TriggerEntityRegister triggerEntityRegister;
    @Mock
    private DataFrameOverrideAudit dataFrameOverrideAudit;
    @Mock
    private CreateEventAuditRequest createEventAuditRequest;
    @Mock
    private GetDataFrameAuditRequest getDataFrameAuditRequest;
    @Mock
    private Function<RequestBuilder, RequestBuilder> function;
    @Mock
    private UpdateEntityBatchRequest updateEntityBatchRequest;
    @Mock
    private PipelineStepRuntimeConfig pipelineStepRuntimeConfig;
    @Mock
    private GetWorkflowDetailsRequest getWorkflowDetailsRequest;
    @Mock
    private GetPipelineStepByIdRequest getPipelineStepByIdRequest;
    @Mock
    private CreateDataFrameAuditRequest createDataFrameAuditRequest;
    @Mock
    private GetRequestStatusByRequestId getRequestStatusByRequestId;
    @Mock
    private GetDataFrameAuditByIdRequest getDataFrameAuditByIdRequest;
    @Mock
    private ExecutionEnvironmentSnapshot executionEnvironmentSnapshot;
    @Mock
    private CreatePipelineStepAuditRequest createPipelineStepAuditRequest;
    @Mock
    private GetNotificationPreferenceRequest getNotificationPreferenceRequest;
    @Mock
    private CreatePipelineStepSgAuditRequest createPipelineStepSgAuditRequest;
    @Mock
    private GetDataFrameOverrideAuditRequest getDataFrameOverrideAuditRequest;
    @Mock
    private CreateDataFrameOverrideAuditRequest createDataFrameOverrideAuditRequest;
    @Mock
    private CreateRequestDataFrameAuditsRequest createRequestDataFrameAuditsRequest;
    @Mock
    private UpdateDataFrameOverrideAuditRequest updateDataFrameOverrideAuditRequest;
    @Mock
    private GetPipelineStepRuntimeConfigRequest getPipelineStepRuntimeConfigRequest;
    @Mock
    private GetDataFrameOverrideAuditByIdRequest getDataFrameOverrideAuditByIdRequest;
    @Mock
    private CreatePipelineStepRuntimeConfigRequest createPipelineStepRuntimeConfigRequest;
    @Mock
    private PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest;
    @Mock
    private CreateExecutionEnvironmentSnapshotRequest createExecutionEnvironmentSnapshotRequest;
    @Mock
    private GetExternalCredentialsByClientAliasRequest getExternalCredentialsByClientAliasRequest;
    @Mock
    private GetPipelineStepRuntimeConfigByScopeRequest getPipelineStepRuntimeConfigByScopeRequest;
    @Mock
    private GetLatestDataFrameAuditByDataFrameIdRequest getLatestDataFrameAuditByDataFrameIdRequest;
    @Mock
    private GetDataframeOverrideAuditByIdRequestTypeRequest getDataframeOverrideAuditByIdRequestTypeRequest;
    @Mock
    private GetPipelineStepAuditsByPipelineExecutionIdRequest getPipelineStepAuditsByPipelineExecutionIdRequest;
    @Mock
    private SendEmailNotificationForPartitionStateChangeRequest sendEmailNotificationForPartitionStateChangeRequest;
    @Mock
    private GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(HTTPRequestUtil.class);
    }

    @Test
    public void testCloseCase1() {
        when(asyncHttpClient.isClosed()).thenReturn(false);
        doNothing().when(asyncHttpClient).close();
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        dspServiceClient.close();
        verify(asyncHttpClient).isClosed();
        verify(asyncHttpClient).close();
    }

    @Test
    public void testCloseCase2() {
        when(asyncHttpClient.isClosed()).thenReturn(false);
        doThrow(new RuntimeException()).when(asyncHttpClient).close();
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        dspServiceClient.close();
        verify(asyncHttpClient).isClosed();
        verify(asyncHttpClient).close();
    }

    @Test
    public void testExecuteSyncSuccessCase1() throws Exception {
        JavaType type = JsonUtils.DEFAULT.mapper.constructType(InputStream.class);
        when(function.apply(any())).thenReturn(requestBuilder);
        when(HTTPRequestUtil.executeWithRetries(any(), anyInt(), anyInt())).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBodyAsStream()).thenReturn(inputStream);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir);
        InputStream expected = dspServiceClient.executesSync(method, path, type, function);
        assertEquals(inputStream, expected);
        verify(function).apply(any());
        verify(response, times(2)).getStatusCode();
        verify(response).getResponseBodyAsStream();
    }

    @Test
    public void testExecuteSyncSuccessCase2() throws Exception {
        JavaType type = JsonUtils.DEFAULT.mapper.constructType(Map.class);
        Map<String, String> responseBodyMap = new HashMap<>();
        responseBodyMap.put("object1", "object2");
        String responseBody = new ObjectMapper().writeValueAsString(responseBodyMap);

        when(function.apply(any())).thenReturn(requestBuilder);
        when(HTTPRequestUtil.executeWithRetries(any(), anyInt(), anyInt())).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn(responseBody);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir);
        Map<String, String> expected = dspServiceClient.executesSync(method, path, type, function);
        assertTrue(expected.containsKey("object1"));
        assertEquals(expected.get("object1"), "object2");
        verify(function).apply(any());
        verify(response, times(2)).getStatusCode();
        verify(response, times(2)).getResponseBody();
    }

    @Test
    public void testExecuteSyncSuccessCase3() throws Exception {
        JavaType type = JsonUtils.DEFAULT.mapper.constructType(Void.class);

        when(function.apply(any())).thenReturn(requestBuilder);
        when(HTTPRequestUtil.executeWithRetries(any(), anyInt(), anyInt())).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir);
        Object expected = dspServiceClient.executesSync(method, path, type, function);
        assertNull(expected);
        verify(function).apply(any());
        verify(response, times(2)).getStatusCode();
    }

    @Test
    public void testExecuteSyncFailure() throws Exception {
        boolean isException = false;
        JavaType type = JsonUtils.DEFAULT.mapper.constructType(Void.class);
        Error error = Error.builder().errorCode(404).errorMsg("errorMsg").build();

        when(function.apply(any())).thenReturn(requestBuilder);
        when(HTTPRequestUtil.executeWithRetries(any(), anyInt(), anyInt())).thenReturn(response);
        when(response.getStatusCode()).thenReturn(404);
        when(response.getResponseBody()).thenReturn(new ObjectMapper().writeValueAsString(error));
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis);

        try {
            dspServiceClient.executesSync(method, path, type, function);
        } catch (DSPServiceException e) {
            isException = true;
            assertEquals(e.getMessage(), "errorMsg");
        }
        assertTrue(isException);
        verify(function).apply(any());
        verify(response, times(2)).getStatusCode();
        verify(response, times(2)).getResponseBody();
    }

    @Test
    public void testExecuteSyncFailureCase2() throws Exception {
        boolean isException = false;
        JavaType type = JsonUtils.DEFAULT.mapper.constructType(Void.class);

        when(function.apply(any())).thenReturn(requestBuilder);
        when(HTTPRequestUtil.executeWithRetries(any(), anyInt(), anyInt())).thenThrow(new Exception());
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis);

        try {
            dspServiceClient.executesSync(method, path, type, function);
        } catch (DSPClientException e) {
            isException = true;
        }
        assertTrue(isException);
        verify(function).apply(any());
    }

    @Test
    public void testGetWorkflowDetails() throws Exception {
        Long workflowId = 1L;
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetWorkflowDetailsRequest.class).withArguments(dspServiceClient, workflowId).thenReturn(getWorkflowDetailsRequest);
        when(getWorkflowDetailsRequest.executeSync()).thenReturn(workflowDetails);
        WorkflowDetails expected = dspServiceClient.getWorkflowDetails(workflowId);
        assertEquals(expected, workflowDetails);
        verifyNew(GetWorkflowDetailsRequest.class).withArguments(dspServiceClient, workflowId);
        verify(getWorkflowDetailsRequest).executeSync();
    }

    @Test
    public void testGetDataFrameOverrideAuditByIdRequestAndType() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataframeOverrideAuditByIdRequestTypeRequest.class).withArguments(dspServiceClient, dataFrameId, requestId,
                DataFrameOverrideType.CSV).thenReturn(getDataframeOverrideAuditByIdRequestTypeRequest);
        when(getDataframeOverrideAuditByIdRequestTypeRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dspServiceClient.getDataFrameOverrideAuditByIdRequestAndType(dataFrameId, requestId, DataFrameOverrideType.CSV);
        assertNotNull(expected);
        verifyNew(GetDataframeOverrideAuditByIdRequestTypeRequest.class).withArguments(dspServiceClient, dataFrameId, requestId, DataFrameOverrideType.CSV);
        verify(getDataframeOverrideAuditByIdRequestTypeRequest).executeSync();
    }


    @Test
    public void testGetDataTable() throws Exception {
        String tableName = "tableName";
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataTableRequest.class).withArguments(dspServiceClient, tableName).thenReturn(getDataTableRequest);
        when(getDataTableRequest.executeSync()).thenReturn(dataTable);
        DataTable expected = dspServiceClient.getDataTable(tableName);
        assertNotNull(expected);
        verifyNew(GetDataTableRequest.class).withArguments(dspServiceClient, tableName);
        verify(getDataTableRequest).executeSync();
    }

    @Test
    public void testUpdateWorkFlowAuditStatusBatch() throws Exception {
        List<UpdateEntityDTO> updateEntityDTOList = new ArrayList<>();
        updateEntityDTOList.add(updateEntityDTO);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(UpdateEntityBatchRequest.class).withArguments(dspServiceClient, updateEntityDTOList).thenReturn(updateEntityBatchRequest);
        when(updateEntityBatchRequest.executeSync()).thenReturn(null);
        dspServiceClient.updateWorkFlowAuditStatusBatch(updateEntityDTOList);
        verifyNew(UpdateEntityBatchRequest.class).withArguments(dspServiceClient, updateEntityDTOList);
        verify(updateEntityBatchRequest).executeSync();
    }

    @Test
    public void testTriggerEntityRegisterForWorkflow() throws Exception {
        String serializedTableList = "serializedTableList";
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(TriggerEntityRegister.class).withArguments(dspServiceClient, requestId, serializedTableList).thenReturn(triggerEntityRegister);
        when(triggerEntityRegister.executeSync()).thenReturn(null);
        dspServiceClient.triggerEntityRegisterForWorkflow(requestId, serializedTableList);
        verifyNew(TriggerEntityRegister.class).withArguments(dspServiceClient, requestId, serializedTableList);
        verify(triggerEntityRegister).executeSync();
    }

    @Test
    public void testGetRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetRequestByRequestId.class).withArguments(dspServiceClient, requestId).thenReturn(getRequestByRequestId);
        when(getRequestByRequestId.executeSync()).thenReturn(request);
        Request expected = dspServiceClient.getRequest(requestId);
        assertEquals(expected, request);
        verifyNew(GetRequestByRequestId.class).withArguments(dspServiceClient, requestId);
        verify(getRequestByRequestId).executeSync();
    }

    @Test
    public void testGetRequestStatusById() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetRequestStatusByRequestId.class).withArguments(dspServiceClient, requestId).thenReturn(getRequestStatusByRequestId);
        when(getRequestStatusByRequestId.executeSync()).thenReturn(RequestStatus.CREATED);
        RequestStatus expected = dspServiceClient.getRequestStatusById(requestId);
        assertEquals(expected, RequestStatus.CREATED);
        verifyNew(GetRequestStatusByRequestId.class).withArguments(dspServiceClient, requestId);
        verify(getRequestStatusByRequestId).executeSync();
    }

    @Test
    public void testGetDataFrameAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataFrameAuditByIdRequest.class).withArguments(dspServiceClient, dataFrameAuditId).thenReturn(getDataFrameAuditByIdRequest);
        when(getDataFrameAuditByIdRequest.executeSync()).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dspServiceClient.getDataFrameAudit(dataFrameAuditId);
        assertEquals(expected, dataFrameAudit);
        verifyNew(GetDataFrameAuditByIdRequest.class).withArguments(dspServiceClient, dataFrameAuditId);
        verify(getDataFrameAuditByIdRequest).executeSync();
    }

    @Test
    public void testGetDataFrameAuditCase2() throws Exception {
        String partitions = "partitions";
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameId, dataFrameOverrideAuditId, partitions).thenReturn(getDataFrameAuditRequest);
        when(getDataFrameAuditRequest.executeSync()).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dspServiceClient.getDataFrameAudit(dataFrameAuditId, dataFrameOverrideAuditId, partitions);
        assertEquals(expected, dataFrameAudit);
        verifyNew(GetDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameId, dataFrameOverrideAuditId, partitions);
        verify(getDataFrameAuditRequest).executeSync();
    }

    @Test
    public void testGetLatestDataFrameAuditByDataFrameId() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetLatestDataFrameAuditByDataFrameIdRequest.class).withArguments(dspServiceClient, dataFrameId).thenReturn(getLatestDataFrameAuditByDataFrameIdRequest);
        when(getLatestDataFrameAuditByDataFrameIdRequest.executeSync()).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameAuditId);
        assertEquals(expected, dataFrameAudit);
        verifyNew(GetLatestDataFrameAuditByDataFrameIdRequest.class).withArguments(dspServiceClient, dataFrameId);
        verify(getLatestDataFrameAuditByDataFrameIdRequest).executeSync();
    }

    @Test
    public void testSaveDataFrameAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameAudit).thenReturn(createDataFrameAuditRequest);
        when(createDataFrameAuditRequest.executeSync()).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dspServiceClient.saveDataFrameAudit(dataFrameAudit);
        assertEquals(expected, dataFrameAudit);
        verifyNew(CreateDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameAudit);
        verify(createDataFrameAuditRequest).executeSync();
    }

    @Test
    public void testPersistRequestDataframeAudit() throws Exception {
        Long workflowId = 1L;
        Set<DataFrameAudit> dataFrameAudits = new HashSet<>();
        dataFrameAudits.add(dataFrameAudit);
        Map<String, List<String>> outputMap = new HashMap<>();
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateRequestDataFrameAuditsRequest.class).withArguments(dspServiceClient, requestId, workflowId, pipelineStepId, dataFrameAudits).thenReturn(createRequestDataFrameAuditsRequest);
        when(createRequestDataFrameAuditsRequest.executeSync()).thenReturn(outputMap);
        Map<String, List<String>> expected = dspServiceClient.persistRequestDataframeAudit(requestId, workflowId, pipelineStepId, dataFrameAudits);
        assertEquals(expected, outputMap);
        verifyNew(CreateRequestDataFrameAuditsRequest.class).withArguments(dspServiceClient, requestId, workflowId, pipelineStepId, dataFrameAudits);
        verify(createRequestDataFrameAuditsRequest).executeSync();
    }

    @Test
    public void testUpdateDataFrameAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameAudit).thenReturn(createDataFrameAuditRequest);
        when(createDataFrameAuditRequest.executeSync()).thenReturn(dataFrameAudit);
        dspServiceClient.updateDataFrameAudit(dataFrameAudit);
        verifyNew(CreateDataFrameAuditRequest.class).withArguments(dspServiceClient, dataFrameAudit);
        verify(createDataFrameAuditRequest).executeSync();
    }

    @Test
    public void testSaveDataFrameOverrideAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameOverrideAudit).thenReturn(createDataFrameOverrideAuditRequest);
        when(createDataFrameOverrideAuditRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dspServiceClient.saveDataFrameOverrideAudit(dataFrameOverrideAudit);
        assertEquals(expected, dataFrameOverrideAudit);
        verifyNew(CreateDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameOverrideAudit);
        verify(createDataFrameOverrideAuditRequest).executeSync();
    }

    @Test
    public void testUpdateDataFrameOverrideAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(UpdateDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameOverrideAudit).thenReturn(updateDataFrameOverrideAuditRequest);
        when(updateDataFrameOverrideAuditRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        dspServiceClient.updateDataFrameOverrideAudit(dataFrameOverrideAudit);
        verifyNew(UpdateDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameOverrideAudit);
        verify(updateDataFrameOverrideAuditRequest).executeSync();
    }

    @Test
    public void testGetDataFrameOverrideAudit() throws Exception {
        String inputDataId = "inputDataId";
        DataFrameOverrideType dataFrameOverrideType = DataFrameOverrideType.HIVE;
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameId, inputDataId, dataFrameOverrideType).thenReturn(getDataFrameOverrideAuditRequest);
        when(getDataFrameOverrideAuditRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dspServiceClient.getDataFrameOverrideAudit(dataFrameId, inputDataId, dataFrameOverrideType);
        assertEquals(expected, dataFrameOverrideAudit);
        verifyNew(GetDataFrameOverrideAuditRequest.class).withArguments(dspServiceClient, dataFrameId, inputDataId, dataFrameOverrideType);
        verify(getDataFrameOverrideAuditRequest).executeSync();
    }

    @Test
    public void testGetDataFrameOverrideAuditById() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataFrameOverrideAuditByIdRequest.class).withArguments(dspServiceClient, dataFrameOverrideAuditId).thenReturn(getDataFrameOverrideAuditByIdRequest);
        when(getDataFrameOverrideAuditByIdRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dspServiceClient.getDataFrameOverrideAuditById(dataFrameOverrideAuditId);
        assertEquals(expected, dataFrameOverrideAudit);
        verifyNew(GetDataFrameOverrideAuditByIdRequest.class).withArguments(dspServiceClient, dataFrameOverrideAuditId);
        verify(getDataFrameOverrideAuditByIdRequest).executeSync();
    }

    @Test
    public void testGetDataFrameOverrideAuditByIdAndRequestId() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.class).withArguments(dspServiceClient,
                dataFrameId, requestId).thenReturn(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest);
        when(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.executeSync()).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dspServiceClient.getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId);
        assertEquals(expected, dataFrameOverrideAudit);
        verifyNew(GetDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest.class).withArguments(dspServiceClient, dataFrameId, requestId);
        verify(getDataFrameOverrideAuditByDataFrameIdAndRequestIdRequest).executeSync();
    }

    @Test
    public void testGetScriptMetaById() throws Exception {
        Long scriptId = 1L;
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetScriptMetaRequest.class).withArguments(dspServiceClient, scriptId).thenReturn(getScriptMetaRequest);
        when(getScriptMetaRequest.executeSync()).thenReturn(scriptMeta);
        ScriptMeta expected = dspServiceClient.getScriptMetaById(scriptId);
        assertEquals(expected, scriptMeta);
        verifyNew(GetScriptMetaRequest.class).withArguments(dspServiceClient, scriptId);
        verify(getScriptMetaRequest).executeSync();
    }

    @Test
    public void testCreatePipelineStepRuntimeConfigRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreatePipelineStepRuntimeConfigRequest.class).withArguments(any(DSPServiceClient.class), any(PipelineStepRuntimeConfig.class)).thenReturn(createPipelineStepRuntimeConfigRequest);
        when(createPipelineStepRuntimeConfigRequest.executeSync()).thenReturn(null);
        dspServiceClient.createPipelineStepRuntimeConfig(pipelineStepId, runConfig, configPayload);
        verifyNew(CreatePipelineStepRuntimeConfigRequest.class).withArguments(any(DSPServiceClient.class), any(PipelineStepRuntimeConfig.class));
    }

    @Test
    public void testGetPipelineStepRuntimeConfig() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        String pipelineExecutionId = "pipelineExecutionId";
        PowerMockito.whenNew(GetPipelineStepRuntimeConfigRequest.class).withArguments(dspServiceClient,
                pipelineExecutionId, pipelineStepId).thenReturn(getPipelineStepRuntimeConfigRequest);
        when(getPipelineStepRuntimeConfigRequest.executeSync()).thenReturn(pipelineStepRuntimeConfig);
        PipelineStepRuntimeConfig expected = dspServiceClient.getPipelineStepRuntimeConfig(pipelineExecutionId, pipelineStepId);
        assertEquals(expected, pipelineStepRuntimeConfig);
        verifyNew(GetPipelineStepRuntimeConfigRequest.class).withArguments(dspServiceClient, pipelineExecutionId, pipelineStepId);
        verify(getPipelineStepRuntimeConfigRequest).executeSync();
    }

    @Test
    public void testGetPipelineStepRuntimeConfigCase2() throws Exception {
        String scope = "scope";
        String workflowExecutionId = "workflowExecutionId";
        List<PipelineStepRuntimeConfig> pipelineStepRuntimeConfigs = new ArrayList<>();
        pipelineStepRuntimeConfigs.add(pipelineStepRuntimeConfig);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetPipelineStepRuntimeConfigByScopeRequest.class).withArguments(dspServiceClient,
                workflowExecutionId, scope).thenReturn(getPipelineStepRuntimeConfigByScopeRequest);
        when(getPipelineStepRuntimeConfigByScopeRequest.executeSync()).thenReturn(pipelineStepRuntimeConfigs);
        List<PipelineStepRuntimeConfig> expected = dspServiceClient.getPipelineStepRuntimeConfig(workflowExecutionId, scope);
        assertEquals(expected, pipelineStepRuntimeConfigs);
        verifyNew(GetPipelineStepRuntimeConfigByScopeRequest.class).withArguments(dspServiceClient, workflowExecutionId, scope);
        verify(getPipelineStepRuntimeConfigByScopeRequest).executeSync();
    }

    @Test
    public void testSavePipelineStepAuditRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreatePipelineStepAuditRequest.class).withArguments(dspServiceClient, pipelineStepAudit).thenReturn(createPipelineStepAuditRequest);
        when(createPipelineStepAuditRequest.executeSync()).thenReturn(pipelineStepAudit);
        PipelineStepAudit expected = dspServiceClient.savePipelineStepAuditRequest(pipelineStepAudit);
        assertEquals(expected, pipelineStepAudit);
        verifyNew(CreatePipelineStepAuditRequest.class).withArguments(dspServiceClient, pipelineStepAudit);
        verify(createPipelineStepAuditRequest).executeSync();
    }

    @Test
    public void testSavePipelineStepSgAuditRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreatePipelineStepSgAuditRequest.class).withArguments(dspServiceClient, pipelineStepSGAudit).thenReturn(createPipelineStepSgAuditRequest);
        when(createPipelineStepSgAuditRequest.executeSync()).thenReturn(1L);
        long expected = dspServiceClient.savePipelineStepSgAuditRequest(pipelineStepSGAudit);
        assertEquals(expected, 1L);
        verifyNew(CreatePipelineStepSgAuditRequest.class).withArguments(dspServiceClient, pipelineStepSGAudit);
        verify(createPipelineStepSgAuditRequest).executeSync();
    }

    @Test
    public void testGetPipelineStepLogDetails() throws Exception {
        Integer attempt = 1;
        String pipelineExecutionId = "pipelineExecutionId";
        List<PipelineStepAudit> pipelineStepAudits = new ArrayList<>();
        pipelineStepAudits.add(pipelineStepAudit);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetPipelineStepAuditsByPipelineExecutionIdRequest.class).withArguments(dspServiceClient, attempt,
                requestId, pipelineStepId, pipelineExecutionId).thenReturn(getPipelineStepAuditsByPipelineExecutionIdRequest);
        when(getPipelineStepAuditsByPipelineExecutionIdRequest.executeSync()).thenReturn(pipelineStepAudits);
        List<PipelineStepAudit> expected = dspServiceClient.getPipelineStepLogDetails(attempt, pipelineStepId, pipelineExecutionId, requestId);
        assertEquals(expected, pipelineStepAudits);
        verifyNew(GetPipelineStepAuditsByPipelineExecutionIdRequest.class).withArguments(dspServiceClient, attempt, requestId, pipelineStepId, pipelineExecutionId);
        verify(getPipelineStepAuditsByPipelineExecutionIdRequest).executeSync();
    }

    @Test
    public void testGetQueueInfo() throws Exception {
        Long workflowId = 1L;
        List<PipelineStepAudit> pipelineStepAudits = new ArrayList<>();
        pipelineStepAudits.add(pipelineStepAudit);
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetQueueInfoRequest.class).withArguments(dspServiceClient, workflowId).thenReturn(getQueueInfoRequest);
        when(getQueueInfoRequest.executeSync()).thenReturn(queueInfoDTO);
        QueueInfoDTO expected = dspServiceClient.getQueueInfo(workflowId);
        assertEquals(expected, queueInfoDTO);
        verifyNew(GetQueueInfoRequest.class).withArguments(dspServiceClient, workflowId);
        verify(getQueueInfoRequest).executeSync();
    }

    @Test
    public void testSaveEventAudit() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateEventAuditRequest.class).withArguments(dspServiceClient, eventAudit).thenReturn(createEventAuditRequest);
        when(createEventAuditRequest.executeSync()).thenReturn(null);
        dspServiceClient.saveEventAudit(eventAudit);
        verifyNew(CreateEventAuditRequest.class).withArguments(dspServiceClient, eventAudit);
        verify(createEventAuditRequest).executeSync();
    }

    @Test
    public void testSaveExecutionEnvironmentSnapshotRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(CreateExecutionEnvironmentSnapshotRequest.class).withArguments(dspServiceClient,
                executionEnvironmentSnapshot).thenReturn(createExecutionEnvironmentSnapshotRequest);
        when(createExecutionEnvironmentSnapshotRequest.executeSync()).thenReturn(null);
        dspServiceClient.saveExecutionEnvironmentSnapshotRequest(executionEnvironmentSnapshot);
        verifyNew(CreateExecutionEnvironmentSnapshotRequest.class).withArguments(dspServiceClient, executionEnvironmentSnapshot);
        verify(createExecutionEnvironmentSnapshotRequest).executeSync();
    }

    @Test
    public void testGetExternalCredentials() throws Exception {
        String clientAlias = "clientAlias";
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetExternalCredentialsByClientAliasRequest.class).withArguments(dspServiceClient, clientAlias).thenReturn(getExternalCredentialsByClientAliasRequest);
        when(getExternalCredentialsByClientAliasRequest.executeSync()).thenReturn(externalCredentials);
        ExternalCredentials expected = dspServiceClient.getExternalCredentials(clientAlias);
        assertEquals(expected, externalCredentials);
        verifyNew(GetExternalCredentialsByClientAliasRequest.class).withArguments(dspServiceClient, clientAlias);
        verify(getExternalCredentialsByClientAliasRequest).executeSync();
    }

    @Test
    public void testSendEmailNotificationForPartitionStateChangeRequest() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(SendEmailNotificationForPartitionStateChangeRequest.class).withArguments(dspServiceClient, partitionDetailsEmailNotificationRequest)
                .thenReturn(sendEmailNotificationForPartitionStateChangeRequest);
        when(sendEmailNotificationForPartitionStateChangeRequest.executeSync()).thenReturn(null);
        dspServiceClient.sendEmailNotificationForPartitionStateChangeRequest(partitionDetailsEmailNotificationRequest);
        verifyNew(SendEmailNotificationForPartitionStateChangeRequest.class).withArguments(dspServiceClient, partitionDetailsEmailNotificationRequest);
        verify(sendEmailNotificationForPartitionStateChangeRequest).executeSync();
    }

    @Test
    public void testGetNotificationPreference() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetNotificationPreferenceRequest.class).withArguments(dspServiceClient, 1L).thenReturn(getNotificationPreferenceRequest);
        when(getNotificationPreferenceRequest.executeSync()).thenReturn(notificationPreference);
        NotificationPreference expected = dspServiceClient.getNotificationPreference(1L);
        assertEquals(expected, notificationPreference);
        verifyNew(GetNotificationPreferenceRequest.class).withArguments(dspServiceClient, 1L);
        verify(getNotificationPreferenceRequest).executeSync();
    }

    @Test
    public void testGetPipelineStepById() throws Exception {
        DSPServiceClient dspServiceClient = new DSPServiceClient(host, port, maxRetries, retryGapInMillis, scriptBaseDir, requestTimeoutMillis, asyncHttpClient);
        PowerMockito.whenNew(GetPipelineStepByIdRequest.class).withArguments(dspServiceClient, 1L).thenReturn(getPipelineStepByIdRequest);
        when(getPipelineStepByIdRequest.executeSync()).thenReturn(pipelineStep);
        PipelineStep expected = dspServiceClient.getPipelineStepById(1L);
        assertEquals(expected, pipelineStep);
        verifyNew(GetPipelineStepByIdRequest.class).withArguments(dspServiceClient, 1L);
        verify(getPipelineStepByIdRequest).executeSync();
    }

}
