package com.flipkart.dsp.sg.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPServiceException;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.sg.core.*;
import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.externalentities.FTPEntity;
import com.flipkart.dsp.models.overrides.FTPDataframeOverride;
import com.flipkart.dsp.models.overrides.HiveDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.utils.DataframeSizeExtractor;
import com.flipkart.dsp.utils.SignalDataTypeMapper;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static com.flipkart.dsp.utils.Constants.dot;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataFrameOverrideHelper.class})
public class DataFrameOverrideHelperTest {
    @Mock
    private Path path;
    @Mock
    private Workflow workflow;
    @Mock
    private DataFrame dataFrame;
    @Mock
    private FTPEntity ftpEntity;
    @Mock
    private HiveClient hiveClient;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private PipelineStep pipelineStep;
    @Mock
    private DataFrameAudit dataFrameAudit;
    @Mock
    private WorkflowDetails workflowDetails;
    @Mock
    private SGUseCasePayload sgUseCasePayload;
    @Mock
    private DSPServiceClient dspServiceClient;
    @Mock
    private LocatedFileStatus locatedFileStatus;
    @Mock
    private ExternalCredentials externalCredentials;
    @Mock
    private FTPDataframeOverride ftpDataframeOverride;
    @Mock
    private SignalDataTypeMapper signalDataTypeMapper;
    @Mock
    private HiveDataframeOverride hiveDataframeOverride;
    @Mock
    private DataFrameOverrideAudit dataFrameOverrideAudit;
    @Mock
    private DataframeSizeExtractor dataframeSizeExtractor;
    @Mock
    private RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long dataFrameId = 1L;
    private Long dataFrameOverrideAuditId = 1L;

    private Object inputMetaData = "inputMetaData";
    private Object outputMetaData = "outputMetaData";

    private String db = "test_db";
    private String table = "test_table";
    private String inputDataId = "inputDataId";
    private String dataFrameName = "dataFrameName";
    private String hiveTableName = db + dot + table;
    private String partitionColumnName = "partition";
    private List<String> partitions = new ArrayList<>();
    private Set<DataFrame> dataFrames = new HashSet<>();
    private DataFrameOverrideHelper dataFrameOverrideHelper;
    private Map<String, Long> tableInformation = new HashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.dataFrameOverrideHelper = PowerMockito.spy(new DataFrameOverrideHelper(hiveClient, fileSystem, objectMapper, dspServiceClient,
                signalDataTypeMapper, dataframeSizeExtractor));
        partitions.add(partitionColumnName);
        dataFrames.add(dataFrame);

    }

    @Test
    public void testGetDataFrameAuditById() {
        Long dataFrameAuditRunId = 1L;
        when(dspServiceClient.getDataFrameAudit(dataFrameAuditRunId)).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dataFrameOverrideHelper.getDataFrameAuditById(dataFrameAuditRunId);
        assertEquals(expected, dataFrameAudit);
    }

    @Test
    public void testGetDataFrameKeys() {
        List<DataFrameKey> expected = dataFrameOverrideHelper.getDataFrameKeys(partitions);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
    }

    @Test
    public void testGetDataFrameValues() {
        LinkedHashSet<String> expected = dataFrameOverrideHelper.getDataframeValues("hdfsLocation");
        assertEquals(expected.size(), 1);
    }

    @Test
    public void testColumnMetaData() {
        LinkedHashMap<String, DataFrameColumnType> expected = dataFrameOverrideHelper.getColumnMetadata(partitions);
        assertNotNull(expected);
        assertTrue(expected.containsKey(partitionColumnName));
        assertEquals(expected.get(partitionColumnName), DataFrameColumnType.IN);
    }

    @Test
    public void testGetDataFrameSize() {
        when(dataframeSizeExtractor.getDataframeSize(sgUseCasePayload)).thenReturn(1L);
        assertEquals(dataFrameOverrideHelper.getDataFrameSize(sgUseCasePayload), 1L);
        verify(dataframeSizeExtractor).getDataframeSize(sgUseCasePayload);
    }

    @Test
    public void testSaveDataFrameAudit() throws Exception {
        when(dspServiceClient.saveDataFrameAudit(any())).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dataFrameOverrideHelper.saveDataFrameAudit(1L, dataFrameOverrideAuditId, partitions, dataFrame, sgUseCasePayload);
        assertEquals(expected, dataFrameAudit);
    }

    @Test
    public void testGetDataFrameId() {
        when(dataFrame.getId()).thenReturn(dataFrameId);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(workflow.getDataFrames()).thenReturn(dataFrames);
        Long expected = dataFrameOverrideHelper.getDataFrameId(dataFrameName, workflow);
        assertEquals(expected, dataFrameId);
        verify(workflow).getDataFrames();
        verify(dataFrame).getName();
        verify(dataFrame).getId();
    }

    @Test
    public void testGetLatestDataFrameAuditByDataFrameId() {
        when(dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameId)).thenReturn(dataFrameAudit);
        DataFrameAudit expected = dataFrameOverrideHelper.getLatestDataFrameAuditByDataFrameId(dataFrameId);
        assertEquals(expected, dataFrameAudit);
        verify(dspServiceClient).getLatestDataFrameAuditByDataFrameId(dataFrameId);
    }

    @Test
    public void testGetFTPClientCredentials() throws Exception {
        String clientAlias = "clientAlias";
        when(ftpDataframeOverride.getClientAlias()).thenReturn(clientAlias);
        when(dspServiceClient.getExternalCredentials(clientAlias)).thenReturn(externalCredentials);
        when(externalCredentials.getDetails()).thenReturn("details");
        when(objectMapper.readValue("details", FTPEntity.class)).thenReturn(ftpEntity);

        FTPEntity expected = dataFrameOverrideHelper.getFTPClientCredentials(ftpDataframeOverride);
        assertEquals(expected, ftpEntity);
        verify(ftpDataframeOverride).getClientAlias();
        verify(dspServiceClient).getExternalCredentials(clientAlias);
        verify(externalCredentials).getDetails();
        verify(objectMapper).readValue("details", FTPEntity.class);
    }

    @Test
    public void testSaveDataframeOverrideAuditCase1() throws Exception {
        String runResponse = "runResponse";
        when(objectMapper.writeValueAsString(runResponse)).thenReturn(runResponse);
        when(dspServiceClient.saveDataFrameOverrideAudit(any())).thenReturn(dataFrameOverrideAudit);

        DataFrameOverrideAudit expected = dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId, workflowId,
                dataFrameId, inputDataId, null, DataFrameOverrideType.HIVE_QUERY);
        assertEquals(expected, dataFrameOverrideAudit);
        verify(objectMapper).writeValueAsString(runResponse);
        verify(dspServiceClient).saveDataFrameOverrideAudit(any());
    }


    @Test
    public void testSaveDataframeOverrideAuditCase2() {
        when(dspServiceClient.saveDataFrameOverrideAudit(any())).thenReturn(dataFrameOverrideAudit);
        dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId, workflowId, dataFrameId, inputDataId, inputMetaData, outputMetaData, DataFrameOverrideType.HIVE);
        verify(dspServiceClient).saveDataFrameOverrideAudit(any());
    }

    @Test
    public void testUpdateDataframeOverrideAudit() {
        when(dspServiceClient.saveDataFrameOverrideAudit(any())).thenReturn(dataFrameOverrideAudit);
        dataFrameOverrideHelper.updateDataframeOverrideAudit(dataFrameOverrideAudit, DataFrameOverrideState.SUCCEDED);
        verify(dspServiceClient).saveDataFrameOverrideAudit(any());
    }


    @Test
    public void testSerializePayloadToString() throws Exception {
        when(objectMapper.writeValueAsString(sgUseCasePayload)).thenReturn("payload");
        assertEquals(dataFrameOverrideHelper.serializePayloadToString(sgUseCasePayload), "payload");
        verify(objectMapper).writeValueAsString(sgUseCasePayload);
    }

    @Test
    public void testGetDataFrameOverrideAudit() {
        when(dspServiceClient.getDataFrameOverrideAudit(dataFrameId, inputDataId, DataFrameOverrideType.FTP)).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dataFrameOverrideHelper.getDataFrameOverrideAudit(dataFrameId, inputDataId, DataFrameOverrideType.FTP);
        assertEquals(expected, dataFrameOverrideAudit);
        verify(dspServiceClient).getDataFrameOverrideAudit(dataFrameId, inputDataId, DataFrameOverrideType.FTP);
    }

    @Test
    public void testGetDataFrameOverrideAuditByRequestAndType() {
        when(dspServiceClient.getDataFrameOverrideAuditByIdRequestAndType(dataFrameId, requestId, DataFrameOverrideType.CSV)).thenReturn(dataFrameOverrideAudit);
        DataFrameOverrideAudit expected = dataFrameOverrideHelper.getDataFrameOverrideAuditByRequestAndType(dataFrameId, requestId, DataFrameOverrideType.CSV);
        assertEquals(expected, dataFrameOverrideAudit);
        verify(dspServiceClient).getDataFrameOverrideAuditByIdRequestAndType(dataFrameId, requestId, DataFrameOverrideType.CSV);
    }

    // Reuse Successful
    @Test
    public void testReuseDataframeAuditSuccessCase1() throws Exception {
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        doReturn(dataFrameId).when(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(dataFrameOverrideAudit.getRequestId()).thenReturn(requestId);
        when(dspServiceClient.getRequestStatusById(requestId)).thenReturn(RequestStatus.CREATED, RequestStatus.ACTIVE, RequestStatus.COMPLETED);
        when(dataFrameOverrideAudit.getState()).thenReturn(DataFrameOverrideState.STARTED, DataFrameOverrideState.STARTED, DataFrameOverrideState.SUCCEDED);
        when(dataFrameOverrideAudit.getId()).thenReturn(dataFrameOverrideAuditId);
        when(dspServiceClient.getDataFrameOverrideAuditById(dataFrameOverrideAuditId)).thenReturn(dataFrameOverrideAudit);
        when(objectMapper.writeValueAsString(partitions)).thenReturn("partitions");
        when(dspServiceClient.getDataFrameAudit(dataFrameId, dataFrameOverrideAuditId, "partitions")).thenReturn(dataFrameAudit);

        Object expected = dataFrameOverrideHelper.reuseDataframeAudit(dataFrameName, workflowDetails, dataFrameOverrideAudit, pipelineStep);
        assertEquals(expected, dataFrameAudit);

        verify(workflowDetails).getWorkflow();
        verify(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        verify(dataFrameOverrideAudit, times(3)).getRequestId();
        verify(dspServiceClient, times(3)).getRequestStatusById(requestId);
        verify(dataFrameOverrideAudit, times(4)).getState();
        verify(dataFrameOverrideAudit, times(4)).getId();
        verify(dspServiceClient, times(2)).getDataFrameOverrideAuditById(dataFrameOverrideAuditId);
        verify(objectMapper).writeValueAsString(partitions);
        verify(dspServiceClient).getDataFrameAudit(dataFrameId, dataFrameOverrideAuditId, "partitions");
    }

    // No Audit Found, not reusing
    @Test
    public void testReuseDataframeAuditSuccessCase2() throws Exception {
        Map<String, Long> tableMap = new HashMap<>();
        tableMap.put(dataFrameName, 1L);
        Error error = new Error(400, "Dataframe audit for DataFrame Id: " + dataFrameId + " not found!");
        String outputMetaData = objectMapper.writeValueAsString(tableMap);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        doReturn(dataFrameId).when(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(dataFrameOverrideAudit.getRequestId()).thenReturn(requestId);
        when(dspServiceClient.getRequestStatusById(requestId)).thenReturn(RequestStatus.CREATED, RequestStatus.ACTIVE, RequestStatus.COMPLETED);
        when(dataFrameOverrideAudit.getState()).thenReturn(DataFrameOverrideState.STARTED, DataFrameOverrideState.STARTED, DataFrameOverrideState.SUCCEDED);
        when(dataFrameOverrideAudit.getId()).thenReturn(dataFrameOverrideAuditId);
        when(dspServiceClient.getDataFrameOverrideAuditById(dataFrameOverrideAuditId)).thenReturn(dataFrameOverrideAudit);
        when(objectMapper.writeValueAsString(partitions)).thenReturn("partitions");
        when(dspServiceClient.getDataFrameAudit(dataFrameId, dataFrameOverrideAuditId, "partitions")).thenThrow(new DSPServiceException(error));
        when(dataFrameOverrideAudit.getOutputMetadata()).thenReturn(outputMetaData);

        dataFrameOverrideHelper.reuseDataframeAudit(dataFrameName, workflowDetails, dataFrameOverrideAudit, pipelineStep);

        verify(workflowDetails).getWorkflow();
        verify(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        verify(dataFrameOverrideAudit, times(3)).getRequestId();
        verify(dspServiceClient, times(3)).getRequestStatusById(requestId);
        verify(dataFrameOverrideAudit, times(4)).getState();
        verify(dataFrameOverrideAudit, times(4)).getId();
        verify(dspServiceClient, times(2)).getDataFrameOverrideAuditById(dataFrameOverrideAuditId);
        verify(objectMapper).writeValueAsString(partitions);
        verify(dspServiceClient).getDataFrameAudit(dataFrameId, dataFrameOverrideAuditId, "partitions");
        verify(dataFrameOverrideAudit).getOutputMetadata();
    }

    @Test
    public void testReuseDataframeAuditFailure() throws Exception {
        Map<String, Long> tableMap = new HashMap<>();

        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        doReturn(dataFrameId).when(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(dataFrameOverrideAudit.getRequestId()).thenReturn(requestId);
        when(dspServiceClient.getRequestStatusById(requestId)).thenReturn(RequestStatus.CREATED, RequestStatus.ACTIVE, RequestStatus.COMPLETED);
        when(dataFrameOverrideAudit.getState()).thenReturn(DataFrameOverrideState.STARTED, DataFrameOverrideState.STARTED, DataFrameOverrideState.STARTED);
        when(dataFrameOverrideAudit.getId()).thenReturn(dataFrameOverrideAuditId);
        when(dspServiceClient.getDataFrameOverrideAuditById(dataFrameOverrideAuditId)).thenReturn(dataFrameOverrideAudit);

        boolean isException = false;
        try {
            dataFrameOverrideHelper.reuseDataframeAudit(dataFrameName, workflowDetails, dataFrameOverrideAudit, pipelineStep);
        } catch (IllegalStateException e) {
            isException = true;
            assertEquals(e.getMessage(), "Dependent job got killed while piggybacking");
        }

        assertTrue(isException);
        verify(workflowDetails).getWorkflow();
        verify(dataFrameOverrideHelper).getDataFrameId(dataFrameName, workflow);
        verify(dataFrameOverrideAudit, times(3)).getRequestId();
        verify(dspServiceClient, times(3)).getRequestStatusById(requestId);
        verify(dataFrameOverrideAudit, times(4)).getState();
        verify(dataFrameOverrideAudit, times(2)).getId();
        verify(dspServiceClient, times(2)).getDataFrameOverrideAuditById(dataFrameOverrideAuditId);
    }

    @Test
    public void testGetOverrideTableInformationForHiveSuccessCase1() throws Exception {
        when(hiveClient.getLatestRefreshId(table)).thenReturn(1L);
        Map<String, Long> overrideTableInformation = dataFrameOverrideHelper.getOverrideTableInformationForHive(null, table, PRODUCTION_HIVE_QUEUE);
        assertEquals(overrideTableInformation.get(table).longValue(), 1L);
        verify(hiveClient).getLatestRefreshId(table);
    }

    @Test
    public void testGetOverrideTableInformationForHiveSuccessCase2() throws Exception {
        Map<String, Long> overrideTableInformation = dataFrameOverrideHelper.getOverrideTableInformationForHive(requestId, table, PRODUCTION_HIVE_QUEUE);
        assertEquals(overrideTableInformation.get(table).longValue(), 1L);
    }

    @Test
    public void testGetOverrideTableInformationForHiveFailure() throws Exception {
        when(hiveClient.getLatestRefreshId(table)).thenThrow(new HiveClientException("Error"));

        boolean isException = false;
        try {
            dataFrameOverrideHelper.getOverrideTableInformationForHive(null, table, PRODUCTION_HIVE_QUEUE);
        } catch (HiveClientException e) {
            isException = true;
            assertEquals(e.getMessage(), "Failed to fetch latest refreshId for hive table!");
        }
        assertTrue(isException);
        verify(hiveClient).getLatestRefreshId(table);
    }

    @Test
    public void testGetInputMetaData() throws Exception {
        when(objectMapper.writeValueAsString(tableInformation)).thenReturn(inputMetaData.toString());
        assertEquals(dataFrameOverrideHelper.getInputMetaData(tableInformation), inputMetaData.toString());
        verify(objectMapper).writeValueAsString(tableInformation);
    }

    @Test
    public void testCreateHiveTableSuccess() throws Exception {
        String createQuery = "createQuery";
        doNothing().when(hiveClient).executeQuery(createQuery);
        dataFrameOverrideHelper.createHiveTable(createQuery);
        verify(hiveClient).executeQuery(createQuery);
    }

    @Test
    public void testCreateHiveTableFailure() throws Exception {
        String createQuery = "createQuery";
        doThrow(new HiveClientException("Error")).when(hiveClient).executeQuery(createQuery);
        boolean isException = false;
        try {
            dataFrameOverrideHelper.createHiveTable(createQuery);
        } catch (HiveClientException e) {
            isException = true;
            assertEquals(e.getMessage(), "One of the Hadoop Dataset override failed!");
        }
        assertTrue(isException);
        verify(hiveClient).executeQuery(createQuery);
    }

    // intermediate  true
    @Test
    public void testGetRefreshIdSuccessCase1() throws Exception {
        when(hiveDataframeOverride.getIsIntermediate()).thenReturn(true);
        Long expected = dataFrameOverrideHelper.getRefreshId(hiveTableName, hiveDataframeOverride, requestId, PRODUCTION_HIVE_QUEUE);
        assertEquals(expected, requestId);
        verify(hiveDataframeOverride).getIsIntermediate();
    }

    // intermediate  false, refresh_id not null in override
    @Test
    public void testGetRefreshIdSuccessCase2() throws Exception {
        when(hiveDataframeOverride.getIsIntermediate()).thenReturn(false);
        when(hiveDataframeOverride.getRefreshId()).thenReturn(requestId);
        Long expected = dataFrameOverrideHelper.getRefreshId(hiveTableName, hiveDataframeOverride, requestId, PRODUCTION_HIVE_QUEUE);
        assertEquals(expected, requestId);
        verify(hiveDataframeOverride).getIsIntermediate();
        verify(hiveDataframeOverride).getRefreshId();
    }

    // intermediate  false, refresh_id null in override
    @Test
    public void testGetRefreshIdSuccessCase3() throws Exception {
        when(hiveDataframeOverride.getIsIntermediate()).thenReturn(false);
        when(hiveDataframeOverride.getRefreshId()).thenReturn(null);
        when(hiveClient.getLatestRefreshId(hiveTableName)).thenReturn(requestId);
        Long expected = dataFrameOverrideHelper.getRefreshId(hiveTableName, hiveDataframeOverride, requestId, PRODUCTION_HIVE_QUEUE);
        assertEquals(expected, requestId);
        verify(hiveDataframeOverride).getIsIntermediate();
        verify(hiveDataframeOverride).getRefreshId();
        verify(hiveClient).getLatestRefreshId(hiveTableName);
    }

    // intermediate  false, refresh_id null in override
    @Test
    public void testGetRefreshIdFailure() throws Exception {
        boolean isException = false;
        when(hiveDataframeOverride.getIsIntermediate()).thenReturn(false);
        when(hiveDataframeOverride.getRefreshId()).thenReturn(null);
        when(hiveClient.getLatestRefreshId(hiveTableName)).thenThrow(new HiveClientException("Error"));

        try {
            dataFrameOverrideHelper.getRefreshId(hiveTableName, hiveDataframeOverride, requestId, PRODUCTION_HIVE_QUEUE);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Failed to fetch latest refreshId for hive table!");
        }

        assertTrue(isException);
        verify(hiveDataframeOverride).getIsIntermediate();
        verify(hiveDataframeOverride).getRefreshId();
        verify(hiveClient).getLatestRefreshId(hiveTableName);
    }
}
