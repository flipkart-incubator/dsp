package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Timestamp;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class EventAuditUtilTest {
    @Mock
    private Workflow workflow;
    @Mock
    private DataFrame dataFrame;
    @Mock
    private DataFrameAudit dataFrameAudit;
    @Mock
    private DSPServiceClient dspServiceClient;
    @Mock
    private PartitionDataframeOverride partitionDataframeOverride;

    private Long runId = 1L;
    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String query = "query";
    private String dbName = "test_db";
    private String payload = "payload";
    private String tableName = "test_table";
    private String errorMessage = "errorMessage";
    private String workflowName = "workflowName";
    private String dataFrameName = "dataFrameName";
    private String outputDetails = "outputDetails";
    private String outputMetaData = "outputMetaData";

    private EventAuditUtil eventAuditUtil;
    private Set<DataFrameAudit> dataFrameAudits = new HashSet<>();
    private Map<String, List<String>> dataframePartitions = new HashMap<>();
    private Map<String, DataframeOverride> dataframeOverrideMap = new HashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.eventAuditUtil = spy(new EventAuditUtil(dspServiceClient));

        dataFrameAudits.add(dataFrameAudit);
        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        doNothing().when(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDataFrameGenerationStartInfoEvent() {
        eventAuditUtil.createDataFrameGenerationStartInfoEvent(requestId, dataFrameName, new Timestamp(10L), workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDataFrameCompletionInfoEvent() {
        eventAuditUtil.createDataFrameCompletionInfoEvent(requestId, dataFrameName, new Timestamp(10L), 1L, workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDataframeQueryGenerationErrorEvent() {
        eventAuditUtil.createDataframeQueryGenerationErrorEvent(requestId, dataFrameName, "error", new Timestamp(10L), workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void CreateDataFrameQueryExecutionErrorEvent() {
        eventAuditUtil.createDataFrameQueryExecutionErrorEvent(requestId, dataFrameName, "error", new Timestamp(10L), workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void CreateDataFrameGenerationErrorEvent() {
        eventAuditUtil.createDataFrameGenerationErrorEvent(requestId, "error", workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void CreateAllDataFrameCompletionInfoEvent() {
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(dataFrame.getName()).thenReturn(dataFrameName);

        eventAuditUtil.createAllDataFrameCompletionInfoEvent(requestId, new Timestamp(10L), workflow);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void CreateAllDataFrameCompletionDebugEvent() {
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(dataFrameAudit.getDataframeSize()).thenReturn(1L);

        eventAuditUtil.createAllDataFrameCompletionDebugEvent(requestId, new Timestamp(10L), workflow, dataFrameAudits, dataframePartitions);
        verify(workflow).getId();
        verify(workflow).getName();
        verify(dataFrameAudit, times(2)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(dataFrameAudit).getDataframeSize();
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateOverrideStartDebugEvent() {
        eventAuditUtil.createOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeOverrideMap);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateOverrideEndDebugEvent() {
        eventAuditUtil.createOverrideEndDebugEvent(requestId, workflowId, workflowName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateRunIdOverrideStartDebugEvent() {
        eventAuditUtil.createRunIdOverrideStartDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateRunIdOverrideReusedDebugEvent() {
        eventAuditUtil.createRunIdOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataFrameName, runId);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateRunIdOverrideErrorEvent() {
        eventAuditUtil.createRunIdOverrideErrorEvent(requestId, workflowId, workflowName, dataFrameName, runId, errorMessage);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreatePartitionOverrideStartDebugEvent() {
        eventAuditUtil.createPartitionOverrideStartDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dspServiceClient).saveEventAudit(any());
    }


    @Test
    public void testCreatePartitionOverrideEndDebugEvent() {
        eventAuditUtil.createPartitionOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName, partitionDataframeOverride);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveTableOverrideManagerStartDebugEvent() {
        eventAuditUtil.createHiveTableOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataFrameName, dbName, tableName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveTableOverrideManagerEndDebugEvent() {
        Long refreshId = 1L;
        eventAuditUtil.createHiveTableOverrideManagerEndDebugEvent(requestId, workflowId, workflowName, dataFrameName, dbName, tableName, refreshId);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDefaultDataframeOverrideStartDebugEvent() {
        eventAuditUtil.createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDefaultDataframeOverrideReusedDebugEvent() {
        eventAuditUtil.createDefaultDataframeOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataFrameName, payload);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateDefaultDataframeOverrideForceRunDebugEvent() {
        eventAuditUtil.createDefaultDataframeOverrideForceRunDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveQueryOverrideManagerStartDebugEvent() {
        eventAuditUtil.createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataFrameName, query);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveQueryOverrideManagerReusedDebugEvent() {
        eventAuditUtil.createHiveQueryOverrideManagerReusedDebugEvent(requestId, workflowId, workflowName, dataFrameName, outputDetails);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveQueryOverrideManagerEndDebugEvent() {
        eventAuditUtil.createHiveQueryOverrideManagerEndDebugEvent(requestId, workflowId, workflowName, dataFrameName, outputDetails);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateHiveQueryOverrideManagerErrorEvent() {
        eventAuditUtil.createHiveQueryOverrideManagerErrorEvent(requestId, workflowId, workflowName, dataFrameName, query, errorMessage);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateCSVOverrideStartDebugEvent() {
        eventAuditUtil.createCSVOverrideStartDebugEvent(requestId, workflowId, workflowName, dataFrameName, query);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateCSVOverrideEndDebugEvent() {
        eventAuditUtil.createCSVOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dspServiceClient).saveEventAudit(any());
    }

    @Test
    public void testCreateCSVOverrideErrorEvent() {
        eventAuditUtil.createCSVOverrideErrorEvent(requestId, workflowId, workflowName, dataFrameName, query);
        verify(dspServiceClient).saveEventAudit(any());
    }
}
