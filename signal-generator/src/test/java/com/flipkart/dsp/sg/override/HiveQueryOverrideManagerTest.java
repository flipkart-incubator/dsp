package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.models.overrides.HiveQueryDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.helper.HiveQueryOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.dsp.utils.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({})
public class HiveQueryOverrideManagerTest {
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private PipelineStep pipelineStep;
    @Mock private WorkflowMeta workflowMeta;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private DataFrameOverrideAudit dataFrameOverrideAudit;
    @Mock private HiveQueryOverrideHelper hiveQueryOverrideHelper;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;
    @Mock private HiveQueryDataframeOverride hiveQueryDataframeOverride;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long dataFrameId = 1L;
    private String query = "query";
    private String overrideHash = "overrideHash";
    private String workflowName = "workflowName";
    private String dataframeName = "dataFrameName";
    private HiveQueryOverrideManager hiveQueryOverrideManager;
    private Map<String, Long> tableInformation = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);

        this.hiveQueryOverrideManager = spy(new HiveQueryOverrideManager(dataframeName, requestId, workflowDetails,
                eventAuditUtil, hiveQueryDataframeOverride, dataFrameOverrideHelper, hiveQueryOverrideHelper, pipelineStep));

        when(dataFrame.getId()).thenReturn(dataFrameId);
        when(dataFrame.getName()).thenReturn(dataframeName);
        when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
        when(workflowMeta.getHiveQueue()).thenReturn(PRODUCTION_HIVE_QUEUE);

        when(hiveQueryDataframeOverride.getQuery()).thenReturn(query);
        when(dataFrameOverrideHelper.getDataFrameId(dataframeName, workflow)).thenReturn(1L);
        when(dataFrameOverrideHelper.getDataFrameByName(dataframeName, workflow)).thenReturn(dataFrame);
        doNothing().when(eventAuditUtil).createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName, query);
        when(hiveQueryOverrideHelper.getOverrideHash(hiveQueryDataframeOverride)).thenReturn(overrideHash);
    }

    @Test
    public void testCallCase1Success() throws Exception {
        when(dataFrameOverrideHelper.getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY)).thenReturn(dataFrameOverrideAudit);
        when(dataFrameOverrideHelper.reuseDataframeAudit(dataframeName, workflowDetails, dataFrameOverrideAudit, pipelineStep)).thenReturn("outputMetaData");

        Object expected = hiveQueryOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, "outputMetaData");

        verify(workflowDetails, times(5)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(hiveQueryDataframeOverride).getQuery();
        verify(eventAuditUtil).createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName, query);
        verify(dataFrameOverrideHelper).getDataFrameByName(dataframeName, workflow);
        verify(dataFrame).getId();
        verify(dataFrame).getName();
        verify(hiveQueryOverrideHelper).getOverrideHash(hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).getDataFrameId(dataframeName, workflow);
        verify(dataFrameOverrideHelper).getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY);
        verify(dataFrameOverrideHelper).reuseDataframeAudit(dataframeName, workflowDetails, dataFrameOverrideAudit, pipelineStep);
        verify(eventAuditUtil).createHiveQueryOverrideManagerReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, "outputMetaData");
    }

    @Test
    public void testCallCase2Success() throws Exception {
        String tableName = dataframeName + underscore + dataFrameId;
        when(dataFrameOverrideHelper.getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY)).thenThrow(new DSPClientException(null));
        when(hiveQueryOverrideHelper.getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride)).thenReturn("createColumnQuery");
        doNothing().when(dataFrameOverrideHelper).createHiveTable(anyString());
        when(hiveQueryOverrideHelper.executeQuery(HIVE_QUERY_DATABASE, tableName, PRODUCTION_HIVE_QUEUE, dataframeName, hiveQueryDataframeOverride)).thenReturn(tableInformation);
        doNothing().when(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId ,overrideHash,
                hiveQueryDataframeOverride, tableInformation, DataFrameOverrideType.HIVE_QUERY);
        doNothing().when(eventAuditUtil).createHiveQueryOverrideManagerEndDebugEvent(requestId, workflowId, workflowName, dataframeName,"outputMetaData");

        Object expected = hiveQueryOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, tableInformation);

        verify(workflowDetails, times(5)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(hiveQueryDataframeOverride).getQuery();
        verify(eventAuditUtil).createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName, query);
        verify(dataFrameOverrideHelper).getDataFrameByName(dataframeName, workflow);
        verify(dataFrame).getId();
        verify(dataFrame).getName();
        verify(hiveQueryOverrideHelper).getOverrideHash(hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).getDataFrameId(dataframeName, workflow);
        verify(dataFrameOverrideHelper).getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY);
        verify(hiveQueryOverrideHelper).getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).createHiveTable(anyString());
        verify(hiveQueryOverrideHelper).executeQuery(HIVE_QUERY_DATABASE, tableName, PRODUCTION_HIVE_QUEUE, dataframeName, hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId ,overrideHash,
                hiveQueryDataframeOverride, tableInformation, DataFrameOverrideType.HIVE_QUERY);
        verify(eventAuditUtil).createHiveQueryOverrideManagerEndDebugEvent(requestId, workflowId, workflowName, dataframeName,tableInformation.toString());
    }
    @Test
    public void testCallFailure() throws Exception {
        String tableName = dataframeName + underscore + dataFrameId;
        when(dataFrameOverrideHelper.getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY)).thenThrow(new DSPClientException(null));
        when(hiveQueryOverrideHelper.getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride)).thenReturn("createColumnQuery");
        doNothing().when(dataFrameOverrideHelper).createHiveTable(anyString());
        when(hiveQueryOverrideHelper.executeQuery(HIVE_QUERY_DATABASE, tableName, PRODUCTION_HIVE_QUEUE, dataframeName, hiveQueryDataframeOverride)).thenThrow(new DataframeOverrideException("Error"));
        doNothing().when(eventAuditUtil).createHiveQueryOverrideManagerErrorEvent(any(), any(), any(), any(), any(), any());

        boolean isException = false;
        try {
            hiveQueryOverrideManager.call();
        } catch (Exception e) {
            isException = true;
        }
        assertTrue(isException);;

        verify(workflowDetails, times(5)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(hiveQueryDataframeOverride).getQuery();
        verify(eventAuditUtil).createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName, query);
        verify(dataFrameOverrideHelper).getDataFrameByName(dataframeName, workflow);
        verify(dataFrame).getId();
        verify(dataFrame).getName();
        verify(hiveQueryOverrideHelper).getOverrideHash(hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).getDataFrameId(dataframeName, workflow);
        verify(dataFrameOverrideHelper).getDataFrameOverrideAudit(dataFrameId, overrideHash, DataFrameOverrideType.HIVE_QUERY);
        verify(hiveQueryOverrideHelper).getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride);
        verify(dataFrameOverrideHelper).createHiveTable(anyString());
        verify(hiveQueryOverrideHelper).executeQuery(HIVE_QUERY_DATABASE, tableName, PRODUCTION_HIVE_QUEUE, dataframeName, hiveQueryDataframeOverride);
        verify(eventAuditUtil).createHiveQueryOverrideManagerErrorEvent(any(), any(), any(), any(), any(), any());
    }
}
