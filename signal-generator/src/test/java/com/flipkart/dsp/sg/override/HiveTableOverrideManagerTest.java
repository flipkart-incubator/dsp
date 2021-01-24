package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideState;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.models.overrides.HiveDataframeOverride;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
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

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static com.flipkart.dsp.utils.Constants.dot;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({})
public class HiveTableOverrideManagerTest {
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private WorkflowMeta workflowMeta;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private HiveDataframeOverride hiveDataframeOverride;
    @Mock private DataFrameOverrideAudit dataFrameOverrideAudit;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;

    private String workflowName = "workflowName";
    private String dataframeName = "dataFrameName";
    private HiveTableOverrideManager hiveTableOverrideManager;
    private Map<String, Long> overrideTableInformation = new HashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(request.getId()).thenReturn(1L);
        when(workflow.getId()).thenReturn(1L);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);

        this.hiveTableOverrideManager = spy(new HiveTableOverrideManager(dataframeName, request, workflowDetails,
                eventAuditUtil, hiveDataframeOverride, dataFrameOverrideHelper));
        when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
        when(workflowMeta.getHiveQueue()).thenReturn(PRODUCTION_HIVE_QUEUE);
    }

    @Test
    public void testCall() throws Exception {
        String dbName = "test_db";
        String tableName = "test_table";
        when(hiveDataframeOverride.getDatabase()).thenReturn(dbName);
        when(hiveDataframeOverride.getTableName()).thenReturn(tableName);
        doNothing().when(eventAuditUtil).createHiveTableOverrideManagerStartDebugEvent(1L, 1L, workflowName
                , dataframeName, dbName, tableName);
        when(dataFrameOverrideHelper.getRefreshId(dbName + dot + tableName, hiveDataframeOverride,1L, PRODUCTION_HIVE_QUEUE)).thenReturn(1L);
        when(dataFrameOverrideHelper.getOverrideTableInformationForHive(1L, tableName, PRODUCTION_HIVE_QUEUE)).thenReturn(overrideTableInformation);
        when(dataFrameOverrideHelper.getDataFrameId(dataframeName, workflow)).thenReturn(1L);
        doNothing().when(dataFrameOverrideHelper).saveDataframeOverrideAudit(1L, 1L, 1L, "1",
                overrideTableInformation, overrideTableInformation, DataFrameOverrideType.HIVE);
        doNothing().when(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataFrameOverrideAudit, DataFrameOverrideState.SUCCEDED);
        doNothing().when(eventAuditUtil).createHiveTableOverrideManagerEndDebugEvent(1L, 1L,
                workflowName, dataframeName, dbName, tableName, 1L);

        Map<String, Long> expected = hiveTableOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, overrideTableInformation);

        verify(request).getId();
        verify(workflowDetails, times(4)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(hiveDataframeOverride, times(3)).getDatabase();
        verify(hiveDataframeOverride, times(3)).getTableName();
        verify(eventAuditUtil).createHiveTableOverrideManagerStartDebugEvent(1L, 1L, workflowName, dataframeName, dbName, tableName);
        verify(dataFrameOverrideHelper).getDataFrameId(dataframeName, workflow);
        verify(dataFrameOverrideHelper).getRefreshId(dbName + dot + tableName, hiveDataframeOverride,1L, PRODUCTION_HIVE_QUEUE);
        verify(dataFrameOverrideHelper).getOverrideTableInformationForHive(1L, dbName + dot + tableName, PRODUCTION_HIVE_QUEUE);
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(1L, 1L, 1L, "1",
                overrideTableInformation, overrideTableInformation, DataFrameOverrideType.HIVE);
        verify(eventAuditUtil).createHiveTableOverrideManagerEndDebugEvent(1L, 1L, workflowName, dataframeName, dbName, tableName, 1L);
    }
}
