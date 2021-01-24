package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PartitionOverrideManager.class})
public class PartitionOverrideManagerTest {
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private PipelineStep pipelineStep;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;

    private String workflowName = "workflowName";
    private String dataframeName = "dataFrameName";
    private List<String> partitions = new ArrayList<>();
    private PartitionOverrideManager partitionOverrideManager;
    private Map<String, DataFrame> dataFrameMap = new HashMap<>();
    private PartitionDataframeOverride partitionDataframeOverride = new PartitionDataframeOverride();

    @Before
    public void setUp() {
        partitionDataframeOverride.put("column1", "hdfsLocation");
        MockitoAnnotations.initMocks(this);

        when(request.getId()).thenReturn(1L);
        when(workflow.getId()).thenReturn(1L);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        this.partitionOverrideManager = spy(new PartitionOverrideManager(dataframeName, request, workflowDetails,
                eventAuditUtil, partitionDataframeOverride, dataFrameOverrideHelper, pipelineStep));

        dataFrameMap.put(dataframeName, dataFrame);
    }

    @Test
    public void testCall() throws Exception {
        doNothing().when(eventAuditUtil).createPartitionOverrideStartDebugEvent(1L, 1L, workflowName, dataframeName);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(dataFrameOverrideHelper.getDataFrameMap(workflow)).thenReturn(dataFrameMap);
        when(dataFrameOverrideHelper.getDataFrameKeys(partitions)).thenReturn(new ArrayList<>());
        when(dataFrameOverrideHelper.getDataframeValues("hdfsLocation")).thenReturn(new LinkedHashSet<>());
        when(dataFrameOverrideHelper.getColumnMetadata(partitions)).thenReturn(new LinkedHashMap<>());
        when(dataFrameOverrideHelper.getDataFrameSize(any())).thenReturn(1L);
        doNothing().when(eventAuditUtil).createPartitionOverrideEndDebugEvent(1L, 1L, workflowName, dataframeName, partitionDataframeOverride);
        when(dataFrameOverrideHelper.saveDataFrameAudit(any(), any(), any(), any(), any())).thenReturn(dataFrameAudit);

        DataFrameAudit expected = partitionOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, dataFrameAudit);
        verify(request).getId();
        verify(workflowDetails).getWorkflow();
        verify(workflow, times(2)).getId();
        verify(workflow, times(2)).getName();
        verify(eventAuditUtil).createPartitionOverrideStartDebugEvent(1L, 1L, workflowName, dataframeName);
        verify(dataFrameOverrideHelper).getDataFrameMap(workflow);
        verify(dataFrameOverrideHelper).getDataFrameKeys(partitions);
        verify(dataFrameOverrideHelper).getDataframeValues("hdfsLocation");
        verify(dataFrameOverrideHelper).getColumnMetadata(partitions);
        verify(dataFrameOverrideHelper).getDataFrameSize(any());
        verify(eventAuditUtil).createPartitionOverrideEndDebugEvent(any(), any(), any(), any(), any());
        verify(dataFrameOverrideHelper).saveDataFrameAudit(any(), any(), any(), any(), any());
    }
}
