package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.FTPDataframeOverride;
import com.flipkart.dsp.sg.helper.FileOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileOverrideManager.class})
public class FileOverrideManagerTest {
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private PipelineStep pipelineStep;
    @Mock private FileOverrideHelper fileOverrideHelper;
    @Mock private FTPDataframeOverride dataframeOverride;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String workflowName = "workflowName";
    private String dataframeName = "dataFrameName";
    private FileOverrideManager fileOverrideManager;
    private List<String> partitions = new ArrayList<>();

    @Before
    public void setUp() throws Exception{
        MockitoAnnotations.initMocks(this);

        when(request.getId()).thenReturn(1L);
        when(workflow.getId()).thenReturn(1L);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);

        this.fileOverrideManager = spy(new FileOverrideManager(dataframeName, request, workflowDetails, eventAuditUtil,
                dataframeOverride, fileOverrideHelper, pipelineStep));

        when(fileOverrideHelper.getHDFSPath(requestId, workflow, dataframeName, dataframeOverride, DataFrameOverrideType.FTP)).thenReturn("hdfsPath");
        doNothing().when(eventAuditUtil).createCSVOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName, "hdfsPath");
        when(fileOverrideHelper.getColumnMapping(dataframeOverride, DataFrameOverrideType.FTP)).thenReturn(new LinkedHashMap<>());
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(fileOverrideHelper.getPartitionColumnsInHeader(any(), anyList())).thenReturn(partitions);
        when(fileOverrideHelper.processNonPartitionedCSV(requestId, dataframeName, "hdfsPath",
                DataFrameOverrideType.FTP, workflowDetails, partitions)).thenReturn(dataFrameAudit);
    }

    @Test
    public void testCallCase1() throws Exception {

        DataFrameAudit expected = fileOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, dataFrameAudit);
        verify(request).getId();
        verify(workflowDetails).getWorkflow();
        verify(fileOverrideHelper).getHDFSPath(requestId, workflow, dataframeName, dataframeOverride, DataFrameOverrideType.FTP);
        verify(eventAuditUtil).createCSVOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName, "hdfsPath");
        verify(fileOverrideHelper).getColumnMapping(dataframeOverride, DataFrameOverrideType.FTP);
        verify(fileOverrideHelper).getPartitionColumnsInHeader(any(), anyList());
        verify(fileOverrideHelper).processNonPartitionedCSV(requestId, dataframeName, "hdfsPath",
                DataFrameOverrideType.FTP, workflowDetails, partitions);
    }
    @Test
    public void testCallCase2() throws Exception {
        partitions.add("column1");
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(fileOverrideHelper.getPartitionColumnsInHeader(any(), anyList())).thenReturn(partitions);
        when(fileOverrideHelper.moveDataFrameInHDFS(requestId, dataframeName, "hdfsPath", workflowDetails,
                dataframeOverride, DataFrameOverrideType.FTP, partitions)).thenReturn(dataFrameAudit);

        DataFrameAudit expected = fileOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, dataFrameAudit);
        verify(request).getId();
        verify(workflowDetails).getWorkflow();
        verify(fileOverrideHelper).getHDFSPath(requestId, workflow, dataframeName, dataframeOverride, DataFrameOverrideType.FTP);
        verify(eventAuditUtil).createCSVOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName, "hdfsPath");
        verify(fileOverrideHelper).getColumnMapping(dataframeOverride, DataFrameOverrideType.FTP);
        verify(fileOverrideHelper).getPartitionColumnsInHeader(any(), anyList());
        verify(fileOverrideHelper).moveDataFrameInHDFS(requestId, dataframeName, "hdfsPath", workflowDetails,
                dataframeOverride, DataFrameOverrideType.FTP, partitions);
    }

}
