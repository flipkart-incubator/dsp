package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.RunIdDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class RunIdDataFrameOverrideManagerTest {

    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private RunIdDataframeOverride runIdDataframeOverride;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;

    private Long runId = 1L;
    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String workflowName = "workFlowName";
    private String dataframeName = "dataFrameName";
    private RunIdDataFrameOverrideManager runIdDataFrameOverrideManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(request.getId()).thenReturn(requestId);
        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);

        this.runIdDataFrameOverrideManager = spy(new RunIdDataFrameOverrideManager(dataframeName, request, workflowDetails,
                eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper));

        doNothing().when(eventAuditUtil).createRunIdOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        when(runIdDataframeOverride.getRunId()).thenReturn(runId);
        when(dataFrameOverrideHelper.getDataFrameAuditById(runId)).thenReturn(dataFrameAudit);
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(dataFrame.getName()).thenReturn(dataframeName);
    }

    @Test
    public void testCallSuccess() {
        doNothing().when(eventAuditUtil).createRunIdOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, runId);

        DataFrameAudit expected = runIdDataFrameOverrideManager.call();
        assertNotNull(expected);
        assertEquals(expected, dataFrameAudit);
        verify(request).getId();
        verify(workflowDetails, times(2)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(eventAuditUtil).createRunIdOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        verify(runIdDataframeOverride).getRunId();
        verify(dataFrameOverrideHelper).getDataFrameAuditById(runId);
        verify(dataFrameAudit).getDataFrame();
        verify(dataFrame).getName();
        verify(eventAuditUtil).createRunIdOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, runId);
    }

    @Test
    public void testCallFailure() {
        when(dataFrame.getName()).thenReturn(workflowName);
        String errorMessage = "DataFrame Run Id: "+ runId + "is not of type: "+ dataframeName;
        doNothing().when(eventAuditUtil).createRunIdOverrideErrorEvent(requestId, workflowId, workflowName, dataframeName, runId, errorMessage);

        boolean isException = false;
        try {
            runIdDataFrameOverrideManager.call();
        } catch (IllegalArgumentException e) {
            isException = true;
            assertEquals(e.getMessage(), errorMessage);
        }

        assertTrue(isException);
        verify(request).getId();
        verify(workflowDetails, times(2)).getWorkflow();
        verify(workflow).getId();
        verify(workflow).getName();
        verify(eventAuditUtil).createRunIdOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        verify(runIdDataframeOverride).getRunId();
        verify(dataFrameOverrideHelper).getDataFrameAuditById(runId);
        verify(dataFrameAudit, times(2)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(eventAuditUtil).createRunIdOverrideErrorEvent(requestId, workflowId, workflowName, workflowName, runId, errorMessage);
    }

}
