package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.DefaultDataframeOverride;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class DefaultDataFrameOverrideManagerTest {
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private SGUseCasePayload sgUseCasePayload;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;
    @Mock private DefaultDataframeOverride defaultDataframeOverride;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String workflowName = "workflowName";
    private String dataframeName = "dataFrameName";
    private DefaultDataframeOverrideManager defaultDataframeOverrideManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(request.getId()).thenReturn(requestId);
        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        this.defaultDataframeOverrideManager = spy(new DefaultDataframeOverrideManager(dataframeName, request, workflowDetails,
                eventAuditUtil, defaultDataframeOverride, dataFrameOverrideHelper));
    }

    @Test
    public void testCallCase1() throws Exception {
        doNothing().when(eventAuditUtil).createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        when(defaultDataframeOverride.getForceRun()).thenReturn(true);
        doNothing().when(eventAuditUtil).createDefaultDataframeOverrideForceRunDebugEvent(requestId, workflowId, workflowName, dataframeName);

        Optional<DataFrameAudit> expected = defaultDataframeOverrideManager.call();
        assertFalse(expected.isPresent());
        verify(request).getId();
        verify(workflowDetails, times(2)).getWorkflow();
        verify(workflow, times(1)).getId();
        verify(workflow, times(1)).getName();
        verify(eventAuditUtil).createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        verify(defaultDataframeOverride).getForceRun();
        verify(eventAuditUtil).createDefaultDataframeOverrideForceRunDebugEvent(requestId, workflowId, workflowName, dataframeName);
    }

    @Test
    public void testCallCase2() throws Exception {
        doNothing().when(eventAuditUtil).createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        when(defaultDataframeOverride.getForceRun()).thenReturn(false);
        when(dataFrameOverrideHelper.getDataFrameId(dataframeName, workflow)).thenReturn(1L);
        when(dataFrameOverrideHelper.getLatestDataFrameAuditByDataFrameId(1L)).thenReturn(dataFrameAudit);
        when(dataFrameAudit.getPayload()).thenReturn(sgUseCasePayload);
        when(dataFrameOverrideHelper.serializePayloadToString(sgUseCasePayload)).thenReturn("payload");
        doNothing().when(eventAuditUtil).createDefaultDataframeOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, "payload");

        Optional<DataFrameAudit> expected = defaultDataframeOverrideManager.call();
        assertTrue(expected.isPresent());
        assertEquals(expected.get(), dataFrameAudit);
        verify(request).getId();
        verify(workflowDetails, times(3)).getWorkflow();
        verify(workflow, times(1)).getId();
        verify(workflow, times(1)).getName();
        verify(eventAuditUtil).createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        verify(defaultDataframeOverride).getForceRun();
        verify(dataFrameOverrideHelper).getDataFrameId(dataframeName, workflow);
        verify(dataFrameOverrideHelper).getLatestDataFrameAuditByDataFrameId(1L);
        verify(dataFrameAudit).getPayload();
        verify(dataFrameOverrideHelper).serializePayloadToString(sgUseCasePayload);
        verify(eventAuditUtil).createDefaultDataframeOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, "payload");
    }
}
