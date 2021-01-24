package com.flipkart.dsp.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.cosmos.CosmosReporter;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.entities.enums.RequestStepType;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.flipkart.dsp.entities.workflow.*;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.helper.CephIngestionHelper;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.enums.WorkflowStepStateNotificationType;
import com.flipkart.dsp.utils.*;
import org.apache.commons.mail.EmailException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AzkabanOutputIngestionNode.class, WorkUnit.class})
public class AzkabanOutputIngestionNodeTest {

    @Mock
    private Request request;
    @Mock
    private Workflow workflow;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private WorkflowMeta workflowMeta;
    @Mock
    private RequestActor requestActor;
    @Mock
    private NodeMetaData nodeMetaData;
    @Mock
    private WorkFlowActor workFlowActor;
    @Mock
    private CosmosReporter cosmosReporter;
    @Mock
    private EventAuditUtil eventAuditUtil;
    @Mock
    private WorkflowDetails workflowDetails;
    @Mock
    private ScriptVariable cephScriptVariable;
    @Mock
    private RequestStepActor requestStepActor;
    @Mock
    private DSPServiceClient dspServiceClient;
    @Mock
    private RequestStepAudit requestStepAudit;
    @Mock
    private CephIngestionHelper cephIngestionHelper;
    @Mock
    private RequestStepAuditActor requestStepAuditActor;
    @Mock
    private EmailNotificationHelper emailNotificationHelper;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long requestStepId = 1L;
    private String workflowName = "workflowName";
    private AzkabanOutputIngestionNode azkabanOutputIngestionNode;
    private List<ScriptVariable> cephScriptVariables = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.azkabanOutputIngestionNode = spy(new AzkabanOutputIngestionNode(requestActor,
                objectMapper,
                workFlowActor,
                cosmosReporter,
                eventAuditUtil,
                dspServiceClient,
                requestStepActor,
                cephIngestionHelper,
                requestStepAuditActor,
                emailNotificationHelper));

        cephScriptVariables.add(cephScriptVariable);

        when(nodeMetaData.getRequestId()).thenReturn(requestId);
        doNothing().when(nodeMetaData).setPrevNode(anyString());
        when(workflowMeta.getHiveQueue()).thenReturn(PRODUCTION_HIVE_QUEUE);
        when(requestActor.getRequest(requestId)).thenReturn(request);

        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(workflowDetails.getCephOutputs()).thenReturn(cephScriptVariables);
    }

    @Test
    public void testGetName() {
        assertEquals(azkabanOutputIngestionNode.getName(), Constants.OUTPUT_INGESTION_NODE);
    }

    @Test
    public void testPerformActionSuccess() throws Exception {
        when(cephIngestionHelper.ingestInCeph(requestId, workflowDetails)).thenReturn(new ArrayList<>());
        doNothing().when(emailNotificationHelper).sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);

        azkabanOutputIngestionNode.performAction(requestStepId, nodeMetaData, workflowDetails);
        verify(nodeMetaData).getRequestId();
        verify(requestActor).getRequest(requestId);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getWorkflowMeta();
        verify(workflowMeta).getHiveQueue();
        verify(workflow).getName();
        verify(workflowDetails).getCephOutputs();
        verify(cephIngestionHelper).ingestInCeph(requestId, workflowDetails);
        verify(emailNotificationHelper).sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);
        verify(nodeMetaData).setPrevNode(anyString());

    }

    @Test
    public void testPerformActionFailure() throws Exception {
        boolean isException = false;
        List<String> errorMessages = new ArrayList<>();
        errorMessages.add("Error");
        when(cephIngestionHelper.ingestInCeph(requestId, workflowDetails)).thenReturn(errorMessages);
        doThrow(new EmailException()).when(emailNotificationHelper).sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);
        when(requestStepAuditActor.createRequestStepAudit(any())).thenReturn(requestStepAudit);
        doNothing().when(eventAuditUtil).creatOutputIngestionErrorEvent(requestId, workflowId, workflowName, String.join(",", errorMessages));

        try {
            azkabanOutputIngestionNode.performAction(requestStepId, nodeMetaData, workflowDetails);
        } catch (AzkabanException e) {
            isException = true;
            assertEquals(e.getMessage(), "Error\nError");
        }

        assertTrue(isException);
        verify(nodeMetaData).getRequestId();
        verify(requestActor).getRequest(requestId);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getWorkflowMeta();
        verify(workflowMeta).getHiveQueue();
        verify(workflow, times(2)).getName();
        verify(workflowDetails).getCephOutputs();
        verify(cephIngestionHelper).ingestInCeph(requestId, workflowDetails);
        verify(emailNotificationHelper).sendWorkflowStateChangeNotification(request, workflowDetails, WorkflowStepStateNotificationType.OUTPUT_INGESTION);
        verify(requestStepAuditActor).createRequestStepAudit(any());
        verify(eventAuditUtil).creatOutputIngestionErrorEvent(requestId, workflowId, workflowName, "Error,Error");
    }


    @Test
    public void testRequestStepType() {
        assertEquals(azkabanOutputIngestionNode.getRequestStepType(), RequestStepType.OI);
    }

    @Test
    public void testSetClientQueue() {
        azkabanOutputIngestionNode.setClientQueue();
    }
}
