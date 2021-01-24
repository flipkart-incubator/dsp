package com.flipkart.dsp.executor.orchestrator;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.executor.persist.VariablePersister;
import com.flipkart.dsp.executor.resolver.VariableResolver;
import com.flipkart.dsp.executor.runner.ScriptRunner;
import com.flipkart.dsp.executor.utils.ScriptHelper;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.*;

public class WorkFlowOrchestratorTest {
    @Mock private Script script;
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private LocalScript localScript;
    @Mock private ScriptHelper scriptHelper;
    @Mock private PipelineStep pipelineStep;
    @Mock private ScriptRunner scriptRunner;
    @Mock private ConfigPayload configPayload;
    @Mock private ScriptVariable scriptVariable;
    @Mock private RequestOverride requestOverride;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private DSPServiceClient dspServiceClient;
    @Mock private VariableResolver variableResolver;
    @Mock private VariablePersister variablePersister;
    @Mock private ExecuteWorkflowRequest executeWorkflowRequest;

    private Long requestId = 1L;
    private String scope = "scope";
    private Long parentWorkflowId = 1L;
    private String workflowName = "workflowName";
    private String pipelineExecutionId = "pipelineExecutionId";
    private String parentWorkflowExecutionId = "parentWorkflowExecutionId";

    private WorkFlowOrchestrator workFlowOrchestrator;
    private Set<DataFrame> dataFrames = new HashSet<>();
    private List<String> partitions = new ArrayList<>();
    private Set<ScriptVariable> scriptVariables = new HashSet<>();
    private Map<String, DataFrame> dataFrameMap = new HashMap<>();
    private List<ObjectOverride> objectOverrides = new ArrayList<>();
    private Map<String, String> inputCsvHdfsLocation = new HashMap<>();


    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.workFlowOrchestrator = spy(new WorkFlowOrchestrator(scriptHelper, scriptRunner, variableResolver, dspServiceClient, variablePersister));

        dataFrames.add(dataFrame);
        String partition = "partition";
        partitions.add(partition);
        scriptVariables.add(scriptVariable);
        String dataFrameName = "dataFrameName";
        dataFrameMap.put(dataFrameName, dataFrame);

        when(configPayload.getScope()).thenReturn(scope);
        when(configPayload.getRefreshId()).thenReturn(requestId);
        when(configPayload.getPipelineExecutionId()).thenReturn(pipelineExecutionId);
        when(configPayload.getParentWorkflowExecutionId()).thenReturn(parentWorkflowExecutionId);

        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(pipelineStep.getScript()).thenReturn(script);
        when(pipelineStep.getPartitions()).thenReturn(partitions);

        when(request.getData()).thenReturn(executeWorkflowRequest);
        when(dspServiceClient.getRequest(requestId)).thenReturn(request);
        when(executeWorkflowRequest.getRequestOverride()).thenReturn(requestOverride);
        when(requestOverride.getObjectOverrideList()).thenReturn(objectOverrides);

        when(scriptHelper.importScript(script)).thenReturn(localScript);
        when(scriptRunner.run(localScript, scriptVariables)).thenReturn(scriptVariables);

        when(workflow.getName()).thenReturn(workflowName);
        when(workflow.getDataFrames()).thenReturn(dataFrames);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(workflow.getParentWorkflowId()).thenReturn(parentWorkflowId);

        when(localScript.getInputVariables()).thenReturn(scriptVariables);
        when(scriptVariable.getDataType()).thenReturn(DataType.DATAFRAME);
        when(variablePersister.moveIntermediateVariablesToHDFS(scriptVariables)).thenReturn(scriptVariables);
        when(variablePersister.persist(pipelineExecutionId, workflowName, scriptVariables, partitions, requestId, scope)).thenReturn(scriptVariables);
        when(variableResolver.resolve(workflowName, parentWorkflowId, requestId, inputCsvHdfsLocation, dataFrameMap, partitions,
                scope, parentWorkflowExecutionId, pipelineExecutionId, localScript, pipelineStep, objectOverrides)).thenReturn(scriptVariables);
    }

    @Test
    public void testRunSuccessCase1() throws Exception {
        when(localScript.getImageLanguageEnum()).thenReturn(ImageLanguageEnum.R);

        workFlowOrchestrator.run(workflowDetails, configPayload, pipelineStep);
        verify(dspServiceClient).getRequest(requestId);
        verify(request).getData();
        verify(executeWorkflowRequest).getRequestOverride();
        verify(requestOverride).getObjectOverrideList();
        verify(configPayload, times(2)).getRefreshId();
        verify(pipelineStep).getScript();
        verify(scriptHelper).importScript(script);
        verify(configPayload).getPipelineExecutionId();
        verify(workflowDetails, times(3)).getWorkflow();
        verify(workflow).getName();
        verify(workflow).getParentWorkflowId();
        verify(workflow).getDataFrames();
        verify(configPayload).getScope();
        verify(localScript).getInputVariables();
        verify(scriptVariable).getDataType();
        verify(variableResolver).resolve(workflowName, parentWorkflowId, requestId, inputCsvHdfsLocation, dataFrameMap, partitions,
                scope, parentWorkflowExecutionId, pipelineExecutionId, localScript, pipelineStep, objectOverrides);
        verify(scriptRunner).run(localScript, scriptVariables);
        verify(variablePersister).moveIntermediateVariablesToHDFS(scriptVariables);
        verify(variablePersister).persist(pipelineExecutionId, workflowName, scriptVariables, partitions, requestId, scope);
    }

    @Test
    public void testRunSuccessCase2() throws Exception {
        when(localScript.getImageLanguageEnum()).thenReturn(ImageLanguageEnum.PYTHON2);

        workFlowOrchestrator.run(workflowDetails, configPayload, pipelineStep);
        verify(dspServiceClient).getRequest(requestId);
        verify(request).getData();
        verify(executeWorkflowRequest).getRequestOverride();
        verify(requestOverride).getObjectOverrideList();
        verify(configPayload, times(2)).getRefreshId();
        verify(pipelineStep).getScript();
        verify(scriptHelper).importScript(script);
        verify(configPayload).getPipelineExecutionId();
        verify(workflowDetails, times(3)).getWorkflow();
        verify(workflow).getName();
        verify(workflow).getParentWorkflowId();
        verify(workflow).getDataFrames();
        verify(configPayload).getScope();
        verify(localScript).getInputVariables();
        verify(scriptVariable).getDataType();
        verify(variableResolver).resolve(workflowName, parentWorkflowId, requestId, inputCsvHdfsLocation, dataFrameMap, partitions,
                scope, parentWorkflowExecutionId, pipelineExecutionId, localScript, pipelineStep, objectOverrides);
        verify(scriptRunner).run(localScript, scriptVariables);
        verify(variablePersister).moveIntermediateVariablesToHDFS(scriptVariables);
        verify(variablePersister).persist(pipelineExecutionId, workflowName, scriptVariables, partitions, requestId, scope);
    }

}
