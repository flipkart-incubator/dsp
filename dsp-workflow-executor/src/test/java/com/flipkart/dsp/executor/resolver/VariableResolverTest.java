package com.flipkart.dsp.executor.resolver;

import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.exception.ResolutionException;
import com.flipkart.dsp.models.ObjectOverride;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.mockito.Mockito.*;

public class VariableResolverTest {
    private VariableResolver variableResolver;
    private PartitionInfoResolver partitionInfoResolver = mock(PartitionInfoResolver.class);
    private DataFrameResolver dataFrameResolver = mock(DataFrameResolver.class);
    private ModelResolver modelResolver = mock(ModelResolver.class);
    private AuditVariableResolver auditVariableResolver = mock(AuditVariableResolver.class);

    @Before
    public void setup() {
        variableResolver = new VariableResolver(dataFrameResolver,partitionInfoResolver,modelResolver, auditVariableResolver, new PremitiveScriptVariableResolver());
    }

    @Test
    public void test1() throws ResolutionException {
        ConfigPayload configPayload = new ConfigPayload();
        configPayload.setFutureCSVLocation(new HashMap<>());
        configPayload.setCsvLocation(new HashMap<>());
        LocalScript script = new LocalScript();
        final HashSet<ScriptVariable> inputVariables = new HashSet<>();
        script.setInputVariables(inputVariables);
        PipelineStep pipelineStep = mock(PipelineStep.class);
        when(pipelineStep.getParentPipelineStepId()).thenReturn(null);
        when(pipelineStep.getName()).thenReturn("step1");
        final ArrayList<ObjectOverride> objectOverridesList = new ArrayList<>();

        String pipelineExecutionId = configPayload.getPipelineExecutionId();
        long refreshId = configPayload.getRefreshId();
        String scope = configPayload.getScope();
        Map<String, String> inputCsvHdfsLocation = configPayload.getCsvLocation();
        inputCsvHdfsLocation.putAll(configPayload.getFutureCSVLocation());
        final String parentWorkflowExecutionId = configPayload.getParentWorkflowExecutionId();

        String workflowName = "workflow1";
        String stepName = "step1";
        variableResolver.resolve(workflowName,10l, refreshId, inputCsvHdfsLocation,new HashMap<>(),
                new ArrayList<>(),scope,parentWorkflowExecutionId,pipelineExecutionId, script,pipelineStep, objectOverridesList);
        verify(partitionInfoResolver).resolve(scope, 0l, stepName);
        verify(dataFrameResolver).resolve(workflowName, 10l, refreshId, inputCsvHdfsLocation, new HashMap<>(), inputVariables, new ArrayList<>());
        verify(modelResolver).resolve(inputVariables, scope, parentWorkflowExecutionId);
    }
}
