package com.flipkart.dsp.executor.orchestrator;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPClientProcessingException;
import com.flipkart.dsp.engine.exception.ScriptExecutionEngineException;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.executor.exception.PersistenceException;
import com.flipkart.dsp.executor.exception.ResolutionException;
import com.flipkart.dsp.executor.persist.VariablePersister;
import com.flipkart.dsp.executor.resolver.VariableResolver;
import com.flipkart.dsp.executor.runner.ScriptRunner;
import com.flipkart.dsp.executor.utils.ScriptHelper;
import com.flipkart.dsp.models.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import com.flipkart.dsp.models.variables.RDataTable;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class WorkFlowOrchestrator {
    private final ScriptHelper scriptHelper;
    private final ScriptRunner scriptRunner;
    private final VariableResolver variableResolver;
    private final DSPServiceClient dspServiceClient;
    private final VariablePersister variablePersister;

    @Timed
    @Metered
    public void run(WorkflowDetails workflowDetails, ConfigPayload configPayload, PipelineStep pipelineStep)
            throws ScriptExecutionEngineException, ResolutionException, PersistenceException, TException, TableNotFoundException, DSPClientProcessingException {
        List<ObjectOverride> objectOverrideList = getObjectOverrides(configPayload.getRefreshId());
        LocalScript script = scriptHelper.importScript(pipelineStep.getScript());

        String pipelineExecutionId = configPayload.getPipelineExecutionId();
        long refreshId = configPayload.getRefreshId();
        String scope = configPayload.getScope();
        Map<String, String> inputCsvHdfsLocation = configPayload.getCsvLocation();
        inputCsvHdfsLocation.putAll(configPayload.getFutureCSVLocation());
        final String parentWorkflowExecutionId = configPayload.getParentWorkflowExecutionId();
        //---------------------------ConfigPayload should not cross this line ------------------------------------------

        String workflowName = workflowDetails.getWorkflow().getName();
        List<String> partitions = pipelineStep.getPartitions();
        Long parentWorkflowId = workflowDetails.getWorkflow().getParentWorkflowId();
        Set<DataFrame> dataFrames = workflowDetails.getWorkflow().getDataFrames();
        Map<String, DataFrame> dataFrameMap = dataFrames.stream().collect(toMap(DataFrame::getName, Function.identity()));

        script.getInputVariables().forEach(scriptVariable -> {
            if (scriptVariable.getDataType().equals(DataType.DATAFRAME) && scriptVariable.getAdditionalVariable() == null) {
                if (script.getImageLanguageEnum().equals(ImageLanguageEnum.R)) {
                    scriptVariable.setAdditionalVariable(new RDataTable());
                } else {
                    scriptVariable.setAdditionalVariable(new PandasDataFrame());
                }
            }

        });
        Set<ScriptVariable> resolvedVariableSet = variableResolver.resolve(workflowName, parentWorkflowId, refreshId,
                inputCsvHdfsLocation, dataFrameMap, partitions, scope, parentWorkflowExecutionId,
                pipelineExecutionId, script, pipelineStep, objectOverrideList);
        log.info("Resolved Variables: {}", resolvedVariableSet);
        Set<ScriptVariable> extractedScriptVariables = runScript(script, resolvedVariableSet);
        log.info("Extracted variables in local: {}", extractedScriptVariables);
        extractedScriptVariables = variablePersister.moveIntermediateVariablesToHDFS(extractedScriptVariables);
        log.info("Extracted variables in remote: {}", extractedScriptVariables);
        Set<ScriptVariable> persistedScriptVariables = variablePersister.persist(pipelineExecutionId, workflowName,
                extractedScriptVariables, partitions, refreshId, scope);
        log.info("Persisted Variables: {}", persistedScriptVariables);
        RunConfig runConfig = new RunConfig(extractedScriptVariables, persistedScriptVariables);
        dspServiceClient.createPipelineStepRuntimeConfig(pipelineStep.getId(), runConfig, configPayload);
    }

    @Timed
    @Metered
    protected Set<ScriptVariable> runScript(LocalScript script, Set<ScriptVariable> resolvedVariableSet) throws ScriptExecutionEngineException {
        return scriptRunner.run(script, resolvedVariableSet);
    }

    private List<ObjectOverride> getObjectOverrides(Long refreshId) {
        Request request = dspServiceClient.getRequest(refreshId);
        RequestOverride requestOverride = request.getData().getRequestOverride();
        return requestOverride == null ? new ArrayList<>() : requestOverride.getObjectOverrideList();
    }

}
