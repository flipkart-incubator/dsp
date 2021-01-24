package com.flipkart.dsp.executor.resolver;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.script.LocalScript;
import com.flipkart.dsp.executor.exception.*;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ObjectOverride;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.ScriptVariableOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.dsp.models.DataType.*;
import static java.util.stream.Collectors.toMap;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class VariableResolver {

    private final DataFrameResolver dataFrameResolver;
    private final PartitionInfoResolver partitionInfoResolver;
    private final ModelResolver modelResolver;
    private final AuditVariableResolver auditVariableResolver;
    private final PremitiveScriptVariableResolver premitiveScriptVariableResolver;
    private Set<DataType> PRIMITIVE_DATAYPES = new HashSet<DataType>() {
        {
            add(STRING);
            add(INT);
            add(DOUBLE);
            add(BOOLEAN);
            add(DATE_TIME);
            add(DATE);
            add(LONG);
        }
    };


    public Set<ScriptVariable> resolve(String workflowName, Long parentWorkflowId, Long refreshId,
                                       Map<String, String> inputCsvHdfsLocation, Map<String, DataFrame> dataFrameMap
            , List<String> partitions, String scope, String parentWorkflowExecutionId, String pipelineExecutionId,
                                       LocalScript script, PipelineStep pipelineStep, @NotNull List<ObjectOverride> objectOverridesList)
            throws ResolutionException {
        if (pipelineStep.getParentPipelineStepId() == null) {
            return resolveAsFirstPipelineStep(workflowName, parentWorkflowId, refreshId, inputCsvHdfsLocation,
                    dataFrameMap, partitions, scope, parentWorkflowExecutionId, script, objectOverridesList,
                    pipelineStep.getName());
        } else {
            return resolveAsFirstPipelineStep(workflowName, parentWorkflowId, refreshId, inputCsvHdfsLocation,
                    dataFrameMap, partitions, scope, parentWorkflowExecutionId, script, objectOverridesList,
                    pipelineStep.getName());
//            return resolveFromAudit(pipelineExecutionId, scope, parentWorkflowExecutionId, parentPipelineStepEntity, scriptEntity);
        }
    }

    private Set<ScriptVariable> resolveFromAudit(String pipelineExecutionId, String scope, final String parentWorkflowExecutionId,
                                                 PipelineStep pipelineStep, LocalScript script)
            throws AuditVariableResolutionException {
        return auditVariableResolver.resolve( pipelineExecutionId, scope, parentWorkflowExecutionId, pipelineStep, script);
    }

    private Set<ScriptVariable> resolveAsFirstPipelineStep(String workflowName,Long parentWorkflowId,Long refreshId,
                                                           Map<String, String> inputCsvHdfsLocation,Map<String, DataFrame> dataFrameMap
                                                           ,List<String> partitions, String scope, String parentWorkflowExecutionId,
                                                           LocalScript script, List<ObjectOverride> objectOverridesList,
                                                           String pipelineStepName)
            throws ModelResolutionException, DataframeResolutionException, ScriptVariableResolutionException {
        Set<ScriptVariable> inputScriptVariables = script.getInputVariables();
        Set<ScriptVariable> resolvedInputVariables = partitionInfoResolver.resolve(scope, refreshId, pipelineStepName);
        resolvedInputVariables.addAll(resolveDataFrames(workflowName, parentWorkflowId, refreshId, inputCsvHdfsLocation,
                dataFrameMap, inputScriptVariables, partitions));
        resolvedInputVariables.addAll(resolveModel(inputScriptVariables, scope, parentWorkflowExecutionId));
        resolvedInputVariables.addAll(resolveStandardDataTypes(inputScriptVariables,objectOverridesList));
        return resolvedInputVariables;
    }

    private Set<ScriptVariable> resolveDataFrames(String workflowName, Long parentWorkflowId, Long refreshId,
                                                  Map<String, String> inputCsvHdfsLocation, Map<String, DataFrame> dataFrameMap,
                                                  Set<ScriptVariable> inputVariables, List<String> partitions)
            throws DataframeResolutionException {
        return dataFrameResolver.resolve(workflowName, parentWorkflowId, refreshId, inputCsvHdfsLocation,
                dataFrameMap, inputVariables, partitions);
    }

    private Set<ScriptVariable> resolveModel(Set<ScriptVariable> scriptVariableSet, String scope, final String parentWorkflowExecutionId)
            throws ModelResolutionException {
        return modelResolver.resolve(scriptVariableSet, scope, parentWorkflowExecutionId);
    }

    private Set<ScriptVariable> resolveStandardDataTypes(Set<ScriptVariable> scriptVariableSet, List<ObjectOverride> objectOverrideList)
            throws ScriptVariableResolutionException {

        final Map<String, ScriptVariableOverride> scriptVariableOverrideMap = objectOverrideList.stream()
                .map(objectOverride -> (ScriptVariableOverride)objectOverride)
                .collect(Collectors.toMap(ScriptVariableOverride::getName, Function.identity(),(x,y)->x,HashMap::new));

        final Set<ScriptVariable> scriptVariables;
        try {
            scriptVariables = scriptVariableSet.stream()
                    .filter(scriptVariable -> PRIMITIVE_DATAYPES.contains(scriptVariable.getDataType()))
                    .map(scriptVariable -> {
                        return premitiveScriptVariableResolver.resolve(scriptVariableOverrideMap, scriptVariable);
                    }).collect(Collectors.toSet());
        } catch (RuntimeException e) {
            throw new ScriptVariableResolutionException(e.getMessage());
        }

        return scriptVariables;
    }

}
