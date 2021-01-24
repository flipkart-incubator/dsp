package com.flipkart.dsp.api;

import com.flipkart.dsp.dto.WorkflowResponseDTO;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.Signal;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.nio.file.Paths;
import java.util.*;

import static java.util.stream.Collectors.toMap;

/**
 * +
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkFlowResponseAPI {
    private final PipelineStepResourceAPI pipelineStepResourceAPI;

    public WorkflowResponseDTO getWorkflowResponse(WorkflowDetails workflowDetails) {

        Workflow workflow = workflowDetails.getWorkflow();
        return WorkflowResponseDTO.builder().workflowName(workflow.getName()).version(workflow.getVersion())
                .isProd(workflow.getIsProd()).steps(getSteps(workflowDetails)).build();
    }

    private List<WorkflowResponseDTO.Steps> getSteps(WorkflowDetails workflowDetails) {
        List<WorkflowResponseDTO.Steps> steps = new ArrayList<>();
        workflowDetails.getPipelineSteps().forEach(step -> {
            WorkflowResponseDTO.Script script = WorkflowResponseDTO.Script.builder().executionEnv(step.getScript().getExecutionEnvironment())
                    .gitCommitId(step.getScript().getGitCommitId()).gitRepo(step.getScript().getGitRepo())
                    .gitLocation(Paths.get(step.getScript().getGitFolder(), step.getScript().getFilePath()).toString()).build();

            WorkflowResponseDTO.Steps pipelineStep = WorkflowResponseDTO.Steps.builder().name(step.getName()).script(script)
                    .resources(pipelineStepResourceAPI.wrapResources(step.getPipelineStepResources()))
                    .inputs(getInputDataFrameDetails(step, workflowDetails.getWorkflow().getDataFrames())).partitions(new HashSet<>(step.getPartitions()))
                    .outputs(step.getScript().getOutputVariables().stream().collect(toMap(ScriptVariable::getName, ScriptVariable::getOutputLocationDetailsList, (a, b) -> b)))
                    .build();
            steps.add(pipelineStep);
        });
        return steps;
    }

    private Map<String, WorkflowResponseDTO.InputParameter> getInputDataFrameDetails(PipelineStep pipelineStep, Set<DataFrame> dataFrames) {
        Map<String, WorkflowResponseDTO.InputParameter> inputs = new LinkedHashMap<>();

        pipelineStep.getScript().getInputVariables().forEach(scriptVariable -> {
            if (scriptVariable.getDataType() == DataType.DATAFRAME)
                inputs.put(scriptVariable.getName(), getInputDataFrame(scriptVariable.getName(), dataFrames));
            else {
                WorkflowResponseDTO.InputParameter variable = new WorkflowResponseDTO.Variables(scriptVariable.getDataType(), scriptVariable.getValue());
                inputs.put(scriptVariable.getName(), variable);
            }
        });
        return inputs;
    }

    private WorkflowResponseDTO.InputParameter getInputDataFrame(String inputVariableName, Set<DataFrame> dataFrames) {
        DataFrame sgDataFrame = dataFrames.stream().filter(dataFrame -> dataFrame.getName().equalsIgnoreCase(inputVariableName)).findFirst().orElse(null);
        if (Objects.isNull(sgDataFrame))
            return null;
        LinkedHashMap<String, WorkflowResponseDTO.ColumnDetails> columnDetails = new LinkedHashMap<>();
        sgDataFrame.getSignalGroup().getSignalMetas().forEach(signalMeta -> {
                    Signal signal = signalMeta.getSignal();
                    columnDetails.put(signal.getName(), new WorkflowResponseDTO.ColumnDetails(signal.getSignalDataType(), signal.getSignalBaseEntity()));
                }
        );
        return new WorkflowResponseDTO.InputDataFrame(sgDataFrame.getId(), columnDetails);
    }

}
