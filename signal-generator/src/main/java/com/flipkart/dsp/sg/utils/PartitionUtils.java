package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * +
 */
public class PartitionUtils {

    public static List<String> getPartitions(String dataFrameName, WorkflowDetails workflowDetails) {
        Optional<PipelineStep> pipelineStepOptional = workflowDetails.getPipelineSteps().stream().filter(pipelineStep ->
                pipelineStep.getScript().getInputVariables().stream().anyMatch(scriptVariable ->
                        scriptVariable.getName().equals(dataFrameName))).findFirst();
        return pipelineStepOptional.isPresent() ? pipelineStepOptional.get().getPartitions() : new ArrayList<>();
    }

}
