package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.PipelineStepActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.api.dataFrame.GetDataFramesAPI;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.exception.CreateWorkflowException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.models.workflow.TrainingWorkflow;
import com.flipkart.dsp.utils.JsonUtils;
import com.flipkart.dsp.utils.WorkflowVersionComparator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static java.util.Objects.isNull;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowDetailsAPI {
    private final ScriptAPI scriptAPI;
    private final WorkFlowActor workFlowActor;
    private final GetDataFramesAPI getDataFramesAPI;
    private final PipelineStepActor pipelineStepActor;
    private final PipelineStepResourceAPI pipelineStepResourceAPI;

    public WorkflowDetails getLatestWorkflowDetails(String workflowName, String workflowGroupName, String version, boolean isProd) {
        List<Workflow> workFlows = workFlowActor.getWorkflow(workflowName, workflowGroupName, isProd);
        if (workFlows.size() != 0) {
            com.flipkart.dsp.entities.workflow.Workflow workflow = workFlows.stream().max(new WorkflowVersionComparator()).get();
            if (Objects.nonNull(version)) {
                Optional<Workflow> workflowEntityOptional = workFlows.stream().filter(workflow1 -> workflow1.getVersion().equalsIgnoreCase(version)).findFirst();
                if (!workflowEntityOptional.isPresent()) {
                    return null;
                }
                workflow = workflowEntityOptional.get();
            }
            List<com.flipkart.dsp.entities.pipelinestep.PipelineStep> pipelineSteps = pipelineStepActor.getPipelineStepsByWorkflowId(workflow.getId());
            return WorkflowDetails.builder().workflow(workflow).pipelineSteps(pipelineSteps).build();
        }
        return null;
    }


    WorkflowDetails convertToWorkflowDetails(String triggeredBy, CreateWorkflowRequest createWorkflowRequest)
            throws CreateWorkflowException, ValidationException {
        CreateWorkflowRequest.Workflow workflowRequest = createWorkflowRequest.getWorkflow();
        Workflow workflow = Workflow.builder().name(workflowRequest.getName()).isProd(workflowRequest.getIsProd())
                .retries(3).description(workflowRequest.getDescription()).parentWorkflowId(getParentWorkflowId(createWorkflowRequest))
                .workflowMeta(WorkflowMeta.builder().mesosQueue(createWorkflowRequest.getMesosQueue()).hiveQueue(createWorkflowRequest.getHiveQueue()).build())
                .dataFrames(getDataFramesAPI.convertDataFrames(workflowRequest.getDataframes())).workflowGroupName(createWorkflowRequest.getWorkflowGroupName())
                .defaultOverrides(JsonUtils.DEFAULT.toJson(createWorkflowRequest.getRequestOverride())).createdBy(triggeredBy).build();

        return WorkflowDetails.builder().workflow(workflow)
                .pipelineSteps(convertPipelineSteps(createWorkflowRequest.getWorkflow().getPipelineSteps())).build();
    }

    private Long getParentWorkflowId(CreateWorkflowRequest createWorkflowRequest) throws CreateWorkflowException {
        TrainingWorkflow trainingWorkflow = createWorkflowRequest.getTrainingWorkflow();
        if (!isNull(trainingWorkflow)) {
            WorkflowDetails parentWorkFlow = getLatestWorkflowDetails(trainingWorkflow.getWorkflowName(),
                    null, null, createWorkflowRequest.getWorkflow().getIsProd());
            if (Objects.isNull(parentWorkFlow)) {
                String error = String.format("Parent workflow Not found for workflowName %s,", trainingWorkflow.getWorkflowName());
                throw new CreateWorkflowException(error);
            }
            return parentWorkFlow.getWorkflow().getId();
        }
        return null;
    }


    private List<PipelineStep> convertPipelineSteps(List<CreateWorkflowRequest.PipelineStep> pipelineSteps) throws ValidationException {
        List<PipelineStep> pipelineStepList = new ArrayList<>();
        for (CreateWorkflowRequest.PipelineStep pipelineStep : pipelineSteps) {
            pipelineStepList.add(convertPipelineStep(pipelineStep));
        }
        return pipelineStepList;
    }

    private PipelineStep convertPipelineStep(CreateWorkflowRequest.PipelineStep pipelineStep) throws ValidationException {
        return PipelineStep.builder().name(pipelineStep.getName()).script(scriptAPI.prepareScript(pipelineStep.getScript()))
                .partitions(new ArrayList<>(pipelineStep.getPartitions())).prevStepName(pipelineStep.getParentStepName())
                .pipelineStepResources(pipelineStepResourceAPI.preparePipelineStepResource(pipelineStep.getResources())).build();
    }
}
