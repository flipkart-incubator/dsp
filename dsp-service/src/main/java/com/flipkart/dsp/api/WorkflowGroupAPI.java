package com.flipkart.dsp.api;

import com.flipkart.dsp.actors.NotificationPreferencesActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.TrainingWorkflow;
import com.flipkart.dsp.models.WorkflowGroupCreateDetails;
import com.flipkart.dsp.models.WorkflowGroupExecuteRequest;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.logging.log4j.util.Strings.isNotBlank;

/**
 */

@Singleton
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowGroupAPI {
    private final WorkFlowActor workFlowActor;
    private final WorkflowVersionAPI workflowVersionAPI;
    private final PipelineStepResourceAPI pipelineStepResourceAPI;
    private final NotificationPreferencesActor notificationPreferencesActor;

    public ExecuteWorkflowRequest convertToWorkflowExecuteRequest(Boolean isProd, Double version,
                                                                  WorkflowGroupExecuteRequest workflowGroupExecuteRequest)  throws DSPSvcException {
        validateNoOfWorkflow(workflowGroupExecuteRequest.getOverrides());
        return ExecuteWorkflowRequest.builder().isProd(isProd).testRun(workflowGroupExecuteRequest.getTestRun())
                .requestId(workflowGroupExecuteRequest.getRequestId()).callBackUrl(workflowGroupExecuteRequest.getCallBackUrl())
                .workflowVersion(workflowVersionAPI.parseVersionToString(version))
                .workflowName(getWorkflowName(workflowGroupExecuteRequest.getOverrides()))
                .requestOverride(getRequestOverride(workflowGroupExecuteRequest.getOverrides()))
                .emailNotifications(workflowGroupExecuteRequest.getEmailNotifications())
                .tables(workflowGroupExecuteRequest.getTables()).build();
    }

    private void validateNoOfWorkflow(Map<String, RequestOverride> requestOverrideMap) throws DSPSvcException {
        if (Objects.nonNull(requestOverrideMap) && requestOverrideMap.size() > 1)
            throw new DSPSvcException("More than one workflowEntity Not Allowed");
    }

    private String getWorkflowName(Map<String, RequestOverride> requestOverrideMap) throws DSPSvcException {
        if (Objects.nonNull(requestOverrideMap)) {
            Optional<String> workflowName = requestOverrideMap.keySet().stream().findFirst();
            if (workflowName.isPresent() && isNotBlank(workflowName.get()))
                return workflowName.get();
            throw new DSPSvcException("Workflow Name can't be  null or empty");
        }
        return null;
    }

    private RequestOverride getRequestOverride(Map<String, RequestOverride> requestOverrideMap) throws DSPSvcException  {
        if (Objects.nonNull(requestOverrideMap)) {
            Optional<RequestOverride> requestOverride = requestOverrideMap.values().stream().findFirst();
            if (requestOverride.isPresent())
                return requestOverride.get();
            throw new DSPSvcException("Request Override can't be  null");
        }
        return null;
    }

    public WorkflowGroupCreateDetails convertToWorkflowGroupCreateDetails(WorkflowDetails workflowDetails) {
        if (Objects.nonNull(workflowDetails)) {
            Workflow workflow = workflowDetails.getWorkflow();
            Map<String, RequestOverride> requestOverrideMap = new HashMap<>();
            if (!Strings.isNullOrEmpty(workflow.getDefaultOverrides()))
                requestOverrideMap.put(workflow.getName(), JsonUtils.DEFAULT.fromJson(workflow.getDefaultOverrides(), RequestOverride.class));
            return WorkflowGroupCreateDetails.builder().callbackUrl(workflow.getWorkflowMeta().getCallbackUrl())
                    .id(workflow.getWorkflowGroupName()).description(workflow.getDescription()).draft(!workflow.getIsProd())
                    .version(workflowVersionAPI.parseVersionToDouble(workflow.getVersion())).workflows(wrapWorkflow(workflowDetails))
                    .emailNotifications(getEmailNotifications(workflow.getId())).overrides(requestOverrideMap).build();
        }
        return null;
    }

    private EmailNotifications getEmailNotifications(Long workflowId) {
        NotificationPreference notificationPreference = notificationPreferencesActor.getNotificationPreference(workflowId);
        if (Objects.nonNull(notificationPreference))
            return notificationPreference.getEmailNotificationPreferences();
        return null;
    }

    private List<WorkflowGroupCreateDetails.Workflow> wrapWorkflow(WorkflowDetails workflowDetails) {
        List<WorkflowGroupCreateDetails.Workflow> workFlows = new ArrayList<>();
        WorkflowGroupCreateDetails.Workflow externalWorkflow = WorkflowGroupCreateDetails.Workflow.builder()
                .id(workflowDetails.getWorkflow().getName()).parentWorkflowId(null)
                .pipelineSteps(wrapPipelineSteps(workflowDetails.getPipelineSteps()))
                .inputPartitions(getPartitions(workflowDetails)).outputPartitions(getPartitions(workflowDetails))
                .dataframes(wrapDataFrames(workflowDetails)).trainingWorkflow(getTrainingWorkflow(workflowDetails))
                .defaultOverrides(workflowDetails.getWorkflow().getDefaultOverrides()).build();
        workFlows.add(externalWorkflow);
        return workFlows;
    }

    private List<WorkflowGroupCreateDetails.PipelineStep> wrapPipelineSteps(List<PipelineStep> pipelineSteps) {
        return pipelineSteps.stream().map(pipelineStep -> WorkflowGroupCreateDetails.PipelineStep.builder()
                .id(pipelineStep.getName()).parentStepId(getParentPipelineStep(pipelineStep)).script(wrapScript(pipelineStep.getScript()))
                .resources(pipelineStepResourceAPI.wrapResources(pipelineStep.getPipelineStepResources()))
                .build()).collect(Collectors.toList());
    }

    private String getParentPipelineStep(PipelineStep pipelineStep) {
        if (Objects.isNull(pipelineStep.getParentPipelineStepId())) return null;
        return pipelineStep.getParentPipelineStepId().toString();
    }

    private WorkflowGroupCreateDetails.Script wrapScript(Script script) {
        return WorkflowGroupCreateDetails.Script.builder().executionEnv(script.getExecutionEnvironment())
                .gitRepo(script.getGitRepo()).gitFolderPath(script.getGitFolder()).filePath(script.getFilePath())
                .gitCommitId(script.getGitCommitId()).inputs(script.getInputVariables())
                .outputs(script.getOutputVariables()).build();
    }

    private List<String> getPartitions(WorkflowDetails workflowDetails) {
        return workflowDetails.getPipelineSteps().get(0).getPartitions();
    }

    private List<WorkflowGroupCreateDetails.Dataframe> wrapDataFrames(WorkflowDetails workflowDetails) {
        return workflowDetails.getWorkflow().getDataFrames().stream().map(d ->
                new WorkflowGroupCreateDetails.Dataframe(d.getId(), d.getName())).collect(toList());
    }

    private TrainingWorkflow getTrainingWorkflow(WorkflowDetails workflowDetails) {
        if (Objects.nonNull(workflowDetails.getWorkflow().getParentWorkflowId())) {
            Workflow parentWorkflow = workFlowActor.getWorkFlowById(workflowDetails.getWorkflow().getParentWorkflowId());
            return TrainingWorkflow.builder().id(parentWorkflow.getName()).groupId(parentWorkflow.getWorkflowGroupName()).build();
        }
        return null;
    }

    public CreateWorkflowRequest convertToCreateWorkflowRequest(String version, WorkflowGroupCreateDetails workflowGroupCreateDetails) {
        RequestOverride requestOverride = null;
        if (Objects.nonNull(workflowGroupCreateDetails.getOverrides()) && !workflowGroupCreateDetails.getOverrides().values().isEmpty()) {
            Optional<RequestOverride> overrideOptional = workflowGroupCreateDetails.getOverrides().values().stream().findFirst();
            if (overrideOptional.isPresent())
                requestOverride = overrideOptional.get();
        }

        return CreateWorkflowRequest.builder().workflow(wrapWorkflow(workflowGroupCreateDetails))
                .mesosQueue(workflowGroupCreateDetails.getMesosQueue()).callbackUrl(workflowGroupCreateDetails.getCallbackUrl())
                .trainingWorkflow(wrapTrainingWorkflow(workflowGroupCreateDetails.getWorkflows().get(0).getTrainingWorkflow()))
                .emailNotifications(workflowGroupCreateDetails.getEmailNotifications()).requestOverride(requestOverride).version(version)
                .datatypeDefaults(workflowGroupCreateDetails.getDatatypeDefaults()).build();
    }

    private CreateWorkflowRequest.Workflow wrapWorkflow(WorkflowGroupCreateDetails workflowGroupCreateDetails) {
        WorkflowGroupCreateDetails.Workflow workflow = workflowGroupCreateDetails.getWorkflows().get(0);
        return CreateWorkflowRequest.Workflow.builder().name(workflow.getId()).isProd(!workflowGroupCreateDetails.isDraft())
                .description(workflowGroupCreateDetails.getDescription()).dataframes(wrapDataFrames(workflow.getDataframes()))
                .pipelineSteps(wrapPipelineSteps(workflowGroupCreateDetails)).build();
    }

    private List<CreateWorkflowRequest.Dataframe> wrapDataFrames(List<WorkflowGroupCreateDetails.Dataframe> dataframes) {
        return dataframes.stream().map(dataframe -> CreateWorkflowRequest.Dataframe.builder().id(dataframe.getId())
                .name(dataframe.getName()).build()).collect(Collectors.toList());
    }

    private List<CreateWorkflowRequest.PipelineStep> wrapPipelineSteps(WorkflowGroupCreateDetails workflowGroupCreateDetails) {
        List<WorkflowGroupCreateDetails.PipelineStep> pipelineSteps = workflowGroupCreateDetails.getWorkflows().get(0).getPipelineSteps();
        List<String> partitions = workflowGroupCreateDetails.getWorkflows().get(0).getInputPartitions();
        return pipelineSteps.stream().map(pipelineStep -> CreateWorkflowRequest.PipelineStep.builder()
                .name(pipelineStep.getId()).parentStepName(pipelineStep.getParentStepId()).partitions(partitions)
                .resources(pipelineStep.getResources()).script(wrapScript(pipelineStep.getScript())).build()).collect(toList());
    }

    private CreateWorkflowRequest.Script wrapScript(WorkflowGroupCreateDetails.Script script) {
        return CreateWorkflowRequest.Script.builder().executionEnv(script.getExecutionEnv()).filePath(script.getFilePath())
                .gitCommitId(script.getGitCommitId()).gitFolderPath(script.getGitFolderPath()).gitRepo(script.getGitRepo())
                .inputs(script.getInputs()).outputs(script.getOutputs()).build();
    }

    private com.flipkart.dsp.models.workflow.TrainingWorkflow wrapTrainingWorkflow(TrainingWorkflow trainingWorkflow) {
        if (Objects.nonNull(trainingWorkflow))
            return com.flipkart.dsp.models.workflow.TrainingWorkflow.builder().workflowName(trainingWorkflow.getId()).build();
        return null;
    }

}
