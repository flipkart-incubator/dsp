package com.flipkart.dsp.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(value = { "draft, version"}, ignoreUnknown = true)
@JsonInclude(NON_NULL)
public class WorkflowGroupCreateDetails {

    @Valid
    @NotNull
    private String id;

    @Valid
    @NotNull
    private String description;

    private boolean draft;

    private double version;

    private String callbackUrl;

    private Map<SignalDataType, String> datatypeDefaults;

    @Valid
    @NotNull
    private List<Workflow> workflows;

    @JsonProperty("mesos_queue")
    private String mesosQueue;

    Map<String/*workflowName*/, RequestOverride> overrides;

    @Valid
    @NotNull
    private EmailNotifications emailNotifications;

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class Workflow {
        @Valid
        @NotNull
        private String id;

        @JsonProperty("parent_workflow_id")
        private String parentWorkflowId;

        @JsonProperty("training_workflow")
        private TrainingWorkflow trainingWorkflow;

        @Valid
        @NotNull
        @JsonProperty("pipeline_steps")
        private List<PipelineStep> pipelineSteps;

        @Valid
        @NotNull
        @JsonProperty("input_partitions")
        private List<String> inputPartitions;

        @Valid
        @JsonProperty("output_partitions")
        private List<String> outputPartitions = new LinkedList<>();

        @Valid
        @NotNull
        private List<Dataframe> dataframes;

        @Valid
        @NotNull
        private String defaultOverrides;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class PipelineStep {
        @Valid
        @NotNull
        private String id;

        @Valid
        @JsonProperty("parent_step_id")
        private String parentStepId;

        @Valid
        @NotNull
        private Script script;

        @Valid
        private Resources resources;


        @Valid
        @NotNull
        @JsonProperty("partitions")
        private List<String> partitions;

    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class Script {
        @Valid
        @NotNull
        private String id;

        @Valid
        @NotNull
        @JsonProperty("execution_env")
        private String executionEnv;

        @Valid
        @NotNull
        @JsonProperty("git_repo")
        private String gitRepo;

        @Valid
        @NotNull
        @JsonProperty("git_folder_path")
        private String gitFolderPath;

        @Valid
        @NotNull
        @JsonProperty("file_path")
        private String filePath;

        @Valid
        @NotNull
        @JsonProperty("git_commit_id")
        private String gitCommitId;

        @Valid
        @NotNull
        private Set<ScriptVariable> inputs;

        @Valid
        @NotNull
        private Set<ScriptVariable> outputs;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Dataframe {

        private Long id;

        @Valid
        @NotNull
        private String name;
    }

}

