package com.flipkart.dsp.models.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.Resources;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(NON_NULL)
public class CreateWorkflowRequest {
    private String version;

    @Valid
    @NotNull
    private Workflow workflow;

    @JsonProperty("hive_queue")
    private String hiveQueue;

    @JsonProperty("mesos_queue")
    private String mesosQueue;

    private String callbackUrl;
    private String workflowGroupName;
    private RequestOverride requestOverride;

    @Valid
    private TrainingWorkflow trainingWorkflow;

    @Valid
    private EmailNotifications emailNotifications;
    private Map<SignalDataType, String> datatypeDefaults;

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class Workflow {
        @Valid
        @NotNull
        private String name;

        @JsonProperty("is_prod")
        private Boolean isProd;

        @Valid
        @NotNull
        private String description;

        @Valid
        @NotNull
        @JsonProperty("pipeline_steps")
        private List<PipelineStep> pipelineSteps;


        @Valid
        @NotNull
        private List<Dataframe> dataframes;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class PipelineStep {
        @Valid
        @NotNull
        private String name;

        @Valid
        @JsonProperty("parent_step_name")
        private String parentStepName;

        @Valid
        @NotNull
        private Script script;


        @Valid
        private Resources resources;

        @Valid
        @JsonProperty("partitions")
        private List<String> partitions = new ArrayList<>();

    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(NON_NULL)
    public static class Script {
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

