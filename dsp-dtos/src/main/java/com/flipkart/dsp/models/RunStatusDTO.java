package com.flipkart.dsp.models;

import lombok.*;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class RunStatusDTO {
    private RequestStatus requestStatus;
    private WorkflowRunStatus workflowRunStatus;

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class WorkflowRunStatus {
        private String workflowName;
        private WorkflowStatus workflowStatus;
        private List<PipelineStepInfo> pipelineStepInfos;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class PipelineStepInfo {
        private String stepName;
        private List<Variable> inputs;
        private List<Variable> outputs;
        private String logLocation;
        private List<PartitionInfo> partitionInfos;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class PartitionInfo {
        private PipelineStepStatus stepStatus;
        private long count;
        private Set<String> partitions;
    }


    @NoArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString(callSuper = true)
    @Setter
    public static class DataFrameVariable extends Variable implements Serializable {
        private Long refreshId;
        private String tableName;
    }

    @NoArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString(callSuper = true)
    @Setter
    public static class ModelGroupVariable extends Variable implements Serializable {
        String modelGroupId;
    }

    @AllArgsConstructor
    public enum InputType {
        MODEL,
        HIVE_TABLE
    }
}
