package com.flipkart.dsp.models.misc;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * +
 */
@Data
@Builder
@JsonSnakeCase
@NoArgsConstructor
@AllArgsConstructor
// Please don't change the variables order, it kept it like for better viewing of information step by step
public class JobDetailsDTO {
    private Long requestId;
    private String requestStatus;
    private String azkabanUrl;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date startTime;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private Date endTime;
    private String executionTime;
    private WorkflowDetails workflowDetails;
    private InputDetails inputDetails;
    private Map<Integer /* Azkaban Attempt*/, Map<String /* pipelineStep */, PipelineStepDetails>> jobDetails;
    private Object outputPayload;
    private List<PreviousRunDetail> previousRunDetails;

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowDetails {
        private Boolean isProd;
        private String workflowName;
        private String workflowVersion;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InputDetails {
        private ExecuteWorkflowRequest inputPayload;
        private Map<String /*pipelineStep Name*/, Map<String /*dataFrame Name */, String /* DataFrame size*/>> inputDataFramesSize;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PipelineStepDetails {
        private List<String> partitions;
        private SGExecutionDetails sgExecutionDetails;
        private List<WorkflowExecutionDetails> workflowExecutionDetails;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SGExecutionDetails {
        private String status;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date startTime;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date endTime;
        private String executionTime;
        private String stdoutLogs;
        private String stderrLogs;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowExecutionDetails {
        private Map<String, Set<String>> partitionDetails;
        private List<WorkflowExecutionAttemptDetails> workflowExecutionAttemptDetails;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkflowExecutionAttemptDetails {
        private Integer attempt;
        private String status;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date startTime;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date endTime;
        private String executionTime;
        private String stdoutLogs;
        private String stderrLogs;
        private Resources resources;
        private String memoryUsageDashboard;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Resources {
        private Double cpu;
        private Double memory;
    }

    @Data
    @Builder
    @JsonSnakeCase
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PreviousRunDetail {
        private Long requestId;
        private String status;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date startTime;
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date endTime;
        private String executionTime;
        private InputDetails inputDetails;
    }

}
