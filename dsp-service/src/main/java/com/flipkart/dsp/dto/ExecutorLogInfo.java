package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class ExecutorLogInfo {
    private String pipelineExecutionId;
    private String scope;
    private String pipelineStep;
    private String workflowName;
    private String logLocation;
    private Integer attempt;
    @Setter
    private Integer stderrOffset = 0;
    @Setter
    private Integer stdoutOffset = 0;
    @Setter
    private String state;

    public String retriveUniqueKey() {
        return String.format("%s-%s",pipelineExecutionId, attempt);
    }
}
