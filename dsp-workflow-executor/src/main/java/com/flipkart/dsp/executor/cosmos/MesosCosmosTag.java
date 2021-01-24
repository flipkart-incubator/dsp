package com.flipkart.dsp.executor.cosmos;

import com.flipkart.dsp.cosmos.CosmosTag;
import com.flipkart.dsp.entities.misc.ConfigPayload;

public class MesosCosmosTag extends CosmosTag {
    public static String role;
    public static long requestId;
    public static Integer attempt;
    public static long pipelineStepId;
    public static String dataframeName;
    public static String workflowName;
    public static String partitionValues;
    public static String workflowExecutionId;

    public static void populateValue(String workflow, String clusterRole, Integer attemptNo,
                                     ConfigPayload configPayload) {
        role = clusterRole;
        attempt = attemptNo;
        workflowName = workflow;
        requestId = configPayload.getRefreshId();
        pipelineStepId = configPayload.getPipelineStepId();
        workflowExecutionId = configPayload.getWorkflowExecutionId();
        partitionValues = configPayload.getPartitionValues().values().toString();
    }
}
