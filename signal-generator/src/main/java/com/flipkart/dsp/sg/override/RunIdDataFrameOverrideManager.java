package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.RunIdDataframeOverride;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class RunIdDataFrameOverrideManager implements Callable<DataFrameAudit> {
    private Long requestId;
    private Long workflowId;
    private String workflowName;
    private String dataframeName;
    private EventAuditUtil eventAuditUtil;
    private RunIdDataframeOverride runIdDataframeOverride;
    private DataFrameOverrideHelper dataFrameOverrideHelper;

    public RunIdDataFrameOverrideManager(String dataframeName, Request request, WorkflowDetails workflowDetails, EventAuditUtil eventAuditUtil,
                                         RunIdDataframeOverride runIdDataframeOverride, DataFrameOverrideHelper dataFrameOverrideHelper) {
        this.requestId = request.getId();
        this.dataframeName = dataframeName;
        this.eventAuditUtil = eventAuditUtil;
        this.runIdDataframeOverride = runIdDataframeOverride;
        this.dataFrameOverrideHelper = dataFrameOverrideHelper;
        this.workflowId = workflowDetails.getWorkflow().getId();
        this.workflowName = workflowDetails.getWorkflow().getName();
    }

    @Override
    public DataFrameAudit call() {
        eventAuditUtil.createRunIdOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        Long runId = runIdDataframeOverride.getRunId();
        DataFrameAudit dataFrameAudit = dataFrameOverrideHelper.getDataFrameAuditById(runId);
        if (dataFrameAudit.getDataFrame().getName().equals(dataframeName)) {
            eventAuditUtil.createRunIdOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, runId);
            return dataFrameAudit;
        } else {
            String errorMessage = "DataFrame Run Id: " + runId + "is not of type: " + dataframeName;
            eventAuditUtil.createRunIdOverrideErrorEvent(requestId, workflowId, workflowName, dataFrameAudit.getDataFrame().getName(), runId, errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
