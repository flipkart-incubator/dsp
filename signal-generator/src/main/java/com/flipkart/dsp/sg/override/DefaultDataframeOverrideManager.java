package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.DefaultDataframeOverride;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.Callable;

@Slf4j
public class DefaultDataframeOverrideManager implements Callable<Optional<DataFrameAudit>> {
    private Long requestId;
    private Long workflowId;
    private String workflowName;
    private String dataframeName;
    private EventAuditUtil eventAuditUtil;
    private WorkflowDetails workflowDetails;
    private DefaultDataframeOverride defaultDataframeOverride;
    private DataFrameOverrideHelper dataFrameOverrideHelper;

    DefaultDataframeOverrideManager(String dataframeName, Request request, WorkflowDetails workflowDetails, EventAuditUtil eventAuditUtil,
                                    DefaultDataframeOverride defaultDataframeOverride, DataFrameOverrideHelper dataFrameOverrideHelper) {
        this.requestId = request.getId();
        this.dataframeName = dataframeName;
        this.eventAuditUtil = eventAuditUtil;
        this.workflowDetails = workflowDetails;
        this.dataFrameOverrideHelper = dataFrameOverrideHelper;
        this.defaultDataframeOverride = defaultDataframeOverride;
        this.workflowId = workflowDetails.getWorkflow().getId();
        this.workflowName = workflowDetails.getWorkflow().getName();
    }

    @Override
    public Optional<DataFrameAudit> call() throws Exception {
        eventAuditUtil.createDefaultDataframeOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeName);
        if (defaultDataframeOverride.getForceRun()) {
            eventAuditUtil.createDefaultDataframeOverrideForceRunDebugEvent(requestId, workflowId, workflowName, dataframeName);
            return Optional.empty();
        } else {
            Long dataFrameId = dataFrameOverrideHelper.getDataFrameId(dataframeName, workflowDetails.getWorkflow());
            DataFrameAudit latestDataFrameAudit = dataFrameOverrideHelper.getLatestDataFrameAuditByDataFrameId(dataFrameId);
            SGUseCasePayload sgUseCasePayload = latestDataFrameAudit.getPayload();
            String payload = dataFrameOverrideHelper.serializePayloadToString(sgUseCasePayload);
            eventAuditUtil.createDefaultDataframeOverrideReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, payload);
            return Optional.of(latestDataFrameAudit);
        }
    }
}
