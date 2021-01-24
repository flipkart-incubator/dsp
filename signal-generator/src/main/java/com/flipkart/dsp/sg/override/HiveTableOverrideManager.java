package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.HiveDataframeOverride;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Callable;

@Slf4j
public class HiveTableOverrideManager implements Callable<Map<String, Long>> {
    private Long requestId;
    private Long workflowId;
    private String workflowName;
    private String dataframeName;
    private EventAuditUtil eventAuditUtil;
    private WorkflowDetails workflowDetails;
    private HiveDataframeOverride hiveDataframeOverride;
    private DataFrameOverrideHelper dataFrameOverrideHelper;

    HiveTableOverrideManager(String dataframeName, Request request, WorkflowDetails workflowDetails, EventAuditUtil eventAuditUtil,
                             HiveDataframeOverride hiveDataframeOverride, DataFrameOverrideHelper dataFrameOverrideHelper) {
        this.requestId = request.getId();
        this.dataframeName = dataframeName;
        this.eventAuditUtil = eventAuditUtil;
        this.workflowDetails = workflowDetails;
        this.hiveDataframeOverride = hiveDataframeOverride;
        this.dataFrameOverrideHelper = dataFrameOverrideHelper;
        this.workflowId = workflowDetails.getWorkflow().getId();
        this.workflowName = workflowDetails.getWorkflow().getName();
    }

    @Override
    public Map<String, Long> call() throws Exception {
        String hiveQueue = workflowDetails.getWorkflow().getWorkflowMeta().getHiveQueue();
        eventAuditUtil.createHiveTableOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName,
                hiveDataframeOverride.getDatabase(), hiveDataframeOverride.getTableName());
        Long dataFrameId = dataFrameOverrideHelper.getDataFrameId(dataframeName, workflowDetails.getWorkflow());
        String hiveTableName = String.format("%s.%s", hiveDataframeOverride.getDatabase(), hiveDataframeOverride.getTableName());
        Long refreshId = dataFrameOverrideHelper.getRefreshId(hiveTableName, hiveDataframeOverride, requestId, hiveQueue);
        Map<String, Long> overrideTableInformation = dataFrameOverrideHelper.getOverrideTableInformationForHive(refreshId, hiveTableName, hiveQueue);
        dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId, workflowId, dataFrameId, refreshId.toString(),
                overrideTableInformation, overrideTableInformation, DataFrameOverrideType.HIVE);
        eventAuditUtil.createHiveTableOverrideManagerEndDebugEvent(requestId, workflowId, workflowName,
                dataframeName, hiveDataframeOverride.getDatabase(), hiveDataframeOverride.getTableName(), refreshId);
        return overrideTableInformation;
    }

}
