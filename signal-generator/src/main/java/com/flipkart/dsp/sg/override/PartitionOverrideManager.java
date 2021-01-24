package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.dto.DataFrameColumnType;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.sg.utils.StrictHashMap;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.Callable;


@Slf4j
public class PartitionOverrideManager implements Callable<DataFrameAudit> {
    private Long requestId;
    private Workflow workflow;
    private String dataFrameName;
    private EventAuditUtil eventAuditUtil;
    private WorkflowDetails workflowDetails;
    private DataFrameOverrideHelper dataFrameOverrideHelper;
    private PartitionDataframeOverride partitionDataframeOverride;
    private PipelineStep pipelineStep;


    PartitionOverrideManager(String dataFrameName, Request request, WorkflowDetails workflowDetails, EventAuditUtil eventAuditUtil,
                             PartitionDataframeOverride partitionDataframeOverride, DataFrameOverrideHelper dataFrameOverrideHelper,
                             PipelineStep pipelineStep) {
        this.requestId = request.getId();
        this.dataFrameName = dataFrameName;
        this.eventAuditUtil = eventAuditUtil;
        this.workflowDetails = workflowDetails;
        this.workflow = workflowDetails.getWorkflow();
        this.dataFrameOverrideHelper = dataFrameOverrideHelper;
        this.partitionDataframeOverride = partitionDataframeOverride;
        this.pipelineStep = pipelineStep;
    }

    @Override
    public DataFrameAudit call() throws Exception {
        eventAuditUtil.createPartitionOverrideStartDebugEvent(requestId, workflow.getId(), workflow.getName(), dataFrameName);
        List<String> partitions = pipelineStep.getPartitions();
        Map<String, DataFrame> dataFrameMap = dataFrameOverrideHelper.getDataFrameMap(workflow);
        StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry = new StrictHashMap<>();
        partitionDataframeOverride.forEach((partitionId, hdfsLocation) -> {
            final List<DataFrameKey> keySet = dataFrameOverrideHelper.getDataFrameKeys(partitions);
            final LinkedHashSet<String> auditValueSet = dataFrameOverrideHelper.getDataframeValues(hdfsLocation);
            auditEntry.put(keySet, auditValueSet);
        });
        LinkedHashMap<String, DataFrameColumnType> columnMetaData = dataFrameOverrideHelper.getColumnMetadata(partitions);
        SGUseCasePayload sgUseCasePayload = new SGUseCasePayload(requestId, dataFrameName, columnMetaData, auditEntry);
        Long dataFrameSize = dataFrameOverrideHelper.getDataFrameSize(sgUseCasePayload);
        eventAuditUtil.createPartitionOverrideEndDebugEvent(requestId, workflow.getId(), workflow.getName(), dataFrameName, partitionDataframeOverride);
        return dataFrameOverrideHelper.saveDataFrameAudit(dataFrameSize, null, partitions,
                dataFrameMap.get(dataFrameName), sgUseCasePayload);
    }

}
