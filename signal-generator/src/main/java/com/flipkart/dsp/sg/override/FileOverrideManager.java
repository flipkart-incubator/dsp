package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.CSVDataframeOverride;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.sg.helper.FileOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;

import static com.flipkart.dsp.entities.sg.core.DataFrameOverrideType.CSV;
import static com.flipkart.dsp.entities.sg.core.DataFrameOverrideType.FTP;

@Slf4j
public class FileOverrideManager implements Callable<DataFrameAudit> {
    private Long requestId;
    private Workflow workflow;
    private String dataFrameName;
    private EventAuditUtil eventAuditUtil;
    private WorkflowDetails workflowDetails;
    private DataframeOverride dataframeOverride;
    private FileOverrideHelper fileOverrideHelper;
    private PipelineStep pipelineStep;


    public FileOverrideManager(String dataFrameName, Request request, WorkflowDetails workflowDetails, EventAuditUtil eventAuditUtil,
                               DataframeOverride dataframeOverride, FileOverrideHelper fileOverrideHelper,
                               PipelineStep pipelineStep) {

        this.requestId = request.getId();
        this.dataFrameName = dataFrameName;
        this.eventAuditUtil = eventAuditUtil;
        this.workflowDetails = workflowDetails;
        this.dataframeOverride = dataframeOverride;
        this.fileOverrideHelper = fileOverrideHelper;
        this.workflow = workflowDetails.getWorkflow();
        this.pipelineStep = pipelineStep;
    }


    @Override
    public DataFrameAudit call() throws Exception {
        DataFrameOverrideType overrideType = dataframeOverride instanceof CSVDataframeOverride ? CSV : FTP;
        log.info("DataFrame generation from " + overrideType.toString() + " started for DataFrame: " + dataFrameName);
        String hdfsPath = fileOverrideHelper.getHDFSPath(requestId, workflow, dataFrameName, dataframeOverride, overrideType);
        eventAuditUtil.createCSVOverrideStartDebugEvent(requestId, workflow.getId(), workflow.getName(), dataFrameName, hdfsPath);
        LinkedHashMap<String, SignalDataType> columnMapping = fileOverrideHelper.getColumnMapping(dataframeOverride, overrideType);
        String[] headers = columnMapping.keySet().toArray(new String[0]);
        List<String> partitions = pipelineStep.getPartitions();
        List<String> partitionList = fileOverrideHelper.getPartitionColumnsInHeader(headers, partitions);
        if (partitionList.isEmpty())
            return fileOverrideHelper.processNonPartitionedCSV(requestId, dataFrameName, hdfsPath, overrideType, workflowDetails, partitions);
        return fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, dataframeOverride, overrideType, partitions);
    }
}
