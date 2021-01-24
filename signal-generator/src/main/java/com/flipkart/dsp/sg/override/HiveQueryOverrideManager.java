package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.HiveQueryDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.helper.DataFrameOverrideHelper;
import com.flipkart.dsp.sg.helper.HiveQueryOverrideHelper;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Callable;

import static com.flipkart.dsp.utils.Constants.HIVE_QUERY_DATABASE;

@Slf4j
public class HiveQueryOverrideManager implements Callable<Object> {
    private Long requestId;
    private Long workflowId;
    private String workflowName;
    private String dataframeName;
    private PipelineStep pipelineStep;
    private EventAuditUtil eventAuditUtil;
    private WorkflowDetails workflowDetails;
    private HiveQueryOverrideHelper hiveQueryOverrideHelper;
    private DataFrameOverrideHelper dataFrameOverrideHelper;
    private HiveQueryDataframeOverride hiveQueryDataframeOverride;

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS `%s.%s`(\n" +
            "   %s)\n" +
            "PARTITIONED BY (refresh_id bigint)" +
            " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' NULL DEFINED AS ''";


    HiveQueryOverrideManager(String dataframeName, Long requestId, WorkflowDetails workflowDetails,
                             EventAuditUtil eventAuditUtil, HiveQueryDataframeOverride hiveQueryDataframeOverride,
                             DataFrameOverrideHelper dataFrameOverrideHelper, HiveQueryOverrideHelper hiveQueryOverrideHelper,
                             PipelineStep pipelineStep) {
        this.requestId = requestId;
        this.pipelineStep = pipelineStep;
        this.dataframeName = dataframeName;
        this.eventAuditUtil = eventAuditUtil;
        this.workflowDetails = workflowDetails;
        this.dataFrameOverrideHelper = dataFrameOverrideHelper;
        this.workflowId = workflowDetails.getWorkflow().getId();
        this.hiveQueryOverrideHelper = hiveQueryOverrideHelper;
        this.workflowName = workflowDetails.getWorkflow().getName();
        this.hiveQueryDataframeOverride = hiveQueryDataframeOverride;
    }

    @Override
    public Object call() throws DataframeOverrideException {
        log.info("Inside thread " + Thread.currentThread().getName());
        String hiveQueue = workflowDetails.getWorkflow().getWorkflowMeta().getHiveQueue();
        String query = hiveQueryDataframeOverride.getQuery();
        eventAuditUtil.createHiveQueryOverrideManagerStartDebugEvent(requestId, workflowId, workflowName, dataframeName, query);
        try {
            DataFrame dataFrame = dataFrameOverrideHelper.getDataFrameByName(dataframeName, workflowDetails.getWorkflow());
            String table = getTable(dataFrame);
            // replace template variables
            hiveQueryOverrideHelper.resolveQuery(hiveQueryDataframeOverride);
            String overrideHash = hiveQueryOverrideHelper.getOverrideHash(hiveQueryDataframeOverride);
            Long dataframeId = dataFrameOverrideHelper.getDataFrameId(dataframeName, workflowDetails.getWorkflow());
            try {
                DataFrameOverrideAudit dataFrameOverrideAudit = dataFrameOverrideHelper.getDataFrameOverrideAudit(dataframeId,
                        overrideHash, DataFrameOverrideType.HIVE_QUERY);
                Object outputMetadata = dataFrameOverrideHelper.reuseDataframeAudit(dataframeName, workflowDetails, dataFrameOverrideAudit, pipelineStep);
                eventAuditUtil.createHiveQueryOverrideManagerReusedDebugEvent(requestId, workflowId, workflowName, dataframeName, outputMetadata.toString());
                return outputMetadata;
            } catch (Exception e) {
                String createColumnQuery = hiveQueryOverrideHelper.getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride);
                String createTableQuery = String.format(CREATE_TABLE_TEMPLATE, getDatabase(), table, createColumnQuery);
                dataFrameOverrideHelper.createHiveTable(createTableQuery);
                Map<String, Long> outputTableInformation = hiveQueryOverrideHelper.executeQuery(getDatabase(), table, hiveQueue, dataframeName, hiveQueryDataframeOverride);
                dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId, workflowId, dataframeId, overrideHash, hiveQueryDataframeOverride,
                        outputTableInformation, DataFrameOverrideType.HIVE_QUERY);
                eventAuditUtil.createHiveQueryOverrideManagerEndDebugEvent(requestId, workflowId, workflowName, dataframeName, outputTableInformation.toString());
                return outputTableInformation;
            }
        } catch (Exception e) {
            eventAuditUtil.createHiveQueryOverrideManagerErrorEvent(requestId, workflowId, workflowName, dataframeName, query, e.getMessage() + " " + e.toString());
            throw new DataframeOverrideException("One of the HiveQuery data frame override failed!", e);
        }
    }

    public static String getTable(DataFrame dataFrame) {
        return dataFrame.getName() + "_" + dataFrame.getId();
    }

    public static String getDatabase() {
        return HIVE_QUERY_DATABASE;
    }
}
