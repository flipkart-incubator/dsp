package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.helper.*;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class OverrideManager {
    private final EventAuditUtil eventAuditUtil;
    private final FileOverrideHelper fileOverrideHelper;
    private final DataFrameOverrideHelper dataFrameOverrideHelper;
    private final HiveQueryOverrideHelper hiveQueryOverrideHelper;

    public Set<DataFrameAudit> override(Request request, WorkflowDetails workflowDetails,
                                        Map<String, DataframeOverride> dataframeOverrideMap,
                                        PipelineStep pipelineStep) throws DataframeOverrideException {
        ExecutorService executor = Executors.newFixedThreadPool(dataframeOverrideMap.size());
        Workflow workflow = workflowDetails.getWorkflow();
        try {
            List<Future<? extends Object>> executionResult = new ArrayList<>();
            Set<DataFrameAudit> overriddenAudits = new HashSet<>();
            Map<String, Long> tableInformation = request.getData().getTables() == null ? new HashMap<>() : request.getData().getTables();
            eventAuditUtil.createOverrideStartDebugEvent(request.getId(), workflow.getId(), workflow.getName(), dataframeOverrideMap);
            for (Map.Entry<String, DataframeOverride> entry : dataframeOverrideMap.entrySet()) {
                String dataframeName = entry.getKey();
                DataframeOverride dataframeOverride = entry.getValue();

                if (dataframeOverride instanceof RunIdDataframeOverride) {
                    RunIdDataFrameOverrideManager runIdDataFrameOverrideManager = new RunIdDataFrameOverrideManager(dataframeName, request, workflowDetails,
                            eventAuditUtil, (RunIdDataframeOverride) dataframeOverride, dataFrameOverrideHelper);
                    Future<DataFrameAudit> future = executor.submit(runIdDataFrameOverrideManager);
                    executionResult.add(future);
                } else if (dataframeOverride instanceof PartitionDataframeOverride) {
                    PartitionOverrideManager partitionOverrideManager = new PartitionOverrideManager(dataframeName, request, workflowDetails,
                            eventAuditUtil, (PartitionDataframeOverride) dataframeOverride, dataFrameOverrideHelper, pipelineStep);
                    Future<DataFrameAudit> future = executor.submit(partitionOverrideManager);
                    executionResult.add(future);
                } else if (dataframeOverride instanceof CSVDataframeOverride || dataframeOverride instanceof FTPDataframeOverride) {
                    FileOverrideManager fileOverrideManager = new FileOverrideManager(dataframeName, request, workflowDetails,
                            eventAuditUtil, dataframeOverride, fileOverrideHelper, pipelineStep);
                    Future<DataFrameAudit> future = executor.submit(fileOverrideManager);
                    executionResult.add(future);
                } else if (dataframeOverride instanceof DefaultDataframeOverride) {
                    DefaultDataframeOverrideManager defaultDataframeOverrideManager = new DefaultDataframeOverrideManager(dataframeName, request, workflowDetails,
                            eventAuditUtil, (DefaultDataframeOverride) dataframeOverride, dataFrameOverrideHelper);
                    Future<Optional<DataFrameAudit>> future = executor.submit(defaultDataframeOverrideManager);
                    executionResult.add(future);
                } else if (dataframeOverride instanceof HiveQueryDataframeOverride) {
                    HiveQueryOverrideManager hiveQueryOverrideManager = new HiveQueryOverrideManager(dataframeName, request.getId(), workflowDetails,
                            eventAuditUtil, (HiveQueryDataframeOverride) dataframeOverride, dataFrameOverrideHelper, hiveQueryOverrideHelper, pipelineStep);
                    Future<Object> future = executor.submit(hiveQueryOverrideManager);
                    executionResult.add(future);

                }
            }
            waitForProcessCompletion(executionResult);
            extractOutput(executionResult, overriddenAudits, tableInformation);
            request.getData().setTables(tableInformation);
            eventAuditUtil.createOverrideEndDebugEvent(request.getId(), workflow.getId(), workflow.getName());
            return overriddenAudits;
        } catch (Exception e) {
            executor.shutdownNow();
            log.info("shutdownNow called, marking unfinished overrides as failed");
            dataFrameOverrideHelper.updateFailedDataframeOverrideAudit(request.getId());
            throw new DataframeOverrideException(e.getMessage(), e);
        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    private void extractOutput(List<Future<?>> executionResult, Set<DataFrameAudit> overriddenAudits, Map<String, Long> tableInformation) throws DataframeOverrideException {
        for (Future<? extends Object> threadState : executionResult) {
            try {
                Object result = threadState.get();
                // check for optional
                if (result instanceof DataFrameAudit) {
                    overriddenAudits.add((DataFrameAudit) result);
                } else if (result instanceof Map) {
                    Map<String, Long> overrideTableDetails = (Map<String, Long>) result;
                    overrideTableDetails.forEach((key, value) -> tableInformation.put(key, value));
                } else if (result instanceof Optional && ((Optional) result).isPresent()) {
                    overriddenAudits.add((DataFrameAudit) ((Optional) result).get());
                }
                log.info("Finish Execution of Thread " + result.toString());
            } catch (Exception e) {
                throw new DataframeOverrideException(e.getMessage(), e);
            }
        }
    }

    private void waitForProcessCompletion(List<Future<?>> executionResult) throws DataframeOverrideException {
        boolean iterate;
        do {
            iterate = false;
            for (Future<? extends Object> threadState : executionResult) {
                try {
                    if (!threadState.isDone()) {
                        iterate = true;
                    } else {
                        // Confirming if an exception is thrown by any of the threads.
                        // In that case will fail fast by killing all the running thread [best effort]
                        threadState.get(5, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception e) {
                    log.error("Issue in downloading of input is detected ", e.getCause());
                    throw new DataframeOverrideException("Issue in downloading of input is detected " + e);
                }
            }

            try {
                Thread.sleep(10000);
                log.info("Waiting for all Signal Generation Threads to get completed");
            } catch (InterruptedException e) {
                log.info("Waiting thread has been interrupted, But nothing to worry " + e.getMessage());
            }
        } while (iterate);
    }

    public DataTable getDataTableForOverride(DataFrame dataFrame, DataframeOverride dataframeOverride, DataTable dataTableOld) {
        String table = null;
        String database = null;
        if (dataframeOverride instanceof DefaultDataframeOverride) {
            table = dataTableOld.getId();
            database = dataTableOld.getDataSource().getId();
        } else if (dataframeOverride instanceof HiveQueryDataframeOverride) {
            table = HiveQueryOverrideManager.getTable(dataFrame);
            database = HiveQueryOverrideManager.getDatabase();
        }
        DataSourceConfiguration dataSourceConfiguration = new DataSourceConfiguration(DataSourceConfigurationType.HIVE, "0.0.0.0", database);
        final DataSource dataSource = new DataSource(database, dataSourceConfiguration);
        return new DataTable(table, "", dataSource);
    }

}
