package com.flipkart.dsp.sg.generator;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameConfig;
import com.flipkart.dsp.models.sg.DataFrameScope;
import com.flipkart.dsp.models.sg.SGType;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.executors.HiveQueryExecutor;
import com.flipkart.dsp.sg.helper.SGTypeHelper;
import com.flipkart.dsp.sg.helper.SGUseCasePayloadBuilder;
import com.flipkart.dsp.sg.helper.ScopeHelper;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.sg.utils.FutureUtils;
import com.flipkart.dsp.utils.DataframeSizeExtractor;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus.*;
import static java.util.stream.Collectors.toList;

/**
 */

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameFactory {
    private final Logger logger;
    private final ScopeHelper scopeHelper;
    private final SGTypeHelper sgTypeHelper;
    private final ObjectMapper objectMapper;
    private final EventAuditUtil eventAuditUtil;
    private final QueryGenerator queryGenerator;
    private final ExecutorService executorService;
    private final DSPServiceClient dspServiceClient;
    private final HiveQueryExecutor hiveQueryExecutor;
    private final DataframeSizeExtractor dataframeSizeExtractor;
    private final SGUseCasePayloadBuilder sgUsecasePayloadBuilder;

    public Set<DataFrameAudit> invoke(Workflow workflow, Long pipelineStepId, DataFrameGenerateRequest dataframeGenerateRequest) throws JsonProcessingException, DataFrameGeneratorException {
        List<Triplet<DataFrameAudit, Table, List<String>>> generatedQueryTriplet = generateDataframeQueries(workflow, pipelineStepId, dataframeGenerateRequest);
        executeDataframeQueries(dataframeGenerateRequest.getRequestId(), workflow, generatedQueryTriplet);
        return persistDataframeAudit(dataframeGenerateRequest, generatedQueryTriplet);
    }

    @Timed
    @Metered
    private List<Triplet<DataFrameAudit, Table, List<String>>> generateDataframeQueries(Workflow workflow, Long pipelineStepId, DataFrameGenerateRequest dataframeGenerateRequest) throws JsonProcessingException, DataFrameGeneratorException {
        List<Triplet<DataFrameAudit, Table, List<String>>> queryTriplet = new ArrayList<>();
        Set<DataFrameAudit> dataFrameAuditSet = new HashSet<>();
        for (DataFrame dataFrame : dataframeGenerateRequest.getDataFrames()) {
            eventAuditUtil.createDataFrameGenerationStartInfoEvent(dataframeGenerateRequest.getRequestId(), dataFrame.getName(),
                    new Timestamp(new Date().getTime()), workflow);
            Stopwatch stopwatch = Stopwatch.createStarted();
            Set<DataFrameScope> finalDataFrameScope = scopeHelper.getFinalDataFrameScope(dataFrame, dataframeGenerateRequest);
            DataFrameAudit dataFrameAudit = generateDataframeAudit(dataFrame, dataframeGenerateRequest, finalDataFrameScope);
            dataFrameAuditSet.add(dataFrameAudit);
            try {
                SGType sgType = sgTypeHelper.calculateSGType(dataFrame, dataframeGenerateRequest, finalDataFrameScope);
                dataFrame.setSgType(sgType);
                dataFrameAudit.getDataFrame().setSgType(sgType);
                Pair<Table, List<String>> dataFrameTableQueryPair = queryGenerator.generateQuery(dataFrameAudit.getRunId(), dataFrame, dataframeGenerateRequest, finalDataFrameScope);
                queryTriplet.add(Triplet.with(dataFrameAudit, dataFrameTableQueryPair.getValue0(), dataFrameTableQueryPair.getValue1()));
            } catch (DataFrameGeneratorException | TableNotFoundException | TException e) {
                updateDataFrameAuditStatus(dataFrameAudit, FAILED);
                eventAuditUtil.createDataframeQueryGenerationErrorEvent(dataframeGenerateRequest.getRequestId(), dataFrame.getName(), e.getMessage(),
                        new Timestamp(new Date().getTime()), workflow);
                dspServiceClient.persistRequestDataframeAudit(dataframeGenerateRequest.getRequestId(), workflow.getId(), pipelineStepId, dataFrameAuditSet);
                throw new DataFrameGeneratorException("Exception generating data-frame " + dataFrame.getId(), e);
            }
            processCosmosLogger(Thread.currentThread().getStackTrace()[1].getMethodName(), dataFrame.getName(),
                    dataframeGenerateRequest.getRequestId(), stopwatch);
        }
        return queryTriplet;
    }

    private DataFrameAudit generateDataframeAudit(DataFrame dataFrame, DataFrameGenerateRequest request, Set<DataFrameScope> finalDataFrameScope) throws JsonProcessingException {
        Long requestId = request.getRequestId();
        String partitionsString = objectMapper.writeValueAsString(dataFrame.getPartitions());
        Long dataFrameOverrideAuditId = getDataFrameOverrideAuditId(dataFrame.getId(), requestId);
        DataFrameConfig dataFrameConfig = new DataFrameConfig(finalDataFrameScope, dataFrame.getDataFrameConfig().getVisibleSignals());
        DataFrameAudit dataFrameAudit = DataFrameAudit.builder().dataFrame(dataFrame)
                .dataFrameAuditStatus(GENERATING_DATAFRAME).dataFrameConfig(dataFrameConfig).dataframeSize(0L)
                .overrideAuditId(dataFrameOverrideAuditId).partitions(partitionsString).build();
        return dspServiceClient.saveDataFrameAudit(dataFrameAudit);
    }

    private Long getDataFrameOverrideAuditId(Long dataFrameId, Long requestId) {
        DataFrameOverrideAudit dataFrameOverrideAudit = dspServiceClient.getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId);
        if (Objects.nonNull(dataFrameOverrideAudit))
            return dataFrameOverrideAudit.getId();
        else
            return null;
    }

    private void updateDataFrameAuditStatus(DataFrameAudit dataFrameAudit, DataFrameAuditStatus status) {
        dataFrameAudit.setDataFrameAuditStatus(FAILED);
        dspServiceClient.updateDataFrameAudit(dataFrameAudit);
    }

    @Timed
    @Metered
    private void executeDataframeQueries(Long requestId, Workflow workflow, List<Triplet<DataFrameAudit, Table, List<String>>> generatedQueryTriplet) throws DataFrameGeneratorException {
        log.info("Beginning Dataframe generation. . .");
        String currentMethodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        List<CompletableFuture<Object>> dataFrameAuditFutures = generatedQueryTriplet.stream()
                .map(quartet -> CompletableFuture.supplyAsync(() -> generateDataFrame(quartet, requestId, workflow, currentMethodName)
                        , executorService)).collect(toList());

        try {
            FutureUtils.getEntitiesFromFutures(dataFrameAuditFutures);
            log.info("Dataframe generation finished");
        } catch (ExecutionException e) {
            throw new DataFrameGeneratorException("Failure occurred while running queries", e);
        }
    }

    protected Object generateDataFrame(Triplet<DataFrameAudit, Table, List<String>> quartet, Long requestId, Workflow workflow, String currentMethodName) {
        try {
            log.info("Beginning Dataframe generation1. . .");
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<String> queryList = quartet.getValue2();
            Table table = quartet.getValue1();
            log.info("Generating dataframe: {}.{}", table.getDbName(), table.getTableName());
            hiveQueryExecutor.executeList(queryList, workflow.getWorkflowMeta().getHiveQueue());
            log.info("Generated dataframe: {}.{}", table.getDbName(), table.getTableName());
            processCosmosLogger(currentMethodName, quartet.getValue0().getDataFrame().getName(), requestId, stopwatch);
            eventAuditUtil.createDataFrameCompletionInfoEvent(requestId, quartet.getValue0().getDataFrame().getName(),
                    new Timestamp(new Date().getTime()), quartet.getValue0().getDataframeSize(), workflow);
        } catch (Exception e) {
            eventAuditUtil.createDataFrameQueryExecutionErrorEvent(requestId, quartet.getValue0().getDataFrame().getName(),
                    e.getMessage(), new Timestamp(new Date().getTime()), workflow);
            updateDataFrameAuditStatus(quartet.getValue0(), FAILED);
            throw new RuntimeException("Failed to run following query " + quartet.getValue2(), e);
        }
        return null;
    }

    private void processCosmosLogger(String currentMethodName, String dataFrameName, Long requestId, Stopwatch stopwatch) {
        stopwatch.stop();
        String LOGGER_FORMAT = "%d %s %d %s=%s %s=%s";
        String name = getAbsoluteMethodName(currentMethodName);
        logger.info(String.format(LOGGER_FORMAT, Instant.now().toEpochMilli() / 1000L, name, stopwatch.elapsed(TimeUnit.MICROSECONDS),
                "dataframeName", dataFrameName, "requestID", requestId));
    }

    private Set<DataFrameAudit> persistDataframeAudit(DataFrameGenerateRequest dataframeGenerateRequest, List<Triplet<DataFrameAudit, Table, List<String>>> generatedQueryTriplet) throws DataFrameGeneratorException {
        Set<DataFrameAudit> dataFrameAudits = new HashSet<>();
        for (Triplet<DataFrameAudit, Table, List<String>> triplet : generatedQueryTriplet) {
            DataFrameAudit dataFrameAudit = triplet.getValue0();
            Table table = triplet.getValue1();
            SGUseCasePayload useCasePayload = sgUsecasePayloadBuilder.build(dataframeGenerateRequest, table,
                    dataFrameAudit.getDataFrameConfig().getDataFrameScopeSet(), dataFrameAudit.getDataFrame());
            long dataframeSize = dataframeSizeExtractor.getDataframeSize(useCasePayload);
            generateSuccessDataframeAudit(dataFrameAudit, useCasePayload, dataframeSize, table);
            dataFrameAudit.setDataframeSize(dataframeSize);
            dataFrameAudits.add(dataFrameAudit);
        }
        return dataFrameAudits;
    }

    private String getAbsoluteMethodName(String methodName) {
        if (methodName.contains("$")) {
            return DataFrameFactory.class.getCanonicalName() + "." + methodName.substring(methodName.indexOf("$") + 1, methodName.lastIndexOf("$"));
        } else {
            return DataFrameFactory.class.getCanonicalName() + "." + methodName;
        }
    }


    private void generateSuccessDataframeAudit(DataFrameAudit dataFrameAudit, SGUseCasePayload useCasePayload, long dataframeSize, Table table) {
        dataFrameAudit.setPayload(useCasePayload);
        dataFrameAudit.setDataFrameAuditStatus(COMPLETED);
        dataFrameAudit.setDataframeSize(dataframeSize);
        dspServiceClient.updateDataFrameAudit(dataFrameAudit);
    }

}
