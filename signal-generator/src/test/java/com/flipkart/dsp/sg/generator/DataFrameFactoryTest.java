/*
package com.flipkart.dsp.sg.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.GravityClient;
import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.QueryExecutionException;
import com.flipkart.dsp.sg.executors.HiveQueryExecutor;
import com.flipkart.dsp.sg.helper.SGTypeHelper;
import com.flipkart.dsp.sg.helper.SGUseCasePayloadBuilder;
import com.flipkart.dsp.sg.helper.ScopeHelper;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.sg.utils.FutureUtils;
import com.flipkart.dsp.utils.DataframeSizeExtractor;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

*/
/**
 * +
 *//*

@RunWith(PowerMockRunner.class)
@PrepareForTest({CompletableFuture.class, DataFrameFactory.class, FutureUtils.class})
public class DataFrameFactoryTest {
    @Mock private Signal signal;
    @Mock private Logger logger;
    @Mock private HiveTable table;
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private Throwable throwable;
    @Mock private ScopeHelper scopeHelper;
    @Mock private HiveConfig hiveConfig;
    @Mock private SGTypeHelper sgTypeHelper;
    @Mock private ObjectMapper objectMapper;
    @Mock private GravityClient gravityClient;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private QueryGenerator queryGenerator;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private DataFrameConfig dataFrameConfig;
    @Mock private ExecutorService executorService;
    @Mock private DSPServiceClient dspServiceClient;
    @Mock private SGUseCasePayload sgUseCasePayload;
    @Mock private HiveQueryExecutor hiveQueryExecutor;
    @Mock private ExecutionException executionException;
    @Mock private CompletableFuture<Object> completableFuture;
    @Mock private DataframeSizeExtractor dataframeSizeExtractor;
    @Mock private DataFrameOverrideAudit dataFrameOverrideAudit;
    @Mock private SGUseCasePayloadBuilder sgUsecasePayloadBuilder;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private Long requestId = 1L;
    private Long dataFrameId = 1L;
    private Long pipelineStepId = 1L;
    private String dbName = "test_db";
    private String tableName = "test_table";
    private DataFrameFactory dataFrameFactory;
    private Set<DataFrame> dataFrames = new HashSet<>();
    private Set<DataFrameScope> dataFrameScopes = new HashSet<>();
    private LinkedHashSet<Signal> signals = new LinkedHashSet<>();
    private Pair<Table, List<String>> dataFrameTableQueryPair = new Pair<>(table, new ArrayList<>());

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(FutureUtils.class);
        PowerMockito.mockStatic(CompletableFuture.class);
        MockitoAnnotations.initMocks(this);
        this.dataFrameFactory = spy(new DataFrameFactory(logger, hiveConfig, scopeHelper, sgTypeHelper, objectMapper, gravityClient, eventAuditUtil, queryGenerator,
                executorService, dspServiceClient, hiveQueryExecutor, dataframeSizeExtractor, sgUsecasePayloadBuilder));

        String dataFrameName = "dataFrameName";

        signals.add(signal);
        dataFrames.add(dataFrame);
        dataFrameScopes.add(dataFrameScope);

        when(table.getDbName()).thenReturn(dbName);
        when(table.getTableName()).thenReturn(tableName);
        when(table.getRefreshId()).thenReturn(requestId);

        when(dataFrame.getId()).thenReturn(dataFrameId);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(dataFrame.getDataFrameConfig()).thenReturn(dataFrameConfig);

        when(dataFrameOverrideAudit.getId()).thenReturn(1L);
        when(dataFrameConfig.getVisibleSignals()).thenReturn(signals);

        when(dataFrameAudit.getRunId()).thenReturn(1L);
        when(dataFrameAudit.getDataframeSize()).thenReturn(1L);
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(dataFrameAudit.getDataFrameConfig()).thenReturn(dataFrameConfig);

        when(dataFrameGenerateRequest.getRequestId()).thenReturn(requestId);
        when(dataFrameGenerateRequest.getDataFrames()).thenReturn(dataFrames);

        when(dspServiceClient.saveDataFrameAudit(any())).thenReturn(dataFrameAudit);
        doNothing().when(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
        when(dspServiceClient.getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId)).thenReturn(dataFrameOverrideAudit);

        doNothing().when(eventAuditUtil).createDataFrameGenerationStartInfoEvent(any(), any(), any(), any());
        doNothing().when(eventAuditUtil).createDataFrameCompletionInfoEvent(any(), any(), any(), any(), any());
        doNothing().when(eventAuditUtil).createDataframeQueryGenerationErrorEvent(any(), any(), any(), any(), any());
        doNothing().when(eventAuditUtil).createDataFrameQueryExecutionErrorEvent(any(), any(), any(), any(), any());

        doNothing().when(hiveQueryExecutor).executeList(anyList(), anyString());
        when(dataFrameConfig.getDataFrameScopeSet()).thenReturn(dataFrameScopes);
        when(dataframeSizeExtractor.getDataframeSize(sgUseCasePayload)).thenReturn(1L);
        PowerMockito.when(CompletableFuture.supplyAsync(any(), any())).thenReturn(completableFuture);
        when(scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest)).thenReturn(dataFrameScopes);
        when(sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes)).thenReturn(SGType.NO_QUERY);
        when(queryGenerator.generateQuery(1L, dataFrame ,dataFrameGenerateRequest, dataFrameScopes)).thenReturn(dataFrameTableQueryPair);
        when(sgUsecasePayloadBuilder.build(dataFrameGenerateRequest, null, dataFrameScopes, dataFrame)).thenReturn(sgUseCasePayload);
    }

    @Test
    public void testInvokeSuccess() throws Exception {
        dataFrameFactory.invoke(workflow, pipelineStepId, dataFrameGenerateRequest);
        verify(dataFrameGenerateRequest).getDataFrames();
        verify(eventAuditUtil).createDataFrameGenerationStartInfoEvent(any(), any(), any(), any());
        verify(scopeHelper).getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        verify(dataFrameGenerateRequest, times(4)).getRequestId();
        verify(dataFrame).getId();
        verify(dspServiceClient).getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(dataFrameOverrideAudit).getId();
        verify(dspServiceClient).saveDataFrameAudit(any());
        verify(sgTypeHelper).calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(dataFrameAudit, times(2)).getDataFrame();
        verify(dataFrameAudit).getRunId();
        verify(queryGenerator).generateQuery(1L, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(dataFrame, times(2)).getName();
        PowerMockito.verifyStatic(CompletableFuture.class);
        CompletableFuture.supplyAsync(any(), any());
        verify(dataFrameAudit).getDataFrameConfig();
        verify(dataFrameConfig).getDataFrameScopeSet();
        verify(sgUsecasePayloadBuilder).build(dataFrameGenerateRequest, null, dataFrameScopes, dataFrame);
        verify(dataframeSizeExtractor).getDataframeSize(sgUseCasePayload);
        verify(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
    }

    @Test
    public void testInvokeFailureCase1() throws Exception {
        when(sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes)).thenThrow(new TableNotFoundException(tableName, "Error"));
        boolean isException = false;
        try {
            dataFrameFactory.invoke(workflow, pipelineStepId, dataFrameGenerateRequest);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "Exception generating data-frame " + dataFrameId);
        }

        assertTrue(isException);
        verify(dataFrameGenerateRequest).getDataFrames();
        verify(eventAuditUtil).createDataFrameGenerationStartInfoEvent(any(), any(), any(), any());
        verify(scopeHelper).getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        verify(dataFrameGenerateRequest, times(4)).getRequestId();
        verify(dataFrame, times(2)).getId();
        verify(dspServiceClient).getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(dataFrameOverrideAudit).getId();
        verify(dspServiceClient).saveDataFrameAudit(any());
        verify(sgTypeHelper).calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
        verify(eventAuditUtil).createDataframeQueryGenerationErrorEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void testInvokeFailureCase2() throws Exception {
        PowerMockito.when(FutureUtils.getEntitiesFromFutures(any())).thenThrow(executionException);
        boolean isException = false;
        try {
            dataFrameFactory.invoke(workflow, pipelineStepId, dataFrameGenerateRequest);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "Failure occurred while running queries");
        }

        assertTrue(isException);
        verify(dataFrameGenerateRequest).getDataFrames();
        verify(eventAuditUtil).createDataFrameGenerationStartInfoEvent(any(), any(), any(), any());
        verify(scopeHelper).getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        verify(dataFrameGenerateRequest, times(4)).getRequestId();
        verify(dataFrame).getId();
        verify(dspServiceClient).getDataFrameOverrideAuditByIdAndRequestId(dataFrameId, requestId);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(dataFrameOverrideAudit).getId();
        verify(dspServiceClient).saveDataFrameAudit(any());
        verify(sgTypeHelper).calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(dataFrameAudit).getDataFrame();
        verify(dataFrameAudit).getRunId();
        verify(queryGenerator).generateQuery(1L, dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        verify(dataFrame, times(2)).getName();
        PowerMockito.verifyStatic(CompletableFuture.class);
        CompletableFuture.supplyAsync(any(), any());
        PowerMockito.verifyStatic(FutureUtils.class);
        FutureUtils.getEntitiesFromFutures(any());
    }

    @Test
    @Ignore
    public void testGenerateDataFrameSuccess() throws Exception {
        doThrow(new DSPClientException(null)).when(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
        Triplet<DataFrameAudit, Table, List<String>> quartet = new Triplet<>(dataFrameAudit, table, new ArrayList<>());

        dataFrameFactory.generateDataFrame(quartet, requestId, workflow, "methodName$final$");
        verify(hiveQueryExecutor).executeList(anyList(), anyString());
        verify(table, times(4)).getDbName();
        verify(table, times(4)).getTableName();
        verify(dataFrameAudit, times(3)).getDataFrame();
        verify(dataFrame, times(3)).getName();
        verify(dataFrameAudit).getDataframeSize();
        verify(table, times(2)).getRefreshId();
        verify(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
        verify(eventAuditUtil).createDataFrameCompletionInfoEvent(any(), any(), any(), any(), any());
    }

    @Test
    @Ignore
    public void testGenerateDataFrameFailure() throws Exception {
        Triplet<DataFrameAudit, Table, List<String>> quartet = new Triplet<>(dataFrameAudit, table, new ArrayList<>());
        doThrow(new QueryExecutionException("Error", throwable)).when(hiveQueryExecutor).executeList(any(), anyString());

        boolean isException = false;
        try {
            dataFrameFactory.generateDataFrame(quartet, requestId, workflow, "methodName$final$");
        } catch (Exception e) {
            isException = true;
            assertTrue(e.getMessage().contains( "Failed to run following query"));
        }

        assertTrue(isException);
        verify(hiveQueryExecutor).executeList(anyList(), anyString());
        verify(table).getDbName();
        verify(table).getTableName();
        verify(dspServiceClient).updateDataFrameAudit(dataFrameAudit);
   }
}
*/
