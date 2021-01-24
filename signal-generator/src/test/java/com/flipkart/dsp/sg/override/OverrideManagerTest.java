package com.flipkart.dsp.sg.override;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.helper.*;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({OverrideManager.class, Executors.class, Thread.class, RunIdDataFrameOverrideManager.class, PartitionOverrideManager.class, FileOverrideManager.class,
        DefaultDataframeOverrideManager.class, HiveTableOverrideManager.class,
        HiveQueryOverrideManager.class})
public class OverrideManagerTest {

    @Mock
    private Request request;
    @Mock
    private Workflow workflow;
    @Mock
    private DataFrame dataFrame;
    @Mock
    private DataTable dataTable;
    @Mock
    private DataSource dataSource;
    @Mock
    private PipelineStep pipelineStep;
    @Mock
    private Future<Object> objectFuture;
    @Mock
    private DataFrameAudit dataFrameAudit;
    @Mock
    private EventAuditUtil eventAuditUtil;
    @Mock
    private WorkflowDetails workflowDetails;
    @Mock
    private ExecutorService executorService;
    @Mock
    private Future<Map<String, Long>> mapFuture;
    @Mock
    private FileOverrideHelper fileOverrideHelper;
    @Mock
    private FileOverrideManager fileOverrideManager;
    @Mock
    private CSVDataframeOverride csvDataframeOverride;
    @Mock
    private HiveDataframeOverride hiveDataframeOverride;
    @Mock
    private Future<DataFrameAudit> dataFrameAuditFuture;
    @Mock
    private RunIdDataframeOverride runIdDataframeOverride;
    @Mock
    private ExecuteWorkflowRequest executeWorkflowRequest;
    @Mock
    private Future<Optional<DataFrameAudit>> optionalFuture;
    @Mock
    private DataFrameOverrideHelper dataFrameOverrideHelper;
    @Mock
    private HiveQueryOverrideHelper hiveQueryOverrideHelper;
    @Mock
    private PartitionOverrideManager partitionOverrideManager;
    @Mock
    private HiveTableOverrideManager hiveTableOverrideManager;
    @Mock
    private DefaultDataframeOverride defaultDataframeOverride;
    @Mock
    private HiveQueryOverrideManager hiveQueryOverrideManager;
    @Mock
    private HiveQueryDataframeOverride hiveQueryDataframeOverride;
    @Mock
    private PartitionDataframeOverride partitionDataframeOverride;
    @Mock
    private RunIdDataFrameOverrideManager runIdDataFrameOverrideManager;
    @Mock
    private DefaultDataframeOverrideManager defaultDataframeOverrideManager;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String workflowName = "workflowName";
    private String csvDataFrameName = "csvDataFrame";
    private String hiveDataFrameName = "hiveDataFrame";
    private String runIdDataFrameName = "runIdDataFrame";
    private String defaultDataFrameName = "defaultDataFrame";
    private String hiveQueryDataFrameName = "hiveQueryDataFrame";
    private String partitionDataFrameName = "partitionDataFrame";

    private OverrideManager overrideManager;
    private Map<String, Long> tableInformation = new HashMap<>();
    private Map<String, DataframeOverride> dataframeOverrideMap = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.mockStatic(Executors.class);
        PowerMockito.mockStatic(FileOverrideManager.class);
        PowerMockito.mockStatic(PartitionOverrideManager.class);
        PowerMockito.mockStatic(HiveTableOverrideManager.class);
        PowerMockito.mockStatic(HiveQueryOverrideManager.class);
        PowerMockito.mockStatic(RunIdDataFrameOverrideManager.class);
        PowerMockito.mockStatic(DefaultDataframeOverrideManager.class);

        MockitoAnnotations.initMocks(this);
        this.overrideManager = spy(new OverrideManager(eventAuditUtil, fileOverrideHelper, dataFrameOverrideHelper, hiveQueryOverrideHelper));

        when(request.getId()).thenReturn(requestId);
        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(request.getData()).thenReturn(executeWorkflowRequest);
        when(executeWorkflowRequest.getTables()).thenReturn(tableInformation);
        doNothing().when(eventAuditUtil).createOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeOverrideMap);
        doNothing().when(eventAuditUtil).createOverrideEndDebugEvent(requestId, workflowId, workflowName);
    }

    @Test
    public void testOverrideCase1Failure() throws Exception {
        dataframeOverrideMap.put(runIdDataFrameName, runIdDataframeOverride);
        PowerMockito.when(Executors.newFixedThreadPool(1)).thenReturn(executorService);
        PowerMockito.whenNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper).thenReturn(runIdDataFrameOverrideManager);
        when(executorService.submit(runIdDataFrameOverrideManager)).thenReturn(dataFrameAuditFuture);
        when(dataFrameAuditFuture.isDone()).thenReturn(false, true);
        when(dataFrameAuditFuture.get(5, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException());
        when(executorService.isShutdown()).thenReturn(false);
        when(executorService.shutdownNow()).thenReturn(new ArrayList<>());

        boolean isException = false;
        try {
            overrideManager.override(request, workflowDetails, dataframeOverrideMap, pipelineStep);
        } catch (DataframeOverrideException e) {
            isException = true;
        }

        assertTrue(isException);

        PowerMockito.verifyStatic(Executors.class);
        Executors.newFixedThreadPool(1);
        verify(workflowDetails, times(1)).getWorkflow();
        verify(request, times(2)).getData();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(request, times(2)).getId();
        verify(workflow).getId();
        verify(workflow).getName();
        verifyNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper);
        verify(executorService).submit(runIdDataFrameOverrideManager);
        verify(dataFrameAuditFuture, times(2)).isDone();
        verify(dataFrameAuditFuture).get(5, TimeUnit.MILLISECONDS);
        verify(executorService).isShutdown();
        verify(executorService).shutdownNow();
        verify(eventAuditUtil).createOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeOverrideMap);
    }


    @Test
    public void testOverrideCase2Failure() throws Exception {
        dataframeOverrideMap.put(runIdDataFrameName, runIdDataframeOverride);
        PowerMockito.when(Executors.newFixedThreadPool(1)).thenReturn(executorService);
        PowerMockito.whenNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper).thenReturn(runIdDataFrameOverrideManager);
        when(executorService.submit(runIdDataFrameOverrideManager)).thenReturn(dataFrameAuditFuture);
        when(dataFrameAuditFuture.isDone()).thenReturn(false, true);
        when(dataFrameAuditFuture.get(5, TimeUnit.MILLISECONDS)).thenReturn(dataFrameAudit);
        when(dataFrameAuditFuture.get()).thenThrow(new InterruptedException());
        when(executorService.isShutdown()).thenReturn(false);
        when(executorService.shutdownNow()).thenReturn(new ArrayList<>());

        boolean isException = false;
        try {
            overrideManager.override(request, workflowDetails, dataframeOverrideMap, pipelineStep);
        } catch (DataframeOverrideException e) {
            isException = true;
        }

        assertTrue(isException);
        PowerMockito.verifyStatic(Executors.class);
        Executors.newFixedThreadPool(1);
        verify(workflowDetails, times(1)).getWorkflow();
        verify(request, times(2)).getData();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(request, times(2)).getId();
        verify(workflow).getId();
        verify(workflow).getName();
        verifyNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper);
        verify(executorService).submit(runIdDataFrameOverrideManager);
        verify(dataFrameAuditFuture, times(2)).isDone();
        verify(dataFrameAuditFuture).get(5, TimeUnit.MILLISECONDS);
        verify(dataFrameAuditFuture).get();
        verify(executorService).isShutdown();
        verify(executorService).shutdownNow();
        verify(eventAuditUtil).createOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeOverrideMap);
    }

    @Test
    public void testOverrideSuccess() throws Exception {
        populateDataFrameMap();

        PowerMockito.when(Executors.newFixedThreadPool(10)).thenReturn(executorService);
        mockManagerCalls();
        mockSubmitCalls();
        mockFutureCalls();
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        Thread.sleep(10000L);
        when(executorService.isShutdown()).thenReturn(false);
        when(executorService.shutdownNow()).thenReturn(new ArrayList<>());

        overrideManager.override(request, workflowDetails, dataframeOverrideMap, pipelineStep);
        PowerMockito.verifyStatic(Executors.class);
        Executors.newFixedThreadPool(10);
        verify(workflowDetails).getWorkflow();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(request, times(3)).getId();
        verify(workflow, times(2)).getId();
        verify(workflow, times(2)).getName();
        verifyManagerCalls();
        verifySubmitCalls();
        verifyFutureCalls();
        verify(executorService).isShutdown();
        verify(eventAuditUtil).createOverrideStartDebugEvent(requestId, workflowId, workflowName, dataframeOverrideMap);
        verify(eventAuditUtil).createOverrideEndDebugEvent(requestId, workflowId, workflowName);
    }

    private void populateDataFrameMap() {
        dataframeOverrideMap.put(runIdDataFrameName, runIdDataframeOverride);
        dataframeOverrideMap.put(csvDataFrameName, csvDataframeOverride);
        dataframeOverrideMap.put(runIdDataFrameName, runIdDataframeOverride);
        dataframeOverrideMap.put(hiveDataFrameName, hiveDataframeOverride);
        dataframeOverrideMap.put(defaultDataFrameName, defaultDataframeOverride);
        dataframeOverrideMap.put(partitionDataFrameName, partitionDataframeOverride);
        dataframeOverrideMap.put(hiveQueryDataFrameName, hiveQueryDataframeOverride);
    }

    private void mockManagerCalls() throws Exception {
        PowerMockito.whenNew(FileOverrideManager.class).withArguments(csvDataFrameName, request, workflowDetails, eventAuditUtil, csvDataframeOverride, fileOverrideHelper, pipelineStep).thenReturn(fileOverrideManager);
        PowerMockito.whenNew(HiveTableOverrideManager.class).withArguments(hiveDataFrameName, request, workflowDetails, eventAuditUtil, hiveDataframeOverride, dataFrameOverrideHelper).thenReturn(hiveTableOverrideManager);
        PowerMockito.whenNew(PartitionOverrideManager.class).withArguments(partitionDataFrameName, request, workflowDetails, eventAuditUtil, partitionDataframeOverride, dataFrameOverrideHelper, pipelineStep).thenReturn(partitionOverrideManager);
        PowerMockito.whenNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper).thenReturn(runIdDataFrameOverrideManager);
        PowerMockito.whenNew(DefaultDataframeOverrideManager.class).withArguments(defaultDataFrameName, request, workflowDetails, eventAuditUtil, defaultDataframeOverride, dataFrameOverrideHelper).thenReturn(defaultDataframeOverrideManager);
        PowerMockito.whenNew(HiveQueryOverrideManager.class).withArguments(hiveQueryDataFrameName, requestId, workflowDetails, eventAuditUtil, hiveQueryDataframeOverride, dataFrameOverrideHelper, hiveQueryOverrideHelper, pipelineStep).thenReturn(hiveQueryOverrideManager);
    }

    private void mockSubmitCalls() {
        when(executorService.submit(hiveTableOverrideManager)).thenReturn(mapFuture);
        when(executorService.submit(hiveQueryOverrideManager)).thenReturn(objectFuture);
        when(executorService.submit(fileOverrideManager)).thenReturn(dataFrameAuditFuture);
        when(executorService.submit(partitionOverrideManager)).thenReturn(dataFrameAuditFuture);
        when(executorService.submit(defaultDataframeOverrideManager)).thenReturn(optionalFuture);
        when(executorService.submit(runIdDataFrameOverrideManager)).thenReturn(dataFrameAuditFuture);
    }

    private void mockFutureCalls() throws Exception {
        String object = "object";
        Optional<DataFrameAudit> dataFrameAuditOptional = Optional.of(dataFrameAudit);

        when(objectFuture.get()).thenReturn(object);
        when(mapFuture.get()).thenReturn(tableInformation);
        when(dataFrameAuditFuture.get()).thenReturn(dataFrameAudit);
        when(optionalFuture.get()).thenReturn(dataFrameAuditOptional);

        when(mapFuture.isDone()).thenReturn(true);
        when(objectFuture.isDone()).thenReturn(true);
        when(optionalFuture.isDone()).thenReturn(true);
        when(dataFrameAuditFuture.isDone()).thenReturn(false, true);

        when(objectFuture.get(5, TimeUnit.MILLISECONDS)).thenReturn(object);
        when(mapFuture.get(5, TimeUnit.MILLISECONDS)).thenReturn(tableInformation);
        when(dataFrameAuditFuture.get(5, TimeUnit.MILLISECONDS)).thenReturn(dataFrameAudit);
        when(optionalFuture.get(5, TimeUnit.MILLISECONDS)).thenReturn(dataFrameAuditOptional);
    }

    private void verifyManagerCalls() throws Exception {
        verifyNew(FileOverrideManager.class).withArguments(csvDataFrameName, request, workflowDetails, eventAuditUtil, csvDataframeOverride, fileOverrideHelper, pipelineStep);
        verifyNew(HiveTableOverrideManager.class).withArguments(hiveDataFrameName, request, workflowDetails, eventAuditUtil, hiveDataframeOverride, dataFrameOverrideHelper);
        verifyNew(PartitionOverrideManager.class).withArguments(partitionDataFrameName, request, workflowDetails, eventAuditUtil, partitionDataframeOverride, dataFrameOverrideHelper, pipelineStep);
        verifyNew(RunIdDataFrameOverrideManager.class).withArguments(runIdDataFrameName, request, workflowDetails, eventAuditUtil, runIdDataframeOverride, dataFrameOverrideHelper);
        verifyNew(DefaultDataframeOverrideManager.class).withArguments(defaultDataFrameName, request, workflowDetails, eventAuditUtil, defaultDataframeOverride, dataFrameOverrideHelper);
        verifyNew(HiveQueryOverrideManager.class).withArguments(hiveQueryDataFrameName, requestId, workflowDetails, eventAuditUtil, hiveQueryDataframeOverride, dataFrameOverrideHelper, hiveQueryOverrideHelper, pipelineStep);
    }

    private void verifySubmitCalls() {
        verify(executorService).submit(fileOverrideManager);
        verify(executorService).submit(partitionOverrideManager);
        verify(executorService).submit(hiveTableOverrideManager);
        verify(executorService).submit(runIdDataFrameOverrideManager);
        verify(executorService).submit(runIdDataFrameOverrideManager);
        verify(executorService).submit(defaultDataframeOverrideManager);
        verify(executorService).submit(hiveQueryOverrideManager);
    }

    private void verifyFutureCalls() throws Exception {
        verify(optionalFuture).get();
        verify(mapFuture, times(2)).get();
        verify(objectFuture, times(4)).get();
        verify(dataFrameAuditFuture, times(3)).get();

        verify(mapFuture, times(4)).isDone();
        verify(objectFuture, times(8)).isDone();
        verify(optionalFuture, times(2)).isDone();
        verify(dataFrameAuditFuture, times(6)).isDone();

        verify(mapFuture, times(4)).get(5, TimeUnit.MILLISECONDS);
        verify(objectFuture, times(8)).get(5, TimeUnit.MILLISECONDS);
        verify(optionalFuture, times(2)).get(5, TimeUnit.MILLISECONDS);
        verify(dataFrameAuditFuture, times(5)).get(5, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testGetDataTableForOverrideCase4() {
        String dbName = "test_db", tableName = "test_table";
        when(hiveDataframeOverride.getDatabase()).thenReturn(dbName);
        when(hiveDataframeOverride.getTableName()).thenReturn(tableName);

        DataTable expected = overrideManager.getDataTableForOverride(dataFrame, hiveDataframeOverride, dataTable);
        assertEquals(expected.getId(), tableName);
        assertEquals(expected.getDataSource().getId(), dbName);
        verify(hiveDataframeOverride).getDatabase();
        verify(hiveDataframeOverride).getTableName();
    }


    @Test
    public void testGetDataTableForOverrideCase6() {
        String dbName = "test_db", tableName = "test_table";
        when(dataTable.getId()).thenReturn(tableName);
        when(dataTable.getDataSource()).thenReturn(dataSource);
        when(dataSource.getId()).thenReturn(dbName);

        DataTable expected = overrideManager.getDataTableForOverride(dataFrame, defaultDataframeOverride, dataTable);
        assertEquals(expected.getId(), tableName);
        assertEquals(expected.getDataSource().getId(), dbName);
        verify(dataTable).getId();
        verify(dataTable).getDataSource();
        verify(dataSource).getId();
    }
}
