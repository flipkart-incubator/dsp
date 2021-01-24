//package com.flipkart.dsp.service;
//
//import com.flipkart.dsp.actors.RequestActor;
//import com.flipkart.dsp.dao.DataFrameAuditDAO;
//import com.flipkart.dsp.dao.RequestDAO;
//import com.flipkart.dsp.dao.WorkflowAuditDAO;
//import com.flipkart.dsp.dao.WorkflowDAO;
//import com.flipkart.dsp.dao.core.TransactionLender;
//import com.flipkart.dsp.dao.core.WorkUnit;
//import com.flipkart.dsp.db.entities.DataFrameAuditEntity;
//import com.flipkart.dsp.db.entities.DataFrameEntity;
//import com.flipkart.dsp.db.entities.RequestEntity;
//import com.flipkart.dsp.db.entities.WorkflowEntity;
//import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
//import com.flipkart.dsp.entities.request.Request;
//import com.flipkart.dsp.entities.script.Script;
//import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
//import com.flipkart.dsp.entities.sg.dto.*;
//import com.flipkart.dsp.entities.workflow.Workflow;
//import com.flipkart.dsp.entities.workflow.WorkflowDetails;
//import com.flipkart.dsp.mesos.WorkflowMesosExecutionDriver;
//import com.flipkart.dsp.models.ScriptVariable;
//import com.flipkart.dsp.utils.DataframeSizeExtractor;
//import com.flipkart.dsp.utils.DataframeUtils;
//import com.flipkart.dsp.validation.Validator;
//import org.hibernate.Session;
//import org.hibernate.SessionFactory;
//import org.hibernate.Transaction;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.util.*;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.*;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({AzkabanWorkflowExecutionService.class, WorkUnit.class, DataframeUtils.class})
//public class AzkabanWorkflowExecutionServiceTest {
//
//    @Mock private Script script;
//    @Mock private Request request;
//    @Mock private Session session;
//    @Mock private WorkUnit workUnit;
//    @Mock private Workflow workflow;
//    @Mock private Validator validator;
//    @Mock private RequestDAO requestDAO;
//    @Mock private WorkflowDAO workflowDAO;
//    @Mock private Transaction transaction;
//    @Mock private PipelineStep pipelineStep;
//    @Mock private RequestActor requestActor;
//    @Mock private RequestEntity requestEntity;
//    @Mock private SessionFactory sessionFactory;
//    @Mock private ScriptVariable scriptVariable;
//    @Mock private DataFrameMultiKey dataFrameKey;
//    @Mock private DataFrameEntity dataFrameEntity;
//    @Mock private WorkflowDetails workflowDetails;
//    @Mock private WorkflowAuditDAO workflowAuditDAO;
//    @Mock private SGUseCasePayload sgUseCasePayload;
//    @Mock private SGUseCasePayload sgUseCasePayload1;
//    @Mock private RequestEntity parentRequestEntity;
//    @Mock private WorkflowEntity parentWorkflowEntity;
//    @Mock private DataFrameAuditDAO dataFrameAuditDAO;
//    @Mock private SGJobOutputPayload sgJobOutputPayload;
//    @Mock private DataFrameAuditEntity dataFrameAuditEntity;
//    @Mock private DataframeSizeExtractor dataframeSizeExtractor;
//    @Mock private WorkflowMesosExecutionDriver workflowMesosExecutionDriver;
//
//    private Long requestId = 1L;
//    private Long dataFrameId = 1L;
//    private Boolean failFirst = true;
//    private Long parentWorkflowId = 2L;
//    private String mesosQueue = "testQueue";
//    private Long parentWorkflowRefreshId = 2L;
//    private String workflowExecutionId = "abcdefghi";
//    private AzkabanWorkflowExecutionService azkabanWorkflowExecutionService;
//
//    private List<String> partitions = new ArrayList<>();
//    private Set<String> dataFramesValues = new HashSet<>();
//    private List<DataFrameKey> dataFrameKeys = new ArrayList<>();
//    private List<PipelineStep> pipelineSteps = new ArrayList<>();
//    private Set<ScriptVariable> scriptVariables = new HashSet<>();
//    private Set<DataFrameEntity> dataFrameEntities = new HashSet<>();
//    private List<WorkflowEntity> workflowEntities = new ArrayList<>();
//    private Set<SGUseCasePayload> sgUseCasePayloadSet = new HashSet<>();
//    private Map<List<DataFrameKey>, Set<String>> dataFrames = new HashMap<>();
//    private LinkedHashMap<String, DataFrameColumnType> columnMetaData = new LinkedHashMap<>();
//
//    @Before
//    public void setUp() throws Exception {
//        PowerMockito.mockStatic(DataframeUtils.class);
//        MockitoAnnotations.initMocks(this);
//        TransactionLender transactionLender = new TransactionLender(sessionFactory);
//        this.azkabanWorkflowExecutionService = spy(new AzkabanWorkflowExecutionService(validator, requestActor ,workflowDAO,
//                workflowAuditDAO, transactionLender, dataFrameAuditDAO, dataframeSizeExtractor, workflowMesosExecutionDriver));
//
//        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
//        when(sessionFactory.getCurrentSession()).thenReturn(session);
//        when(session.getTransaction()).thenReturn(transaction);
//        when(transaction.isActive()).thenReturn(true);
//
//        dataFramesValues.add("value");
//        pipelineSteps.add(pipelineStep);
//        dataFrameKeys.add(dataFrameKey);
//        String partitionColumn = "partition";
//        partitions.add(partitionColumn);
//        scriptVariables.add(scriptVariable);
//        sgUseCasePayloadSet.add(sgUseCasePayload);
//        dataFrameEntities.add(dataFrameEntity);
//        dataFrames.put(dataFrameKeys, dataFramesValues);
//        columnMetaData.put("column1", DataFrameColumnType.IN);
//
//
//        Long workflowId = 1L;
//        when(workflow.getId()).thenReturn(workflowId);
//        String workflowName = "workflowName";
//        when(workflow.getName()).thenReturn(workflowName);
//        when(workflow.getRetries()).thenReturn(1);
//        when(workflow.getParentWorkflowId()).thenReturn(parentWorkflowId);
//
//        when(workflowDetails.getWorkflow()).thenReturn(workflow);
//        when(workflowDetails.getPipelineSteps()).thenReturn(pipelineSteps);
//        when(workflowDetails.getParentWorkflowRefreshId()).thenReturn(null);
//
//        when(pipelineStep.getScript()).thenReturn(script);
//        Long pipelineStepId = 1L;
//        when(pipelineStep.getId()).thenReturn(pipelineStepId);
//        when(pipelineStep.getParentPipelineStepId()).thenReturn(null);
//        when(pipelineStep.getPartitions()).thenReturn(partitions);
//
//
//        when(script.getInputVariables()).thenReturn(scriptVariables);
//        String scriptVariableName = "scriptVariableName";
//        when(scriptVariable.getName()).thenReturn(scriptVariableName);
//
//        when(requestEntity.getId()).thenReturn(requestId);
//        when(parentRequestEntity.getId()).thenReturn(parentWorkflowRefreshId);
//        when(dataFrameEntity.getId()).thenReturn(dataFrameId);
//        when(parentWorkflowEntity.getId()).thenReturn(parentWorkflowId);
//        when(parentWorkflowEntity.getDataFrames()).thenReturn(dataFrameEntities);
//
//        when(sgUseCasePayload.getRequestId()).thenReturn(requestId);
//        when(sgUseCasePayload.getDataframes()).thenReturn(dataFrames);
//        doNothing().when(sgUseCasePayload).addColumnNameToDataFrameKeys();
//        String dataFrameName = "scriptVariableName";
//        when(sgUseCasePayload.getDataFrameId()).thenReturn(dataFrameName);
//        when(sgUseCasePayload.getColumnMetaData()).thenReturn(columnMetaData);
//
//        when(dataFrameKey.getName()).thenReturn(partitionColumn);
//        when(dataFrameKey.getValues()).thenReturn(dataFramesValues);
//        when(request.getWorkflowDetails()).thenReturn(workflowDetails);
//        when(dataFrameAuditEntity.getPayload()).thenReturn(sgUseCasePayload);
//        when(workflowDAO.get(parentWorkflowId)).thenReturn(parentWorkflowEntity);
//        when(sgJobOutputPayload.getSgUseCasePayloadSet()).thenReturn(sgUseCasePayloadSet);
//        when(requestDAO.getLatestSuccessFullRequest(parentWorkflowId)).thenReturn(Optional.of(requestEntity));
//
//        when(validator.validateLatestSuccessRequest(parentWorkflowId)).thenReturn(parentRequestEntity);
//        doNothing().when(validator).validateParentWorkflowRefreshId(requestEntity, parentWorkflowRefreshId, parentWorkflowId);
//        PowerMockito.when(DataframeUtils.getReferenceDFForParallelism(any(), any(), any())).thenReturn(sgUseCasePayload);
//        String parentWorkflowExecutionId = "parentWorkflowExecutionId";
//        when(workflowAuditDAO.getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId)).thenReturn(parentWorkflowExecutionId);
//        when(dataFrameAuditDAO.getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED)).thenReturn(Optional.of(dataFrameAuditEntity));
//    }
//
//    @Test
//    public void testExecuteWorkflowCase1() throws Exception {
//        when(workflowDetails.getParentWorkflowRefreshId()).thenReturn(parentWorkflowRefreshId);
//        azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//
//        verify(workflowDetails, times(3)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflow).getRetries();
//        verify(request).getWorkflowDetails();
//        verify(workflowDetails).getParentWorkflowRefreshId();
//        verify(workflow).getId();
//        verify(parentWorkflowEntity).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(2)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(6)).getColumnMetaData();
//        verify(sgUseCasePayload, times(3)).getDataframes();
//        verify(sgUseCasePayload, times(2)).getRequestId();
//        verify(sgUseCasePayload, times(2)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//        verify(dataFrameAuditEntity, times(1)).getPayload();
//        verify(workflow, times(1)).getName();
//        verify(pipelineStep, times(4)).getPartitions();
//        PowerMockito.mockStatic(DataframeUtils.class);
//        DataframeUtils.getReferenceDFForParallelism(any(), any(), any());
//        verify(dataFrameKey, times(1)).getValues();
//    }
//
//    @Test
//    public void testExecuteWorkflowCase2() throws Exception {
//        when(workflow.getParentWorkflowId()).thenReturn(null);
//        azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//
//        verify(workflowDetails, times(3)).getWorkflow();
//        verify(workflow, times(2)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflow, times(1)).getId();
//        verify(sgUseCasePayload, times(1)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(3)).getColumnMetaData();
//        verify(sgUseCasePayload, times(2)).getDataframes();
//        verify(sgUseCasePayload, times(1)).getRequestId();
//        verify(sgUseCasePayload, times(1)).getDataFrameId();
//        verify(workflow, times(1)).getName();
//        verify(pipelineStep, times(4)).getPartitions();
//        PowerMockito.mockStatic(DataframeUtils.class);
//        DataframeUtils.getReferenceDFForParallelism(any(), any(), any());
//        verify(dataFrameKey, times(1)).getValues();
//    }
//
//    @Test
//    public void testExecuteWorkflowCase3() throws Exception {
//        when(workflowDetails.getParentWorkflowRefreshId()).thenReturn(null);
//        when(parentWorkflowEntity.getId()).thenReturn(2L);
//        when(requestDAO.getLatestSuccessFullRequest(2L)).thenReturn(Optional.of(requestEntity));
//        when(dataFrameAuditEntity.getPayload()).thenReturn(sgUseCasePayload);
//
//        azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//        verify(workflowDetails, times(3)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflowDAO, times(2)).get(parentWorkflowId);
//        verify(workflow).getId();
//        verify(parentWorkflowEntity, times(2)).getId();
//        verify(validator, times(1)).validateLatestSuccessRequest(parentWorkflowRefreshId);
//        verify(parentRequestEntity).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(2)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(6)).getColumnMetaData();
//        verify(sgUseCasePayload, times(3)).getDataframes();
//        verify(sgUseCasePayload, times(2)).getRequestId();
//        verify(sgUseCasePayload, times(2)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//        verify(dataFrameAuditEntity, times(1)).getPayload();
//        verify(workflow, times(1)).getName();
//        verify(pipelineStep, times(4)).getPartitions();
//        PowerMockito.mockStatic(DataframeUtils.class);
//        DataframeUtils.getReferenceDFForParallelism(any(), any(), any());
//        verify(dataFrameKey, times(1)).getValues();
//    }
//
//    //Execution DataFrame Failure
//    @Test
//    public void testExecuteWorkflowCase1Failure() {
//        boolean isException = false;
//        dataFramesValues.clear();
//        dataFramesValues.add("");
//
//        when(parentWorkflowEntity.getId()).thenReturn(2L);
//        when(requestDAO.getLatestSuccessFullRequest(2L)).thenReturn(Optional.of(requestEntity));
//        when(requestEntity.getId()).thenReturn(requestId);
//        when(dataFrameAuditEntity.getPayload()).thenReturn(sgUseCasePayload);
//
//        try {
//            azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//        } catch (Exception e) {
//            isException = true;
//        }
//
//        assertTrue(isException);
//        verify(workflowDetails, times(2)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflowDAO, times(2)).get(2L);
//        verify(parentWorkflowEntity, times(2)).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(2)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(6)).getColumnMetaData();
//        verify(sgUseCasePayload, times(3)).getDataframes();
//        verify(sgUseCasePayload, times(2)).getRequestId();
//        verify(sgUseCasePayload, times(2)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//        verify(dataFrameAuditEntity, times(1)).getPayload();
//        verify(workflow, times(1)).getName();
//        verify(pipelineStep, times(1)).getPartitions();
//        PowerMockito.mockStatic(DataframeUtils.class);
//        DataframeUtils.getReferenceDFForParallelism(any(), any(), any());
//        verify(dataFrameKey, times(1)).getValues();
//    }
//
//    // Training DataFrame Failure
//    @Test
//    public void testExecuteWorkflowCase2Failure()  {
//        when(dataFrameAuditDAO.getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED)).thenReturn(Optional.empty());
//
//        boolean isException = false;
//        try {
//            azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//        } catch (Exception e) {
//            isException = true;
//            assertEquals(e.getCause().getMessage(), "Training dataframe not found for Dataframe : 1");
//        }
//
//        assertTrue(isException);
//        verify(workflowDetails, times(2)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflowDAO, times(2)).get(parentWorkflowId);
//        verify(parentWorkflowEntity, times(2)).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(1)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(3)).getColumnMetaData();
//        verify(sgUseCasePayload, times(1)).getDataframes();
//        verify(sgUseCasePayload, times(1)).getRequestId();
//        verify(sgUseCasePayload, times(1)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//    }
//
//    // Training DataFrame Failure
//    @Test
//    public void testExecuteWorkflowCase3Failure() {
//        when(dataFrameAuditEntity.getPayload()).thenReturn(null);
//
//        boolean isException = false;
//        try {
//            azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//        } catch (Exception e) {
//            isException = true;
//            assertEquals(e.getCause().getMessage(), "Training dataframe does not have output payload");
//        }
//
//        assertTrue(isException);
//
//        verify(workflowDetails, times(2)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflowDAO, times(2)).get(parentWorkflowId);
//        verify(parentWorkflowEntity, times(2)).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(1)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(3)).getColumnMetaData();
//        verify(sgUseCasePayload, times(1)).getDataframes();
//        verify(sgUseCasePayload, times(1)).getRequestId();
//        verify(sgUseCasePayload, times(1)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//        verify(dataFrameAuditEntity, times(1)).getPayload();
//    }
//
//    @Test
//    public void testExecuteWorkflowCase5Failure() {
//        Map<List<DataFrameKey>, Set<String>> dataFrames = new HashMap<>();
//        List<DataFrameKey> dataFrameKeys = new ArrayList<>();
//        Set<String> dataFramesValues = new HashSet<>();
//        dataFrameKeys.add(dataFrameKey);
//        dataFramesValues.add("");
//        dataFrames.put(dataFrameKeys, dataFramesValues);
//
//        when(dataFrameAuditEntity.getPayload()).thenReturn(sgUseCasePayload1);
//        when(sgUseCasePayload1.getColumnMetaData()).thenReturn(columnMetaData);
//        when(sgUseCasePayload1.getDataframes()).thenReturn(dataFrames);
//
//        boolean isException = false;
//        try {
//            azkabanWorkflowExecutionService.executeWorkflow(request, workflowDetails, sgJobOutputPayload, workflowExecutionId, mesosQueue, failFirst, pipelineStep);
//        } catch (Exception e) {
//            isException = true;
//            assertEquals(e.getCause().getMessage(), "More than 50% of the Reference partitions did not get corresponding partitions from other dataframes.");
//        }
//
//        assertTrue(isException);
//        verify(workflowDetails, times(2)).getWorkflow();
//        verify(workflow, times(3)).getParentWorkflowId();
//        verify(sgJobOutputPayload, times(1)).getSgUseCasePayloadSet();
//        verify(workflowDAO, times(2)).get(parentWorkflowId);
//        verify(parentWorkflowEntity, times(2)).getId();
//        verify(workflowAuditDAO, times(1)).getLatestWorkflowExecutionId(parentWorkflowId, parentWorkflowRefreshId);
//        verify(sgUseCasePayload, times(1)).addColumnNameToDataFrameKeys();
//        verify(sgUseCasePayload, times(3)).getColumnMetaData();
//        verify(sgUseCasePayload, times(2)).getDataframes();
//        verify(sgUseCasePayload, times(1)).getRequestId();
//        verify(sgUseCasePayload, times(1)).getDataFrameId();
//        verify(parentWorkflowEntity, times(1)).getDataFrames();
//        verify(dataFrameAuditDAO, times(1)).getLatestDataFrameAudit(dataFrameId, DataFrameAuditStatus.COMPLETED);
//        verify(dataFrameAuditEntity, times(1)).getPayload();
//        verify(workflow, times(1)).getName();
//        PowerMockito.mockStatic(DataframeUtils.class);
//        DataframeUtils.getReferenceDFForParallelism(any(), any(), any());
//        verify(dataFrameKey, times(1)).getValues();
//        verify(sgUseCasePayload1, times(1)).getDataframes();
//        verify(sgUseCasePayload1, times(3)).getColumnMetaData();
//    }
//
//}
