package com.flipkart.dsp.sg.api;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.exceptions.DSPClientException;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.overrides.CSVDataframeOverride;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.overrides.DefaultDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataSource;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.sg.generator.DataFrameFactory;
import com.flipkart.dsp.sg.override.OverrideManager;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.utils.HiveUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveUtils.class, SGApi.class})
public class SGApiTest {
    @Mock private Script script;
    @Mock private Request request;
    @Mock private Workflow workflow;
    @Mock private DataFrame dataFrame;
    @Mock private DataTable dataTable;
    @Mock private DataSource dataSource;
    @Mock private PipelineStep pipelineStep;
    @Mock private ScriptVariable scriptVariable;
    @Mock private ScriptVariable scriptVariable2;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private DataFrameAudit dataFrameAudit;
    @Mock private OverrideManager overrideManager;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private RequestOverride requestOverride;
    @Mock private DSPServiceClient dspServiceClient;
    @Mock private DataFrameFactory dataFrameFactory;
    @Mock private CSVDataframeOverride csvDataframeOverride;
    @Mock private DefaultDataframeOverride defaultDataframeOverride;
    @Mock private ExecuteWorkflowRequest executeWorkflowRequest;

    private SGApi sgApi;
    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long dataFrameId = 1L;
    private Long pipelineStepId = 1L;
    private Long pipelineStepAuditId = 1L;
    private String tableName = "table_name";
    private String dataFrameName = "dataFrameName";
    private String scriptVariableName = "scriptVariableName";

    private Map<String, Long> tables = new HashMap<>();
    private List<String> partitions = new ArrayList<>();
    private Set<DataFrame> dataFrames = new HashSet<>();
    private List<PipelineStep> pipelineSteps = new ArrayList<>();
    private Set<ScriptVariable> scriptVariableSet = new HashSet<>();
    private Set<DataFrameAudit> dataFrameAudits = new HashSet<>();
    private Map<String, DataframeOverride> dataFrameOverrideMap = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(HiveUtils.class);
        MockitoAnnotations.initMocks(this);
        this.sgApi = spy(new SGApi(eventAuditUtil, overrideManager, dspServiceClient, dataFrameFactory));

        String workflowName = "workflowName";

        tables.put(tableName, 1L);
        dataFrames.add(dataFrame);
        pipelineSteps.add(pipelineStep);
        scriptVariableSet.add(scriptVariable);
        dataFrameAudits.add(dataFrameAudit);
        dataFrameOverrideMap.put(scriptVariableName, defaultDataframeOverride);

        when(workflow.getName()).thenReturn(workflowName);
        when(workflow.getDataFrames()).thenReturn(dataFrames);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(workflowDetails.getPipelineSteps()).thenReturn(pipelineSteps);

        when(request.getData()).thenReturn(executeWorkflowRequest);
        when(request.getWorkflowDetails()).thenReturn(workflowDetails);

        when(executeWorkflowRequest.getTables()).thenReturn(tables);
        when(executeWorkflowRequest.getRequestOverride()).thenReturn(requestOverride);

        when(dspServiceClient.getRequest(requestId)).thenReturn(request);
        when(dspServiceClient.getDataTable(tableName)).thenReturn(dataTable);
        when(dspServiceClient.getWorkflowDetails(workflowId)).thenReturn(workflowDetails);

        when(pipelineStep.getScript()).thenReturn(script);
        when(pipelineStep.getId()).thenReturn(pipelineStepId);

        when(script.getInputVariables()).thenReturn(scriptVariableSet);
        when(scriptVariable.getName()).thenReturn(scriptVariableName);
        when(scriptVariable.getDataType()).thenReturn(DataType.DATAFRAME);

        when(dataSource.getId()).thenReturn(tableName);
        when(dataTable.getDataSource()).thenReturn(dataSource);

        when(dataFrame.getId()).thenReturn(1L);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(requestOverride.getDataframeOverrideMap()).thenReturn(dataFrameOverrideMap);
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(dataFrameAudit.getDataFrame()).thenReturn(dataFrame);
        when(overrideManager.override(any(), any(), any(), any())).thenReturn(dataFrameAudits);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        doNothing().when(eventAuditUtil).createDataFrameGenerationErrorEvent(any(), anyString(), any());
    }

    @Test
    public void testSubmitJobCase1Failure() throws Exception {
        when(pipelineStep.getId()).thenReturn(11L);
        boolean isException = false;
        try {
            sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getCause().getMessage(), "Pipeline Step details missing for step id: " + pipelineStepId);
        }

        assertTrue(isException);
        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(1)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(pipelineStep, times(1)).getScript();
        verify(script, times(1)).getInputVariables();
        verify(scriptVariable, times(2)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(1)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(eventAuditUtil, times(1)).createDataFrameGenerationErrorEvent(any(), anyString(), any());
    }

    @Test
    public void testSubmitJobCase2Failure() throws Exception {
        when(overrideManager.override(any(), any(), any(), any())).thenThrow(new DataframeOverrideException("Error"));
        boolean isException = false;
        try {
            sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getCause().getMessage(), "Data frame Generation Failed while applying overrides.");
        }

        assertTrue(isException);
        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(1)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(1)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(eventAuditUtil, times(1)).createDataFrameGenerationErrorEvent(any(), anyString(), any());
    }

    // input variables
    // filter out dataframe not in the particular step
    @Test
    public void testSubmitJobCase1Success() throws Exception {
        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);
        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(requestOverride, times(2)).getDataframeOverrideMap();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(dataFrameAudit, times(1)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(dspServiceClient, times(1)).getDataTable(tableName);
        verify(dataTable, times(1)).getDataSource();
        verify(dataSource, times(1)).getId();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrame, times(2)).getName();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
    }

    // FULL TABLE NAME
    // Default DataFrame, Force_run :true
    @Test
    public void testSubmitJobCase2Success() throws Exception {
        tables.put("db_name.table_name", 1L);
        dataFrameOverrideMap.put(dataFrameName, defaultDataframeOverride);
        scriptVariableSet.add(scriptVariable2);
        when(scriptVariable2.getName()).thenReturn(dataFrameName);
        when(scriptVariable2.getDataType()).thenReturn(DataType.DATAFRAME);
        when(defaultDataframeOverride.getForceRun()).thenReturn(true);
        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);

        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(requestOverride, times(2)).getDataframeOverrideMap();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(dataFrameAudit, times(1)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
        verify(defaultDataframeOverride, times(1)).getForceRun();
    }

    // FULL TABLE NAME
    // Default DataFrame, Force_run :false, Dataframe audit present
    @Test
    public void testSubmitJobCase3Success() throws Exception {
        dataFrameOverrideMap.put(dataFrameName, defaultDataframeOverride);
        scriptVariableSet.add(scriptVariable2);
        when(scriptVariable2.getName()).thenReturn(dataFrameName);
        when(scriptVariable2.getDataType()).thenReturn(DataType.DATAFRAME);
        when(defaultDataframeOverride.getForceRun()).thenReturn(false);
        when(dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameId)).thenReturn(dataFrameAudit);

        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);

        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(requestOverride, times(2)).getDataframeOverrideMap();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(dataFrameAudit, times(1)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
        verify(defaultDataframeOverride, times(1)).getForceRun();
        verify(dspServiceClient, times(1)).getLatestDataFrameAuditByDataFrameId(dataFrameId);
    }

    // FULL TABLE NAME
    // Default DataFrame, Force_run :false, Dataframe audit not present
    @Test
    public void testSubmitJobCase4Success() throws Exception {
        dataFrameOverrideMap.put(dataFrameName, defaultDataframeOverride);
        scriptVariableSet.add(scriptVariable2);
        when(scriptVariable2.getName()).thenReturn(dataFrameName);
        when(scriptVariable2.getDataType()).thenReturn(DataType.DATAFRAME);
        when(defaultDataframeOverride.getForceRun()).thenReturn(false);
        when(dspServiceClient.getLatestDataFrameAuditByDataFrameId(dataFrameId)).thenThrow(new DSPClientException(null));
        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);

        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(requestOverride, times(2)).getDataframeOverrideMap();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(dataFrameAudit, times(1)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
        verify(defaultDataframeOverride, times(1)).getForceRun();
        verify(dspServiceClient, times(1)).getLatestDataFrameAuditByDataFrameId(dataFrameId);
    }

    // FULL TABLE NAME
    // Other than default DataFrame
    @Test
    public void testSubmitJobCase5Success() throws Exception {
        scriptVariableSet.add(scriptVariable2);
        when(scriptVariable2.getName()).thenReturn(dataFrameName);
        when(scriptVariable2.getDataType()).thenReturn(DataType.DATAFRAME);
        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);
        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(3)).getRequestOverride();
        verify(requestOverride, times(2)).getDataframeOverrideMap();
        verify(overrideManager, times(1)).override(any(), any(), any(), any());
        verify(dataFrameAudit, times(1)).getDataFrame();
        verify(dataFrame, times(2)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrame, times(2)).getName();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
    }

    // OverrideMap null
    @Test
    public void testSubmitJobCase6Success() throws Exception {
        when(requestOverride.getDataframeOverrideMap()).thenReturn(null);
        sgApi.submitJob(requestId, workflowId, pipelineStepId, pipelineStepAuditId);
        verify(dspServiceClient, times(1)).getWorkflowDetails(workflowId);
        verify(workflowDetails, times(3)).getWorkflow();
        verify(dspServiceClient, times(1)).getRequest(requestId);
        verify(request, times(1)).getWorkflowDetails();
        verify(workflowDetails, times(2)).getPipelineSteps();
        verify(pipelineStep, times(2)).getScript();
        verify(script, times(2)).getInputVariables();
        verify(scriptVariable, times(3)).getName();
        verify(scriptVariable, times(1)).getDataType();
        verify(request, times(3)).getData();
        verify(executeWorkflowRequest, times(2)).getRequestOverride();
        verify(requestOverride, times(1)).getDataframeOverrideMap();
        verify(dataFrame, times(1)).getName();
        verify(executeWorkflowRequest, times(2)).getTables();
        verify(workflow, times(1)).getDataFrames();
        verify(dataFrame, times(1)).getId();
        verify(dataFrame).getName();
        verify(dataFrameFactory, times(1)).invoke(any(), anyLong(), any());
    }
}
