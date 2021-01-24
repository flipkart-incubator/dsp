package com.flipkart.dsp.entities;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.dot;
import static com.flipkart.dsp.utils.Constants.underscore;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 *
 */
public class WorkFlowDetailsTest {
    @Mock
    private Script script;
    @Mock
    private Workflow workflow;
    @Mock
    private PipelineStep pipelineStep;
    @Mock
    private ScriptVariable scriptVariable;
    @Mock
    private AbstractDataFrame additionalParams;

    private String tableName;
    private String company = "fkint";
    private String namespace = "test";
    private String schemaVersion = "2.0";
    private String organisation = "bigfoot";
    private WorkflowDetails workflowDetails;
    private List<PipelineStep> pipelineSteps = new ArrayList<>();
    private Set<ScriptVariable> scriptVariableSet = new HashSet<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        pipelineSteps.add(pipelineStep);
        workflowDetails = spy(WorkflowDetails.builder().workflow(workflow).pipelineSteps(pipelineSteps).build());

        scriptVariableSet.add(scriptVariable);
        tableName = company + underscore + organisation
                + underscore + namespace + underscore
                + schemaVersion.substring(0, schemaVersion.indexOf(dot))
                + schemaVersion.substring(schemaVersion.indexOf(dot) + 1);

        when(pipelineStep.getScript()).thenReturn(script);
        when(script.getOutputVariables()).thenReturn(scriptVariableSet);
        when(scriptVariable.getDataType()).thenReturn(DataType.DATAFRAME);
        when(scriptVariable.getAdditionalVariable()).thenReturn(additionalParams);
        final ArrayList<OutputLocation> outputLocations = new ArrayList<>();
        when(scriptVariable.getOutputLocationDetailsList()).thenReturn(outputLocations);
    }

}
