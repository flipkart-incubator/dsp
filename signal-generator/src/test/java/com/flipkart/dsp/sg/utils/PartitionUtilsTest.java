package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.ScriptVariable;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * +
 */
public class PartitionUtilsTest {

    @Mock private Script script;
    @Mock private PipelineStep pipelineStep;
    @Mock private ScriptVariable scriptVariable;
    @Mock private WorkflowDetails workflowDetails;
    private List<String> partitions = new ArrayList<>();
    private List<PipelineStep> pipelineSteps = new ArrayList<>();
    private Set<ScriptVariable> scriptVariableSet = new HashSet<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        partitions.add("partition");
        pipelineSteps.add(pipelineStep);
        scriptVariableSet.add(scriptVariable);
    }
}
