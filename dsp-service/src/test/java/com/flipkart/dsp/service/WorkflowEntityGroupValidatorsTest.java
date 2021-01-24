//package com.flipkart.dsp.service;
//
//import com.flipkart.dsp.models.DataType;
//import com.flipkart.dsp.models.ScriptVariable;
//import com.flipkart.dsp.models.WorkflowGroupCreateDetails;
//import com.flipkart.dsp.qe.clients.HiveClient;
//import com.flipkart.dsp.qe.clients.MetaStoreClient;
//import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import static org.junit.Assert.*;
//
//public class WorkflowEntityGroupValidatorsTest {
//    @Mock
//    private HiveClient hiveClient;
//
//    @Mock
//    private MetaStoreClient metaStoreClient;
//
//    private WorkflowGroupValidators workflowGroupValidators;
//
//    WorkflowGroupCreateDetails workflowRequest;
//
//    @Before
//    public void setup() {
//        workflowGroupValidators = new WorkflowGroupValidators(hiveClient,metaStoreClient);
//
//    }
//
//    @Test(expected = WorkflowGroupCreationException.class)
//    public void testInCorrectDatetime() throws WorkflowGroupCreationException {
//        List<WorkflowGroupCreateDetails.Workflow> workflows = new ArrayList<>();
//        WorkflowGroupCreateDetails.Workflow workflow = new WorkflowGroupCreateDetails.Workflow();
//        List<WorkflowGroupCreateDetails.PipelineStep> pipelineSteps = new ArrayList<>();
//        WorkflowGroupCreateDetails.PipelineStep pipelineStep = new WorkflowGroupCreateDetails.PipelineStep();
//        WorkflowGroupCreateDetails.Script script = new WorkflowGroupCreateDetails.Script();
//        Set<ScriptVariable> variables = new HashSet<>();
//        ScriptVariable variable = ScriptVariable.builder().dataType(DataType.DATE_TIME).value("2014/11/15T18:32:17").build();
//        variables.add(variable);
//        script.setInputs(variables);
//        pipelineStep.setScript(script);
//        pipelineSteps.add(pipelineStep);
//        workflow.setPipelineSteps(pipelineSteps);
//        workflows.add(workflow);
//        workflowRequest = new WorkflowGroupCreateDetails();
//        workflowRequest.setWorkflows(workflows);
//        workflowGroupValidators.validatePrimitive(workflowRequest);
//    }
//
//    @Test
//    public void testCorrectDatetime() {
//        List<WorkflowGroupCreateDetails.Workflow> workflows = new ArrayList<>();
//        WorkflowGroupCreateDetails.Workflow workflow = new WorkflowGroupCreateDetails.Workflow();
//        List<WorkflowGroupCreateDetails.PipelineStep> pipelineSteps = new ArrayList<>();
//        WorkflowGroupCreateDetails.PipelineStep pipelineStep = new WorkflowGroupCreateDetails.PipelineStep();
//        WorkflowGroupCreateDetails.Script script = new WorkflowGroupCreateDetails.Script();
//        Set<ScriptVariable> variables = new HashSet<>();
//        ScriptVariable variable = ScriptVariable.builder().dataType(DataType.DATE_TIME).value("2014-11-15T18:32:17").build();
//        variables.add(variable);
//        script.setInputs(variables);
//        pipelineStep.setScript(script);
//        pipelineSteps.add(pipelineStep);
//        workflow.setPipelineSteps(pipelineSteps);
//        workflows.add(workflow);
//        workflowRequest = new WorkflowGroupCreateDetails();
//        workflowRequest.setWorkflows(workflows);
//        boolean flag_success = true;
//        try {
//            workflowGroupValidators.validatePrimitive(workflowRequest);
//        } catch (WorkflowGroupCreationException e) {
//            flag_success = false;
//        }
//        assertTrue(flag_success);
//    }
//
//    @Test(expected = WorkflowGroupCreationException.class)
//    public void testInCorrectDate() throws WorkflowGroupCreationException {
//        List<WorkflowGroupCreateDetails.Workflow> workflows = new ArrayList<>();
//        WorkflowGroupCreateDetails.Workflow workflow = new WorkflowGroupCreateDetails.Workflow();
//        List<WorkflowGroupCreateDetails.PipelineStep> pipelineSteps = new ArrayList<>();
//        WorkflowGroupCreateDetails.PipelineStep pipelineStep = new WorkflowGroupCreateDetails.PipelineStep();
//        WorkflowGroupCreateDetails.Script script = new WorkflowGroupCreateDetails.Script();
//        Set<ScriptVariable> variables = new HashSet<>();
//        ScriptVariable variable = ScriptVariable.builder().dataType(DataType.DATE).value("2014/11/15").build();
//        variables.add(variable);
//        script.setInputs(variables);
//        pipelineStep.setScript(script);
//        pipelineSteps.add(pipelineStep);
//        workflow.setPipelineSteps(pipelineSteps);
//        workflows.add(workflow);
//        workflowRequest = new WorkflowGroupCreateDetails();
//        workflowRequest.setWorkflows(workflows);
//        workflowGroupValidators.validatePrimitive(workflowRequest);
//    }
//
//    @Test
//    public void testCorrectDate() {
//        List<WorkflowGroupCreateDetails.Workflow> workflows = new ArrayList<>();
//        WorkflowGroupCreateDetails.Workflow workflow = new WorkflowGroupCreateDetails.Workflow();
//        List<WorkflowGroupCreateDetails.PipelineStep> pipelineSteps = new ArrayList<>();
//        WorkflowGroupCreateDetails.PipelineStep pipelineStep = new WorkflowGroupCreateDetails.PipelineStep();
//        WorkflowGroupCreateDetails.Script script = new WorkflowGroupCreateDetails.Script();
//        Set<ScriptVariable> variables = new HashSet<>();
//        ScriptVariable variable = ScriptVariable.builder().dataType(DataType.DATE).value("2014-11-15").build();
//        variables.add(variable);
//        script.setInputs(variables);
//        pipelineStep.setScript(script);
//        pipelineSteps.add(pipelineStep);
//        workflow.setPipelineSteps(pipelineSteps);
//        workflows.add(workflow);
//        workflowRequest = new WorkflowGroupCreateDetails();
//        workflowRequest.setWorkflows(workflows);
//        boolean flag_success = true;
//        try {
//            workflowGroupValidators.validatePrimitive(workflowRequest);
//        } catch (WorkflowGroupCreationException e) {
//            flag_success = false;
//        }
//        assertTrue(flag_success);
//    }
//
//}