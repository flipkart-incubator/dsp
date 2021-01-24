//package com.flipkart.dsp.api;
//
//import com.flipkart.dsp.actors.DataFrameActor;
//import com.flipkart.dsp.dao.PipelineDAO;
//import com.flipkart.dsp.dao.PipelineStepDAO;
//import com.flipkart.dsp.dao.ScriptDAO;
//import com.flipkart.dsp.dao.WorkflowDAO;
//import com.flipkart.dsp.entities.pipeline.PipelineDetails;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowEntity;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowDetails;
//import com.flipkart.dsp.service.WorkflowGroupService;
//import com.flipkart.dsp.exception.ScriptValidationException;
//import org.junit.After;
//import org.junit.Before;
//
//import java.util.List;
//
////import static com.flipkart.dsp.api.mock.MockEntities.*;
//
///**
// */
//public class WorkflowAPITest extends ScriptAPITest {
//
//    public static final String GIT_FOLDER = "/production/FSN_FORECASTING_V0";
//    private WorkflowAPI workflowAPI;
//    private com.flipkart.dsp.service.WorkflowAPI workflowAPICore;
//    private long id;
//
//    @Before
//    public void setup() {
//        super.setup();
//        WorkflowDAO workflowDAO = new WorkflowDAO(sessionFactory);
//        workflowAPI = new WorkflowAPI(workflowDAO, new PipelineDAO(sessionFactory),
//                new PipelineStepDAO(sessionFactory), new ScriptDAO(sessionFactory), transactionLender, null, workflowAPICore);
//    }
//
//    @After
//    public void tearDown() {
//        super.tearDown();
//    }
//
////    @Test
////    public void testWorkflowAPI() throws Exception {
////        createRequiredScripts();
////        testRegisterWorkflow();
////        testGetWorkflowFromId();
////        testGetWorkflowByName();
////        testGetWorkflowIdsBySignalGroups();
////    }
//
//    protected void createRequiredScripts() throws ScriptValidationException {
//        long id1 = scriptAPI.registerScript(SCRIPT_META_1.getGitRepo(), SCRIPT_META_1.getGitFolder(), SCRIPT_META_1.getGitFilePath(), SCRIPT_META_1.getGitCommitId(), SCRIPT_META_1.getExecEnv(), SCRIPT_INPUT_VARIABLES_1, SCRIPT_META_1.getOutputVariables(), SCRIPT_META_1.getMetadata()).getId();
//        long id2 = scriptAPI.registerScript(SCRIPT_META_2.getGitRepo(), SCRIPT_META_2.getGitFolder(), SCRIPT_META_2.getGitFilePath(), SCRIPT_META_2.getGitCommitId(), SCRIPT_META_2.getExecEnv(), SCRIPT_INPUT_VARIABLES_2, SCRIPT_META_2.getOutputVariables(), SCRIPT_META_2.getMetadata()).getId();
//        assert (id1 == 1);
//        assert (id2 == 2);
//    }
//
//    protected WorkflowDetails testRegisterWorkflow() {
//        WorkflowDetails workflowDetails = workflowAPI.registerWorkflow(WORKFLOW_DETAILS_1);
//        testWorkflowDetails(workflowDetails);
//        id = workflowDetails.getWorkflowEntity().getId();
//        return workflowDetails;
//    }
//
//    private void testGetWorkflowFromId() {
//        testWorkflowDetails(workflowAPICore.getWorkflowFromId(id));
//    }
//
//    private void testGetWorkflowByName() {
//        testWorkflowDetails(workflowAPI.getWorkflowDetailsByName("workflow1"));
//    }
//
//    private void testGetWorkflowIdsBySignalGroups() {
//        List<Long> workflowIds = workflowAPI.getWorkflowIdsBySignalGroups(com.google.common.collect.Sets.newHashSet(1l));
//        assert (workflowIds != null);
//        assert (workflowIds.size() == 1);
//    }
//
//    private void testWorkflowDetails(WorkflowDetails workflowDetails) {
//        assert (workflowDetails != null);
//        WorkflowEntity workflowEntity = workflowDetails.getWorkflowEntity();
//        List<PipelineDetails> pipelineDetailsList = workflowDetails.getPipelines();
//        assert (workflowEntity != null);
//        assert (workflowEntity.getName().equals("workflow1"));
//
//        assert (pipelineDetailsList != null);
//        assert (pipelineDetailsList.size() == 2);
//
//        pipelineDetailsList.forEach(pipelineDetails -> {
//            assert (pipelineDetails.getPipeline() != null);
//            assert (pipelineDetails.getPipelineSteps() != null);
//
//            assert (pipelineDetails.getPipeline().getId() > id);
//            assert (pipelineDetails.getPipelineSteps().size() == 2);
//        });
//    }
//
//}
