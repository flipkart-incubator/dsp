//package com.flipkart.dsp.api;
//
//import com.flipkart.dsp.config.ScriptConstants;
//import com.flipkart.dsp.dao.PipelineStepDAO;
//import com.flipkart.dsp.dao.PipelineStepRuntimeConfigDAO;
//import com.flipkart.dsp.entities.dataframe.DataFrame;
//import com.flipkart.dsp.models.DataType;
//import com.flipkart.dsp.entities.pipeline.PipelineStepRuntimeConfigEntity;
//import com.flipkart.dsp.models.ScriptVariable;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowDetails;
//import com.flipkart.dsp.entities.run.config.RunConfig;
//import com.flipkart.dsp.exception.DSPSvcException;
//import com.google.common.collect.Lists;
//import org.junit.After;
//import org.junit.Before;
//
//import java.sql.Timestamp;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
///**
// */
//public class PipelineStepRuntimeConfigAPITest extends WorkflowAPITest {
//
//    private PipelineStepRuntimeConfigAPI pipelineStepRuntimeConfigAPI;
//    private WorkflowDetails workflowDetails;
//
//    @Before
//    public void setup() {
//        super.setup();
//        pipelineStepRuntimeConfigAPI = new PipelineStepRuntimeConfigAPI(new PipelineStepRuntimeConfigDAO(sessionFactory),
//                new PipelineStepDAO(sessionFactory), transactionLender);
//    }
//
//    @After
//    public void tearDown() {
//        super.tearDown();
//    }
//
////    @Test
////    public void testPipelineStepRuntimeConfigAPI() throws Exception {
////        createRequiredScripts();
////        workflowDetails = testRegisterWorkflow();
////        testSavePipelineStepRuntimeConfig();
////        testGetPipelineStepRuntimeConfig();
////    }
//
//    private void testSavePipelineStepRuntimeConfig() throws DSPSvcException {
//        DataFrame dataFrameEntity = new DataFrame();
//        dataFrameEntity.setName("abcd");
//        dataFrameEntity.setDataLocation("/path");
//        dataFrameEntity.setColumns(Lists.newArrayList(new DataFrame.Column("x", DataType.DOUBLE)));
//        Map<String,DataFrame> dataFrameMap = new HashMap<>();
//        dataFrameMap.put(ScriptConstants.FULL_DATA_FRAME,dataFrameEntity);
//        ScriptVariable inputScriptVariable = new ScriptVariable("model_byte_arra", DataType.MODEL,
//                "dev/shm/dsp-temp/model_byte_arrayMODELf4fb65a3-b07a-48b2-af01-25cf1ca186a8.csv", null, null, false);
//
//        Set<ScriptVariable> inputScriptVariableSet = new HashSet<>();
//        inputScriptVariableSet.add(inputScriptVariable);
//
//        Set<ScriptVariable> outputScriptVariableSet = new HashSet<>();
//        ScriptVariable outputScriptVariable = new ScriptVariable("model_byte_arra", DataType.MODEL,
//                "{\"id\":\"MD22823\",\"version\":\"1.0.130\"}", null, null, false);
//        outputScriptVariableSet.add(outputScriptVariable);
//
//        RunConfig runConfig = new RunConfig(inputScriptVariableSet, outputScriptVariableSet);
//
//        long id = pipelineStepRuntimeConfigAPI.savePipelineStepRuntimeConfig(new PipelineStepRuntimeConfigEntity(0L, "1",
//                "ABCDE", workflowDetails.getPipelines().get(0).getPipelineSteps().get(0).getId(), "SC = SC1",
//                runConfig, new Timestamp(System.currentTimeMillis())));
//        assert (id > 1);
//    }
//
//    private void testGetPipelineStepRuntimeConfig() throws DSPSvcException {
//        PipelineStepRuntimeConfigEntity pipelineStepRuntimeConfig = pipelineStepRuntimeConfigAPI.getPipelineStepRuntimeConfig("ABCDE",
//                workflowDetails.getPipelines().get(0).getPipelineSteps().get(0).getId());
//        assert (pipelineStepRuntimeConfig != null);
//        assert (pipelineStepRuntimeConfig.getWorkflowExecutionId() == "1");
//    }
//
//}
