//package com.flipkart.dsp.api.mock;
//
//import com.flipkart.dsp.entities.pipeline.Pipeline;
//import com.flipkart.dsp.entities.pipeline.PipelineDetails;
//import com.flipkart.dsp.entities.pipeline.PipelineStepEntity;
//import com.flipkart.dsp.entities.pipeline.PipelineStepResources;
//import com.flipkart.dsp.entities.scriptEntity.ScriptMeta;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowEntity;
//import com.flipkart.dsp.entities.workflowEntity.WorkflowDetails;
//import com.flipkart.dsp.models.*;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//
//import java.util.Set;
//
///**
// */
//public class MockEntities {
//    public static final String GIT_REPO_NAME = "ipp-models";
//    public static final String GIT_FOLDER = "/production/FSN_FORECASTING_V0";
//    public static final String GIT_COMMIT_ID = "dummy";
//    public static final String GIT_FILE_PATH_1 = "filepath1";
//    public static final String GIT_FILE_PATH_2 = "filepath2";
//    public static final Set<ScriptVariable> SCRIPT_OUTPUT_VARIABLES_1 = Sets.newHashSet();
//    public static final Set<ScriptVariable> SCRIPT_OUTPUT_VARIABLES_2 = Sets.newHashSet();
//    public static final Set<ScriptVariable> SCRIPT_INPUT_VARIABLES_1 = Sets.newHashSet();
//    public static final Set<ScriptVariable> SCRIPT_INPUT_VARIABLES_2 = Sets.newHashSet();
//
//
//    static {
//        SCRIPT_OUTPUT_VARIABLES_1.add(new ScriptVariable("o1", DataType.STRING, null, null, null,false));
//        SCRIPT_OUTPUT_VARIABLES_1.add(new ScriptVariable("o2", DataType.DOUBLE, null, null, null,false));
//        SCRIPT_OUTPUT_VARIABLES_2.add(new ScriptVariable("o3", DataType.DOUBLE, null, null, null,false));
//        SCRIPT_OUTPUT_VARIABLES_2.add(new ScriptVariable("o4", DataType.STRING, null, null, null,false));
//
//        SCRIPT_INPUT_VARIABLES_1.add(new ScriptVariable("i1", DataType.STRING, null, null, null,false));
//        SCRIPT_INPUT_VARIABLES_1.add(new ScriptVariable("i2", DataType.STRING, null, null, null,false));
//        SCRIPT_INPUT_VARIABLES_1.add(new ScriptVariable("i3", DataType.STRING, null, null, null,false));
//
//        SCRIPT_INPUT_VARIABLES_2.add(new ScriptVariable("i4", DataType.STRING,  null, null, null,false));
//        SCRIPT_INPUT_VARIABLES_2.add(new ScriptVariable("i5", DataType.STRING,  null, null, null,false));
//        SCRIPT_INPUT_VARIABLES_2.add(new ScriptVariable("i6", DataType.STRING,  null, null, null,false));
//        SCRIPT_INPUT_VARIABLES_2.add(new ScriptVariable("i7", DataType.STRING,  null, null, null,false));
//    }
//
//    public static final ScriptMeta SCRIPT_META_1 = new ScriptMeta(1L, GIT_FILE_PATH_1, GIT_COMMIT_ID, GIT_FOLDER, GIT_REPO_NAME, "PYTHON", "imagePath","ss",SCRIPT_INPUT_VARIABLES_1, SCRIPT_OUTPUT_VARIABLES_1, "", ImageLanguageEnum.PYTHON2);
//    public static final ScriptMeta SCRIPT_META_2 = new ScriptMeta(2L, GIT_FILE_PATH_2, GIT_COMMIT_ID, GIT_FOLDER, GIT_REPO_NAME, "PYTHON", "imagePath", "ss",Sets.newHashSet(SCRIPT_INPUT_VARIABLES_1), SCRIPT_OUTPUT_VARIABLES_1, "", ImageLanguageEnum.PYTHON2);
//    public static final PipelineStepResources pipelineStepResources = new PipelineStepResources(1L,100.0,1.3,3.4,3.4,4.5);
//    public static final PipelineDetails PIPELINE_DETAILS_1 = new PipelineDetails(new Pipeline(1L, 1L, "RF", "{\"sc\" : \"sc1\"}"), Lists.newArrayList(new PipelineStepEntity(1L, "step1", 1L, null, PipelineStepType.FE, null, "abcde", pipelineStepResources, null, null), new PipelineStepEntity(2L,  "step2", 2L, null, PipelineStepType.MODEL_TRAINER, null, "abcde", pipelineStepResources, null, null)));
//    public static final PipelineDetails PIPELINE_DETAILS_2 = new PipelineDetails(new Pipeline(2L, 1L, "Cubist", "{\"sc\" : \"sc1\"}"), Lists.newArrayList(new PipelineStepEntity(1L, "step1", 1L, null, PipelineStepType.FE, null, "abcde", pipelineStepResources, null, null), new PipelineStepEntity(2L,  "step2",2L, null, PipelineStepType.MODEL_TRAINER, null, "abcde", pipelineStepResources, null, null)));
//
//    public static final WorkflowDetails
//        WORKFLOW_DETAILS_1 =
//            new WorkflowDetails(new WorkflowEntity(1L, "workflow1", "test", null,
//                    WorkflowExecutionType.ML_TRAIN.name(), false, 3,
//                    null, 1.0, true, null, null, null),
//            Lists.newArrayList(PIPELINE_DETAILS_1, PIPELINE_DETAILS_2));
//
//}
