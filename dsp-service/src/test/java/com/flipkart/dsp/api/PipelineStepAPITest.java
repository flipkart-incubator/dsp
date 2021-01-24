//package com.flipkart.dsp.api;
//
//import com.flipkart.dsp.dao.PipelineStepDAO;
//import com.flipkart.dsp.db.entities.Pipeline;
//import com.flipkart.dsp.db.entities.PipelineStepEntity;
//import com.flipkart.dsp.db.entities.ScriptEntity;
//import com.flipkart.dsp.entities.pipeline_step.PipelineStepResources;
//import com.flipkart.dsp.models.PipelineStepType;
//import com.flipkart.dsp.utils.JsonUtils;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.mockito.Mockito.*;
//
///**
// * +
// */
//public class PipelineStepAPITest {
//
//    @Mock private ScriptEntity scriptEntity;
//    @Mock private com.flipkart.dsp.entities.scriptEntity.ScriptEntity scriptEntity;
//    @Mock private Pipeline pipeline;
//    @Mock private PipelineStepEntity pipelineStepEntity;
//    @Mock private PipelineStepEntity pipelineStepEntity;
//
//    @Mock private ScriptAPI scriptAPI;
//    @Mock private PipelineStepDAO pipelineStepDAO;
//
//    private Long pipelineId = 1L;
//    private Long pipelineStepId = 1L;
//    private Long parentPipelineStepId = 2L;
//    private String runConfig = "runConfig";
//    private String pipelineStepName = "pipelineStepName";
//    private PipelineStepAPI pipelineStepAPI;
//    private List<PipelineStepEntity> pipelineSteps = new ArrayList<>();
//
//    private PipelineStepResources pipelineStepResources;
//
//    @Before
//    public void setUp()  throws Exception {
//        MockitoAnnotations.initMocks(this);
//        this.pipelineStepAPI = spy(new PipelineStepAPI(scriptAPI, pipelineStepDAO));
//
//        pipelineStepResources = PipelineStepResources.builder().build();
//        pipelineSteps.add(pipelineStepEntity);
//
//        when(pipelineStepEntity.getId()).thenReturn(pipelineStepId);
//        when(pipelineStepEntity.getPipeline()).thenReturn(pipeline);
//        when(pipelineStepEntity.getScriptEntity()).thenReturn(scriptEntity);
//        when(pipelineStepEntity.getName()).thenReturn(pipelineStepName);
//        when(pipelineStepEntity.getPipelineStepConfig()).thenReturn(runConfig);
//        when(pipelineStepEntity.getParentPipelineStepEntity()).thenReturn(pipelineStepEntity);
//        when(pipelineStepEntity.getPipelineStepType()).thenReturn(PipelineStepType.MODEL_TRAINER);
//        when(pipelineStepEntity.getPipelineStepResources()).thenReturn(JsonUtils.DEFAULT.mapper.writeValueAsString(pipelineStepResources));
//
//        when(pipeline.getId()).thenReturn(pipelineId);
//        when(scriptAPI.unWrap(scriptEntity)).thenReturn(scriptEntity);
//        when(pipelineStepEntity.getId()).thenReturn(parentPipelineStepId);
//    }
//
//    @Test
//    public void testGetPipelineStepsByPipelineId() {
//        when(pipelineStepDAO.getPipelineStepsByPipelineId(pipelineId)).thenReturn(pipelineSteps);
//        List<com.flipkart.dsp.entities.pipeline_step.PipelineStepEntity> expected = pipelineStepAPI.getPipelineStepsByWorkflowId(pipelineId);
//        assertNotNull(expected);
//        assertEquals(expected.size(), 1);
//        assertEquals(pipelineStepId.longValue(), expected.get(0).getId());
//        assertEquals(pipelineStepName, expected.get(0).getName());
//        assertEquals(pipelineId.longValue(), expected.get(0).getPipelineId());
//        assertEquals(parentPipelineStepId.longValue(), expected.get(0).getParentPipelineStepId().longValue());
//        assertEquals(PipelineStepType.MODEL_TRAINER, expected.get(0).getPipelineStepType());
//
//        verify(pipelineStepDAO).getPipelineStepsByPipelineId(pipelineId);
//        verify(pipelineStepEntity).getId();
//        verify(pipelineStepEntity).getName();
//        verify(pipelineStepEntity).getPipeline();
//        verify(pipeline).getId();
//        verify(pipelineStepEntity, times(2)).getParentPipelineStepEntity();
//        verify(pipelineStepEntity).getId();
//        verify(pipelineStepEntity).getPipelineStepType();
//        verify(pipelineStepEntity, times(2)).getScriptEntity();
//        verify(scriptAPI).unWrap(scriptEntity);
//        verify(pipelineStepEntity).getPipelineStepConfig();
//        verify(pipelineStepEntity, times(2)).getPipelineStepResources();
//    }
//
//    @Test
//    public void testGetPipelineStepsById() {
//        when(pipelineStepDAO.getPipelineStepById(pipelineStepId)).thenReturn(pipelineStepEntity);
//        com.flipkart.dsp.entities.pipeline_step.PipelineStepEntity expected = pipelineStepAPI.getPipelineStepById(pipelineStepId);
//        assertNotNull(expected);
//        assertEquals(pipelineStepId.longValue(), expected.getId());
//        assertEquals(pipelineStepName, expected.getName());
//        assertEquals(pipelineId.longValue(), expected.getPipelineId());
//        assertEquals(parentPipelineStepId.longValue(), expected.getParentPipelineStepId().longValue());
//        assertEquals(PipelineStepType.MODEL_TRAINER, expected.getPipelineStepType());
//
//        verify(pipelineStepDAO).getPipelineStepById(pipelineId);
//        verify(pipelineStepEntity).getId();
//        verify(pipelineStepEntity).getName();
//        verify(pipelineStepEntity).getPipeline();
//        verify(pipeline).getId();
//        verify(pipelineStepEntity, times(2)).getParentPipelineStepEntity();
//        verify(pipelineStepEntity).getId();
//        verify(pipelineStepEntity).getPipelineStepType();
//        verify(pipelineStepEntity, times(2)).getScriptEntity();
//        verify(scriptAPI).unWrap(scriptEntity);
//        verify(pipelineStepEntity).getPipelineStepConfig();
//        verify(pipelineStepEntity, times(2)).getPipelineStepResources();
//
//    }
//}
