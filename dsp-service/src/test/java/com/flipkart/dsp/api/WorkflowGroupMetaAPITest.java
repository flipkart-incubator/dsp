//package com.flipkart.dsp.api;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.mockito.Mockito.*;
//
///**
// */
//public class WorkflowGroupMetaAPITest extends AbstractAPITest{
//    WorkflowGroupMetaAPI mockedWorkflowGroupMetaAPI;
//
//    @Before
//    public void setUp() throws Exception {
//        super.setup();
//        this.mockedWorkflowGroupMetaAPI = mock(WorkflowGroupMetaAPI.class);
//        when(this.mockedWorkflowGroupMetaAPI.getWorkflowGroupMetaById(anyInt())).thenReturn(createWorkflowGroupMeta());
//    }
//
//    @After
//    public void tearDown() {
//        super.setup();
//    }
//
//    @Test
//    @Ignore
//    //todo: fix this
//    public void getWorkflowGroupMetaById() throws Exception {
//        WorkflowGroupMeta workflowGroupMetaExpected = createWorkflowGroupMeta();
//        WorkflowGroupMeta workflowGroupMetaActual = this.mockedWorkflowGroupMetaAPI.getWorkflowGroupMetaById(18l);
//        assertNotNull(workflowGroupMetaActual);
//        assertEquals(workflowGroupMetaExpected.getId(), workflowGroupMetaActual.getId());
//    }
//
//    private WorkflowGroupMeta createWorkflowGroupMeta(){
//        WorkflowGroupMeta workflowGroupMeta = new WorkflowGroupMeta();
//        workflowGroupMeta.setId(18l);
//        workflowGroupMeta.setCallbackUrl("");
//        return workflowGroupMeta;
//    }
//
//}
