//package com.flipkart.dsp.service;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.actors.DataFrameActor;
//import com.flipkart.dsp.actors.WorkFlowActor;
//import com.flipkart.dsp.api.*;
//import com.flipkart.dsp.api.WorkflowAPI;
//import com.flipkart.dsp.api.WorkflowGroupAPI;
//import com.flipkart.dsp.dao.RequestDAO;
//import com.flipkart.dsp.dao.WorkflowDAO;
//import com.flipkart.dsp.dto.WorkflowResponseDTO;
//import com.flipkart.dsp.models.workflowEntity.WorkflowGroupCreateDetails;
//import com.flipkart.dsp.models.sg.DataFrame;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.MockitoAnnotations;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.io.IOException;
//import java.net.URISyntaxException;
//import java.net.URL;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
//@RunWith(PowerMockRunner.class)
//public class WorkflowGroupServiceTest {
//
//    @Mock private WorkflowAPI workflowAPI;
//    @Mock private WorkflowGroupAPI workflowGroupAPI;
//    @Mock private WorkFlowActor workFlowActor;
//    @Mock private ScriptAPI scriptAPI;
//    @Mock private DataFrameActor dataFrameActor;
//    @Mock private WorkflowGroupDAO workflowGroupDAO;
//    @Mock private WorkflowDAO workflowDAO;
//    @Mock private RequestDAO requestDAO;
//    @Mock private ObjectMapper objectMapper;
//    @Mock private WorkflowGroupValidators workflowGroupValidators;
//    @Mock private NotificationPreferenceAPI notificationPreferenceAPI;
//    @Mock private DataFrameActor sgDataFrameActor;
//
//    ObjectMapper objectMapperActual;
//    private WorkflowGroupService workflowGroupService;
//
//    @Before
//    public void setup() {
//        MockitoAnnotations.initMocks(this);
//        objectMapperActual = new ObjectMapper();
//        workflowGroupService = new WorkflowGroupService(workflowAPI, workflowGroupAPI, workFlowActor,
//                scriptAPI, dataFrameActor, workflowGroupDAO, workflowDAO, requestDAO, objectMapper, workflowGroupValidators, notificationPreferenceAPI,
//                sgDataFrameActor);
//    }
//
//    @Test
//    public void testGetWorkflowDetailsSuccess() throws Exception {
//        WorkflowGroupService workflowGroupServiceSpy = Mockito.spy(workflowGroupService);
//        WorkflowGroupCreateDetails workflowDetails = getWorkflowSandboxDetailsObject();
//        Mockito.doReturn(workflowDetails).when(workflowGroupServiceSpy).getWorkflowGroupDetailsFromId("DSP_SANITY_WG_new_test",
//                true, null);
//        DataFrame sgDataFrame = getSGDataFrame();
//        Mockito.when(sgDataFrameActor.getDataframe(10l)).thenReturn(sgDataFrame);
//        WorkflowResponseDTO actualWorkFlowDTO = workflowGroupServiceSpy.getWorkflowDetails("DSP_SANITY_WG_new_test",
//                true, null);
//        assertEquals("DSP_SANITY_WG_new_test", actualWorkFlowDTO.getWorkflowGroupName());
//        assertNotNull(actualWorkFlowDTO);
//    }
//
//    private Path getPath(String filePath) throws URISyntaxException {
//        URL url = getClass().getResource(filePath);
//        return Paths.get(url.toURI().getPath());
//    }
//
//    private WorkflowGroupCreateDetails getWorkflowSandboxDetailsObject() throws URISyntaxException, IOException {
//        Path expectedFilePath = getPath("/fixtures/workflowGroup_create_details.json");
//        String expectedValue =  new String(Files.readAllBytes(expectedFilePath), StandardCharsets.UTF_8);
//        return objectMapperActual.readValue(expectedValue, WorkflowGroupCreateDetails.class);
//    }
//
//    private DataFrame getSGDataFrame() throws URISyntaxException, IOException {
//        Path expectedFilePath = getPath("/fixtures/sg_dataframe.json");
//        String expectedValue =  new String(Files.readAllBytes(expectedFilePath), StandardCharsets.UTF_8);
//        return objectMapperActual.readValue(expectedValue, DataFrame.class);
//    }
//}
