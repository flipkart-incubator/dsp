//package com.flipkart.dsp.api;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.dto.RunDetailsDTO;
//import com.flipkart.dsp.db.entities.DataFrameAuditEntity;
//import com.flipkart.dsp.sg.dao.DataFrameAuditDAO;
//import org.apache.commons.io.IOUtils;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//@RunWith(MockitoJUnitRunner.class)
//public class RunDetailsAPITest {
//    @Mock
//    private DataFrameAuditDAO dataFrameAuditDAO;
//    private RunDetailsAPI runDetailsAPI;
//    private ObjectMapper objectMapper;
//
//    @Before
//    public void setUp() {
//        runDetailsAPI = new RunDetailsAPI(dataFrameAuditDAO);
//        objectMapper = new ObjectMapper();
//    }
//
//    @Test
//    public void RunDetailsAPISuccessTest() throws IOException {
//        List<DataFrameAuditEntity> dataFrameAuditEntityList = getDataFrameAuditEntity();
//        Mockito.when(dataFrameAuditDAO.getLatestSuccessfulDataFrameAudits("dataFrameName", 1))
//                .thenReturn(dataFrameAuditEntityList);
//        RunDetailsDTO runDetailsDTO = runDetailsAPI.getRunIDDetails("dataFrameName", 1);
//        Assert.assertEquals(runDetailsDTO.getRunIDMap().size(), 1);
//    }
//
//    private List<DataFrameAuditEntity> getDataFrameAuditEntity() throws IOException {
//        InputStream inputStream = this.getClass().getResourceAsStream("/fixtures/DataFrameAuditList.json");
//        String json = IOUtils.toString(inputStream, "UTF-8");
//        DataFrameAuditEntity dataFrameAuditEntity = objectMapper.readValue(json, DataFrameAuditEntity.class);
//        return new ArrayList<>(
//                Arrays.asList(dataFrameAuditEntity));
//    }
//}
