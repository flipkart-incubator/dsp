//package com.flipkart.dsp.configurableSG;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.actors.*;
//import com.flipkart.dsp.api.SignalAPI;
//import com.flipkart.dsp.api.dataFrame.CreateDataFramesAPI;
//import com.flipkart.dsp.db.entities.DataFrameEntity;
//import com.flipkart.dsp.db.entities.DataSourceEntity;
//import com.flipkart.dsp.db.entities.DataTableEntity;
//import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
//import com.flipkart.dsp.service.CreateSGPreProcessor;
//import com.flipkart.dsp.utils.SGDefaultValuePopulator;
//import com.google.inject.Inject;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Answers;
//import org.mockito.Mock;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import java.io.IOException;
//import java.util.Collections;
//import java.util.List;
//
//import static io.dropwizard.testing.FixtureHelpers.fixture;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.when;
//
//@RunWith(MockitoJUnitRunner.class)
//public class CreateDataFramesAPITest {
//
//    @Mock
//    private SignalActor signalActor;
//
//    @Mock
//    private DataTableActor dataTableActor;
//    @Mock
//    private DataFrameActor dataFrameActor;
//
//    @Mock
//    private DataSourceActor dataSourceActor;
//
//    @Mock
//    private DataSourceEntity dataSourceEntity;
//
//    @Mock
//    private SignalGroupToSignalActor signalGroupToSignalActor;
//
//    @Mock
//    private SignalGroupActor signalGroupActor;
//
//    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
//    private DataTableEntity dataTableEntity;
//
//    private ObjectMapper objectMapper;
//    @Inject
//    private CreateDataFramesAPI createDataFramesAPI;
//
//    @Mock
//    private DataFrameEntity dataFrameEntity;
//
//    @Before
//    public void setUp() {
//        objectMapper = new ObjectMapper();
//        CreateSGPreProcessor createSGPreProcessor = new CreateSGPreProcessor(new SGDefaultValuePopulator());
//        createDataFramesAPI = new CreateDataFramesAPI(signalActor,
//                dataTableActor,
//                dataFrameActor,
//                dataSourceActor,
//                createSGPreProcessor, signalGroupToSignalActor,signalGroupActor);
//    }
//
//    @Test
//    public void testCreateDataFramesAPI() throws IOException {
//        ConfigurableSGDTO actualConfigurableSGDTO = getConfigurableSGDTO();
//        when(dataTableEntity.getDataSource().getId()).thenReturn("123");
//        when(dataFrameActor.persist(any())).thenReturn(dataFrameEntity);
//        when(dataSourceActor.getDataSourceEntity(any())).thenReturn(dataSourceEntity);
//        when(dataFrameActor.persist(any())).thenReturn(dataFrameEntity);
//
//        final List<DataFrameEntity> sgDataFrameEntities = createDataFramesAPI.perform(Collections.singletonList(actualConfigurableSGDTO));
//        Assert.assertEquals(sgDataFrameEntities.get(0), dataFrameEntity);
//    }
//
//    @Test
//    public void testFailureCreateDataFrameAPI() throws IOException {
//        ConfigurableSGDTO actualConfigurableSGDTO = getConfigurableSGDTO();
//        when(dataSourceActor.getDataSourceEntity(any())).thenReturn(dataSourceEntity);
//        createDataFramesAPI.perform(Collections.singletonList(actualConfigurableSGDTO));
//    }
//
//    private ConfigurableSGDTO getConfigurableSGDTO() throws IOException {
//        return objectMapper.readValue(fixture("fixtures/configurable_sg_input.json"), ConfigurableSGDTO.class);
//    }
//}
