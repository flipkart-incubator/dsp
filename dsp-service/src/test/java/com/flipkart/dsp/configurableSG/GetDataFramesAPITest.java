//package com.flipkart.dsp.configurableSG;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.actors.DataFrameActor;
//import com.flipkart.dsp.api.dataFrame.GetDataFramesAPI;
//import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
//import com.flipkart.dsp.models.sg.DataFrame;
//import com.flipkart.dsp.exceptions.ConfigurableSGException;
//import org.json.JSONException;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.runners.MockitoJUnitRunner;
//import org.skyscreamer.jsonassert.JSONCompareMode;
//
//import java.io.IOException;
//import java.util.Collections;
//
//import static io.dropwizard.testing.FixtureHelpers.fixture;
//import static org.mockito.Mockito.when;
//import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
//
//@RunWith(MockitoJUnitRunner.class)
//public class GetDataFramesAPITest {
//
//    @Mock
//    private DataFrameActor dataFrameActor;
//
//    private GetDataFramesAPI getDataFramesAPI;
//    private ObjectMapper objectMapper;
//
//    @Before
//    public void setUp() {
//        objectMapper = new ObjectMapper();
//        getDataFramesAPI = new GetDataFramesAPI(dataFrameActor);
//    }
//
//    @Test
//    @Ignore
//    public void testGetDataFramesAPISuccess() throws IOException, JSONException {
//        DataFrame sgDataFrame = getSGDataFrameDTO();
////        when(dataFrameActor.verifyDataFrameExistence("dataFrameName")).thenReturn(true);
//        when(dataFrameActor.getDataframe(1L)).thenReturn(sgDataFrame);
//        ConfigurableSGDTO configurableSGDTO = getDataFramesAPI.perform(Collections.singletonList(1L));
//        assertEquals(fixture("fixtures/configurable_sg_input.json"), objectMapper.writeValueAsString(configurableSGDTO), JSONCompareMode.STRICT);
//    }
//
//    @Test(expected = ConfigurableSGException.class)
//    public void testGetDataFramesAPIFailure() {
////        when(dataFrameActor.verifyDataFrameExistence("dataFrameName")).thenReturn(false);
//        getDataFramesAPI.perform(Collections.singletonList(2L));
//    }
//
//
//    private DataFrame getSGDataFrameDTO() throws IOException {
//        return objectMapper.readValue(fixture("fixtures/signal_dataFrame.json"), DataFrame.class);
//    }
//}
