//package com.flipkart.dsp.configurableSG;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
//import com.flipkart.dsp.dto.ConfigurableSG.ConfigurableSGPreProcessedDTO;
//import com.flipkart.dsp.service.CreateSGPreProcessor;
//import com.flipkart.dsp.utils.SGDefaultValuePopulator;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//
//import static io.dropwizard.testing.FixtureHelpers.fixture;
//import static org.junit.Assert.assertEquals;
//
//
//public class CreateSGPreProcessorTest {
//
//    CreateSGPreProcessor createSGPreProcessor;
//    ObjectMapper objectMapper;
//    @Before
//    public void setUp() {
//        objectMapper = new ObjectMapper();
//        SGDefaultValuePopulator SGDefaultValuePopulator = new SGDefaultValuePopulator();
//        createSGPreProcessor = new CreateSGPreProcessor(SGDefaultValuePopulator);
//    }
//
//    @Test
//    public void getSGEntitiesTest() throws IOException {
//        ConfigurableSGDTO configurableSGDTO = getConfigurableSGDTOObject();
//        ConfigurableSGPreProcessedDTO configurableSgPreProcessedDTO = createSGPreProcessor.getSGEntities(configurableSGDTO);
//        assertEquals(1, configurableSgPreProcessedDTO.getSgEntityList().size());
//    }
//
//    private ConfigurableSGDTO getConfigurableSGDTOObject() throws IOException {
//       return objectMapper.readValue(fixture("fixtures/configurable_sg_input.json"), ConfigurableSGDTO.class);
//    }
//}
