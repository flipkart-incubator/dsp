package com.flipkart.dsp.client.pipelinestep;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class GetPipelineStepRuntimeConfigRequestTest {

    @Mock
    DSPServiceClient serviceClient;
    private GetPipelineStepRuntimeConfigRequest getPipelineStepRuntimeConfigRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        getPipelineStepRuntimeConfigRequest = spy(new GetPipelineStepRuntimeConfigRequest(serviceClient, "pipelineExecutionId", 1L));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getPipelineStepRuntimeConfigRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getPipelineStepRuntimeConfigRequest.getPath(), "/v1/pipelineSteps/runtimeConfigs");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getPipelineStepRuntimeConfigRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory()
                .constructType(PipelineStepRuntimeConfig.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getPipelineStepRuntimeConfigRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
