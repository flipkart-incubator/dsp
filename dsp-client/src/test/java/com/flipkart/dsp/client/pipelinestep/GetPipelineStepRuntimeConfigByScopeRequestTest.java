package com.flipkart.dsp.client.pipelinestep;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class GetPipelineStepRuntimeConfigByScopeRequestTest {

    @Mock
    DSPServiceClient serviceClient;
    private Long requestId = 1L;
    private Long dataFrameId = 1L;
    private GetPipelineStepRuntimeConfigByScopeRequest getPipelineStepRuntimeConfigByScopeRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        getPipelineStepRuntimeConfigByScopeRequest = spy(new GetPipelineStepRuntimeConfigByScopeRequest(serviceClient,"workflowExecutionId", "scope"));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getPipelineStepRuntimeConfigByScopeRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getPipelineStepRuntimeConfigByScopeRequest.getPath(),"/v1/pipelineSteps/runtimeConfigs/scope");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getPipelineStepRuntimeConfigByScopeRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory()
                .constructCollectionType(List.class, PipelineStepRuntimeConfig.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getPipelineStepRuntimeConfigByScopeRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
