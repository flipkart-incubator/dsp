package com.flipkart.dsp.client.pipelinestep;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.entities.run.config.RunConfig;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class CreatePipelineStepRuntimeConfigRequestTest {

    @Mock private RunConfig runConfig;
    @Mock private DSPServiceClient serviceClient;
    private PipelineStepRuntimeConfig pipelineStepRuntimeConfig;
    private CreatePipelineStepRuntimeConfigRequest createPipelineStepRuntimeConfigRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        pipelineStepRuntimeConfig = PipelineStepRuntimeConfig.builder().build();
        createPipelineStepRuntimeConfigRequest = spy(new CreatePipelineStepRuntimeConfigRequest(serviceClient, pipelineStepRuntimeConfig));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createPipelineStepRuntimeConfigRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createPipelineStepRuntimeConfigRequest.getPath(),"/v1/pipelineSteps/runtimeConfigs");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createPipelineStepRuntimeConfigRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createPipelineStepRuntimeConfigRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }

    @Test
    public void testValidateReadinessSuccess() {
        pipelineStepRuntimeConfig = PipelineStepRuntimeConfig.builder().workflowExecutionId("1L").pipelineExecutionId("1L")
                .pipelineStepId(1L).scope("scope").runConfig(runConfig).build();
        createPipelineStepRuntimeConfigRequest = spy(new CreatePipelineStepRuntimeConfigRequest(serviceClient, pipelineStepRuntimeConfig));
        createPipelineStepRuntimeConfigRequest.validateReadiness();
    }

    @Test
    public void testValidateReadinessFailure() {
        boolean isException = false;

        try {
            createPipelineStepRuntimeConfigRequest.validateReadiness();
        } catch (NullPointerException e) {
            isException = true;
            assertEquals(e.getMessage(), "workflowExecutionId is required");
        }
        assertTrue(isException);
    }

}
