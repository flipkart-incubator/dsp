package com.flipkart.dsp.client.pipelinestep;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
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
public class CreatePipelineStepAuditRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private CreatePipelineStepAuditRequest createPipelineStepAuditRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PipelineStepAudit pipelineStepAudit = PipelineStepAudit.builder().build();
        createPipelineStepAuditRequest = spy(new CreatePipelineStepAuditRequest(serviceClient, pipelineStepAudit));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createPipelineStepAuditRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createPipelineStepAuditRequest.getPath(), "/v1/pipeline_step_audit");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createPipelineStepAuditRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(PipelineStepAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createPipelineStepAuditRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
