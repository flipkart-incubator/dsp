package com.flipkart.dsp.client.pipelinestep;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
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
public class GetPipelineStepAuditsByPipelineExecutionIdRequestTest {

    @Mock
    DSPServiceClient serviceClient;
    private GetPipelineStepAuditsByPipelineExecutionIdRequest getPipelineStepAuditsByPipelineExecutionIdRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        getPipelineStepAuditsByPipelineExecutionIdRequest = spy(new GetPipelineStepAuditsByPipelineExecutionIdRequest(serviceClient, 0, 1L, 1L, "pipelineExecutionId"));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getPipelineStepAuditsByPipelineExecutionIdRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getPipelineStepAuditsByPipelineExecutionIdRequest.getPath(), "/v1/pipeline_step_audit/pipelineStepDetails");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getPipelineStepAuditsByPipelineExecutionIdRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructCollectionType(List.class, PipelineStepAudit.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();

        RequestBuilder actual = getPipelineStepAuditsByPipelineExecutionIdRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
