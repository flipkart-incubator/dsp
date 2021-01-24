package com.flipkart.dsp.client.workflow;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JsonUtils.class})
public class GetWorkflowDetailsRequestTest {

    @Mock private DSPServiceClient serviceClient;

    private Long workflowId = 1L;
    private GetWorkflowDetailsRequest getWorkflowDetailsRequest;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);

        getWorkflowDetailsRequest = spy(new GetWorkflowDetailsRequest(serviceClient, workflowId));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getWorkflowDetailsRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        assertEquals(getWorkflowDetailsRequest.getPath(),"/v1/workflow/details/" + workflowId);
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getWorkflowDetailsRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(WorkflowDetails.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getWorkflowDetailsRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
