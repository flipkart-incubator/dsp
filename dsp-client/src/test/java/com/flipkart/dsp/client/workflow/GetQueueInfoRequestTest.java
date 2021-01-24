package com.flipkart.dsp.client.workflow;

import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.dto.QueueInfoDTO;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JsonUtils.class})
public class GetQueueInfoRequestTest {

    @Mock private DSPServiceClient serviceClient;
    private GetQueueInfoRequest getQueueInfoRequest;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        MockitoAnnotations.initMocks(this);;
        getQueueInfoRequest = spy(new GetQueueInfoRequest(serviceClient, 1L));
    }

    @Test
    public void testGetMethod() {
        assertEquals(getQueueInfoRequest.getMethod(), "GET");
    }

    @Test
    public void testGetPath() {
        Long workflowId = 1L;
        assertEquals(getQueueInfoRequest.getPath(),"/v1/workflow/" + workflowId + "/queue");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(getQueueInfoRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(QueueInfoDTO.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = getQueueInfoRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
